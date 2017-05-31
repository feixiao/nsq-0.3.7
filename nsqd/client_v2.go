package nsqd

import (
	"bufio"
	"compress/flate"
	"crypto/tls"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"
	snappystream "github.com/mreiferson/go-snappystream"
	"github.com/feixiao/nsq-0.3.7/internal/auth"
)

const defaultBufferSize = 16 * 1024

// 状态
const (
	stateInit = iota
	stateDisconnected
	stateConnected
	stateSubscribed
	stateClosing
)

// 客户端认证上传的数据
type identifyDataV2 struct {
	ShortID string `json:"short_id"` // TODO: deprecated, remove in 1.0
	LongID  string `json:"long_id"`  // TODO: deprecated, remove in 1.0

	ClientID            string `json:"client_id"`
	Hostname            string `json:"hostname"`
	HeartbeatInterval   int    `json:"heartbeat_interval"`
	OutputBufferSize    int    `json:"output_buffer_size"`
	OutputBufferTimeout int    `json:"output_buffer_timeout"`
	FeatureNegotiation  bool   `json:"feature_negotiation"`
	TLSv1               bool   `json:"tls_v1"`
	Deflate             bool   `json:"deflate"`
	DeflateLevel        int    `json:"deflate_level"`
	Snappy              bool   `json:"snappy"`
	SampleRate          int32  `json:"sample_rate"`
	UserAgent           string `json:"user_agent"`
	MsgTimeout          int    `json:"msg_timeout"`
}

// 认证事件
type identifyEvent struct {
	OutputBufferTimeout time.Duration
	HeartbeatInterval   time.Duration
	SampleRate          int32
	MsgTimeout          time.Duration
}

type clientV2 struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	ReadyCount    int64  // 客户端反馈这次最多接收多少条消息
	InFlightCount int64  // 发送但是没有获得确认的信息数量
	MessageCount  uint64 // 发送的信息数量
	FinishCount   uint64 // 发送但是获得确认的信息数量
	RequeueCount  uint64 // 消息投递失败需要再次被投递的小心数量

	writeLock sync.RWMutex
	metaLock  sync.RWMutex

	ID        int64    // 客户端id
	ctx       *context // 上下文对象，这里指NSQD
	UserAgent string

	// original connection
	net.Conn // 继承net.Conn

	// connections based on negotiated features
	tlsConn     *tls.Conn
	flateWriter *flate.Writer

	// reading/writing interfaces
	Reader *bufio.Reader // 将net.Conn封装为Reader对象
	Writer *bufio.Writer // 将net.Conn封装为Writer对象

	OutputBufferSize    int
	OutputBufferTimeout time.Duration

	HeartbeatInterval time.Duration

	MsgTimeout time.Duration // 消息的超时

	State          int32
	ConnectTime    time.Time
	Channel        *Channel // 自己所订阅的Channel
	ReadyStateChan chan int // protocolV2的messagePump处理
	ExitChan       chan int // protocolV2的messagePump处理

	ClientID string // 客户端ID,由外部分配
	Hostname string // 对端host

	SampleRate int32 // for what ？？

	IdentifyEventChan chan identifyEvent
	SubEventChan      chan *Channel

	TLS     int32
	Snappy  int32
	Deflate int32

	// re-usable buffer for reading the 4-byte lengths off the wire
	lenBuf   [4]byte // [ 4-byte size in bytes ] 即消息体中前面四个字节表示消息长度（不包括这四个字节的长度）
	lenSlice []byte  // 将上面lenBuf数组以切片的形式操作

	AuthSecret string
	AuthState  *auth.State
}

// 创建ClientV2对象
func newClientV2(id int64, conn net.Conn, ctx *context) *clientV2 {
	var identifier string
	if conn != nil {
		identifier, _, _ = net.SplitHostPort(conn.RemoteAddr().String()) // 获取对端host
	}

	c := &clientV2{
		ID:  id,
		ctx: ctx,

		Conn: conn,

		// 读写接口
		Reader: bufio.NewReaderSize(conn, defaultBufferSize),
		Writer: bufio.NewWriterSize(conn, defaultBufferSize),

		OutputBufferSize:    defaultBufferSize,
		OutputBufferTimeout: 250 * time.Millisecond,

		MsgTimeout: ctx.nsqd.getOpts().MsgTimeout,

		// ReadyStateChan has a buffer of 1 to guarantee that in the event
		// there is a race the state update is not lost
		ReadyStateChan: make(chan int, 1),
		ExitChan:       make(chan int),
		ConnectTime:    time.Now(),
		State:          stateInit,

		ClientID: identifier,
		Hostname: identifier,

		SubEventChan:      make(chan *Channel, 1),
		IdentifyEventChan: make(chan identifyEvent, 1),

		// heartbeats are client configurable but default to 30s
		HeartbeatInterval: ctx.nsqd.getOpts().ClientTimeout / 2,
	}
	c.lenSlice = c.lenBuf[:]
	return c
}

//  返回host：ip字符串
func (c *clientV2) String() string {
	return c.RemoteAddr().String()
}

// 使用identifyDataV2信息给客户端认证信息
func (c *clientV2) Identify(data identifyDataV2) error {
	c.ctx.nsqd.logf("[%s] IDENTIFY: %+v", c, data)

	// TODO: for backwards compatibility, remove in 1.0
	hostname := data.Hostname
	if hostname == "" {
		hostname = data.LongID
	}
	// TODO: for backwards compatibility, remove in 1.0
	clientID := data.ClientID
	if clientID == "" {
		clientID = data.ShortID
	}

	c.metaLock.Lock()
	c.ClientID = clientID // data.ClientID
	c.Hostname = hostname // data.Hostname
	c.UserAgent = data.UserAgent
	c.metaLock.Unlock()

	// 设置心跳超时
	err := c.SetHeartbeatInterval(data.HeartbeatInterval)
	if err != nil {
		return err
	}

	// 设置writer的buffer大小
	err = c.SetOutputBufferSize(data.OutputBufferSize)
	if err != nil {
		return err
	}

	// 设置buffer flush的超时时间
	err = c.SetOutputBufferTimeout(data.OutputBufferTimeout)
	if err != nil {
		return err
	}
	// for what ？？
	err = c.SetSampleRate(data.SampleRate)
	if err != nil {
		return err
	}

	// 设置Message消息超时
	err = c.SetMsgTimeout(data.MsgTimeout)
	if err != nil {
		return err
	}

	// 创建identifyEvent，用于通知相关参数
	ie := identifyEvent{
		OutputBufferTimeout: c.OutputBufferTimeout,
		HeartbeatInterval:   c.HeartbeatInterval,
		SampleRate:          c.SampleRate,
		MsgTimeout:          c.MsgTimeout,
	}

	// update the client's message pump
	select {
	case c.IdentifyEventChan <- ie: // protocolV2的messagePump处理
	default:
	}

	return nil
}

// 返回ClientStats对象
func (c *clientV2) Stats() ClientStats {
	c.metaLock.RLock()
	// TODO: deprecated, remove in 1.0
	name := c.ClientID

	clientID := c.ClientID
	hostname := c.Hostname
	userAgent := c.UserAgent
	var identity string
	var identityURL string
	if c.AuthState != nil {
		identity = c.AuthState.Identity
		identityURL = c.AuthState.IdentityURL
	}
	c.metaLock.RUnlock()

	// 复制客户端状态信息
	stats := ClientStats{
		// TODO: deprecated, remove in 1.0
		Name: name,

		Version:         "V2",
		RemoteAddress:   c.RemoteAddr().String(),
		ClientID:        clientID,
		Hostname:        hostname,
		UserAgent:       userAgent,
		State:           atomic.LoadInt32(&c.State),
		ReadyCount:      atomic.LoadInt64(&c.ReadyCount),
		InFlightCount:   atomic.LoadInt64(&c.InFlightCount),
		MessageCount:    atomic.LoadUint64(&c.MessageCount),
		FinishCount:     atomic.LoadUint64(&c.FinishCount),
		RequeueCount:    atomic.LoadUint64(&c.RequeueCount),
		ConnectTime:     c.ConnectTime.Unix(),
		SampleRate:      atomic.LoadInt32(&c.SampleRate),
		TLS:             atomic.LoadInt32(&c.TLS) == 1,
		Deflate:         atomic.LoadInt32(&c.Deflate) == 1,
		Snappy:          atomic.LoadInt32(&c.Snappy) == 1,
		Authed:          c.HasAuthorizations(),
		AuthIdentity:    identity,
		AuthIdentityURL: identityURL,
	}
	if stats.TLS {
		// 如果使用ＴＬＳ
		p := prettyConnectionState{c.tlsConn.ConnectionState()}
		stats.CipherSuite = p.GetCipherSuite()
		stats.TLSVersion = p.GetVersion()
		stats.TLSNegotiatedProtocol = p.NegotiatedProtocol
		stats.TLSNegotiatedProtocolIsMutual = p.NegotiatedProtocolIsMutual
	}
	return stats
}

// struct to convert from integers to the human readable strings
type prettyConnectionState struct {
	tls.ConnectionState
}

// 获取加密类型
func (p *prettyConnectionState) GetCipherSuite() string {
	switch p.CipherSuite {
	case tls.TLS_RSA_WITH_RC4_128_SHA:
		return "TLS_RSA_WITH_RC4_128_SHA"
	case tls.TLS_RSA_WITH_3DES_EDE_CBC_SHA:
		return "TLS_RSA_WITH_3DES_EDE_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_RC4_128_SHA:
		return "TLS_ECDHE_ECDSA_WITH_RC4_128_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_ECDSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_RC4_128_SHA:
		return "TLS_ECDHE_RSA_WITH_RC4_128_SHA"
	case tls.TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_3DES_EDE_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA:
		return "TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA"
	case tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256"
	case tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256:
		return "TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256"
	}
	return fmt.Sprintf("Unknown %d", p.CipherSuite)
}

// 获取TLS/SSL的版本
func (p *prettyConnectionState) GetVersion() string {
	switch p.Version {
	case tls.VersionSSL30:
		return "SSL30"
	case tls.VersionTLS10:
		return "TLS1.0"
	case tls.VersionTLS11:
		return "TLS1.1"
	case tls.VersionTLS12:
		return "TLS1.2"
	default:
		return fmt.Sprintf("Unknown %d", p.Version)
	}
}

// 是否准备好接收消息
func (c *clientV2) IsReadyForMessages() bool {
	if c.Channel.IsPaused() {
		// Channel为暂停状态，那么没有准备好
		return false
	}

	readyCount := atomic.LoadInt64(&c.ReadyCount)
	inFlightCount := atomic.LoadInt64(&c.InFlightCount)

	if c.ctx.nsqd.getOpts().Verbose {
		c.ctx.nsqd.logf("[%s] state rdy: %4d inflt: %4d",
			c, readyCount, inFlightCount)
	}

	// 没有确认的消息大于消费者最大的接收数据
	if inFlightCount >= readyCount || readyCount <= 0 {
		return false
	}

	return true
}

// 设置本次客户端最多接收的消息数量
func (c *clientV2) SetReadyCount(count int64) {
	atomic.StoreInt64(&c.ReadyCount, count)
	c.tryUpdateReadyState()
}

func (c *clientV2) tryUpdateReadyState() {
	// you can always *try* to write to ReadyStateChan because in the cases
	// where you cannot the message pump loop would have iterated anyway.
	// the atomic integer operations guarantee correctness of the value.
	select {
	case c.ReadyStateChan <- 1: // ??
	default:
	}
}

// 统计发送并获取对方确认
func (c *clientV2) FinishedMessage() {
	atomic.AddUint64(&c.FinishCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

//
func (c *clientV2) Empty() {
	atomic.StoreInt64(&c.InFlightCount, 0)
	c.tryUpdateReadyState()
}

// 统计发送的数据
func (c *clientV2) SendingMessage() {
	atomic.AddInt64(&c.InFlightCount, 1)
	atomic.AddUint64(&c.MessageCount, 1)
}

// 消息超时，那么需要等待的信息减１
func (c *clientV2) TimedOutMessage() {
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

// 消息重新投递，更新相应的数值
func (c *clientV2) RequeuedMessage() {
	atomic.AddUint64(&c.RequeueCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

func (c *clientV2) StartClose() {
	// Force the client into ready 0
	c.SetReadyCount(0)
	// mark this client as closing
	atomic.StoreInt32(&c.State, stateClosing)
}

func (c *clientV2) Pause() {
	c.tryUpdateReadyState()
}

func (c *clientV2) UnPause() {
	c.tryUpdateReadyState()
}

func (c *clientV2) SetHeartbeatInterval(desiredInterval int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	switch {
	case desiredInterval == -1:
		c.HeartbeatInterval = 0
	case desiredInterval == 0:
		// do nothing (use default)
	case desiredInterval >= 1000 &&
		desiredInterval <= int(c.ctx.nsqd.getOpts().MaxHeartbeatInterval/time.Millisecond):
		c.HeartbeatInterval = time.Duration(desiredInterval) * time.Millisecond
	default:
		return fmt.Errorf("heartbeat interval (%d) is invalid", desiredInterval)
	}

	return nil
}

// 设置writer的buffer大小
func (c *clientV2) SetOutputBufferSize(desiredSize int) error {
	var size int

	switch {
	case desiredSize == -1:
		// 直接使用net.Conn
		size = 1
	case desiredSize == 0:
		// 使用默认值
	case desiredSize >= 64 && desiredSize <= int(c.ctx.nsqd.getOpts().MaxOutputBufferSize):
		// 在为64到MaxOutputBufferSize范围()的值
		size = desiredSize
	default:
		return fmt.Errorf("output buffer size (%d) is invalid", desiredSize)
	}

	if size > 0 {
		c.writeLock.Lock()
		defer c.writeLock.Unlock()
		c.OutputBufferSize = size
		err := c.Writer.Flush()
		if err != nil {
			return err
		}
		c.Writer = bufio.NewWriterSize(c.Conn, size) // 改变buffer大小
	}

	return nil
}

// 设置buffer flush的超时时间
func (c *clientV2) SetOutputBufferTimeout(desiredTimeout int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	switch {
	case desiredTimeout == -1:
		c.OutputBufferTimeout = 0
	case desiredTimeout == 0:
		// do nothing (use default)
	case desiredTimeout >= 1 &&
		desiredTimeout <= int(c.ctx.nsqd.getOpts().MaxOutputBufferTimeout/time.Millisecond):

		c.OutputBufferTimeout = time.Duration(desiredTimeout) * time.Millisecond
	default:
		return fmt.Errorf("output buffer timeout (%d) is invalid", desiredTimeout)
	}

	return nil
}

func (c *clientV2) SetSampleRate(sampleRate int32) error {
	if sampleRate < 0 || sampleRate > 99 {
		return fmt.Errorf("sample rate (%d) is invalid", sampleRate)
	}
	atomic.StoreInt32(&c.SampleRate, sampleRate)
	return nil
}

func (c *clientV2) SetMsgTimeout(msgTimeout int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	switch {
	case msgTimeout == 0:
		// do nothing (use default)
	case msgTimeout >= 1000 &&
		msgTimeout <= int(c.ctx.nsqd.getOpts().MaxMsgTimeout/time.Millisecond):

		c.MsgTimeout = time.Duration(msgTimeout) * time.Millisecond
	default:
		return fmt.Errorf("msg timeout (%d) is invalid", msgTimeout)
	}

	return nil
}

func (c *clientV2) UpgradeTLS() error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	tlsConn := tls.Server(c.Conn, c.ctx.nsqd.tlsConfig)
	tlsConn.SetDeadline(time.Now().Add(5 * time.Second))
	err := tlsConn.Handshake()
	if err != nil {
		return err
	}
	c.tlsConn = tlsConn

	c.Reader = bufio.NewReaderSize(c.tlsConn, defaultBufferSize)
	c.Writer = bufio.NewWriterSize(c.tlsConn, c.OutputBufferSize)

	atomic.StoreInt32(&c.TLS, 1)

	return nil
}

func (c *clientV2) UpgradeDeflate(level int) error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	c.Reader = bufio.NewReaderSize(flate.NewReader(conn), defaultBufferSize)

	fw, _ := flate.NewWriter(conn, level)
	c.flateWriter = fw
	c.Writer = bufio.NewWriterSize(fw, c.OutputBufferSize)

	atomic.StoreInt32(&c.Deflate, 1)

	return nil
}

func (c *clientV2) UpgradeSnappy() error {
	c.writeLock.Lock()
	defer c.writeLock.Unlock()

	conn := c.Conn
	if c.tlsConn != nil {
		conn = c.tlsConn
	}

	c.Reader = bufio.NewReaderSize(snappystream.NewReader(conn, snappystream.SkipVerifyChecksum), defaultBufferSize)
	c.Writer = bufio.NewWriterSize(snappystream.NewWriter(conn), c.OutputBufferSize)

	atomic.StoreInt32(&c.Snappy, 1)

	return nil
}

func (c *clientV2) Flush() error {
	var zeroTime time.Time
	if c.HeartbeatInterval > 0 {
		c.SetWriteDeadline(time.Now().Add(c.HeartbeatInterval))
	} else {
		c.SetWriteDeadline(zeroTime)
	}

	err := c.Writer.Flush()
	if err != nil {
		return err
	}

	if c.flateWriter != nil {
		return c.flateWriter.Flush()
	}

	return nil
}

// 获取认证
func (c *clientV2) QueryAuthd() error {
	remoteIP, _, err := net.SplitHostPort(c.String())
	if err != nil {
		return err
	}

	tls := atomic.LoadInt32(&c.TLS) == 1
	tlsEnabled := "false"
	if tls {
		tlsEnabled = "true"
	}

	authState, err := auth.QueryAnyAuthd(c.ctx.nsqd.getOpts().AuthHTTPAddresses,
		remoteIP, tlsEnabled, c.AuthSecret)
	if err != nil {
		return err
	}
	c.AuthState = authState
	return nil
}

func (c *clientV2) Auth(secret string) error {
	c.AuthSecret = secret
	return c.QueryAuthd()
}

func (c *clientV2) IsAuthorized(topic, channel string) (bool, error) {
	if c.AuthState == nil {
		return false, nil
	}
	if c.AuthState.IsExpired() {
		err := c.QueryAuthd()
		if err != nil {
			return false, err
		}
	}
	if c.AuthState.IsAllowed(topic, channel) {
		return true, nil
	}
	return false, nil
}

func (c *clientV2) HasAuthorizations() bool {
	if c.AuthState != nil {
		return len(c.AuthState.Authorizations) != 0
	}
	return false
}
