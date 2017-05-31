package nsqd

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/feixiao/nsq-0.3.7/internal/protocol"
	"github.com/feixiao/nsq-0.3.7/internal/version"
)

const maxTimeout = time.Hour

const (
	frameTypeResponse int32 = 0
	frameTypeError    int32 = 1
	frameTypeMessage  int32 = 2
)

var separatorBytes = []byte(" ")
var heartbeatBytes = []byte("_heartbeat_")
var okBytes = []byte("OK")

type protocolV2 struct {
	ctx *context
}

func (p *protocolV2) IOLoop(conn net.Conn) error {
	var err error
	var line []byte
	var zeroTime time.Time

	clientID := atomic.AddInt64(&p.ctx.nsqd.clientIDSequence, 1)
	// 创建一个新的Client对象(处理数据的工作交给了clientV2)
	client := newClientV2(clientID, conn, p.ctx)

	// 开启另一个goroutine，定时发送心跳信息，客户端收到心跳信息后要回复。
	// 如果nsqd长时间未收到该连接的心跳回复说明连接已出问题，会断开连接，这就是nsq的心跳实现机制
	messagePumpStartedChan := make(chan bool)
	go p.messagePump(client, messagePumpStartedChan)
	<-messagePumpStartedChan // 确保messagePump已经运行才继续往下走

	for {
		// 如果超过client.HeartbeatInterval * 2时间间隔内未收到客户端发送的命令，说明连接处问题了，需要关闭此链接
		// 正常情况下每隔HeartbeatInterval时间客户端都会发送一个心跳回复。
		if client.HeartbeatInterval > 0 {
			client.SetReadDeadline(time.Now().Add(client.HeartbeatInterval * 2))
		} else {
			client.SetReadDeadline(zeroTime)
		}

		// ReadSlice does not allocate new space for the data each request
		// ie. the returned slice is only valid until the next call to it
		// nsq规定所有的命令以 "\n" 结尾，命令与参数之间以空格分隔
		line, err = client.Reader.ReadSlice('\n')
		if err != nil {
			if err == io.EOF {
				err = nil
			} else {
				err = fmt.Errorf("failed to read command - %s", err)
			}
			break
		}

		// trim the '\n'
		line = line[:len(line)-1]
		// optionally trim the '\r'
		if len(line) > 0 && line[len(line)-1] == '\r' {
			line = line[:len(line)-1]
		}
		// 上传的数据
		// 字符串按一个separatorBytes分割，并获取相应的Commad 以及该command 的相应的params
		params := bytes.Split(line, separatorBytes)

		if p.ctx.nsqd.getOpts().Verbose {
			p.ctx.nsqd.logf("PROTOCOL(V2): [%s] %s", client, params)
		}

		// 处理客户端发送过来的命令
		var response []byte
		response, err = p.Exec(client, params)
		if err != nil {
			// 处理命令发生错误
			ctx := ""
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			p.ctx.nsqd.logf("ERROR: [%s] - %s%s", client, err, ctx)

			sendErr := p.Send(client, frameTypeError, []byte(err.Error()))
			if sendErr != nil {
				p.ctx.nsqd.logf("ERROR: [%s] - %s%s", client, sendErr, ctx)
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}
		// 将命令的处理结果发送给客户端
		if response != nil {
			err = p.Send(client, frameTypeResponse, response)
			if err != nil {
				err = fmt.Errorf("failed to send response - %s", err)
				break
			}
		}
	}
	// 连接出问题了，需要关闭连接
	p.ctx.nsqd.logf("PROTOCOL(V2): [%s] exiting ioloop", client)
	conn.Close()
	close(client.ExitChan)

	// client.Channel记录的是该客户端订阅的Channel,客户端关闭的时候需要从Channel中移除这个订阅者。
	if client.Channel != nil {
		client.Channel.RemoveClient(client.ID)
	}

	return err
}

// 将msg发送给客户端，buf为了减少不必要的bytes.Buffer分配
func (p *protocolV2) SendMessage(client *clientV2, msg *Message, buf *bytes.Buffer) error {
	if p.ctx.nsqd.getOpts().Verbose {
		p.ctx.nsqd.logf("PROTOCOL(V2): writing msg(%s) to client(%s) - %s",
			msg.ID, client, msg.Body)
	}

	buf.Reset()
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}

	err = p.Send(client, frameTypeMessage, buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

// 发送给客户端响应
func (p *protocolV2) Send(client *clientV2, frameType int32, data []byte) error {
	client.writeLock.Lock()

	// 设置写超时
	var zeroTime time.Time
	if client.HeartbeatInterval > 0 {
		// 设置连接的超时时间net包中，Conn的函数
		client.SetWriteDeadline(time.Now().Add(client.HeartbeatInterval))
	} else {
		client.SetWriteDeadline(zeroTime)
	}

	_, err := protocol.SendFramedResponse(client.Writer, frameType, data)
	if err != nil {
		client.writeLock.Unlock()
		return err
	}

	if frameType != frameTypeMessage {
		// 如果不是Mesaage就刷新一边。保证数据全部发送出去
		err = client.Flush()
	}

	client.writeLock.Unlock()

	return err
}

// http://nsq.io/clients/tcp_protocol_spec.html
// 处理协议中的命令
func (p *protocolV2) Exec(client *clientV2, params [][]byte) ([]byte, error) {
	if bytes.Equal(params[0], []byte("IDENTIFY")) {
		return p.IDENTIFY(client, params)
	}
	err := enforceTLSPolicy(client, p, params[0])
	if err != nil {
		return nil, err
	}

	// 处理不同的命令，clientV2完成具体的工作
	switch {
	case bytes.Equal(params[0], []byte("FIN")):
		return p.FIN(client, params)
	case bytes.Equal(params[0], []byte("RDY")):
		return p.RDY(client, params)
	case bytes.Equal(params[0], []byte("REQ")):
		return p.REQ(client, params)
	case bytes.Equal(params[0], []byte("PUB")):
		return p.PUB(client, params)
	case bytes.Equal(params[0], []byte("MPUB")):
		return p.MPUB(client, params)
	case bytes.Equal(params[0], []byte("DPUB")):
		return p.DPUB(client, params)
	case bytes.Equal(params[0], []byte("NOP")):
		return p.NOP(client, params)
	case bytes.Equal(params[0], []byte("TOUCH")):
		return p.TOUCH(client, params)
	case bytes.Equal(params[0], []byte("SUB")):
		return p.SUB(client, params)
	case bytes.Equal(params[0], []byte("CLS")):
		return p.CLS(client, params)
	case bytes.Equal(params[0], []byte("AUTH")):
		return p.AUTH(client, params)
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

// 消息泵,每个客户端连接对象都会启动
func (p *protocolV2) messagePump(client *clientV2, startedChan chan bool) {
	var err error
	var buf bytes.Buffer
	var clientMsgChan chan *Message
	var subChannel *Channel
	// NOTE: `flusherChan` is used to bound message latency for
	// the pathological case of a channel on a low volume topic
	// with >1 clients having >1 RDY counts
	var flusherChan <-chan time.Time
	var sampleRate int32

	subEventChan := client.SubEventChan
	identifyEventChan := client.IdentifyEventChan

	outputBufferTicker := time.NewTicker(client.OutputBufferTimeout)
	// 处理心跳的timer
	heartbeatTicker := time.NewTicker(client.HeartbeatInterval)
	// 产生tick的channel，通过它定时发送心跳包
	heartbeatChan := heartbeatTicker.C
	msgTimeout := client.MsgTimeout

	// v2 opportunistically buffers data to clients to reduce write system calls
	// we force flush in two cases:
	//    1. when the client is not ready to receive messages
	//    2. we're buffered and the channel has nothing left to send us
	//       (ie. we would block in this loop anyway)
	//
	flushed := true

	// signal to the goroutine that started the messagePump
	// that we've started up
	close(startedChan) // 通知外面我们运行了

	for {
		// IsReadyForMessages就是检查Client的RDY命令所设置的ReadyCount，判断是否可以继续向Client发送消息
		if subChannel == nil || !client.IsReadyForMessages() {
			// the client is not ready to receive messages...
			// 客户端还未做好准备则将clientMsgChan设置为nil
			clientMsgChan = nil
			flusherChan = nil
			// force flush
			client.writeLock.Lock()
			err = client.Flush()
			client.writeLock.Unlock()
			if err != nil {
				goto exit
			}
			flushed = true
		} else if flushed {
			// last iteration we flushed...
			// do not select on the flusher ticker channel
			//客户端做好准备，则试图从订阅的Channel的clientMsgChan中读取			消息
			clientMsgChan = subChannel.clientMsgChan
			flusherChan = nil
		} else {
			// we're buffered (if there isn't any more data we should flush)...
			// select on the flusher ticker channel, too
			clientMsgChan = subChannel.clientMsgChan
			flusherChan = outputBufferTicker.C
		}

		select {
		// 接收到客户端发送的RDY命令后，则会向ReadyStateChan中写入消息，下面的case条件则可满足，重新进入for循环
		case <-flusherChan:
			// if this case wins, we're either starved
			// or we won the race between other channels...
			// in either case, force flush

			client.writeLock.Lock()
			err = client.Flush()
			client.writeLock.Unlock()
			if err != nil {
				goto exit
			}
			flushed = true
		case <-client.ReadyStateChan:
		case subChannel = <-subEventChan:
			// you can't SUB anymore
			// 接收到客户端发送的SUB命令后，会向subEventChan中写入消息，
			// subEventChan则被置为nil，所以一个客户端只能订阅一次Channel
			subEventChan = nil
		case identifyData := <-identifyEventChan:
			// you can't IDENTIFY anymore
			// 只能认证一次
			identifyEventChan = nil

			outputBufferTicker.Stop()
			if identifyData.OutputBufferTimeout > 0 {
				outputBufferTicker = time.NewTicker(identifyData.OutputBufferTimeout)
			}

			heartbeatTicker.Stop()
			heartbeatChan = nil
			if identifyData.HeartbeatInterval > 0 {
				heartbeatTicker = time.NewTicker(identifyData.HeartbeatInterval)
				heartbeatChan = heartbeatTicker.C
			}

			if identifyData.SampleRate > 0 {
				sampleRate = identifyData.SampleRate
			}

			msgTimeout = identifyData.MsgTimeout
		case <-heartbeatChan:
			// 发送心跳消息
			err = p.Send(client, frameTypeResponse, heartbeatBytes)
			if err != nil {
				goto exit
			}
		case msg, ok := <-clientMsgChan:
			// 因为每个客户端都启动了goroutine,然后select其关联的Channel
			// 会有N个消费者共同监听channel.clientMsgChan,一条消息只能被一个消费者抢到
			if !ok {
				goto exit
			}

			if sampleRate > 0 && rand.Int31n(100) > sampleRate {
				continue
			}
			// 以消息的发送时间排序，将消息放在一个最小时间堆上，如果在规定时间内收到对该消息的确认回复(FIN messageId),
			// 说明消息以被消费者成功处理，会将该消息从堆中删除

			// 如果超过一定时间没有接受 FIN messageId，会从堆中取出该消息重新发送，所以nsq能确保一个消息至少被一个i消费处理。
			subChannel.StartInFlightTimeout(msg, client.ID, msgTimeout)	// 添加到队列
			client.SendingMessage()						// 消息统计
			err = p.SendMessage(client, msg, &buf)				// 发送到网络
			if err != nil {
				goto exit
			}
			flushed = false
		case <-client.ExitChan:
			goto exit
		}
	}

exit:
	p.ctx.nsqd.logf("PROTOCOL(V2): [%s] exiting messagePump", client)
	heartbeatTicker.Stop()
	outputBufferTicker.Stop()
	if err != nil {
		p.ctx.nsqd.logf("PROTOCOL(V2): [%s] messagePump error - %s", client, err)
	}
}

// IDENTIFY ： Update client metadata on the server and negotiate features
func (p *protocolV2) IDENTIFY(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	// 如果客户端不是stateInit状态直接返回错误
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot IDENTIFY in current state")
	}

	// 读取前面四个字节，并获取消息体的长度
	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	// 如果消息体的最大长度超过最大的消息体的长度,则返回错误
	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY body too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxBodySize))
	}

	// 如果长度小于0也是错误数据
	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("IDENTIFY invalid body size %d", bodyLen))
	}

	// 获取消息体数据
	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	// 因为是json数据，我们Unmarshal到结构体
	var identifyData identifyDataV2
	err = json.Unmarshal(body, &identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	if p.ctx.nsqd.getOpts().Verbose {
		p.ctx.nsqd.logf("PROTOCOL(V2): [%s] %+v", client, identifyData)
	}

	// client对象处理Identify
	err = client.Identify(identifyData)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY "+err.Error())
	}

	// bail out early if we're not negotiating features
	if !identifyData.FeatureNegotiation {
		return okBytes, nil
	}

	tlsv1 := p.ctx.nsqd.tlsConfig != nil && identifyData.TLSv1
	deflate := p.ctx.nsqd.getOpts().DeflateEnabled && identifyData.Deflate
	deflateLevel := 0
	if deflate {
		if identifyData.DeflateLevel <= 0 {
			deflateLevel = 6
		}
		deflateLevel = int(math.Min(float64(deflateLevel), float64(p.ctx.nsqd.getOpts().MaxDeflateLevel)))
	}
	snappy := p.ctx.nsqd.getOpts().SnappyEnabled && identifyData.Snappy

	if deflate && snappy {
		return nil, protocol.NewFatalClientErr(nil, "E_IDENTIFY_FAILED", "cannot enable both deflate and snappy compression")
	}

	//　回复
	resp, err := json.Marshal(struct {
		MaxRdyCount         int64  `json:"max_rdy_count"`
		Version             string `json:"version"`
		MaxMsgTimeout       int64  `json:"max_msg_timeout"`
		MsgTimeout          int64  `json:"msg_timeout"`
		TLSv1               bool   `json:"tls_v1"`
		Deflate             bool   `json:"deflate"`
		DeflateLevel        int    `json:"deflate_level"`
		MaxDeflateLevel     int    `json:"max_deflate_level"`
		Snappy              bool   `json:"snappy"`
		SampleRate          int32  `json:"sample_rate"`
		AuthRequired        bool   `json:"auth_required"`
		OutputBufferSize    int    `json:"output_buffer_size"`
		OutputBufferTimeout int64  `json:"output_buffer_timeout"`
	}{
		MaxRdyCount:         p.ctx.nsqd.getOpts().MaxRdyCount,
		Version:             version.Binary,
		MaxMsgTimeout:       int64(p.ctx.nsqd.getOpts().MaxMsgTimeout / time.Millisecond),
		MsgTimeout:          int64(client.MsgTimeout / time.Millisecond),
		TLSv1:               tlsv1,
		Deflate:             deflate,
		DeflateLevel:        deflateLevel,
		MaxDeflateLevel:     p.ctx.nsqd.getOpts().MaxDeflateLevel,
		Snappy:              snappy,
		SampleRate:          client.SampleRate,
		AuthRequired:        p.ctx.nsqd.IsAuthEnabled(),
		OutputBufferSize:    client.OutputBufferSize,
		OutputBufferTimeout: int64(client.OutputBufferTimeout / time.Millisecond),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	err = p.Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
	}

	// ???
	if tlsv1 {
		p.ctx.nsqd.logf("PROTOCOL(V2): [%s] upgrading connection to TLS", client)
		err = client.UpgradeTLS()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if snappy {
		p.ctx.nsqd.logf("PROTOCOL(V2): [%s] upgrading connection to snappy", client)
		err = client.UpgradeSnappy()
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	if deflate {
		p.ctx.nsqd.logf("PROTOCOL(V2): [%s] upgrading connection to deflate", client)
		err = client.UpgradeDeflate(deflateLevel)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}

		err = p.Send(client, frameTypeResponse, okBytes)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_IDENTIFY_FAILED", "IDENTIFY failed "+err.Error())
		}
	}

	return nil, nil
}

// If the IDENTIFY response indicates auth_required=true the client must send AUTH before any SUB, PUB or MPUB commands.
// If auth_required is not present (or false), a client must not authorize.
// 认证阶段如果auth_required=true，那么客户端必须在其他明确之前先进行认证。
func (p *protocolV2) AUTH(client *clientV2, params [][]byte) ([]byte, error) {

	// 只有在stateInit才处理AUTH
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot AUTH in current state")
	}

	// 检查参数的合法性
	if len(params) != 1 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "AUTH invalid number of parameters")
	}

	// 获取前面四个字节（表示消息体长度）
	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body size")
	}

	// 如果消息体的最大长度超过最大的消息体的长度,则返回错误
	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH body too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxBodySize))
	}
	// 如果长度小于0也是错误数据
	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("AUTH invalid body size %d", bodyLen))
	}

	// 读取全部的数据（auth secret）
	body := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, body)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "AUTH failed to read body")
	}

	// 判断是已经认证
	if client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "AUTH Already set")
	}

	// 判断是否可以认证（通过有没有设置AuthHTTPAddresses判断）
	if !client.ctx.nsqd.IsAuthEnabled() {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_DISABLED", "AUTH Disabled")
	}
	// 设在auth secret
	if err := client.Auth(string(body)); err != nil {
		// we don't want to leak errors contacting the auth server to untrusted clients
		p.ctx.nsqd.logf("PROTOCOL(V2): [%s] Auth Failed %s", client, err)
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_FAILED", "AUTH failed")
	}

	if !client.HasAuthorizations() {
		return nil, protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED", "AUTH No authorizations found")
	}

	// 回复内容
	resp, err := json.Marshal(struct {
		Identity        string `json:"identity"`
		IdentityURL     string `json:"identity_url"`
		PermissionCount int    `json:"permission_count"`
	}{
		Identity:        client.AuthState.Identity,
		IdentityURL:     client.AuthState.IdentityURL,
		PermissionCount: len(client.AuthState.Authorizations),
	})
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	err = p.Send(client, frameTypeResponse, resp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_AUTH_ERROR", "AUTH error "+err.Error())
	}

	return nil, nil

}

func (p *protocolV2) CheckAuth(client *clientV2, cmd, topicName, channelName string) error {
	// if auth is enabled, the client must have authorized already
	// compare topic/channel against cached authorization data (refetching if expired)
	if client.ctx.nsqd.IsAuthEnabled() {
		if !client.HasAuthorizations() {
			// 客户端没有获取认证
			return protocol.NewFatalClientErr(nil, "E_AUTH_FIRST",
				fmt.Sprintf("AUTH required before %s", cmd))
		}
		ok, err := client.IsAuthorized(topicName, channelName)
		if err != nil {
			// we don't want to leak errors contacting the auth server to untrusted clients
			// 认证失败
			p.ctx.nsqd.logf("PROTOCOL(V2): [%s] Auth Failed %s", client, err)
			return protocol.NewFatalClientErr(nil, "E_AUTH_FAILED", "AUTH failed")
		}
		if !ok {
			return protocol.NewFatalClientErr(nil, "E_UNAUTHORIZED",
				fmt.Sprintf("AUTH failed for %s on %q %q", cmd, topicName, channelName))
		}
	}
	return nil // 不想要认证，
}

// Subscribe to a topic/channel
// 订阅某个topic下面的某个channel
func (p *protocolV2) SUB(client *clientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateInit {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot SUB in current state")
	}

	// 没有设置心跳的时候我们不允许sub操作
	if client.HeartbeatInterval <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot SUB with heartbeats disabled")
	}

	// 检查参数是否正确 SUB <topic_name> <channel_name>\n
	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "SUB insufficient number of parameters")
	}

	// 获取topic名字
	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("SUB topic name %q is not valid", topicName))
	}
	// 获取channel名字
	channelName := string(params[2])
	if !protocol.IsValidChannelName(channelName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL",
			fmt.Sprintf("SUB channel name %q is not valid", channelName))
	}

	if err := p.CheckAuth(client, "SUB", topicName, channelName); err != nil {
		return nil, err
	}

	// 将Client与Channel建立关联关系
	topic := p.ctx.nsqd.GetTopic(topicName)
	channel := topic.GetChannel(channelName)
	channel.AddClient(client.ID, client)

	//  改变客户端的状态为stateSubscribed
	atomic.StoreInt32(&client.State, stateSubscribed)
	// 引用自己所订阅的Channel
	client.Channel = channel

	// update message pump
	// 触发SubEvent，处理的逻辑是将SubEventChan设置为nil(也就是说不支持这个客户端改变Channel了)
	// 同时messagePump中的subChannel知道引用哪个Channel了
	client.SubEventChan <- channel

	return okBytes, nil
}

// Update RDY state (indicate you are ready to receive N messages)
func (p *protocolV2) RDY(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)

	// 如果状态是stateClosing就不回复了
	if state == stateClosing {
		// just ignore ready changes on a closing channel
		p.ctx.nsqd.logf(
			"PROTOCOL(V2): [%s] ignoring RDY after CLS in state ClientStateV2Closing",
			client)
		return nil, nil
	}

	// 只有在stateSubscribed状态下RDY命令才能被处理
	if state != stateSubscribed {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot RDY in current state")
	}

	// RDY <count>\n
	count := int64(1)
	if len(params) > 1 {
		//将参数转换成十进制
		b10, err := protocol.ByteToBase10(params[1])
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_INVALID",
				fmt.Sprintf("RDY could not parse count %s", params[1]))
		}
		count = int64(b10)
	}

	// 判断数据是否合法
	if count < 0 || count > p.ctx.nsqd.getOpts().MaxRdyCount {
		// this needs to be a fatal error otherwise clients would have
		// inconsistent state
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("RDY count %d out of range 0-%d", count, p.ctx.nsqd.getOpts().MaxRdyCount))
	}

	// 设置本次客户端最多接收的消息数量
	client.SetReadyCount(count)

	return nil, nil
}

// Finish a message (indicate successful processing)
func (p *protocolV2) FIN(client *clientV2, params [][]byte) ([]byte, error) {
	// 判断客户端状态是否正确
	state := atomic.LoadInt32(&client.State)
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot FIN in current state")
	}
	// 判断参数是否正确 FIN <message_id>\n
	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "FIN insufficient number of params")
	}

	// 获取 message_id
	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	// 告知Channel这个id的消息已经被成功的处理
	// 具体处理方式就是从inFlightMessages队列中移除消息
	err = client.Channel.FinishMessage(client.ID, *id)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_FIN_FAILED",
			fmt.Sprintf("FIN %s failed %s", *id, err.Error()))
	}

	client.FinishedMessage()

	return nil, nil
}

// Re-queue a message (indicate failure to process)
// 要求nsqd重新排序该消息(暗示消息出来出错)
func (p *protocolV2) REQ(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	// 验证状态
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot REQ in current state")
	}

	// 判断参数合法性
	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "REQ insufficient number of params")
	}

	// 获取消息id和超时值
	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	timeoutMs, err := protocol.ByteToBase10(params[2])
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID",
			fmt.Sprintf("REQ could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	if timeoutDuration < 0 || timeoutDuration > p.ctx.nsqd.getOpts().MaxReqTimeout {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("REQ timeout %d out of range 0-%d", timeoutDuration, p.ctx.nsqd.getOpts().MaxReqTimeout))
	}
	// 重新排序消息
	err = client.Channel.RequeueMessage(client.ID, *id, timeoutDuration)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_REQ_FAILED",
			fmt.Sprintf("REQ %s failed %s", *id, err.Error()))
	}

	client.RequeuedMessage()

	return nil, nil
}

// Cleanly close your connection (no more messages are sent)
// 关闭连接，没有消息会被发送
func (p *protocolV2) CLS(client *clientV2, params [][]byte) ([]byte, error) {
	if atomic.LoadInt32(&client.State) != stateSubscribed {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot CLS in current state")
	}

	// 关闭
	client.StartClose()

	return []byte("CLOSE_WAIT"), nil
}

// do nothing
func (p *protocolV2) NOP(client *clientV2, params [][]byte) ([]byte, error) {
	return nil, nil
}

// Publish a message to a topic
// 发送消息给指定的TOPIC
func (p *protocolV2) PUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error
	//  验证参数的正确性
	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "PUB insufficient number of parameters")
	}

	// 获取TOPIC的名字
	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("PUB topic name %q is not valid", topicName))
	}

	// 获取body的长度
	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body size")
	}

	// 验证长度的合法性
	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB invalid message body size %d", bodyLen))
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxMsgSize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("PUB message too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxMsgSize))
	}
	// 获取body内容
	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "PUB failed to read message body")
	}

	if err := p.CheckAuth(client, "PUB", topicName, ""); err != nil {
		return nil, err
	}

	// 根据名字获取到topic实例
	topic := p.ctx.nsqd.GetTopic(topicName)
	// 将内容封装成Message对象
	msg := NewMessage(<-p.ctx.nsqd.idChan, messageBody)
	// 发送消息
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_PUB_FAILED", "PUB failed "+err.Error())
	}

	return okBytes, nil
}

// Publish multiple messages to a topic (atomically)
func (p *protocolV2) MPUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "MPUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("E_BAD_TOPIC MPUB topic name %q is not valid", topicName))
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read body size")
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid body size %d", bodyLen))
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxBodySize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY",
			fmt.Sprintf("MPUB body too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxBodySize))
	}

	messages, err := readMPUB(client.Reader, client.lenSlice, p.ctx.nsqd.idChan,
		p.ctx.nsqd.getOpts().MaxMsgSize)
	if err != nil {
		return nil, err
	}

	if err := p.CheckAuth(client, "MPUB", topicName, ""); err != nil {
		return nil, err
	}

	topic := p.ctx.nsqd.GetTopic(topicName)

	// if we've made it this far we've validated all the input,
	// the only possible error is that the topic is exiting during
	// this next call (and no messages will be queued in that case)
	err = topic.PutMessages(messages)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_MPUB_FAILED", "MPUB failed "+err.Error())
	}

	return okBytes, nil
}

func (p *protocolV2) DPUB(client *clientV2, params [][]byte) ([]byte, error) {
	var err error

	if len(params) < 3 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "DPUB insufficient number of parameters")
	}

	topicName := string(params[1])
	if !protocol.IsValidTopicName(topicName) {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_TOPIC",
			fmt.Sprintf("DPUB topic name %q is not valid", topicName))
	}

	timeoutMs, err := protocol.ByteToBase10(params[2])
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_INVALID",
			fmt.Sprintf("DPUB could not parse timeout %s", params[2]))
	}
	timeoutDuration := time.Duration(timeoutMs) * time.Millisecond

	if timeoutDuration < 0 || timeoutDuration > p.ctx.nsqd.getOpts().MaxReqTimeout {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("DPUB timeout %d out of range 0-%d",
				timeoutMs, p.ctx.nsqd.getOpts().MaxReqTimeout/time.Millisecond))
	}

	bodyLen, err := readLen(client.Reader, client.lenSlice)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "DPUB failed to read message body size")
	}

	if bodyLen <= 0 {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("DPUB invalid message body size %d", bodyLen))
	}

	if int64(bodyLen) > p.ctx.nsqd.getOpts().MaxMsgSize {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
			fmt.Sprintf("DPUB message too big %d > %d", bodyLen, p.ctx.nsqd.getOpts().MaxMsgSize))
	}

	messageBody := make([]byte, bodyLen)
	_, err = io.ReadFull(client.Reader, messageBody)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "DPUB failed to read message body")
	}

	if err := p.CheckAuth(client, "DPUB", topicName, ""); err != nil {
		return nil, err
	}

	topic := p.ctx.nsqd.GetTopic(topicName)
	msg := NewMessage(<-p.ctx.nsqd.idChan, messageBody)
	msg.deferred = timeoutDuration
	err = topic.PutMessage(msg)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_DPUB_FAILED", "DPUB failed "+err.Error())
	}

	return okBytes, nil
}

// Reset the timeout for an in-flight message
// 重置待确认的消息的超时时间
func (p *protocolV2) TOUCH(client *clientV2, params [][]byte) ([]byte, error) {
	state := atomic.LoadInt32(&client.State)
	// 验证状态
	if state != stateSubscribed && state != stateClosing {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "cannot TOUCH in current state")
	}
	// 验证参数个数
	if len(params) < 2 {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "TOUCH insufficient number of params")
	}
	// 获取消息ID
	id, err := getMessageID(params[1])
	if err != nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", err.Error())
	}

	// 设置超时时间
	client.writeLock.RLock()
	msgTimeout := client.MsgTimeout
	client.writeLock.RUnlock()
	err = client.Channel.TouchMessage(client.ID, *id, msgTimeout)
	if err != nil {
		return nil, protocol.NewClientErr(err, "E_TOUCH_FAILED",
			fmt.Sprintf("TOUCH %s failed %s", *id, err.Error()))
	}

	return nil, nil
}

// 解析多条消息
func readMPUB(r io.Reader, tmp []byte, idChan chan MessageID, maxMessageSize int64) ([]*Message, error) {
	// 读取前面的四个字节，表示消息的个数
	numMessages, err := readLen(r, tmp)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "MPUB failed to read message count")
	}

	// 检验消息个数的正确性
	if numMessages <= 0 {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY",
			fmt.Sprintf("MPUB invalid message count %d", numMessages))
	}
	// 创建数组
	messages := make([]*Message, 0, numMessages)
	for i := int32(0); i < numMessages; i++ {
		// 获取前面的四个字节，表示该条消息的长度
		messageSize, err := readLen(r, tmp)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB failed to read message(%d) body size", i))
		}
		// 验证消息的长度的合法性
		if messageSize <= 0 {
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB invalid message(%d) body size %d", i, messageSize))
		}
		if int64(messageSize) > maxMessageSize {
			return nil, protocol.NewFatalClientErr(nil, "E_BAD_MESSAGE",
				fmt.Sprintf("MPUB message too big %d > %d", messageSize, maxMessageSize))
		}

		// 获取该条消息的内容
		msgBody := make([]byte, messageSize)
		_, err = io.ReadFull(r, msgBody)
		if err != nil {
			return nil, protocol.NewFatalClientErr(err, "E_BAD_MESSAGE", "MPUB failed to read message body")
		}
		// 创建消息结构，并添加到数组
		messages = append(messages, NewMessage(<-idChan, msgBody))
	}

	return messages, nil
}

// validate and cast the bytes on the wire to a message ID
func getMessageID(p []byte) (*MessageID, error) {
	if len(p) != MsgIDLength {
		return nil, errors.New("Invalid Message ID")
	}
	return (*MessageID)(unsafe.Pointer(&p[0])), nil
}

// 读取前面四个字节，并获取消息的长度
func readLen(r io.Reader, tmp []byte) (int32, error) {
	_, err := io.ReadFull(r, tmp)
	if err != nil {
		return 0, err
	}
	return int32(binary.BigEndian.Uint32(tmp)), nil
}

func enforceTLSPolicy(client *clientV2, p *protocolV2, command []byte) error {
	if p.ctx.nsqd.getOpts().TLSRequired != TLSNotRequired && atomic.LoadInt32(&client.TLS) != 1 {
		return protocol.NewFatalClientErr(nil, "E_INVALID",
			fmt.Sprintf("cannot %s in current state (TLS required)", command))
	}
	return nil
}
