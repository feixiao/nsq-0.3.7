package nsqd

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"

	// https://github.com/nsqio/go-nsq/blob/v1.0.5/command.go
	"github.com/nsqio/go-nsq"
)

// lookupPeer is a low-level type for connecting/reading/writing to nsqlookupd
//
// A lookupPeer instance is designed to connect lazily to nsqlookupd and reconnect
// gracefully (i.e. it is all handled by the library).  Clients can simply use the
// Command interface to perform a round-trip.
// 连接到lookupd的对象
type lookupPeer struct {
	l               logger
	addr            string				// 需要连接到的lookupd的地址信息
	conn            net.Conn			// 连接对象
	state           int32				// 状态信息 client_v2.go中定义
	connectCallback func(*lookupPeer)	// 连接状态回调函数
	maxBodySize     int64				// 设置的最大消息体长度
	Info            peerInfo			// 本端信息
}

// peerInfo contains metadata for a lookupPeer instance (and is JSON marshalable)
// 需要告知lookupd本端的信息
type peerInfo struct {
	TCPPort          int    `json:"tcp_port"`
	HTTPPort         int    `json:"http_port"`
	Version          string `json:"version"`
	BroadcastAddress string `json:"broadcast_address"`
}

// newLookupPeer creates a new lookupPeer instance connecting to the supplied address.
//
// The supplied connectCallback will be called *every* time the instance connects.
// 创建lookupPeer对象
func newLookupPeer(addr string, maxBodySize int64, l logger, connectCallback func(*lookupPeer)) *lookupPeer {
	return &lookupPeer{
		l:               l,
		addr:            addr,
		state:           stateDisconnected,
		maxBodySize:     maxBodySize,
		connectCallback: connectCallback,
	}
}

// Connect will Dial the specified address, with timeouts
// 带有1秒超时的连接操作
func (lp *lookupPeer) Connect() error {
	lp.l.Output(2, fmt.Sprintf("LOOKUP connecting to %s", lp.addr))
	conn, err := net.DialTimeout("tcp", lp.addr, time.Second)
	if err != nil {
		return err
	}
	lp.conn = conn	// 如果连接成功，获取到连接对象
	return nil
}

// String returns the specified address
// 返回本对象连接的lookupd的地址信息
func (lp *lookupPeer) String() string {
	return lp.addr
}

// Read implements the io.Reader interface, adding deadlines
// 实现io.Reader接口，并带有1妙的读超时
func (lp *lookupPeer) Read(data []byte) (int, error) {
	lp.conn.SetReadDeadline(time.Now().Add(time.Second))
	return lp.conn.Read(data)
}

// Write implements the io.Writer interface, adding deadlines
// 实现io.Writer接口，并带有1妙的写超时
func (lp *lookupPeer) Write(data []byte) (int, error) {
	lp.conn.SetWriteDeadline(time.Now().Add(time.Second))
	return lp.conn.Write(data)
}

// Close implements the io.Closer interface
// 实现io.Closer接口
func (lp *lookupPeer) Close() error {
	lp.state = stateDisconnected		// 设置为连接断开状态
	if lp.conn != nil {
		return lp.conn.Close()			// 并关闭连接状态
	}
	return nil
}

// Command performs a round-trip for the specified Command.
//
// It will lazily connect to nsqlookupd and gracefully handle
// reconnecting in the event of a failure.
//
// It returns the response from nsqlookupd as []byte
// 执行命令，并返回nsqlookupd的返回信息
// nsq : "github.com/nsqio/go-nsq"
func (lp *lookupPeer) Command(cmd *nsq.Command) ([]byte, error) {
	initialState := lp.state
	if lp.state != stateConnected {
		// 如果不是连接状态，那么我们自己连接
		err := lp.Connect()
		if err != nil {
			return nil, err
		}
		lp.state = stateConnected
		lp.Write(nsq.MagicV1)
		if initialState == stateDisconnected {
			lp.connectCallback(lp)	// 如果是断开连接切换到连接状态，那么调用回调函数通知外面
		}
	}
	// 如果cmd==nil，只是个连接操作
	if cmd == nil {
		return nil, nil
	}
	// 发送cmd，因为这边lp实现了io.Writer接口
	_, err := cmd.WriteTo(lp)
	if err != nil {
		lp.Close()
		return nil, err
	}
	// 读取nsqlookupd的返回信息，因为这边lp实现了io.Reader接口
	resp, err := readResponseBounded(lp, lp.maxBodySize)
	if err != nil {
		lp.Close()
		return nil, err
	}
	return resp, nil
}

// 读取回复消息
func readResponseBounded(r io.Reader, limit int64) ([]byte, error) {
	var msgSize int32

	// message size
	// 读取前面四个四个字节的大小信息
	err := binary.Read(r, binary.BigEndian, &msgSize)
	if err != nil {
		return nil, err
	}

	// 判断消息大小的合法性
	if int64(msgSize) > limit {
		return nil, fmt.Errorf("response body size (%d) is greater than limit (%d)",
			msgSize, limit)
	}

	// message binary data
	// 读取全部的消息内容
	buf := make([]byte, msgSize)
	_, err = io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}
	// 返回读取的信息内容
	return buf, nil
}
