package nsqd

import (
	"io"
	"net"

	"github.com/feixiao/nsq-0.3.7/internal/protocol"
)


// 实现了nsqio/nsq/internal/protocol下面的tcp_server.go
type tcpServer struct {
	ctx *context
}

// NSQD实例处理长连接
func (p *tcpServer) Handle(clientConn net.Conn) {
	p.ctx.nsqd.logf("TCP: new client(%s)", clientConn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...

	// 读取前面四个字节，验证版本号
	buf := make([]byte, 4)
	_, err := io.ReadFull(clientConn, buf)
	if err != nil {
		p.ctx.nsqd.logf("ERROR: failed to read protocol version - %s", err)
		return
	}
	protocolMagic := string(buf)

	p.ctx.nsqd.logf("CLIENT(%s): desired protocol magic '%s'",
		clientConn.RemoteAddr(), protocolMagic)

	var prot protocol.Protocol
	switch protocolMagic {
	// 如果数据正确，那么创建protocolV2进行处理
	case "  V2":
		prot = &protocolV2{ctx: p.ctx}
	default:
		// 数据错误则返回错误信息断开tcp连接
		protocol.SendFramedResponse(clientConn, frameTypeError, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		p.ctx.nsqd.logf("ERROR: client(%s) bad protocol magic '%s'",
			clientConn.RemoteAddr(), protocolMagic)
		return
	}

	// protocolV2的IOLoop处理长连接的业务数据
	err = prot.IOLoop(clientConn)
	if err != nil {
		p.ctx.nsqd.logf("ERROR: client(%s) - %s", clientConn.RemoteAddr(), err)
		return
	}
}
