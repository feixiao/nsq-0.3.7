package nsqlookupd

import (
	"io"
	"net"

	"github.com/feixiao/nsq-0.3.7/internal/protocol"
)

type tcpServer struct {
	ctx *Context
}

// 这个肯定在goroutine里面被调用
func (p *tcpServer) Handle(clientConn net.Conn) {
	p.ctx.nsqlookupd.logf("TCP: new client(%s)", clientConn.RemoteAddr())

	// The client should initialize itself by sending a 4 byte sequence indicating
	// the version of the protocol that it intends to communicate, this will allow us
	// to gracefully upgrade the protocol away from text/line oriented to whatever...
	buf := make([]byte, 4)  // 初始化4个字节的buf,用来获取协议中的版本号
	_, err := io.ReadFull(clientConn, buf)
	if err != nil {
		p.ctx.nsqlookupd.logf("ERROR: failed to read protocol version - %s", err)
		return
	}
	// 获取协议版本号
	protocolMagic := string(buf)

	p.ctx.nsqlookupd.logf("CLIENT(%s): desired protocol magic '%s'",
		clientConn.RemoteAddr(), protocolMagic)

	var prot protocol.Protocol  // 协议接口
	switch protocolMagic {
	case "  V1":  // 如果是  V1，注意4个字节哦，前面有两个空格字符
		// 初始化一个lookupProtocolV1的指针，将Context 组合进去 具体参考nsq/nsqlookupd/lookup_protocol_v1.go文件里的代码
		prot = &LookupProtocolV1{ctx: p.ctx}
	default:
		// 如果不是  V1 则关闭连接，返回E_BAD_PROTOCOL
		protocol.SendResponse(clientConn, []byte("E_BAD_PROTOCOL"))
		clientConn.Close()
		p.ctx.nsqlookupd.logf("ERROR: client(%s) bad protocol magic '%s'",
			clientConn.RemoteAddr(), protocolMagic)
		return
	}
	// 到这里才是整个tcpServer中的重头戏
	err = prot.IOLoop(clientConn)
	if err != nil {
		p.ctx.nsqlookupd.logf("ERROR: client(%s) - %s", clientConn.RemoteAddr(), err)
		return
	}
}
