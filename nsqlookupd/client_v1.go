package nsqlookupd

import (
	"net"
)

type ClientV1 struct {
	net.Conn           // 组合net.Conn接口
	peerInfo *PeerInfo // client的信息也就是前面所讲的product的信息
}

// 初始化一个ClientV
func NewClientV1(conn net.Conn) *ClientV1 {
	return &ClientV1{
		Conn: conn,
	}
}

// 实现String接口
func (c *ClientV1) String() string {
	return c.RemoteAddr().String()
}
