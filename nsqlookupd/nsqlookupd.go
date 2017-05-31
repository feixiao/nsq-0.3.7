package nsqlookupd

import (
	"fmt"
	"net"
	"os"
	"sync"

	"github.com/feixiao/nsq-0.3.7/internal/http_api"
	"github.com/feixiao/nsq-0.3.7/internal/protocol"
	"github.com/feixiao/nsq-0.3.7/internal/util"
	"github.com/feixiao/nsq-0.3.7/internal/version"
)

type NSQLookupd struct {
	sync.RWMutex          // 读写锁
	opts         *Options // nsqlookupd 配置信息 定义文件路径为nsq/nsqlookupd/options.go
	tcpListener  net.Listener
	httpListener net.Listener
	waitGroup    util.WaitGroupWrapper // WaitGroup 典型应用 用于开启两个goroutine，一个监听HTTP 一个监听TCP
	DB           *RegistrationDB       // product 注册数据库 具体分析后面章节再讲
}

// 初始化NSQLookupd实例
func New(opts *Options) *NSQLookupd {
	n := &NSQLookupd{
		opts: opts,
		DB:   NewRegistrationDB(), // 初始化DB实例
	}
	n.logf(version.String("nsqlookupd"))
	return n
}

func (l *NSQLookupd) logf(f string, args ...interface{}) {
	if l.opts.Logger == nil {
		return
	}
	l.opts.Logger.Output(2, fmt.Sprintf(f, args...))
}

func (l *NSQLookupd) Main() {
	// 初始化Context实例将NSQLookupd指针放入Context实例中
	// Context结构请参考文件nsq/nsqlookupd/context.go
	// Context用于nsqlookupd中的tcpServer 和 httpServer中

	ctx := &Context{l}

	tcpListener, err := net.Listen("tcp", l.opts.TCPAddress)
	if err != nil {
		l.logf("FATAL: listen (%s) failed - %s", l.opts.TCPAddress, err)
		os.Exit(1)
	}
	l.Lock()
	l.tcpListener = tcpListener
	l.Unlock()

	// 创建一个tcpServer, tcpServer 实现了nsq/internal/protocol包中的TCPHandler接口( 即Handle(net.Conn) )
	tcpServer := &tcpServer{ctx: ctx}
	l.waitGroup.Wrap(func() {
		// protocol.TCPServer方法的过程就是tcpListener accept tcp的连接
		// 然后通过tcpServer中的Handle分析报文，然后处理相关的协议
		protocol.TCPServer(tcpListener, tcpServer, l.opts.Logger)
	}) // 把tcpServer加入到waitGroup

	// http服务
	httpListener, err := net.Listen("tcp", l.opts.HTTPAddress)
	if err != nil {
		l.logf("FATAL: listen (%s) failed - %s", l.opts.HTTPAddress, err)
		os.Exit(1)
	}
	l.Lock()
	l.httpListener = httpListener
	l.Unlock()
	httpServer := newHTTPServer(ctx)
	l.waitGroup.Wrap(func() {
		http_api.Serve(httpListener, httpServer, "HTTP", l.opts.Logger)
	})
}

func (l *NSQLookupd) RealTCPAddr() *net.TCPAddr {
	l.RLock()
	defer l.RUnlock()
	return l.tcpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) RealHTTPAddr() *net.TCPAddr {
	l.RLock()
	defer l.RUnlock()
	return l.httpListener.Addr().(*net.TCPAddr)
}

func (l *NSQLookupd) Exit() {
	if l.tcpListener != nil {
		l.tcpListener.Close()
	}

	if l.httpListener != nil {
		l.httpListener.Close()
	}
	l.waitGroup.Wait()
}
