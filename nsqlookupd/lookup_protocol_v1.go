package nsqlookupd

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/feixiao/nsq-0.3.7/internal/protocol"
	"github.com/feixiao/nsq-0.3.7/internal/version"
)

type LookupProtocolV1 struct {
	ctx *Context // 一直贯穿整个代码的Context，具体可翻看前面章节
}

// 协议的主要处理函数
func (p *LookupProtocolV1) IOLoop(conn net.Conn) error {
	var err error
	var line string

	client := NewClientV1(conn) // 新建一个client版本为V1
	// 由client 创建一个 带有buffer 的Reader 默认 buffer size 为4096，
	// 这里的NewReader参数为io.Reader 接口，为何net.Conn接口也能作为参数呢？
	// 因为net.Conn 接口其实也是实现了io.Reader接口了
	reader := bufio.NewReader(client)
	for {
		line, err = reader.ReadString('\n') // 按行读取
		if err != nil {
			break // 长连接出错就离开
		}
		// 去掉这行两头的空格符
		line = strings.TrimSpace(line)
		// 字符串按一个空格字符串分割，并获取相应的Commad 以及 该command 的相应的params
		params := strings.Split(line, " ")

		var response []byte
		response, err = p.Exec(client, reader, params) // 执行相应的Command
		if err != nil {
			ctx := ""
			if parentErr := err.(protocol.ChildErr).Parent(); parentErr != nil {
				ctx = " - " + parentErr.Error()
			}
			p.ctx.nsqlookupd.logf("ERROR: [%s] - %s%s", client, err, ctx)

			_, sendErr := protocol.SendResponse(client, []byte(err.Error())) // 返回错误给client
			if sendErr != nil {
				p.ctx.nsqlookupd.logf("ERROR: [%s] - %s%s", client, sendErr, ctx)
				break
			}

			// errors of type FatalClientErr should forceably close the connection
			if _, ok := err.(*protocol.FatalClientErr); ok {
				break
			}
			continue
		}

		if response != nil {
			_, err = protocol.SendResponse(client, response) // 返回命令处理结果给client
			if err != nil {
				break
			}
		}
	}

	// for 循环结束了 说明程序要退出了，调用RegistrationDB 中的 RemoveProducer从producer 的注册数据中删除 producer信息
	p.ctx.nsqlookupd.logf("CLIENT(%s): closing", client)
	if client.peerInfo != nil {
		registrations := p.ctx.nsqlookupd.DB.LookupRegistrations(client.peerInfo.id)
		for _, r := range registrations {
			if removed, _ := p.ctx.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
				p.ctx.nsqlookupd.logf("DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
					client, r.Category, r.Key, r.SubKey)
			}
		}
	}
	return err
}

// 这个方法就是执行相应的命令动作 有 PING IDENTIFY REGISTER UNREGISTER 这四个类型
func (p *LookupProtocolV1) Exec(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	switch params[0] {
	case "PING": // 用于client的心跳处理
		return p.PING(client, params)
	case "IDENTIFY": // 用于client端的信息注册，执行PING REGISTER UNREGISTER 命令前必须先执行此命令
		return p.IDENTIFY(client, reader, params[1:])
	case "REGISTER": // 用于client端注册topic以及channel的命令
		return p.REGISTER(client, reader, params[1:])
	case "UNREGISTER": // 执行与REGISTER命令相反的逻辑
		return p.UNREGISTER(client, reader, params[1:])
	}
	return nil, protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("invalid command %s", params[0]))
}

// 获取params中的topic 以及 channel
func getTopicChan(command string, params []string) (string, string, error) {
	if len(params) == 0 {
		return "", "", protocol.NewFatalClientErr(nil, "E_INVALID", fmt.Sprintf("%s insufficient number of params", command))
	}

	topicName := params[0]
	var channelName string
	if len(params) >= 2 {
		channelName = params[1]
	}
	// 校验是否是topic
	if !protocol.IsValidTopicName(topicName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_TOPIC", fmt.Sprintf("%s topic name '%s' is not valid", command, topicName))
	}
	// 校验是否是channel
	if channelName != "" && !protocol.IsValidChannelName(channelName) {
		return "", "", protocol.NewFatalClientErr(nil, "E_BAD_CHANNEL", fmt.Sprintf("%s channel name '%s' is not valid", command, channelName))
	}

	return topicName, channelName, nil
}

// REGISTER 命令 用于注册client的topic 以及 channel信息

// 一个topic下可以有多个channel
// 一个消费者订阅的是一个topic 那么 生成者给这个topic 下的channel的信息 这个消费者也能接收得到这个信息，如果消费者订阅的不是这个channel的信息，那么这个消费者则接受不到这个信息
// nsq topic 与channel的关系，大家可以多搜索下资料，我这里感觉讲的也不太清晰，请大家谅解一下
// REGISTER 命令必须在INDENTIFY 之后才能调用

// 具体协议报文如下
// REGISTER topic1\n 		这个只创建一个名字为topic1的topic
// 或如下报文
// REGISTER topic1 channel1\n 	这个只创建一个名字为topic1的topic 且topic1下面创建一个名字为channel1的channel
func (p *LookupProtocolV1) REGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}

	// 获取REGISTER 命令中的topic 以及channel名字
	topic, channel, err := getTopicChan("REGISTER", params)
	if err != nil {
		return nil, err
	}

	if channel != "" {
		// 生成channel的key：Category 为"channel"字符串，Key为topic，SubKey为channel
		key := Registration{"channel", topic, channel}
		// 将生存者添加到这个key中
		if p.ctx.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
			p.ctx.nsqlookupd.logf("DB: client(%s) REGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
	}
	// 生成topic的key
	key := Registration{"topic", topic, ""}
	// 将生存者添加到这个key中
	if p.ctx.nsqlookupd.DB.AddProducer(key, &Producer{peerInfo: client.peerInfo}) {
		p.ctx.nsqlookupd.logf("DB: client(%s) REGISTER category:%s key:%s subkey:%s",
			client, "topic", topic, "")
	}

	return []byte("OK"), nil
}

// UNREGISTER命令用于取消注册topic 或取消注册某个topic下的某一个channel

// 报文格式如下
// UNREGISTER topic1 channel1\n 这个报文表示取消注册名字为topic1的topic下的名字为channel1的channel，这个名字 只取消注册这个channel1，不取消注册topic1下的其他channel 以及这个topic1本身
// 或这个格式的报文
// UNREGISTER topic1\n 这个报文表示取消注册名字为topic1的topic，这个时候topic1以及topic1下的所有channel都取消注册了

func (p *LookupProtocolV1) UNREGISTER(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	if client.peerInfo == nil {
		return nil, protocol.NewFatalClientErr(nil, "E_INVALID", "client must IDENTIFY")
	}

	// 获取topic 以及channel 的名字
	topic, channel, err := getTopicChan("UNREGISTER", params)
	if err != nil {
		return nil, err
	}

	if channel != "" {
		// 如果有channel 则只取消注册这个topic下的这个channel
		// 生成key
		key := Registration{"channel", topic, channel}
		// 移除这个可以下面的端点
		removed, left := p.ctx.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id)
		if removed {
			p.ctx.nsqlookupd.logf("DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "channel", topic, channel)
		}
		// for ephemeral channels, remove the channel as well if it has no producers
		// 如果没有其他生存者了同时channel名字含有“ephemeral”的缀，那么删除key
		if left == 0 && strings.HasSuffix(channel, "#ephemeral") {
			p.ctx.nsqlookupd.DB.RemoveRegistration(key)
		}
	} else {
		// no channel was specified so this is a topic unregistration
		// remove all of the channel registrations...
		// normally this shouldn't happen which is why we print a warning message
		// if anything is actually removed

		// 如果没有channel字段，说明反注册的是整个topic以及这个topic下的所有channel

		// 删除这个topic下的所有channel
		registrations := p.ctx.nsqlookupd.DB.FindRegistrations("channel", topic, "*")
		for _, r := range registrations {
			if removed, _ := p.ctx.nsqlookupd.DB.RemoveProducer(r, client.peerInfo.id); removed {
				p.ctx.nsqlookupd.logf("WARNING: client(%s) unexpected UNREGISTER category:%s key:%s subkey:%s",
					client, "channel", topic, r.SubKey)
			}
		}
		// 删除这个topic
		key := Registration{"topic", topic, ""}
		if removed, _ := p.ctx.nsqlookupd.DB.RemoveProducer(key, client.peerInfo.id); removed {
			p.ctx.nsqlookupd.logf("DB: client(%s) UNREGISTER category:%s key:%s subkey:%s",
				client, "topic", topic, "")
		}
	}

	return []byte("OK"), nil
}

// INDENTIFY命令处理逻辑

// 该命令用于注册client的producer信息，并返回nsqlookupd的TCP 以及 HTTP 端口信息给client
// 大致的报文如下
//  V1 INDENTIFY\n   注意了这里前面提过的V1前面是两个空格字符
//	123\n  这里是后面json数据（producer 信息的json数据的字节长度）
//	{....}\n 一串表示producer信息的json数据，具体的可参考 nsq/nsqlookupd/registration_db.go文件中的PeerInfo struct

func (p *LookupProtocolV1) IDENTIFY(client *ClientV1, reader *bufio.Reader, params []string) ([]byte, error) {
	var err error

	if client.peerInfo != nil { // 如果有该client 的PeerInfo数据则返回错误
		return nil, protocol.NewFatalClientErr(err, "E_INVALID", "cannot IDENTIFY again")
	}

	var bodyLen int32
	// 获取producer PeerInfo json数据的字节长度 大端二进制格式
	err = binary.Read(reader, binary.BigEndian, &bodyLen)
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body size")
	}

	body := make([]byte, bodyLen)      // 初始化一个producer PeerInfo json数据长度的bytes
	_, err = io.ReadFull(reader, body) // 读取所有的json数据
	if err != nil {
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to read body")
	}

	// body is a json structure with producer information
	peerInfo := PeerInfo{id: client.RemoteAddr().String()} // 实例化一个PeerInfo
	err = json.Unmarshal(body, &peerInfo)                  // 解析producer PeerInfo json数据到peerInfo
	if err != nil {
		// json 数据解析失败 返回错误
		return nil, protocol.NewFatalClientErr(err, "E_BAD_BODY", "IDENTIFY failed to decode JSON body")
	}

	peerInfo.RemoteAddress = client.RemoteAddr().String() // 获取PeerInfo remote address

	// require all fields
	// 校验获取的PeerInfo 数据
	if peerInfo.BroadcastAddress == "" || peerInfo.TCPPort == 0 || peerInfo.HTTPPort == 0 || peerInfo.Version == "" {
		return nil, protocol.NewFatalClientErr(nil, "E_BAD_BODY", "IDENTIFY missing fields")
	}

	// 将当前系统时间（纳秒）更新到PeerInfo 中的lastUpdate中 用于 client的心跳判断
	atomic.StoreInt64(&peerInfo.lastUpdate, time.Now().UnixNano())

	p.ctx.nsqlookupd.logf("CLIENT(%s): IDENTIFY Address:%s TCP:%d HTTP:%d Version:%s",
		client, peerInfo.BroadcastAddress, peerInfo.TCPPort, peerInfo.HTTPPort, peerInfo.Version)

	client.peerInfo = &peerInfo
	// 注册producer PeerInfo 信息到 RegistrationDB中，其中Registration的 Category 为client，Key 和 SubKey为空
	if p.ctx.nsqlookupd.DB.AddProducer(Registration{"client", "", ""}, &Producer{peerInfo: client.peerInfo}) {
		p.ctx.nsqlookupd.logf("DB: client(%s) REGISTER category:%s key:%s subkey:%s", client, "client", "", "")
	}

	// 将nsqlookupd的TCP端口，HTTP端口信息，版本信息，broadcast address信息，
	// 以及host name信息以json数据格式返回给client
	// build a response
	data := make(map[string]interface{})
	data["tcp_port"] = p.ctx.nsqlookupd.RealTCPAddr().Port
	data["http_port"] = p.ctx.nsqlookupd.RealHTTPAddr().Port
	data["version"] = version.Binary
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatalf("ERROR: unable to get hostname %s", err)
	}
	data["broadcast_address"] = p.ctx.nsqlookupd.opts.BroadcastAddress
	data["hostname"] = hostname

	response, err := json.Marshal(data)
	if err != nil {
		p.ctx.nsqlookupd.logf("ERROR: marshaling %v", data)
		return []byte("OK"), nil
	}
	return response, nil
}

// PING 用于client中的心跳处理命令
func (p *LookupProtocolV1) PING(client *ClientV1, params []string) ([]byte, error) {
	if client.peerInfo != nil {
		// we could get a PING before other commands on the same client connection
		// 获取上一次心跳的时间
		cur := time.Unix(0, atomic.LoadInt64(&client.peerInfo.lastUpdate))
		// 获取当前时间
		now := time.Now()
		// 这里日志输出两次心跳之间的间隔时间
		p.ctx.nsqlookupd.logf("CLIENT(%s): pinged (last ping %s)", client.peerInfo.id,
			now.Sub(cur))
		// 更新PeerInfo中的lastUpdate时间为当前时间
		atomic.StoreInt64(&client.peerInfo.lastUpdate, now.UnixNano())
	}
	return []byte("OK"), nil
}
