package nsqd

import (
	"bytes"
	"encoding/json"
	"net"
	"os"
	"strconv"
	"time"

	"github.com/feixiao/nsq-0.3.7/internal/version"
	"github.com/nsqio/go-nsq"
)

// 连接成功之后调用
func connectCallback(n *NSQD, hostname string, syncTopicChan chan *lookupPeer) func(*lookupPeer) {
	return func(lp *lookupPeer) {
		ci := make(map[string]interface{})
		ci["version"] = version.Binary          // nsqd的版本号
		ci["tcp_port"] = n.RealTCPAddr().Port   // tcp服务的端口
		ci["http_port"] = n.RealHTTPAddr().Port // http服务的端口
		ci["hostname"] = hostname
		ci["broadcast_address"] = n.getOpts().BroadcastAddress // for what ??

		// https://github.com/feixiao/go-nsq/blob/master/command.go
		// 发送认证命令，具体生成的命令，请查看上述源码
		cmd, err := nsq.Identify(ci)
		if err != nil {
			lp.Close()
			return
		}
		// 发送命令并获取回复
		resp, err := lp.Command(cmd)
		if err != nil {
			// 出错
			n.logf("LOOKUPD(%s): ERROR %s - %s", lp, cmd, err)
		} else if bytes.Equal(resp, []byte("E_INVALID")) {
			// 回复出错
			n.logf("LOOKUPD(%s): lookupd returned %s", lp, resp)
		} else {
			// 正确结果
			err = json.Unmarshal(resp, &lp.Info)
			if err != nil {
				n.logf("LOOKUPD(%s): ERROR parsing response - %s", lp, resp)
			} else {
				n.logf("LOOKUPD(%s): peer info %+v", lp, lp.Info)
			}
		}

		go func() {
			syncTopicChan <- lp
		}()
	}
}

func (n *NSQD) lookupLoop() {
	var lookupPeers []*lookupPeer // 保存完成连接的lookupd对象信息
	var lookupAddrs []string      // 保存已经连接好的lookupd地址信息
	syncTopicChan := make(chan *lookupPeer)
	connect := true

	hostname, err := os.Hostname() // 获取host名字
	if err != nil {
		n.logf("FATAL: failed to get hostname - %s", err)
		os.Exit(1)
	}

	// for announcements, lookupd determines the host automatically
	ticker := time.Tick(15 * time.Second)
	for {
		if connect {
			// 连接全部的lookupd
			for _, host := range n.getOpts().NSQLookupdTCPAddresses {
				if in(host, lookupAddrs) {
					continue // 如果已经完成连接，那么我们继续下一个
				}
				n.logf("LOOKUP(%s): adding peer", host)
				// 创建连接到lookupd的连接对象
				lookupPeer := newLookupPeer(host, n.getOpts().MaxBodySize, n.getOpts().Logger,
					connectCallback(n, hostname, syncTopicChan))
				lookupPeer.Command(nil) // start the connection
				lookupPeers = append(lookupPeers, lookupPeer)
				lookupAddrs = append(lookupAddrs, host)
			}
			n.lookupPeers.Store(lookupPeers)
			connect = false
		}

		select {
		case <-ticker:
			// send a heartbeat and read a response (read detects closed conns)
			for _, lookupPeer := range lookupPeers {
				n.logf("LOOKUPD(%s): sending heartbeat", lookupPeer)
				cmd := nsq.Ping()
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err)
				}
			}
		case val := <-n.notifyChan: //
			var cmd *nsq.Command
			var branch string

			switch val.(type) {
			case *Channel:
				// notify all nsqlookupds that a new channel exists, or that it's removed
				branch = "channel"
				channel := val.(*Channel)
				if channel.Exiting() == true {
					cmd = nsq.UnRegister(channel.topicName, channel.name)
				} else {
					cmd = nsq.Register(channel.topicName, channel.name)
				}
			case *Topic:
				// notify all nsqlookupds that a new topic exists, or that it's removed
				branch = "topic"
				topic := val.(*Topic)
				if topic.Exiting() == true {
					cmd = nsq.UnRegister(topic.name, "")
				} else {
					cmd = nsq.Register(topic.name, "")
				}
			}

			for _, lookupPeer := range lookupPeers {
				n.logf("LOOKUPD(%s): %s %s", lookupPeer, branch, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err)
				}
			}
		case lookupPeer := <-syncTopicChan:
			var commands []*nsq.Command
			// build all the commands first so we exit the lock(s) as fast as possible
			n.RLock()
			for _, topic := range n.topicMap {
				topic.RLock()
				if len(topic.channelMap) == 0 {
					commands = append(commands, nsq.Register(topic.name, ""))
				} else {
					for _, channel := range topic.channelMap {
						commands = append(commands, nsq.Register(channel.topicName, channel.name))
					}
				}
				topic.RUnlock()
			}
			n.RUnlock()

			for _, cmd := range commands {
				n.logf("LOOKUPD(%s): %s", lookupPeer, cmd)
				_, err := lookupPeer.Command(cmd)
				if err != nil {
					n.logf("LOOKUPD(%s): ERROR %s - %s", lookupPeer, cmd, err)
					break
				}
			}
		case <-n.optsNotificationChan:
			var tmpPeers []*lookupPeer
			var tmpAddrs []string
			for _, lp := range lookupPeers {
				if in(lp.addr, n.getOpts().NSQLookupdTCPAddresses) {
					tmpPeers = append(tmpPeers, lp)
					tmpAddrs = append(tmpAddrs, lp.addr)
					continue
				}
				n.logf("LOOKUP(%s): removing peer", lp)
				lp.Close()
			}
			lookupPeers = tmpPeers
			lookupAddrs = tmpAddrs
			connect = true
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	n.logf("LOOKUP: closing")
}

func in(s string, lst []string) bool {
	for _, v := range lst {
		if s == v {
			return true
		}
	}
	return false
}

func (n *NSQD) lookupdHTTPAddrs() []string {
	var lookupHTTPAddrs []string
	lookupPeers := n.lookupPeers.Load()
	if lookupPeers == nil {
		return nil
	}
	for _, lp := range lookupPeers.([]*lookupPeer) {
		if len(lp.Info.BroadcastAddress) <= 0 {
			continue
		}
		addr := net.JoinHostPort(lp.Info.BroadcastAddress, strconv.Itoa(lp.Info.HTTPPort))
		lookupHTTPAddrs = append(lookupHTTPAddrs, addr)
	}
	return lookupHTTPAddrs
}
