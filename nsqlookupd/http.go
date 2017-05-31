package nsqlookupd

import (
	"fmt"
	"net/http"
	"net/http/pprof"
	"sync/atomic"

	"github.com/julienschmidt/httprouter"
	"github.com/feixiao/nsq-0.3.7/internal/http_api"
	"github.com/feixiao/nsq-0.3.7/internal/protocol"
	"github.com/feixiao/nsq-0.3.7/internal/version"
)

type httpServer struct {
	ctx    *Context
	router http.Handler
}

// 对外的http服务
func newHTTPServer(ctx *Context) *httpServer {
	log := http_api.Log(ctx.nsqlookupd.opts.Logger)

	router := httprouter.New()
	router.HandleMethodNotAllowed = true
	router.PanicHandler = http_api.LogPanicHandler(ctx.nsqlookupd.opts.Logger)
	router.NotFound = http_api.LogNotFoundHandler(ctx.nsqlookupd.opts.Logger)
	router.MethodNotAllowed = http_api.LogMethodNotAllowedHandler(ctx.nsqlookupd.opts.Logger)
	s := &httpServer{
		ctx:    ctx,
		router: router, // router传入
	}

	// 函数的处理过程：
	// 	http_api.PlainText
	//			--> log
	//				--> s.pingHandler
	router.Handle("GET", "/ping", http_api.Decorate(s.pingHandler, log, http_api.PlainText))

	// v1 negotiate
	router.Handle("GET", "/debug", http_api.Decorate(s.doDebug, log, http_api.NegotiateVersion))
	router.Handle("GET", "/lookup", http_api.Decorate(s.doLookup, log, http_api.NegotiateVersion))
	router.Handle("GET", "/topics", http_api.Decorate(s.doTopics, log, http_api.NegotiateVersion))
	router.Handle("GET", "/channels", http_api.Decorate(s.doChannels, log, http_api.NegotiateVersion))
	router.Handle("GET", "/nodes", http_api.Decorate(s.doNodes, log, http_api.NegotiateVersion))

	// only v1
	router.Handle("POST", "/topic/create", http_api.Decorate(s.doCreateTopic, log, http_api.V1))
	router.Handle("POST", "/topic/delete", http_api.Decorate(s.doDeleteTopic, log, http_api.V1))
	router.Handle("POST", "/channel/create", http_api.Decorate(s.doCreateChannel, log, http_api.V1))
	router.Handle("POST", "/channel/delete", http_api.Decorate(s.doDeleteChannel, log, http_api.V1))
	router.Handle("POST", "/topic/tombstone", http_api.Decorate(s.doTombstoneTopicProducer, log, http_api.V1))

	// deprecated, v1 negotiate 废弃的接口
	router.Handle("GET", "/info", http_api.Decorate(s.doInfo, log, http_api.NegotiateVersion))
	router.Handle("POST", "/create_topic", http_api.Decorate(s.doCreateTopic, log, http_api.NegotiateVersion))
	router.Handle("POST", "/delete_topic", http_api.Decorate(s.doDeleteTopic, log, http_api.NegotiateVersion))
	router.Handle("POST", "/create_channel", http_api.Decorate(s.doCreateChannel, log, http_api.NegotiateVersion))
	router.Handle("POST", "/delete_channel", http_api.Decorate(s.doDeleteChannel, log, http_api.NegotiateVersion))
	router.Handle("POST", "/tombstone_topic_producer", http_api.Decorate(s.doTombstoneTopicProducer, log, http_api.NegotiateVersion))
	router.Handle("GET", "/create_topic", http_api.Decorate(s.doCreateTopic, log, http_api.NegotiateVersion))
	router.Handle("GET", "/delete_topic", http_api.Decorate(s.doDeleteTopic, log, http_api.NegotiateVersion))
	router.Handle("GET", "/create_channel", http_api.Decorate(s.doCreateChannel, log, http_api.NegotiateVersion))
	router.Handle("GET", "/delete_channel", http_api.Decorate(s.doDeleteChannel, log, http_api.NegotiateVersion))
	router.Handle("GET", "/tombstone_topic_producer", http_api.Decorate(s.doTombstoneTopicProducer, log, http_api.NegotiateVersion))

	// debug 语言自带的调试接口
	router.HandlerFunc("GET", "/debug/pprof", pprof.Index)
	router.HandlerFunc("GET", "/debug/pprof/cmdline", pprof.Cmdline)
	router.HandlerFunc("GET", "/debug/pprof/symbol", pprof.Symbol)
	router.HandlerFunc("POST", "/debug/pprof/symbol", pprof.Symbol) // for what ？？
	router.HandlerFunc("GET", "/debug/pprof/profile", pprof.Profile)
	router.Handler("GET", "/debug/pprof/heap", pprof.Handler("heap"))
	router.Handler("GET", "/debug/pprof/goroutine", pprof.Handler("goroutine"))
	router.Handler("GET", "/debug/pprof/block", pprof.Handler("block"))
	router.Handler("GET", "/debug/pprof/threadcreate", pprof.Handler("threadcreate"))

	return s
}

func (s *httpServer) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	s.router.ServeHTTP(w, req)
}

//  GET /ping
// 心跳处理
func (s *httpServer) pingHandler(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return "OK", nil
}

// GET  /info
// 返回程序版本信息
func (s *httpServer) doInfo(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	return struct {
		Version string `json:"version"`
	}{
		Version: version.Binary,
	}, nil
}

// GET  /topics
// 获取全部的的topic信息
func (s *httpServer) doTopics(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	topics := s.ctx.nsqlookupd.DB.FindRegistrations("topic", "*", "").Keys()
	return map[string]interface{}{
		"topics": topics,
	}, nil
}

// GET /channels
// 获取指定topic下面的channel信息
func (s *httpServer) doChannels(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	// 从请求中获取topic名字
	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	// 获取指定topic下面的全部channel信息
	channels := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	return map[string]interface{}{
		"channels": channels,
	}, nil
}

// GET  /lookup
// 获取指定topic下面的channel和生产者的对应关系
func (s *httpServer) doLookup(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	// 获取请求中topic名字
	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}
	// 查找topic信息
	registration := s.ctx.nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	if len(registration) == 0 {
		return nil, http_api.Err{404, "TOPIC_NOT_FOUND"}
	}
	// 获取全部的channels信息
	channels := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*").SubKeys()
	// 获取指定topic下面可以用的生产者信息
	producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")
	producers = producers.FilterByActive(s.ctx.nsqlookupd.opts.InactiveProducerTimeout,
		s.ctx.nsqlookupd.opts.TombstoneLifetime)

	return map[string]interface{}{
		"channels":  channels,
		"producers": producers.PeerInfo(),
	}, nil
}

// POST /create_topic
// 创建topic
func (s *httpServer) doCreateTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	// 获取topic名字
	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}

	// 判断名字有效性： nsq/internal/protocol/name.go中实现
	if !protocol.IsValidTopicName(topicName) {
		return nil, http_api.Err{400, "INVALID_ARG_TOPIC"}
	}

	s.ctx.nsqlookupd.logf("DB: adding topic(%s)", topicName)
	// 生成关于topic的key
	key := Registration{"topic", topicName, ""}
	// 添加key
	s.ctx.nsqlookupd.DB.AddRegistration(key)

	return nil, nil
}

// 删除topic
func (s *httpServer) doDeleteTopic(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	// 获取topic的名字
	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}
	// 查找这个topic下面的全部channel信息，然后删除
	registrations := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, "*")
	for _, registration := range registrations {
		s.ctx.nsqlookupd.logf("DB: removing channel(%s) from topic(%s)", registration.SubKey, topicName)
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration)
	}

	// 查找这个topic的信息，然后删除
	registrations = s.ctx.nsqlookupd.DB.FindRegistrations("topic", topicName, "")
	for _, registration := range registrations {
		s.ctx.nsqlookupd.logf("DB: removing topic(%s)", topicName)
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration)
	}

	return nil, nil
}

// 让指定topic下面的一个生产者失效
func (s *httpServer) doTombstoneTopicProducer(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}
	// 获取topic信息
	topicName, err := reqParams.Get("topic")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_TOPIC"}
	}
	// 获取node信息
	node, err := reqParams.Get("node")
	if err != nil {
		return nil, http_api.Err{400, "MISSING_ARG_NODE"}
	}

	s.ctx.nsqlookupd.logf("DB: setting tombstone for producer@%s of topic(%s)", node, topicName)
	// 获取这个topic下面的全部生产者信息
	producers := s.ctx.nsqlookupd.DB.FindProducers("topic", topicName, "")
	for _, p := range producers {
		// 如果找到这个生成者那么让其失效
		thisNode := fmt.Sprintf("%s:%d", p.peerInfo.BroadcastAddress, p.peerInfo.HTTPPort)
		if thisNode == node {
			p.Tombstone()
		}
	}

	return nil, nil
}

// 创建channel
func (s *httpServer) doCreateChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	// 获取topic和channel的名字，并检查其有效性
	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	s.ctx.nsqlookupd.logf("DB: adding channel(%s) in topic(%s)", channelName, topicName)
	// 生成关于channel的key并添加
	key := Registration{"channel", topicName, channelName}
	s.ctx.nsqlookupd.DB.AddRegistration(key)

	s.ctx.nsqlookupd.logf("DB: adding topic(%s)", topicName)
	// 生成关于topic的key并添加
	key = Registration{"topic", topicName, ""}
	s.ctx.nsqlookupd.DB.AddRegistration(key)

	return nil, nil
}

// 删除channel
func (s *httpServer) doDeleteChannel(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	reqParams, err := http_api.NewReqParams(req)
	if err != nil {
		return nil, http_api.Err{400, "INVALID_REQUEST"}
	}

	// 获取topic和channel的名字，并检查其有效性
	topicName, channelName, err := http_api.GetTopicChannelArgs(reqParams)
	if err != nil {
		return nil, http_api.Err{400, err.Error()}
	}

	// 查找这个topic下面的全部channel
	registrations := s.ctx.nsqlookupd.DB.FindRegistrations("channel", topicName, channelName)
	if len(registrations) == 0 {
		return nil, http_api.Err{404, "CHANNEL_NOT_FOUND"}
	}

	s.ctx.nsqlookupd.logf("DB: removing channel(%s) from topic(%s)", channelName, topicName)
	// 删除这个channel下面全部生成者信息
	for _, registration := range registrations {
		s.ctx.nsqlookupd.DB.RemoveRegistration(registration)
	}

	return nil, nil
}

// 节点结构信息
type node struct {
	RemoteAddress    string   `json:"remote_address"`
	Hostname         string   `json:"hostname"`
	BroadcastAddress string   `json:"broadcast_address"`
	TCPPort          int      `json:"tcp_port"`
	HTTPPort         int      `json:"http_port"`
	Version          string   `json:"version"`
	Tombstones       []bool   `json:"tombstones"`
	Topics           []string `json:"topics"`
}

// 获取生产者节点信息
func (s *httpServer) doNodes(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	// dont filter out tombstoned nodes
	// 获取全部可用的生产者信息
	producers := s.ctx.nsqlookupd.DB.FindProducers("client", "", "").FilterByActive(
		s.ctx.nsqlookupd.opts.InactiveProducerTimeout, 0)

	nodes := make([]*node, len(producers))
	for i, p := range producers {
		topics := s.ctx.nsqlookupd.DB.LookupRegistrations(p.peerInfo.id).Filter("topic", "*", "").Keys()

		// for each topic find the producer that matches this peer
		// to add tombstone information
		tombstones := make([]bool, len(topics))
		for j, t := range topics {
			topicProducers := s.ctx.nsqlookupd.DB.FindProducers("topic", t, "")
			for _, tp := range topicProducers {
				if tp.peerInfo == p.peerInfo {
					tombstones[j] = tp.IsTombstoned(s.ctx.nsqlookupd.opts.TombstoneLifetime)
				}
			}
		}

		nodes[i] = &node{
			RemoteAddress:    p.peerInfo.RemoteAddress,
			Hostname:         p.peerInfo.Hostname,
			BroadcastAddress: p.peerInfo.BroadcastAddress,
			TCPPort:          p.peerInfo.TCPPort,
			HTTPPort:         p.peerInfo.HTTPPort,
			Version:          p.peerInfo.Version,
			Tombstones:       tombstones,
			Topics:           topics,
		}
	}

	return map[string]interface{}{
		"producers": nodes,
	}, nil
}

// 返回全部的producers信息
func (s *httpServer) doDebug(w http.ResponseWriter, req *http.Request, ps httprouter.Params) (interface{}, error) {
	s.ctx.nsqlookupd.DB.RLock()
	defer s.ctx.nsqlookupd.DB.RUnlock()

	data := make(map[string][]map[string]interface{})
	for r, producers := range s.ctx.nsqlookupd.DB.registrationMap {
		key := r.Category + ":" + r.Key + ":" + r.SubKey
		for _, p := range producers {
			m := map[string]interface{}{
				"id":                p.peerInfo.id,
				"hostname":          p.peerInfo.Hostname,
				"broadcast_address": p.peerInfo.BroadcastAddress,
				"tcp_port":          p.peerInfo.TCPPort,
				"http_port":         p.peerInfo.HTTPPort,
				"version":           p.peerInfo.Version,
				"last_update":       atomic.LoadInt64(&p.peerInfo.lastUpdate),
				"tombstoned":        p.tombstoned,
				"tombstoned_at":     p.tombstonedAt.UnixNano(),
			}
			data[key] = append(data[key], m)
		}
	}

	return data, nil
}
