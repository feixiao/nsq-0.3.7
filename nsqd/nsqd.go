package nsqd

import (
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net"
	"os"
	"path"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	simplejson "github.com/bitly/go-simplejson"

	"github.com/feixiao/nsq-0.3.7/internal/clusterinfo"
	"github.com/feixiao/nsq-0.3.7/internal/dirlock"
	"github.com/feixiao/nsq-0.3.7/internal/http_api"
	"github.com/feixiao/nsq-0.3.7/internal/protocol"
	"github.com/feixiao/nsq-0.3.7/internal/statsd"
	"github.com/feixiao/nsq-0.3.7/internal/util"
	"github.com/feixiao/nsq-0.3.7/internal/version"
)

const (
	TLSNotRequired = iota
	TLSRequiredExceptHTTP
	TLSRequired
)

type errStore struct {
	err error
}

// http://nsq.io/components/nsqd.html

type NSQD struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	clientIDSequence int64

	sync.RWMutex

	opts atomic.Value

	dl        *dirlock.DirLock // 文件锁
	isLoading int32
	errValue  atomic.Value // 表示健康状况的错误值
	startTime time.Time    // 启动时间

	// 一个nsqd实例可以有多个Topic,使用sync.RWMutex加锁
	topicMap map[string]*Topic

	lookupPeers atomic.Value

	// 服务监听对象
	tcpListener   net.Listener
	httpListener  net.Listener
	httpsListener net.Listener
	tlsConfig     *tls.Config

	poolSize int

	idChan               chan MessageID
	notifyChan           chan interface{}
	optsNotificationChan chan struct{}

	// 通知整体退出
	exitChan chan int
	// 等待goroutine退出
	waitGroup util.WaitGroupWrapper

	ci *clusterinfo.ClusterInfo
}

func New(opts *Options) *NSQD {
	// 存储数据的目录
	dataPath := opts.DataPath // 数据持久化的路径
	if opts.DataPath == "" {
		// 为空就选择当前目录
		cwd, _ := os.Getwd()
		dataPath = cwd
	}

	// 创建NSQD对象，并初始化参数
	n := &NSQD{
		startTime:            time.Now(),
		topicMap:             make(map[string]*Topic),
		idChan:               make(chan MessageID, 4096),
		exitChan:             make(chan int),
		notifyChan:           make(chan interface{}),
		optsNotificationChan: make(chan struct{}, 1),
		ci:                   clusterinfo.New(opts.Logger, http_api.NewClient(nil)), // log输出接口，http客户端对象
		dl:                   dirlock.New(dataPath),
	}
	// 存储参数
	n.swapOpts(opts)
	n.errValue.Store(errStore{})

	// 锁定数据目录（Exit函数中解锁）
	err := n.dl.Lock()
	if err != nil {
		// 失败就退出，说明其他实例在访问
		n.logf("FATAL: --data-path=%s in use (possibly by another instance of nsqd)", dataPath)
		os.Exit(1)
	}

	// 最大的压缩比率等级
	if opts.MaxDeflateLevel < 1 || opts.MaxDeflateLevel > 9 {
		n.logf("FATAL: --max-deflate-level must be [1,9]")
		os.Exit(1)
	}

	// work-id范围是[0,1024)
	if opts.ID < 0 || opts.ID >= 1024 {
		n.logf("FATAL: --worker-id must be [0,1024)")
		os.Exit(1)
	}

	if opts.StatsdPrefix != "" {
		var port string
		_, port, err = net.SplitHostPort(opts.HTTPAddress)
		if err != nil {
			n.logf("ERROR: failed to parse HTTP address (%s) - %s", opts.HTTPAddress, err)
			os.Exit(1)
		}
		statsdHostKey := statsd.HostKey(net.JoinHostPort(opts.BroadcastAddress, port))
		prefixWithHost := strings.Replace(opts.StatsdPrefix, "%s", statsdHostKey, -1)
		if prefixWithHost[len(prefixWithHost)-1] != '.' {
			prefixWithHost += "."
		}
		opts.StatsdPrefix = prefixWithHost
	}

	if opts.TLSClientAuthPolicy != "" && opts.TLSRequired == TLSNotRequired {
		opts.TLSRequired = TLSRequired
	}

	tlsConfig, err := buildTLSConfig(opts)
	if err != nil {
		n.logf("FATAL: failed to build TLS config - %s", err)
		os.Exit(1)
	}
	if tlsConfig == nil && opts.TLSRequired != TLSNotRequired {
		n.logf("FATAL: cannot require TLS client connections without TLS key and cert")
		os.Exit(1)
	}
	n.tlsConfig = tlsConfig

	n.logf(version.String("nsqd"))
	n.logf("ID: %d", opts.ID)

	return n
}

func (n *NSQD) logf(f string, args ...interface{}) {
	if n.getOpts().Logger == nil {
		return
	}
	n.getOpts().Logger.Output(2, fmt.Sprintf(f, args...))
}

// 获取参数对象
func (n *NSQD) getOpts() *Options {
	return n.opts.Load().(*Options)
}

// 存储参数对象
func (n *NSQD) swapOpts(opts *Options) {
	n.opts.Store(opts)
}

// 作为什么的通知呢?
func (n *NSQD) triggerOptsNotification() {
	select {
	case n.optsNotificationChan <- struct{}{}:
	default:
	}
}

// 获取TCP监听的IP和端口
func (n *NSQD) RealTCPAddr() *net.TCPAddr {
	n.RLock()
	defer n.RUnlock()
	return n.tcpListener.Addr().(*net.TCPAddr)
}

// 获取HTTP监听的IP和端口
func (n *NSQD) RealHTTPAddr() *net.TCPAddr {
	n.RLock()
	defer n.RUnlock()
	return n.httpListener.Addr().(*net.TCPAddr)
}

// 获取HTTPS监听的IP和端口
func (n *NSQD) RealHTTPSAddr() *net.TCPAddr {
	n.RLock()
	defer n.RUnlock()
	return n.httpsListener.Addr().(*net.TCPAddr)
}

// 存储最近的错误值
func (n *NSQD) SetHealth(err error) {
	n.errValue.Store(errStore{err: err})
}

// 判断实例是否健康
func (n *NSQD) IsHealthy() bool {
	return n.GetError() == nil
}

// 获取最近的错误值
func (n *NSQD) GetError() error {
	errValue := n.errValue.Load()
	return errValue.(errStore).err
}

func (n *NSQD) GetHealth() string {
	err := n.GetError()
	if err != nil {
		return fmt.Sprintf("NOK - %s", err)
	}
	return "OK"
}

// 获取实例启动的时间
func (n *NSQD) GetStartTime() time.Time {
	return n.startTime
}

// 主业务函数
func (n *NSQD) Main() {
	var httpListener net.Listener
	var httpsListener net.Listener

	// 上下文对象
	ctx := &context{n}

	// 创建tcp监听对象
	tcpListener, err := net.Listen("tcp", n.getOpts().TCPAddress)
	if err != nil {
		n.logf("FATAL: listen (%s) failed - %s", n.getOpts().TCPAddress, err)
		os.Exit(1)
	}
	n.Lock()
	n.tcpListener = tcpListener // 为什么要加锁？
	n.Unlock()

	// TCP服务器
	tcpServer := &tcpServer{ctx: ctx} // 实现了Handle函数
	n.waitGroup.Wrap(func() {
		protocol.TCPServer(n.tcpListener, tcpServer, n.getOpts().Logger)
	})

	// HTTP服务器 和 HTTPS服务器
	if n.tlsConfig != nil && n.getOpts().HTTPSAddress != "" {
		httpsListener, err = tls.Listen("tcp", n.getOpts().HTTPSAddress, n.tlsConfig)
		if err != nil {
			n.logf("FATAL: listen (%s) failed - %s", n.getOpts().HTTPSAddress, err)
			os.Exit(1)
		}
		n.Lock()
		n.httpsListener = httpsListener
		n.Unlock()
		httpsServer := newHTTPServer(ctx, true, true)
		n.waitGroup.Wrap(func() {
			http_api.Serve(n.httpsListener, httpsServer, "HTTPS", n.getOpts().Logger)
		})
	}
	httpListener, err = net.Listen("tcp", n.getOpts().HTTPAddress)
	if err != nil {
		n.logf("FATAL: listen (%s) failed - %s", n.getOpts().HTTPAddress, err)
		os.Exit(1)
	}
	n.Lock()
	n.httpListener = httpListener
	n.Unlock()
	httpServer := newHTTPServer(ctx, false, n.getOpts().TLSRequired == TLSRequired)
	n.waitGroup.Wrap(func() {
		http_api.Serve(n.httpListener, httpServer, "HTTP", n.getOpts().Logger)
	})

	// 启动四个goroutine处理业务
	n.waitGroup.Wrap(func() { n.queueScanLoop() }) // 用若干个worker来扫描并处理当前在投递中以及等待重新投递的消息。worker的个数由配置和当前Channel数量共同决定。
	n.waitGroup.Wrap(func() { n.idPump() })        // 不断产生全局唯一的MessageID
	n.waitGroup.Wrap(func() { n.lookupLoop() })    // loopup.go
	if n.getOpts().StatsdAddress != "" {
		n.waitGroup.Wrap(func() { n.statsdLoop() })
	}
}

// 载入原数据
func (n *NSQD) LoadMetadata() {

	// n.isLoading暗示数据是否在载入过程中
	atomic.StoreInt32(&n.isLoading, 1)       // 表明数据在载入过程中
	defer atomic.StoreInt32(&n.isLoading, 0) // 表明数据完成载入过程

	// 获取数据的完整路径
	fn := fmt.Sprintf(path.Join(n.getOpts().DataPath, "nsqd.%d.dat"), n.getOpts().ID)
	// 一次性读取全部的数据
	data, err := ioutil.ReadFile(fn)
	if err != nil {
		// 如果是文件不存在的错误就输出log进行提示
		if !os.IsNotExist(err) {
			n.logf("ERROR: failed to read channel metadata from %s - %s", fn, err)
		}
		return // 其他错误就直接退出（也不说明错误原因的...）
	}

	// 使用文件中的数据构建Json对象
	js, err := simplejson.NewJson(data)
	if err != nil {
		n.logf("ERROR: failed to parse metadata - %s", err)
		return
	}

	// 获取全部的topic
	topics, err := js.Get("topics").Array()
	if err != nil {
		n.logf("ERROR: failed to parse metadata - %s", err)
		return
	}

	for ti := range topics {
		topicJs := js.Get("topics").GetIndex(ti)

		topicName, err := topicJs.Get("name").String()
		if err != nil {
			n.logf("ERROR: failed to parse metadata - %s", err)
			return
		}
		if !protocol.IsValidTopicName(topicName) {
			n.logf("WARNING: skipping creation of invalid topic %s", topicName)
			continue
		}
		topic := n.GetTopic(topicName)

		paused, _ := topicJs.Get("paused").Bool()
		if paused {
			topic.Pause()
		}

		channels, err := topicJs.Get("channels").Array()
		if err != nil {
			n.logf("ERROR: failed to parse metadata - %s", err)
			return
		}

		for ci := range channels {
			channelJs := topicJs.Get("channels").GetIndex(ci)

			channelName, err := channelJs.Get("name").String()
			if err != nil {
				n.logf("ERROR: failed to parse metadata - %s", err)
				return
			}
			if !protocol.IsValidChannelName(channelName) {
				n.logf("WARNING: skipping creation of invalid channel %s", channelName)
				continue
			}
			channel := topic.GetChannel(channelName)

			paused, _ = channelJs.Get("paused").Bool()
			if paused {
				channel.Pause()
			}
		}
	}
}

// 持久化元数据
func (n *NSQD) PersistMetadata() error {
	// persist metadata about what topics/channels we have
	// so that upon restart we can get back to the same state
	fileName := fmt.Sprintf(path.Join(n.getOpts().DataPath, "nsqd.%d.dat"), n.getOpts().ID)
	n.logf("NSQ: persisting topic/channel metadata to %s", fileName)

	js := make(map[string]interface{})
	topics := []interface{}{}
	// 遍历map中的全部元素
	for _, topic := range n.topicMap {
		if topic.ephemeral {
			continue
		}
		topicData := make(map[string]interface{})
		topicData["name"] = topic.name
		topicData["paused"] = topic.IsPaused()
		channels := []interface{}{}
		topic.Lock()
		for _, channel := range topic.channelMap {
			channel.Lock()
			if channel.ephemeral {
				channel.Unlock()
				continue
			}
			channelData := make(map[string]interface{})
			channelData["name"] = channel.name
			channelData["paused"] = channel.IsPaused()
			channels = append(channels, channelData)
			channel.Unlock()
		}
		topic.Unlock()
		topicData["channels"] = channels
		topics = append(topics, topicData)
	}
	js["version"] = version.Binary
	js["topics"] = topics

	data, err := json.Marshal(&js)
	if err != nil {
		return err
	}

	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())
	f, err := os.OpenFile(tmpFileName, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}

	_, err = f.Write(data)
	if err != nil {
		f.Close()
		return err
	}
	f.Sync()
	f.Close()

	err = atomicRename(tmpFileName, fileName)
	if err != nil {
		return err
	}

	return nil
}

// 实例退出处理
func (n *NSQD) Exit() {
	if n.tcpListener != nil {
		n.tcpListener.Close()
	}

	if n.httpListener != nil {
		n.httpListener.Close()
	}

	if n.httpsListener != nil {
		n.httpsListener.Close()
	}

	n.Lock()
	err := n.PersistMetadata()
	if err != nil {
		n.logf("ERROR: failed to persist metadata - %s", err)
	}
	n.logf("NSQ: closing topics")
	for _, topic := range n.topicMap {
		topic.Close() // 关闭全部的topic
	}
	n.Unlock()

	// we want to do this last as it closes the idPump (if closed first it
	// could potentially starve items in process and deadlock)
	// 通知四个goroutine退出
	close(n.exitChan)
	// 等待wg对象下面的四个goroutine退出
	n.waitGroup.Wait()

	n.dl.Unlock() // 解锁数据路径
}

// GetTopic performs a thread safe operation
// to return a pointer to a Topic object (potentially new)
// 获取指定的topic的实例，如果不存在就创建一个
func (n *NSQD) GetTopic(topicName string) *Topic {
	// most likely, we already have this topic, so try read lock first.
	n.RLock()
	t, ok := n.topicMap[topicName]
	n.RUnlock()
	if ok {
		return t
	}

	n.Lock()

	t, ok = n.topicMap[topicName]
	if ok {
		n.Unlock()
		return t
	}
	deleteCallback := func(t *Topic) {
		n.DeleteExistingTopic(t.name)
	}
	t = NewTopic(topicName, &context{n}, deleteCallback)
	n.topicMap[topicName] = t

	n.logf("TOPIC(%s): created", t.name)

	// release our global nsqd lock, and switch to a more granular topic lock while we init our
	// channels from lookupd. This blocks concurrent PutMessages to this topic.
	t.Lock()
	n.Unlock()

	// if using lookupd, make a blocking call to get the topics, and immediately create them.
	// this makes sure that any message received is buffered to the right channels
	lookupdHTTPAddrs := n.lookupdHTTPAddrs()
	if len(lookupdHTTPAddrs) > 0 {
		channelNames, _ := n.ci.GetLookupdTopicChannels(t.name, lookupdHTTPAddrs)
		for _, channelName := range channelNames {
			if strings.HasSuffix(channelName, "#ephemeral") {
				// we don't want to pre-create ephemeral channels
				// because there isn't a client connected
				continue
			}
			t.getOrCreateChannel(channelName)
		}
	}

	t.Unlock()

	// NOTE: I would prefer for this to only happen in topic.GetChannel() but we're special
	// casing the code above so that we can control the locks such that it is impossible
	// for a message to be written to a (new) topic while we're looking up channels
	// from lookupd...
	//
	// update messagePump state
	select {
	case t.channelUpdateChan <- 1:
	case <-t.exitChan:
	}
	return t
}

// GetExistingTopic gets a topic only if it exists
// 通过名字获取Topic对象
func (n *NSQD) GetExistingTopic(topicName string) (*Topic, error) {
	n.RLock()
	defer n.RUnlock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		return nil, errors.New("topic does not exist")
	}
	return topic, nil
}

// DeleteExistingTopic removes a topic only if it exists
// 通过名字删除Topic对象
func (n *NSQD) DeleteExistingTopic(topicName string) error {
	n.RLock()
	topic, ok := n.topicMap[topicName]
	if !ok {
		n.RUnlock()
		return errors.New("topic does not exist")
	}
	n.RUnlock()

	// delete empties all channels and the topic itself before closing
	// (so that we dont leave any messages around)
	//
	// we do this before removing the topic from map below (with no lock)
	// so that any incoming writes will error and not create a new topic
	// to enforce ordering
	topic.Delete()

	n.Lock()
	delete(n.topicMap, topicName)
	n.Unlock()

	return nil
}

// 参考资料《全局唯一MessageID》
// http://www.zhaoxiaodan.com/%E6%BA%90%E7%A0%81%E9%98%85%E8%AF%BB/nsq%E6%BA%90%E7%A0%81%E9%98%85%E8%AF%BB(6)-%E5%85%A8%E5%B1%80%E5%94%AF%E4%B8%80MessageID.html
// 不断产生全局唯一的MessageID
func (n *NSQD) idPump() {
	factory := &guidFactory{}
	lastError := time.Unix(0, 0)
	workerID := n.getOpts().ID
	for {
		// 生成GUID，具体算法后面分析（因为可以做成 nsqd 集群, 所以 msgid 关联 nsq id）
		id, err := factory.NewGUID(workerID)
		if err != nil {
			now := time.Now()
			if now.Sub(lastError) > time.Second {
				// only print the error once/second
				//	log 每一秒才输出一次

				// 生成的太快了, 一个ts 中 生成了 4096个 id(sequence)
				// ts + sequence 满了, 休息一会, 让ts 发生改变
				n.logf("ERROR: %s", err)
				lastError = now
			}
			runtime.Gosched()
			continue
		}

		// 这里利用了 golang 的chan 满了阻塞的特性,如果n.idChan 这个id缓存池满了, 就阻塞不会继续生产
		select {
		// 传递id(MessageID)值给idChan，用于创建MessageD对象 msg := NewMessage(<-s.ctx.nsqd.idChan, body)
		case n.idChan <- id.Hex():
		case <-n.exitChan:
			goto exit
		}
	}

exit:
	n.logf("ID: closing")
}

func (n *NSQD) Notify(v interface{}) {
	// since the in-memory metadata is incomplete,
	// should not persist metadata while loading it.
	// nsqd will call `PersistMetadata` it after loading
	persist := atomic.LoadInt32(&n.isLoading) == 0
	n.waitGroup.Wrap(func() {
		// by selecting on exitChan we guarantee that
		// we do not block exit, see issue #123
		select {
		case <-n.exitChan:
		case n.notifyChan <- v: // 写入notifyChan，然后等待处理（lookupLoop中会处理n.notifyChan）
			if !persist {
				return
			}
			n.Lock()
			err := n.PersistMetadata() // 持久化元数据
			if err != nil {
				n.logf("ERROR: failed to persist metadata - %s", err)
			}
			n.Unlock()
		}
	})
}

// channels returns a flat slice of all channels in all topics
// 返回全部的Channel对象
func (n *NSQD) channels() []*Channel {
	var channels []*Channel
	n.RLock()
	for _, t := range n.topicMap {
		t.RLock()
		for _, c := range t.channelMap {
			channels = append(channels, c)
		}
		t.RUnlock()
	}
	n.RUnlock()
	return channels
}

// resizePool adjusts the size of the pool of queueScanWorker goroutines
//
// 	1 <= pool <= min(num * 0.25, QueueScanWorkerPoolMax)
//
// 调整queueScanWorker的goroutines数量
func (n *NSQD) resizePool(num int, workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	idealPoolSize := int(float64(num) * 0.25) // 理想的大小（num为当前topic中Channel的数量）
	if idealPoolSize < 1 {
		idealPoolSize = 1
	} else if idealPoolSize > n.getOpts().QueueScanWorkerPoolMax {
		// 超过最大值就根据最大值进行调整
		idealPoolSize = n.getOpts().QueueScanWorkerPoolMax
	}
	for {
		if idealPoolSize == n.poolSize {
			// 如果重新分配的poolSize没有改变我们就什么都不做
			break
		} else if idealPoolSize < n.poolSize {
			// contract
			// 如果现在需要的idealPoolSize比实际存在的小
			closeCh <- 1 // queueScanWorker中处理，返回即减少一个queueScanWorker
			n.poolSize--
		} else {
			// expand
			n.waitGroup.Wrap(func() {
				n.queueScanWorker(workCh, responseCh, closeCh) // 创建一个新的queueScanWorker
			})
			n.poolSize++
		}
	}
}

// queueScanWorker receives work (in the form of a channel) from queueScanLoop
// and processes the deferred and in-flight queues
// 处理workCh并返回结果给responseCh，表明消息是否已经被投递
func (n *NSQD) queueScanWorker(workCh chan *Channel, responseCh chan bool, closeCh chan int) {
	for {
		select {
		case c := <-workCh:
			now := time.Now().UnixNano()
			dirty := false
			if c.processInFlightQueue(now) {
				dirty = true
			}
			if c.processDeferredQueue(now) {
				dirty = true
			}
			responseCh <- dirty
		case <-closeCh:
			// 退出当前的goroutine，即减少一个queueScanWorker
			return
		}
	}
}

// queueScanLoop runs in a single goroutine to process in-flight and deferred
// priority queues. It manages a pool of queueScanWorker (configurable max of
// QueueScanWorkerPoolMax (default: 4)) that process channels concurrently.
//
// It copies Redis's probabilistic expiration algorithm: it wakes up every
// QueueScanInterval (default: 100ms) to select a random QueueScanSelectionCount
// (default: 20) channels from a locally cached list (refreshed every
// QueueScanRefreshInterval (default: 5s)).
//
// If either of the queues had work to do the channel is considered "dirty".
//
// If QueueScanDirtyPercent (default: 25%) of the selected channels were dirty,
// the loop continues without sleep.
// queueScanLoop 扫描和处理InFlightQueue和DeferredQueue；
// 用若干个worker来扫描并处理当前在投递中以及等待重新投递的消息。worker的个数由配置和当前Channel数量共同决定。
func (n *NSQD) queueScanLoop() {
	// 控制worker的输入
	workCh := make(chan *Channel, n.getOpts().QueueScanSelectionCount)
	// 控制worker的输出
	responseCh := make(chan bool, n.getOpts().QueueScanSelectionCount)
	// 控制worker的关闭
	closeCh := make(chan int)

	// workTicker定时器触发扫描流程。
	workTicker := time.NewTicker(n.getOpts().QueueScanInterval)
	// refreshTicker定时器触发更新Channel列表流程。
	refreshTicker := time.NewTicker(n.getOpts().QueueScanRefreshInterval)

	// 获取当前的Channel集合，并且调用resizePool函数来启动指定数量的worker。
	channels := n.channels()
	n.resizePool(len(channels), workCh, responseCh, closeCh)

	// 在循环中，等待两个定时器，workTicker和refreshTicker，定时时间分别由由配置中的QueueScanInterval和QueueScanRefreshInterval决定。
	for {
		select {
		case <-workTicker.C:
			if len(channels) == 0 {
				continue
			}
		case <-refreshTicker.C:
			/*
				refreshTicker定时器触发更新Channel列表流程。
					这个流程比较简单，先获取一次Channel列表，再调用resizePool重新分配worker。
			*/
			channels = n.channels()
			n.resizePool(len(channels), workCh, responseCh, closeCh)
			continue
		case <-n.exitChan:
			goto exit
		}

		num := n.getOpts().QueueScanSelectionCount
		if num > len(channels) {
			num = len(channels)
		}

	loop:

		/*
			workTicker定时器触发扫描流程。
				1: nsqd采用了Redis的probabilistic expiration算法来进行扫描。
				2: 首先从所有Channel中随机选取部分Channel，然后遍历被选取的Channel，投到workerChan中，并且等待反馈结果，结果有两种，dirty和非dirty，
					如果dirty的比例超过配置中设定的QueueScanDirtyPercent，那么不进入休眠，继续扫描，如果比例较低，则重新等待定时器触发下一轮扫描。
				3: 这种机制可以在保证处理延时较低的情况下减少对CPU资源的浪费。
		*/
		for _, i := range util.UniqRands(num, len(channels)) {
			workCh <- channels[i]
		}

		numDirty := 0
		for i := 0; i < num; i++ {
			if <-responseCh {
				numDirty++
			}
		}

		if float64(numDirty)/float64(num) > n.getOpts().QueueScanDirtyPercent {
			goto loop // 如果消息投递比例高于设定值，就继续扫描
		}
	} // end of for

exit:
	n.logf("QUEUESCAN: closing")
	close(closeCh)
	workTicker.Stop()
	refreshTicker.Stop()
}

func buildTLSConfig(opts *Options) (*tls.Config, error) {
	var tlsConfig *tls.Config

	if opts.TLSCert == "" && opts.TLSKey == "" {
		return nil, nil
	}

	tlsClientAuthPolicy := tls.VerifyClientCertIfGiven

	cert, err := tls.LoadX509KeyPair(opts.TLSCert, opts.TLSKey)
	if err != nil {
		return nil, err
	}
	switch opts.TLSClientAuthPolicy {
	case "require":
		tlsClientAuthPolicy = tls.RequireAnyClientCert
	case "require-verify":
		tlsClientAuthPolicy = tls.RequireAndVerifyClientCert
	default:
		tlsClientAuthPolicy = tls.NoClientCert
	}

	tlsConfig = &tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tlsClientAuthPolicy,
		MinVersion:   opts.TLSMinVersion,
		MaxVersion:   tls.VersionTLS12, // enable TLS_FALLBACK_SCSV prior to Go 1.5: https://go-review.googlesource.com/#/c/1776/
	}

	if opts.TLSRootCAFile != "" {
		tlsCertPool := x509.NewCertPool()
		caCertFile, err := ioutil.ReadFile(opts.TLSRootCAFile)
		if err != nil {
			return nil, err
		}
		if !tlsCertPool.AppendCertsFromPEM(caCertFile) {
			return nil, errors.New("failed to append certificate to pool")
		}
		tlsConfig.ClientCAs = tlsCertPool
	}

	tlsConfig.BuildNameToCertificate()

	return tlsConfig, nil
}

func (n *NSQD) IsAuthEnabled() bool {
	return len(n.getOpts().AuthHTTPAddresses) != 0
}
