package nsqd

import (
	"bytes"
	"errors"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/feixiao/nsq-0.3.7/internal/quantile"
	"github.com/feixiao/nsq-0.3.7/internal/util"
)

type Topic struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	messageCount uint64

	sync.RWMutex
	// topic的名字
	name string
	// 一个Topic实例下有多个Channel
	channelMap map[string]*Channel
	// 队列
	backend BackendQueue

	// 如果想要往该topic发布消息，只需要将消息写到Topic.memoryMsgChan中
	// 创建Topic时会开启一个新的goroutine(messagePump)负责监听Topic.memoryMsgChan，当有新消息时会将将消息复制N份发送到该Topic下的所有Channel中
	memoryMsgChan chan *Message

	// 通知相关的goroutine退出
	exitChan chan int

	channelUpdateChan chan int // 通知channelMap的更新，在messagePump中处理
	// 等待全部的goroutine退出
	waitGroup util.WaitGroupWrapper

	// 标记是否退出
	exitFlag int32

	ephemeral bool // 临时(代码的意思是如果设置为ephemeral的topic在不存在channel的情况下面要销毁 )

	// 删除topic的回调
	deleteCallback func(*Topic)
	deleter        sync.Once

	// for what ???
	paused    int32
	pauseChan chan bool

	ctx *context
}

// Topic constructor
// 创建Topic对象（主题名字、上下文对象(NSQD)、删除主题回调对象）
func NewTopic(topicName string, ctx *context, deleteCallback func(*Topic)) *Topic {
	t := &Topic{
		name: topicName,
		// 该Topic下的所有Channel
		channelMap: make(map[string]*Channel),
		// 消息到达的数量，可以通过MemQueueSize配置大小
		memoryMsgChan:     make(chan *Message, ctx.nsqd.getOpts().MemQueueSize),
		exitChan:          make(chan int),
		channelUpdateChan: make(chan int),
		ctx:               ctx,
		pauseChan:         make(chan bool),
		deleteCallback:    deleteCallback,
	}

	/*
		Note, a topic/channel whose name ends in the string #ephemeral will not be buffered to disk and will instead drop messages after passing the mem-queue-size.
		This enables consumers which do not need message guarantees to subscribe to a channel.These ephemeral channels will also disappear after its last client disconnects.
	*/
	if strings.HasSuffix(topicName, "#ephemeral") {
		// http://nsq.io/overview/design.html
		// 名字含有“#ephemeral”结尾的主题，数据不想要保存到磁盘，
		t.ephemeral = true
		t.backend = newDummyBackendQueue()
	} else {
		// 名字不含有“#ephemeral”结尾，我们需要持久化到磁盘
		t.backend = newDiskQueue(topicName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength, // 大小为什么是这样的需要分析message结构
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			ctx.nsqd.getOpts().Logger)
	}

	// 开启一个goroutine负责监听写到该Topic的消息
	t.waitGroup.Wrap(func() { t.messagePump() })

	t.ctx.nsqd.Notify(t)

	return t
}

// Exiting returns a boolean indicating if this topic is closed/exiting
// 将exitFlag标记为1,说明需要正在退出
func (t *Topic) Exiting() bool {
	return atomic.LoadInt32(&t.exitFlag) == 1
}

// GetChannel performs a thread safe operation
// to return a pointer to a Channel object (potentially new)
// for the given Topic
func (t *Topic) GetChannel(channelName string) *Channel {
	t.Lock()
	// 获取channel,如果不存在就创建一个新的
	channel, isNew := t.getOrCreateChannel(channelName)
	t.Unlock()

	if isNew {
		// update messagePump state
		// 如果是新，那么更新channel
		select {
		case t.channelUpdateChan <- 1: // 处理channelMap的更新
		case <-t.exitChan:
		}
	}

	return channel
}

// this expects the caller to handle locking
func (t *Topic) getOrCreateChannel(channelName string) (*Channel, bool) {
	channel, ok := t.channelMap[channelName]
	if !ok {
		// 找不到就创建新的channel
		deleteCallback := func(c *Channel) {
			t.DeleteExistingChannel(c.name)
		}
		channel = NewChannel(t.name, channelName, t.ctx, deleteCallback)
		// 更新channelMap
		t.channelMap[channelName] = channel
		t.ctx.nsqd.logf("TOPIC(%s): new channel(%s)", t.name, channel.name)
		return channel, true
	}
	return channel, false
}

// 获取名为channelName的Channel对象
func (t *Topic) GetExistingChannel(channelName string) (*Channel, error) {
	t.RLock()
	defer t.RUnlock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		return nil, errors.New("channel does not exist")
	}
	return channel, nil
}

// DeleteExistingChannel removes a channel from the topic only if it exists
// 删除名为channelName的Channel
func (t *Topic) DeleteExistingChannel(channelName string) error {
	t.Lock()
	channel, ok := t.channelMap[channelName]
	if !ok {
		// 不存在直接返回
		t.Unlock()
		return errors.New("channel does not exist")
	}
	// 从map中删除
	delete(t.channelMap, channelName)
	// not defered so that we can continue while the channel async closes
	numChannels := len(t.channelMap)
	t.Unlock()

	t.ctx.nsqd.logf("TOPIC(%s): deleting channel %s", t.name, channel.name)

	// delete empties the channel before closing
	// (so that we dont leave any messages around)
	// 删除Channle，具体怎么删除下面分析
	channel.Delete()

	// update messagePump state
	select {
	case t.channelUpdateChan <- 1: // 通知channelMap已经被更新
	case <-t.exitChan:
	}

	// 如果这个topic不存在channel了同时是ephemeral的情况下，需要删除这个topic
	if numChannels == 0 && t.ephemeral == true {
		go t.deleter.Do(func() { t.deleteCallback(t) })
	}

	return nil
}

// PutMessage writes a Message to the queue
// 发送一条消息到队列
func (t *Topic) PutMessage(m *Message) error {
	t.RLock()
	defer t.RUnlock()
	// 退出状态中就直接退出
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}

	// 发送消息
	err := t.put(m)
	if err != nil {
		return err
	}
	// 累计发送的消息
	atomic.AddUint64(&t.messageCount, 1)
	return nil
}

// PutMessages writes multiple Messages to the queue
// 发送多条消息
func (t *Topic) PutMessages(msgs []*Message) error {
	t.RLock()
	defer t.RUnlock()
	if atomic.LoadInt32(&t.exitFlag) == 1 {
		return errors.New("exiting")
	}
	for _, m := range msgs {
		err := t.put(m)
		if err != nil {
			return err
		}
	}
	atomic.AddUint64(&t.messageCount, uint64(len(msgs)))
	return nil
}

// 将消息写到topic的channel中，如果topic的memoryMsgChan已满则将topic写到磁盘文件中
func (t *Topic) put(m *Message) error {
	select {
	case t.memoryMsgChan <- m: // 发送消息
	default:
		// 如果memoryMsgChan已经满了

		// buffer池是为了避免重复的创建销毁buffer对象
		b := bufferPoolGet()
		// 写入BackendQueue中
		err := writeMessageToBackend(b, m, t.backend)
		// 返回给bufferBool
		bufferPoolPut(b)
		t.ctx.nsqd.SetHealth(err) // 如果出错就更新错误值
		if err != nil {
			t.ctx.nsqd.logf(
				"TOPIC(%s) ERROR: failed to write message to backend - %s",
				t.name, err)
			return err
		}
	}
	return nil
}

// 深度： 内存中的消息个数 + （BackendQueue）队列中的数据
func (t *Topic) Depth() int64 {
	return int64(len(t.memoryMsgChan)) + t.backend.Depth()
}

// messagePump selects over the in-memory and backend queue and
// writes messages to every channel for this topic
// topic的主业务处理函数
func (t *Topic) messagePump() {
	var msg *Message
	var buf []byte
	var err error
	var chans []*Channel
	var memoryMsgChan chan *Message
	var backendChan chan []byte

	t.RLock()
	// 取出该Topic下所有的Channel
	for _, c := range t.channelMap {
		chans = append(chans, c) // 保存topic下面的全部channel，下面用于分发数据
	}
	t.RUnlock()

	// 如果存在channel，我们就需要分发数据，数据来源memoryMsgChan和backend
	if len(chans) > 0 {
		memoryMsgChan = t.memoryMsgChan
		backendChan = t.backend.ReadChan()
	}

	// 从memoryMsgChan及DiskQueue.ReadChan中取消息
	for {
		//从memoryMsgChan中取出一个消息，并将消息复制N份，发送到N个Channel中
		select {
		// msg来自两个地方
		case msg = <-memoryMsgChan: // 什么都不做，只是把消息分发出去
		case buf = <-backendChan:
			msg, err = decodeMessage(buf)
			if err != nil {
				t.ctx.nsqd.logf("ERROR: failed to decode message - %s", err)
				continue
			}
		case <-t.channelUpdateChan:
			// 处理channelMap的更新
			chans = chans[:0]
			t.RLock()
			for _, c := range t.channelMap {
				chans = append(chans, c) // 因为channelMap已经被更新，我们需要更新chans
			}
			t.RUnlock()
			if len(chans) == 0 || t.IsPaused() {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case pause := <-t.pauseChan:
			// 暂停接收信息
			if pause || len(chans) == 0 {
				memoryMsgChan = nil
				backendChan = nil
			} else {
				// 重新开始接收消息
				memoryMsgChan = t.memoryMsgChan
				backendChan = t.backend.ReadChan()
			}
			continue
		case <-t.exitChan:
			goto exit
		}

		// 将msg复制N份，发送到topic下的N个Channel中
		for i, channel := range chans {
			chanMsg := msg
			// copy the message because each channel
			// needs a unique instance but...
			// fastpath to avoid copy if its the first channel
			// (the topic already created the first copy)
			if i > 0 {
				chanMsg = NewMessage(msg.ID, msg.Body)
				chanMsg.Timestamp = msg.Timestamp
				chanMsg.deferred = msg.deferred
			}
			if chanMsg.deferred != 0 {
				// 延迟发送的消息
				channel.StartDeferredTimeout(chanMsg, chanMsg.deferred)
				continue
			}
			// 立即发送的消息
			err := channel.PutMessage(chanMsg)
			if err != nil {
				t.ctx.nsqd.logf(
					"TOPIC(%s) ERROR: failed to put msg(%s) to channel(%s) - %s",
					t.name, msg.ID, channel.name, err)
			}
		}
	}

exit:
	t.ctx.nsqd.logf("TOPIC(%s): closing ... messagePump", t.name)
}

// Delete empties the topic and all its channels and closes
// 删除Topic
func (t *Topic) Delete() error {
	return t.exit(true)
}

// Close persists all outstanding topic data and closes all its channels
// 关闭Topic
func (t *Topic) Close() error {
	return t.exit(false)
}

func (t *Topic) exit(deleted bool) error {
	// 如果exitFlag=0,那么设置为1,否则说明程序正在退出
	if !atomic.CompareAndSwapInt32(&t.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	// 参数只影响
	if deleted {
		t.ctx.nsqd.logf("TOPIC(%s): deleting", t.name)

		// since we are explicitly deleting a topic (not just at system exit time)
		// de-register this from the lookupd

		t.ctx.nsqd.Notify(t) // 通知NSQD，这个topic被删除了，更新元数据
	} else {
		t.ctx.nsqd.logf("TOPIC(%s): closing", t.name)
	}

	close(t.exitChan) // 通知相关goroutine退出

	// synchronize the close of messagePump()
	// 等待goroutine退出
	t.waitGroup.Wait()

	// 如果是删除操作，删除channelMap，清理数据
	if deleted {
		t.Lock()
		for _, channel := range t.channelMap {
			delete(t.channelMap, channel.name)
			channel.Delete()
		}
		t.Unlock()

		// empty the queue (deletes the backend files, too)
		t.Empty()
		return t.backend.Delete()
	}

	// close all the channels
	// 如果是关闭操作，我们不清理数据，只关闭channel
	for _, channel := range t.channelMap {
		err := channel.Close()
		if err != nil {
			// we need to continue regardless of error to close all the channels
			t.ctx.nsqd.logf("ERROR: channel(%s) close - %s", channel.name, err)
		}
	}

	// write anything leftover to disk
	t.flush()
	return t.backend.Close()
}

// 清理内存和磁盘中的数据
func (t *Topic) Empty() error {
	for {
		select {
		case <-t.memoryMsgChan: // 情况通道里面的数据
		default:
			goto finish
		}
	}

finish:
	return t.backend.Empty() // 清理BackendQueue上面的数据
}

func (t *Topic) flush() error {
	var msgBuf bytes.Buffer

	if len(t.memoryMsgChan) > 0 {
		t.ctx.nsqd.logf(
			"TOPIC(%s): flushing %d memory messages to backend",
			t.name, len(t.memoryMsgChan))
	}

	for {
		select {
		// 将内存中的数据写入backend（根据backend的类型判断写入目的地）
		case msg := <-t.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, t.backend)
			if err != nil {
				t.ctx.nsqd.logf(
					"ERROR: failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	return nil
}

func (t *Topic) AggregateChannelE2eProcessingLatency() *quantile.Quantile {
	var latencyStream *quantile.Quantile
	t.RLock()
	realChannels := make([]*Channel, 0, len(t.channelMap))
	for _, c := range t.channelMap {
		realChannels = append(realChannels, c)
	}
	t.RUnlock()
	for _, c := range realChannels {
		if c.e2eProcessingLatencyStream == nil {
			continue
		}
		if latencyStream == nil {
			latencyStream = quantile.New(
				t.ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
				t.ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles)
		}
		latencyStream.Merge(c.e2eProcessingLatencyStream)
	}
	return latencyStream
}

// 暂停操作
func (t *Topic) Pause() error {
	return t.doPause(true)
}

// 重启操作
func (t *Topic) UnPause() error {
	return t.doPause(false)
}

// 请求暂停/重启操作
func (t *Topic) doPause(pause bool) error {
	// 改变状态
	if pause {
		atomic.StoreInt32(&t.paused, 1)
	} else {
		atomic.StoreInt32(&t.paused, 0)
	}

	select {
	case t.pauseChan <- pause: // 请求暂停/重启操作，messagePump中处理
	case <-t.exitChan:
	}

	return nil
}

// 判断是否为暂停状态
func (t *Topic) IsPaused() bool {
	return atomic.LoadInt32(&t.paused) == 1
}
