package nsqd

import (
	"bytes"
	"container/heap"
	"errors"
	"math"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feixiao/nsq-0.3.7/internal/pqueue"
	"github.com/feixiao/nsq-0.3.7/internal/quantile"
)

// Consumer接口，定义消费者行为
type Consumer interface {
	UnPause()
	Pause()
	Close() error
	TimedOutMessage()
	Stats() ClientStats
	Empty()
}

// Channel represents the concrete type for a NSQ channel (and also
// implements the Queue interface)
//
// There can be multiple channels per topic, each with there own unique set
// of subscribers (clients).
//
// Channels maintain all client and message metadata, orchestrating in-flight
// messages, timeouts, requeuing, etc.
type Channel struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	requeueCount uint64
	messageCount uint64
	timeoutCount uint64

	sync.RWMutex

	topicName string   // 所属topic的名字
	name      string   // channel的名字
	ctx       *context // 上下文对象（NSQD）
	// 数据持久化
	backend BackendQueue // channel自己的数据队列
	// memoryMsgChan负责接收写到该Channel的所有消息
	// 创建创建Channel时会开启一个新的goroutine(messagePump)负责监听memoryMsgChan，当有消息时会将该消息写到clientMsgChan中，
	// 订阅该channel的consumer都会试图从clientMsgChan中取消息，一条消息只能被一个consumer抢到  ???
	memoryMsgChan chan *Message
	// 对外暴露，获取数据（protocol_v2.go中的PumpMessage）
	clientMsgChan chan *Message

	exitChan  chan int // 通知goroutine退出
	exitFlag  int32    // 标记是否正在退出
	exitMutex sync.RWMutex

	// state tracking
	clients   map[int64]Consumer // 维护该channel下面的Consumer
	paused    int32
	ephemeral bool // 暂时的，设置为true的时候当memoryMsgChan写满的时候我们直接丢弃数据

	deleteCallback func(*Channel) // 删除Channel的回调函数
	deleter        sync.Once

	// Stats tracking
	e2eProcessingLatencyStream *quantile.Quantile // 主要用于统计消息投递的延迟等

	// TODO: these can be DRYd up
	deferredMessages map[MessageID]*pqueue.Item
	deferredPQ       pqueue.PriorityQueue // 投递失败，等待重新投递的消息
	deferredMutex    sync.Mutex
	inFlightMessages map[MessageID]*Message // 管理发送出去但是对端没有确认收到的消息
	inFlightPQ       inFlightPqueue         // 正在投递但还没确认投递成功的消息 type inFlightPqueue []*Message
	inFlightMutex    sync.Mutex

	// stat counters
	bufferedCount int32 // ???
}

// NewChannel creates a new instance of the Channel type and returns a pointer
// 创建一个新的Channel
func NewChannel(topicName string, channelName string, ctx *context,
	deleteCallback func(*Channel)) *Channel {

	c := &Channel{
		topicName:      topicName,
		name:           channelName,
		memoryMsgChan:  make(chan *Message, ctx.nsqd.getOpts().MemQueueSize),
		clientMsgChan:  make(chan *Message),
		exitChan:       make(chan int),
		clients:        make(map[int64]Consumer),
		deleteCallback: deleteCallback,
		ctx:            ctx,
	}
	if len(ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles) > 0 {
		c.e2eProcessingLatencyStream = quantile.New(
			ctx.nsqd.getOpts().E2EProcessingLatencyWindowTime,
			ctx.nsqd.getOpts().E2EProcessingLatencyPercentiles,
		)
	}

	c.initPQ() // 初始化消息管理队列包括inFlightPQ和deferredPQ

	if strings.HasSuffix(channelName, "#ephemeral") {
		c.ephemeral = true
		c.backend = newDummyBackendQueue() // 写入该队列的数据都是丢弃的
	} else {
		// backend names, for uniqueness, automatically include the topic...
		// 获取backend的名字,格式为<topic>:<channel>
		backendName := getBackendName(topicName, channelName)
		// 创建DiskQueue对象，进行数据的持久化工作
		c.backend = newDiskQueue(backendName,
			ctx.nsqd.getOpts().DataPath,
			ctx.nsqd.getOpts().MaxBytesPerFile,
			int32(minValidMsgLength),
			int32(ctx.nsqd.getOpts().MaxMsgSize)+minValidMsgLength, //  minValidMsgLength为消息头大小
			ctx.nsqd.getOpts().SyncEvery,
			ctx.nsqd.getOpts().SyncTimeout,
			ctx.nsqd.getOpts().Logger)
	}

	go c.messagePump() // Channel的主业务处理函数

	c.ctx.nsqd.Notify(c) // 并没有持久化自己啊？？

	return c
}

// 初始化消息管理队列包括inFlightPQ和deferredPQ
func (c *Channel) initPQ() {
	pqSize := int(math.Max(1, float64(c.ctx.nsqd.getOpts().MemQueueSize)/10))

	c.inFlightMessages = make(map[MessageID]*Message)
	c.deferredMessages = make(map[MessageID]*pqueue.Item)

	c.inFlightMutex.Lock()
	c.inFlightPQ = newInFlightPqueue(pqSize) // nsq/nsqd/in_flight_pqueue.go是nsq实现的一个优先级队列，提供了常用的队列操作
	c.inFlightMutex.Unlock()

	c.deferredMutex.Lock()
	c.deferredPQ = pqueue.New(pqSize) // nsq/internal/pqueue/pqueue.go 也是一个优先队列，稍后比较差异
	c.deferredMutex.Unlock()
}

// Exiting returns a boolean indicating if this channel is closed/exiting
// 标记Channel正在退出
func (c *Channel) Exiting() bool {
	return atomic.LoadInt32(&c.exitFlag) == 1
}

// Delete empties the channel and closes
// 删除Channel
func (c *Channel) Delete() error {
	return c.exit(true)
}

// Close cleanly closes the Channel
// 关闭Channel
func (c *Channel) Close() error {
	return c.exit(false)
}

// 删除和关闭Channel的具体实现
func (c *Channel) exit(deleted bool) error {
	c.exitMutex.Lock()
	defer c.exitMutex.Unlock()

	// 如果exitFlag为0,那么exitFlag设在为1
	if !atomic.CompareAndSwapInt32(&c.exitFlag, 0, 1) {
		return errors.New("exiting")
	}

	if deleted {
		c.ctx.nsqd.logf("CHANNEL(%s): deleting", c.name)

		// since we are explicitly deleting a channel (not just at system exit time)
		// de-register this from the lookupd
		// 删除Channel需要通知NSQD
		c.ctx.nsqd.Notify(c) // 修改持久化元数据
	} else {
		c.ctx.nsqd.logf("CHANNEL(%s): closing", c.name)
	}

	// this forceably closes client connections
	c.RLock()
	// 关闭下面的全部客户端
	for _, client := range c.clients {
		client.Close()
	}
	c.RUnlock()

	close(c.exitChan) // 通知goroutine退出

	if deleted {
		// empty the queue (deletes the backend files, too)
		// 如果是删除操作，那么清空Channel中的数据和backend中的数据
		c.Empty()
		return c.backend.Delete()
	}

	// write anything leftover to disk
	c.flush()
	return c.backend.Close()
}

// 清空Channel
func (c *Channel) Empty() error {
	c.Lock()
	defer c.Unlock()

	c.initPQ() // 这个是重新生成新的队列

	for _, client := range c.clients {
		client.Empty()
	}

	clientMsgChan := c.clientMsgChan
	for {
		select {
		case _, ok := <-clientMsgChan:
			// 清理完clientMsgChan中的数据
			if !ok {
				// c.clientMsgChan may be closed while in this loop
				// so just remove it from the select so we can make progress
				clientMsgChan = nil // 清理完之后设置为nil，这样就不能写入了
			}
		case <-c.memoryMsgChan: // 清理完memoryMsgChan中的数据
		default:
			goto finish 	// 上面两个通道全部被清理完之后就退出
		}
	}

finish:
	return c.backend.Empty()
}

// flush persists all the messages in internal memory buffers to the backend
// it does not drain inflight/deferred because it is only called in Close
// 将全部内存中的信息写入backend
func (c *Channel) flush() error {
	var msgBuf bytes.Buffer

	// messagePump is responsible for closing the channel it writes to
	// this will read until it's closed (exited)
	for msg := range c.clientMsgChan {
		c.ctx.nsqd.logf("CHANNEL(%s): recovered buffered message from clientMsgChan", c.name)
		writeMessageToBackend(&msgBuf, msg, c.backend)
	}

	if len(c.memoryMsgChan) > 0 || len(c.inFlightMessages) > 0 || len(c.deferredMessages) > 0 {
		c.ctx.nsqd.logf("CHANNEL(%s): flushing %d memory %d in-flight %d deferred messages to backend",
			c.name, len(c.memoryMsgChan), len(c.inFlightMessages), len(c.deferredMessages))
	}

	for {
		select {
		case msg := <-c.memoryMsgChan:
			err := writeMessageToBackend(&msgBuf, msg, c.backend)
			if err != nil {
				c.ctx.nsqd.logf("ERROR: failed to write message to backend - %s", err)
			}
		default:
			goto finish
		}
	}

finish:
	for _, msg := range c.inFlightMessages {
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf("ERROR: failed to write message to backend - %s", err)
		}
	}

	for _, item := range c.deferredMessages {
		msg := item.Value.(*Message)
		err := writeMessageToBackend(&msgBuf, msg, c.backend)
		if err != nil {
			c.ctx.nsqd.logf("ERROR: failed to write message to backend - %s", err)
		}
	}

	return nil
}

// Deepth函数返回内存，磁盘以及正在投递的消息数量之和，也就是尚未投递成功的消息数。
func (c *Channel) Depth() int64 {
	return int64(len(c.memoryMsgChan)) + c.backend.Depth() + int64(atomic.LoadInt32(&c.bufferedCount))
}

// 暂停操作
func (c *Channel) Pause() error {
	return c.doPause(true)
}

// 重启操作
func (c *Channel) UnPause() error {
	return c.doPause(false)
}

// 暂停和恢复的具体实现
func (c *Channel) doPause(pause bool) error {
	// 设置状态(bool转换成Int32)
	if pause {
		atomic.StoreInt32(&c.paused, 1)
	} else {
		atomic.StoreInt32(&c.paused, 0)
	}

	c.RLock()
	// 暂停或者恢复下面的全部客户端
	for _, client := range c.clients {
		if pause {
			client.Pause()
		} else {
			client.UnPause()
		}
	}
	c.RUnlock()
	return nil
}

// 判断是否为暂停状态
func (c *Channel) IsPaused() bool {
	return atomic.LoadInt32(&c.paused) == 1
}

// PutMessage writes a Message to the queue
// 写消息写入队列
func (c *Channel) PutMessage(m *Message) error {
	c.RLock()
	defer c.RUnlock()
	if atomic.LoadInt32(&c.exitFlag) == 1 {
		return errors.New("exiting")
	}
	err := c.put(m)
	if err != nil {
		return err
	}
	// 累计发送出去的消息数量
	atomic.AddUint64(&c.messageCount, 1)
	return nil
}

// 往channel中写入消息
func (c *Channel) put(m *Message) error {
	select {
	// 如果memoryMsgChan == nil，那么一直执行的是default
	case c.memoryMsgChan <- m:
	default:
		// 如果队列满了，我们就存储起来
		b := bufferPoolGet()
		err := writeMessageToBackend(b, m, c.backend)
		bufferPoolPut(b)
		c.ctx.nsqd.SetHealth(err)
		if err != nil {
			c.ctx.nsqd.logf("CHANNEL(%s) ERROR: failed to write message to backend - %s",
				c.name, err)
			return err
		}
	}
	return nil
}

// TouchMessage resets the timeout for an in-flight message
// 消费者发送TOUCH，表明该消息的超时值需要被重置。
// 从inFlightPQ中取出消息，设置新的超时值后重新放入队列，新的超时值由当前时间、客户端通过IDENTIFY设置的超时值、配置中允许的最大超时值MaxMsgTimeout共同决定。
func (c *Channel) TouchMessage(clientID int64, id MessageID, clientMsgTimeout time.Duration) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)

	// 计算下一次超时的时间
	newTimeout := time.Now().Add(clientMsgTimeout)
	if newTimeout.Sub(msg.deliveryTS) >=
		c.ctx.nsqd.getOpts().MaxMsgTimeout {
		// we would have gone over, set to the max
		newTimeout = msg.deliveryTS.Add(c.ctx.nsqd.getOpts().MaxMsgTimeout)
	}

	msg.pri = newTimeout.UnixNano()
	err = c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

// FinishMessage successfully discards an in-flight message
// 消费者发送FIN，表明消息已经被接收并正确处理。
// FinishMessage分别调用popInFlightMessage和removeFromInFlightPQ将消息从inFlightMessages和inFlightPQ中删除。最后，统计该消息的投递情况。
func (c *Channel) FinishMessage(clientID int64, id MessageID) error {
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)
	if c.e2eProcessingLatencyStream != nil {
		c.e2eProcessingLatencyStream.Insert(msg.Timestamp)
	}
	return nil
}

// RequeueMessage requeues a message based on `time.Duration`, ie:
//
// `timeoutMs` == 0 - requeue a message immediately
// `timeoutMs`  > 0 - asynchronously wait for the specified timeout
//     and requeue a message (aka "deferred requeue")
//
// 客户端发送REQ，表明消息投递失败，需要再次被投递。
// Channel在RequeueMessage函数对消息投递失败进行处理。该函数将消息从inFlightMessages和inFlightPQ中删除，随后进行重新投递。
// 发送REQ时有一个附加参数timeout，该值为0时表示立即重新投递，大于0时表示等待timeout时间之后投递。
func (c *Channel) RequeueMessage(clientID int64, id MessageID, timeout time.Duration) error {
	// remove from inflight first
	msg, err := c.popInFlightMessage(clientID, id)
	if err != nil {
		return err
	}
	c.removeFromInFlightPQ(msg)

	if timeout == 0 {
		c.exitMutex.RLock()
		err := c.doRequeue(msg) // 立即投递
		c.exitMutex.RUnlock()
		return err
	}

	// deferred requeue
	return c.StartDeferredTimeout(msg, timeout) // 延迟一段时间投递
}

// AddClient adds a client to the Channel's client list
// 添加客户端到这个Channel
func (c *Channel) AddClient(clientID int64, client Consumer) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if ok {
		return
	}
	c.clients[clientID] = client
}

// RemoveClient removes a client from the Channel's client list
// 从这个Channel删除客户端
func (c *Channel) RemoveClient(clientID int64) {
	c.Lock()
	defer c.Unlock()

	_, ok := c.clients[clientID]
	if !ok {
		return
	}
	delete(c.clients, clientID)

	if len(c.clients) == 0 && c.ephemeral == true {
		// 如果这个Channle没有客户端关注，同时设置为ephemeral，那么删除这个Channel
		go c.deleter.Do(func() { c.deleteCallback(c) })
	}
}

// 填充消息的消费者ID、投送时间、优先级，然后调用pushInFlightMessage函数将消息放入inFlightMessages字典中。最后调用addToInFlightPQ将消息放入inFlightPQ队列中。
// 至此，消息投递流程完成，接下来需要等待消费者对投送结果的反馈。消费者通过发送FIN、REQ、TOUCH来回复对消息的处理结果。
func (c *Channel) StartInFlightTimeout(msg *Message, clientID int64, timeout time.Duration) error {
	now := time.Now()
	msg.clientID = clientID
	msg.deliveryTS = now
	msg.pri = now.Add(timeout).UnixNano()
	err := c.pushInFlightMessage(msg)
	if err != nil {
		return err
	}
	c.addToInFlightPQ(msg)
	return nil
}

// 如果timeout大于0，则调用StartDeferredTimeout进行延迟投递。首先计算延迟投递的时间点，
// 然后调用pushDeferredMessage将消息加入deferredMessage字典，最后将消息放入deferredPQ队列。
// 延迟投递的消息会被专门的worker扫描并在延迟投递的时间点后进行投递。需要注意的是，立即重新投递的消息不会进入deferredPQ队列。
func (c *Channel) StartDeferredTimeout(msg *Message, timeout time.Duration) error {
	absTs := time.Now().Add(timeout).UnixNano()
	item := &pqueue.Item{Value: msg, Priority: absTs}
	err := c.pushDeferredMessage(item)
	if err != nil {
		return err
	}
	c.addToDeferredPQ(item)
	return nil
}

// doRequeue performs the low level operations to requeue a message
//
// Callers of this method need to ensure that a simultaneous exit will not occur
// 立即投递使用doRequeue函数，该函数简单地调用put函数重新进行消息的投递，并自增requeueCount，该变量在统计消息投递情况时用到。
func (c *Channel) doRequeue(m *Message) error {
	err := c.put(m)
	if err != nil {
		return err
	}
	atomic.AddUint64(&c.requeueCount, 1)
	return nil
}

// pushInFlightMessage atomically adds a message to the in-flight dictionary
// 向inFlightMessages中添加消息
func (c *Channel) pushInFlightMessage(msg *Message) error {
	c.inFlightMutex.Lock()
	_, ok := c.inFlightMessages[msg.ID]
	if ok {
		c.inFlightMutex.Unlock()
		return errors.New("ID already in flight")
	}
	c.inFlightMessages[msg.ID] = msg
	c.inFlightMutex.Unlock()
	return nil
}

// popInFlightMessage atomically removes a message from the in-flight dictionary
// 从inFlightMessages中移除消息
func (c *Channel) popInFlightMessage(clientID int64, id MessageID) (*Message, error) {
	c.inFlightMutex.Lock()
	msg, ok := c.inFlightMessages[id]
	if !ok {
		c.inFlightMutex.Unlock()
		return nil, errors.New("ID not in flight")
	}
	if msg.clientID != clientID {
		c.inFlightMutex.Unlock()
		return nil, errors.New("client does not own message")
	}
	delete(c.inFlightMessages, id)
	c.inFlightMutex.Unlock()
	return msg, nil
}

// 消息添加到inFlightPQ队列
func (c *Channel) addToInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	c.inFlightPQ.Push(msg)
	c.inFlightMutex.Unlock()
}

// 从inFlightPQ队列中删除消息
func (c *Channel) removeFromInFlightPQ(msg *Message) {
	c.inFlightMutex.Lock()
	if msg.index == -1 {
		// this item has already been popped off the pqueue
		c.inFlightMutex.Unlock()
		return
	}
	c.inFlightPQ.Remove(msg.index)
	c.inFlightMutex.Unlock()
}

// 添加消息到deferredMessages
func (c *Channel) pushDeferredMessage(item *pqueue.Item) error {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	id := item.Value.(*Message).ID
	_, ok := c.deferredMessages[id]
	if ok {
		c.deferredMutex.Unlock()
		return errors.New("ID already deferred")
	}
	c.deferredMessages[id] = item
	c.deferredMutex.Unlock()
	return nil
}

// 从deferredMessages中删除消息
func (c *Channel) popDeferredMessage(id MessageID) (*pqueue.Item, error) {
	c.deferredMutex.Lock()
	// TODO: these map lookups are costly
	item, ok := c.deferredMessages[id]
	if !ok {
		c.deferredMutex.Unlock()
		return nil, errors.New("ID not deferred")
	}
	delete(c.deferredMessages, id)
	c.deferredMutex.Unlock()
	return item, nil
}

// 添加信息到deferredPQ
func (c *Channel) addToDeferredPQ(item *pqueue.Item) {
	c.deferredMutex.Lock()
	heap.Push(&c.deferredPQ, item)
	c.deferredMutex.Unlock()
}

// messagePump reads messages from either memory or backend and sends
// messages to clients over a go chan
func (c *Channel) messagePump() {
	var msg *Message
	var buf []byte
	var err error

	for {
		// do an extra check for closed exit before we select on all the memory/backend/exitChan
		// this solves the case where we are closed and something else is draining clientMsgChan into
		// backend. we don't want to reverse that
		if atomic.LoadInt32(&c.exitFlag) == 1 {
			goto exit
		}

		select {
		case msg = <-c.memoryMsgChan:
		case buf = <-c.backend.ReadChan():
			msg, err = decodeMessage(buf)
			if err != nil {
				c.ctx.nsqd.logf("ERROR: failed to decode message - %s", err)
				continue
			}
		case <-c.exitChan:
			goto exit
		}

		msg.Attempts++ // 该变量用于保存投递尝试的次数。

		// 在消息投递前会将bufferedCount置为1，在投递后置为0。该变量在Depth函数中被调用。
		atomic.StoreInt32(&c.bufferedCount, 1)
		c.clientMsgChan <- msg // 发送数据给clientMsgChan，在nsqd\protocol_v2.go的messagePump中处理
		atomic.StoreInt32(&c.bufferedCount, 0)
		// the client will call back to mark as in-flight w/ its info
	}

exit:
	c.ctx.nsqd.logf("CHANNEL(%s): closing ... messagePump", c.name)
	close(c.clientMsgChan)
}

// 处理队列deferredPQ
// 返回值为false的情况：没有获取到Message并投递即为false
func (c *Channel) processDeferredQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.deferredMutex.Lock()
		item, _ := c.deferredPQ.PeekAndShift(t)
		c.deferredMutex.Unlock()

		if item == nil {
			goto exit
		}
		dirty = true

		msg := item.Value.(*Message)
		_, err := c.popDeferredMessage(msg.ID)
		if err != nil {
			goto exit
		}
		c.doRequeue(msg)
	}

exit:
	return dirty
}

// 处理InFlightQueue
// 返回值为false的情况：没有获取到Message并投递即为false
func (c *Channel) processInFlightQueue(t int64) bool {
	c.exitMutex.RLock()
	defer c.exitMutex.RUnlock()

	if c.Exiting() {
		return false
	}

	dirty := false
	for {
		c.inFlightMutex.Lock()
		msg, _ := c.inFlightPQ.PeekAndShift(t)
		c.inFlightMutex.Unlock()

		if msg == nil {
			goto exit
		}
		dirty = true

		_, err := c.popInFlightMessage(msg.clientID, msg.ID)
		if err != nil {
			goto exit
		}
		atomic.AddUint64(&c.timeoutCount, 1)
		c.RLock()
		client, ok := c.clients[msg.clientID]
		c.RUnlock()
		if ok {
			client.TimedOutMessage()
		}
		c.doRequeue(msg)
	}

exit:
	return dirty
}
