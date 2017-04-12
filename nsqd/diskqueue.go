package nsqd

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path"
	"sync"
	"sync/atomic"
	"time"
)

// diskQueue implements the BackendQueue interface
// providing a filesystem backed FIFO queue
type diskQueue struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms

	// run-time state (also persisted to disk)
	readPos      int64
	writePos     int64
	readFileNum  int64
	writeFileNum int64
	depth        int64	// 队列的深度？

	sync.RWMutex

	// instantiation time metadata
	name            string							// 一般是主题的名字
	dataPath        string							// 数据保存在磁盘的位置
	maxBytesPerFile int64 // currently this cannot change once created	// 每个文件的最大大小，一旦创建就不能改变
	minMsgSize      int32							// 消息的最小长度
	maxMsgSize      int32							// 消息的最大长度
	syncEvery       int64         // number of writes per fsync		// 每次同步写的次数？？
	syncTimeout     time.Duration // duration of time per fsync		// 每次同步的超时？？
	exitFlag        int32							// 标记是否正在退出
	needSync        bool

	// keeps track of the position where we have read
	// (but not yet sent over readChan)
	nextReadPos     int64
	nextReadFileNum int64

	readFile  *os.File
	writeFile *os.File
	reader    *bufio.Reader
	writeBuf  bytes.Buffer

	// exposed via ReadChan()
	readChan chan []byte

	// internal channels
	writeChan         chan []byte		// 写数据请求通道
	writeResponseChan chan error		// 返回写操作的结果
	emptyChan         chan int		// 清空数据请求通道
	emptyResponseChan chan error		// 清空数据请求回复
	exitChan          chan int		// 通知相关的goroutine退出
	exitSyncChan      chan int		// 通知ioLoop已经退出

	logger logger
}

// newDiskQueue instantiates a new instance of diskQueue, retrieving metadata
// from the filesystem and starting the read ahead goroutine
func newDiskQueue(name string, dataPath string, maxBytesPerFile int64,
	minMsgSize int32, maxMsgSize int32,
	syncEvery int64, syncTimeout time.Duration,
	logger logger) BackendQueue {
	d := diskQueue{
		name:              name,		// 队列的名字，一般是主题的名字
		dataPath:          dataPath,		// 数据保存的位置
		maxBytesPerFile:   maxBytesPerFile,	// 每个文件最大的数据量
		minMsgSize:        minMsgSize,		// 消息的最小长度
		maxMsgSize:        maxMsgSize,		// 消息的最大长度
		readChan:          make(chan []byte),	// 想要读取队列的数据关注这个channel皆可
		writeChan:         make(chan []byte),	// 写数据操作通道
		writeResponseChan: make(chan error),	// 返回写操作的结果
		emptyChan:         make(chan int),	// 清空数据请求通道
		emptyResponseChan: make(chan error),	// 清空数据请求回复
		exitChan:          make(chan int),	// 通知相关的goroutine退出
		exitSyncChan:      make(chan int),	// 通知ioLoop已经退出
		syncEvery:         syncEvery,		// 每次同步写的次数？？
		syncTimeout:       syncTimeout,		// 每次同步的超时？？
		logger:            logger,
	}

	// no need to lock here, nothing else could possibly be touching this instance
	err := d.retrieveMetaData()
	if err != nil && !os.IsNotExist(err) {
		d.logf("ERROR: diskqueue(%s) failed to retrieveMetaData - %s", d.name, err)
	}

	go d.ioLoop()   // 队列的业务主要routine

	return &d
}

func (d *diskQueue) logf(f string, args ...interface{}) {
	if d.logger == nil {
		return
	}
	d.logger.Output(2, fmt.Sprintf(f, args...))
}

// Depth returns the depth of the queue
func (d *diskQueue) Depth() int64 {
	return atomic.LoadInt64(&d.depth)
}

// ReadChan returns the []byte channel for reading data
// 想要读取队列的数据关注这个channel皆可
func (d *diskQueue) ReadChan() chan []byte {
	return d.readChan
}

// Put writes a []byte to the queue
// 将数据写入队列
func (d *diskQueue) Put(data []byte) error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	// 写数据的方式通过channel完成，通过writeResponseChan返回结果（在ioLoop中完成）
	d.writeChan <- data
	return <-d.writeResponseChan
}

// Close cleans up the queue and persists metadata
// 关闭、清理队列和持久化元数据
func (d *diskQueue) Close() error {
	err := d.exit(false)	// 参数只影响日志输出，关闭还是删除
	if err != nil {
		return err
	}
	return d.sync()
}

// 删除(关闭读写文件)
func (d *diskQueue) Delete() error {
	return d.exit(true)
}

// 退出处理
func (d *diskQueue) exit(deleted bool) error {
	d.Lock()
	defer d.Unlock()

	// 标记正在退出中
	d.exitFlag = 1

	// 是否删除数据
	if deleted {
		d.logf("DISKQUEUE(%s): deleting", d.name)
	} else {
		d.logf("DISKQUEUE(%s): closing", d.name)
	}

	// 通知goroutine退出
	close(d.exitChan)
	// ensure that ioLoop has exited
	// 确保ioLoop退出
	<-d.exitSyncChan

	// 关闭读写文件
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	return nil
}

// Empty destructively clears out any pending data in the queue
// by fast forwarding read positions and removing intermediate files
func (d *diskQueue) Empty() error {
	d.RLock()
	defer d.RUnlock()

	if d.exitFlag == 1 {
		return errors.New("exiting")
	}

	d.logf("DISKQUEUE(%s): emptying", d.name)

	// 发送数据清空数据的请求，由ioLoop处理
	d.emptyChan <- 1
	return <-d.emptyResponseChan
}

// 删除全部文件
func (d *diskQueue) deleteAllFiles() error {
	err := d.skipToNextRWFile()	// 删除全部的文件

	// 删除全部的元数据文件（就一个）
	innerErr := os.Remove(d.metaDataFileName())
	if innerErr != nil && !os.IsNotExist(innerErr) {
		d.logf("ERROR: diskqueue(%s) failed to remove metadata file - %s", d.name, innerErr)
		return innerErr
	}

	return err
}

// 删除全部的数据文件，并调整文件读写的编号
func (d *diskQueue) skipToNextRWFile() error {
	var err error

	// 关闭读写文件
	if d.readFile != nil {
		d.readFile.Close()
		d.readFile = nil
	}

	if d.writeFile != nil {
		d.writeFile.Close()
		d.writeFile = nil
	}

	for i := d.readFileNum; i <= d.writeFileNum; i++ {
		// 获取文件的完整名字
		fn := d.fileName(i)
		// 删除
		innerErr := os.Remove(fn)
		if innerErr != nil && !os.IsNotExist(innerErr) {
			d.logf("ERROR: diskqueue(%s) failed to remove data file - %s", d.name, innerErr)
			err = innerErr
		}
	}

	d.writeFileNum++
	d.writePos = 0
	d.readFileNum = d.writeFileNum
	d.readPos = 0
	d.nextReadFileNum = d.writeFileNum
	d.nextReadPos = 0
	atomic.StoreInt64(&d.depth, 0)

	return err
}

// readOne performs a low level filesystem read for a single []byte
// while advancing read positions and rolling files, if necessary
func (d *diskQueue) readOne() ([]byte, error) {
	var err error
	var msgSize int32

	if d.readFile == nil {
		curFileName := d.fileName(d.readFileNum)
		d.readFile, err = os.OpenFile(curFileName, os.O_RDONLY, 0600)
		if err != nil {
			return nil, err
		}

		d.logf("DISKQUEUE(%s): readOne() opened %s", d.name, curFileName)

		if d.readPos > 0 {
			_, err = d.readFile.Seek(d.readPos, 0)
			if err != nil {
				d.readFile.Close()
				d.readFile = nil
				return nil, err
			}
		}

		d.reader = bufio.NewReader(d.readFile)
	}

	err = binary.Read(d.reader, binary.BigEndian, &msgSize)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	if msgSize < d.minMsgSize || msgSize > d.maxMsgSize {
		// this file is corrupt and we have no reasonable guarantee on
		// where a new message should begin
		d.readFile.Close()
		d.readFile = nil
		return nil, fmt.Errorf("invalid message read size (%d)", msgSize)
	}

	readBuf := make([]byte, msgSize)
	_, err = io.ReadFull(d.reader, readBuf)
	if err != nil {
		d.readFile.Close()
		d.readFile = nil
		return nil, err
	}

	totalBytes := int64(4 + msgSize)

	// we only advance next* because we have not yet sent this to consumers
	// (where readFileNum, readPos will actually be advanced)
	d.nextReadPos = d.readPos + totalBytes
	d.nextReadFileNum = d.readFileNum

	// TODO: each data file should embed the maxBytesPerFile
	// as the first 8 bytes (at creation time) ensuring that
	// the value can change without affecting runtime
	if d.nextReadPos > d.maxBytesPerFile {
		if d.readFile != nil {
			d.readFile.Close()
			d.readFile = nil
		}

		d.nextReadFileNum++
		d.nextReadPos = 0
	}

	return readBuf, nil
}

// writeOne performs a low level filesystem write for a single []byte
// while advancing write positions and rolling files, if necessary
func (d *diskQueue) writeOne(data []byte) error {
	var err error

	if d.writeFile == nil {
		curFileName := d.fileName(d.writeFileNum)
		d.writeFile, err = os.OpenFile(curFileName, os.O_RDWR|os.O_CREATE, 0600)
		if err != nil {
			return err
		}

		d.logf("DISKQUEUE(%s): writeOne() opened %s", d.name, curFileName)

		if d.writePos > 0 {
			_, err = d.writeFile.Seek(d.writePos, 0)
			if err != nil {
				d.writeFile.Close()
				d.writeFile = nil
				return err
			}
		}
	}

	dataLen := int32(len(data))

	if dataLen < d.minMsgSize || dataLen > d.maxMsgSize {
		return fmt.Errorf("invalid message write size (%d) maxMsgSize=%d", dataLen, d.maxMsgSize)
	}

	d.writeBuf.Reset()
	err = binary.Write(&d.writeBuf, binary.BigEndian, dataLen)
	if err != nil {
		return err
	}

	_, err = d.writeBuf.Write(data)
	if err != nil {
		return err
	}

	// only write to the file once
	_, err = d.writeFile.Write(d.writeBuf.Bytes())
	if err != nil {
		d.writeFile.Close()
		d.writeFile = nil
		return err
	}

	totalBytes := int64(4 + dataLen)
	d.writePos += totalBytes
	atomic.AddInt64(&d.depth, 1)

	if d.writePos > d.maxBytesPerFile {
		d.writeFileNum++
		d.writePos = 0

		// sync every time we start writing to a new file
		err = d.sync()
		if err != nil {
			d.logf("ERROR: diskqueue(%s) failed to sync - %s", d.name, err)
		}

		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
	}

	return err
}

// sync fsyncs the current writeFile and persists metadata
// 数据同步
func (d *diskQueue) sync() error {
	if d.writeFile != nil {
		// 如果文件没有被关闭就同步文件（内存数据刷到磁盘），然后关闭
		err := d.writeFile.Sync()
		if err != nil {
			d.writeFile.Close()
			d.writeFile = nil
			return err
		}
	}

	// 持久化元数据
	err := d.persistMetaData()
	if err != nil {
		return err
	}

	d.needSync = false
	return nil
}

// retrieveMetaData initializes state from the filesystem
func (d *diskQueue) retrieveMetaData() error {
	var f *os.File
	var err error

	fileName := d.metaDataFileName()
	f, err = os.OpenFile(fileName, os.O_RDONLY, 0600)
	if err != nil {
		return err
	}
	defer f.Close()

	var depth int64
	_, err = fmt.Fscanf(f, "%d\n%d,%d\n%d,%d\n",
		&depth,
		&d.readFileNum, &d.readPos,
		&d.writeFileNum, &d.writePos)
	if err != nil {
		return err
	}
	atomic.StoreInt64(&d.depth, depth)
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = d.readPos

	return nil
}

// persistMetaData atomically writes state to the filesystem
// 持久化元数据
func (d *diskQueue) persistMetaData() error {
	var f *os.File
	var err error

	// 获取元数据文件路径和名字
	fileName := d.metaDataFileName()
	// 临时文件
	tmpFileName := fmt.Sprintf("%s.%d.tmp", fileName, rand.Int())

	// write to tmp file
	f, err = os.OpenFile(tmpFileName, os.O_RDWR|os.O_CREATE, 0600)
	if err != nil {
		return err
	}

	// 将元数据信息写到临时文件
	_, err = fmt.Fprintf(f, "%d\n%d,%d\n%d,%d\n",
		atomic.LoadInt64(&d.depth),
		d.readFileNum, d.readPos,
		d.writeFileNum, d.writePos)

	if err != nil {
		f.Close()
		return err
	}
	// 同步和关闭文件
	f.Sync()
	f.Close()

	// atomically rename
	// 用临时文件替换之前的文件
	return atomicRename(tmpFileName, fileName)
}

// 获取元数据文件路径和名字
func (d *diskQueue) metaDataFileName() string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.meta.dat"), d.name)
}

// 获取文件名字
func (d *diskQueue) fileName(fileNum int64) string {
	return fmt.Sprintf(path.Join(d.dataPath, "%s.diskqueue.%06d.dat"), d.name, fileNum)
}

func (d *diskQueue) checkTailCorruption(depth int64) {
	if d.readFileNum < d.writeFileNum || d.readPos < d.writePos {
		return
	}

	// we've reached the end of the diskqueue
	// if depth isn't 0 something went wrong
	if depth != 0 {
		if depth < 0 {
			d.logf(
				"ERROR: diskqueue(%s) negative depth at tail (%d), metadata corruption, resetting 0...",
				d.name, depth)
		} else if depth > 0 {
			d.logf(
				"ERROR: diskqueue(%s) positive depth at tail (%d), data loss, resetting 0...",
				d.name, depth)
		}
		// force set depth 0
		atomic.StoreInt64(&d.depth, 0)
		d.needSync = true
	}

	if d.readFileNum != d.writeFileNum || d.readPos != d.writePos {
		if d.readFileNum > d.writeFileNum {
			d.logf(
				"ERROR: diskqueue(%s) readFileNum > writeFileNum (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readFileNum, d.writeFileNum)
		}

		if d.readPos > d.writePos {
			d.logf(
				"ERROR: diskqueue(%s) readPos > writePos (%d > %d), corruption, skipping to next writeFileNum and resetting 0...",
				d.name, d.readPos, d.writePos)
		}

		d.skipToNextRWFile()
		d.needSync = true
	}
}

func (d *diskQueue) moveForward() {
	oldReadFileNum := d.readFileNum
	d.readFileNum = d.nextReadFileNum
	d.readPos = d.nextReadPos
	depth := atomic.AddInt64(&d.depth, -1)

	// see if we need to clean up the old file
	if oldReadFileNum != d.nextReadFileNum {
		// sync every time we start reading from a new file
		d.needSync = true

		fn := d.fileName(oldReadFileNum)
		err := os.Remove(fn)
		if err != nil {
			d.logf("ERROR: failed to Remove(%s) - %s", fn, err)
		}
	}

	d.checkTailCorruption(depth)
}

func (d *diskQueue) handleReadError() {
	// jump to the next read file and rename the current (bad) file
	if d.readFileNum == d.writeFileNum {
		// if you can't properly read from the current write file it's safe to
		// assume that something is fucked and we should skip the current file too
		if d.writeFile != nil {
			d.writeFile.Close()
			d.writeFile = nil
		}
		d.writeFileNum++
		d.writePos = 0
	}

	badFn := d.fileName(d.readFileNum)
	badRenameFn := badFn + ".bad"

	d.logf(
		"NOTICE: diskqueue(%s) jump to next file and saving bad file as %s",
		d.name, badRenameFn)

	err := atomicRename(badFn, badRenameFn)
	if err != nil {
		d.logf(
			"ERROR: diskqueue(%s) failed to rename bad diskqueue file %s to %s",
			d.name, badFn, badRenameFn)
	}

	d.readFileNum++
	d.readPos = 0
	d.nextReadFileNum = d.readFileNum
	d.nextReadPos = 0

	// significant state change, schedule a sync on the next iteration
	d.needSync = true
}

// ioLoop provides the backend for exposing a go channel (via ReadChan())
// in support of multiple concurrent queue consumers
//
// it works by looping and branching based on whether or not the queue has data
// to read and blocking until data is either read or written over the appropriate
// go channels
//
// conveniently this also means that we're asynchronously reading from the filesystem
func (d *diskQueue) ioLoop() {
	var dataRead []byte
	var err error
	var count int64
	var r chan []byte

	syncTicker := time.NewTicker(d.syncTimeout)

	for {
		// dont sync all the time :)
		if count == d.syncEvery {
			count = 0
			d.needSync = true
		}

		if d.needSync {
			err = d.sync()
			if err != nil {
				d.logf("ERROR: diskqueue(%s) failed to sync - %s", d.name, err)
			}
		}

		if (d.readFileNum < d.writeFileNum) || (d.readPos < d.writePos) {
			if d.nextReadPos == d.readPos {
				dataRead, err = d.readOne()
				if err != nil {
					d.logf("ERROR: reading from diskqueue(%s) at %d of %s - %s",
						d.name, d.readPos, d.fileName(d.readFileNum), err)
					d.handleReadError()
					continue
				}
			}
			r = d.readChan
		} else {
			r = nil
		}

		select {
		// the Go channel spec dictates that nil channel operations (read or write)
		// in a select are skipped, we set r to d.readChan only when there is data to read
		case r <- dataRead:
			// moveForward sets needSync flag if a file is removed
			d.moveForward()
		case <-d.emptyChan:		// 清空数据
			d.emptyResponseChan <- d.deleteAllFiles()
			count = 0
		case dataWrite := <-d.writeChan:	// 处理写数据
			count++
			d.writeResponseChan <- d.writeOne(dataWrite)
		case <-syncTicker.C:
			if count > 0 {
				count = 0
				d.needSync = true
			}
		case <-d.exitChan:
			goto exit
		}
	}

exit:
	d.logf("DISKQUEUE(%s): closing ... ioLoop", d.name)
	syncTicker.Stop()
	d.exitSyncChan <- 1
}
