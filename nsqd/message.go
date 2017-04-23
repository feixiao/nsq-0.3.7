package nsqd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/ioutil"
	"time"
)

const (
	MsgIDLength       = 16
	minValidMsgLength = MsgIDLength + 8 + 2 // MsgIDLength + Timestamp + Attempts
)

type MessageID [MsgIDLength]byte


// http://nsq.io/clients/tcp_protocol_spec.html  DataFormat
/*
Message的结构

[x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x][x]...
|       (int64)        ||    ||      (hex string encoded in ASCII)           || (binary)
|       8-byte         ||    ||                 16-byte                      || N-byte
------------------------------------------------------------------------------------------...
  nanosecond timestamp    ^^                   message ID                       message body
                       (uint16)
                        2-byte
                       attempts
*/

// 定义Message结构
type Message struct {
	ID        MessageID		// 16字节的MessageID
	Body      []byte
	Timestamp int64			// nanosecond timestamp(创建的时间)
	Attempts  uint16		// (uint16)  2-byte attempts

	// for in-flight handling
	deliveryTS time.Time
	clientID   int64
	pri        int64
	index      int
	deferred   time.Duration
}

// 创建一个Message
func NewMessage(id MessageID, body []byte) *Message {
	return &Message{
		ID:        id,
		Body:      body,
		Timestamp: time.Now().UnixNano(),
	}
}

// 写入实现了io.Writer接口的对象
func (m *Message) WriteTo(w io.Writer) (int64, error) {
	var buf [10]byte
	var total int64

	// 分别将Timestamp和Attempts以大端的方式写入数组buf
	binary.BigEndian.PutUint64(buf[:8], uint64(m.Timestamp))
	binary.BigEndian.PutUint16(buf[8:10], uint16(m.Attempts))

	// 数组buf写入实现了io.Writer接口的对象
	n, err := w.Write(buf[:])
	total += int64(n)
	if err != nil {
		return total, err
	}

	// ID写入实现了io.Writer接口的对象
	n, err = w.Write(m.ID[:])
	total += int64(n)
	if err != nil {
		return total, err
	}
	// Body写入实现了io.Writer接口的对象
	n, err = w.Write(m.Body)
	total += int64(n)
	if err != nil {
		return total, err
	}

	return total, nil
}

// 从切片数据中解析出Message对象
func decodeMessage(b []byte) (*Message, error) {
	var msg Message

	// 大小都不到Message的最小长度就退出
	if len(b) < minValidMsgLength {
		return nil, fmt.Errorf("invalid message buffer size (%d)", len(b))
	}

	// 读取Timestamp和Attempts
	msg.Timestamp = int64(binary.BigEndian.Uint64(b[:8]))
	msg.Attempts = binary.BigEndian.Uint16(b[8:10])

	// 下面从索引10的位置开始读取
	buf := bytes.NewBuffer(b[10:])

	// 从buf中去的长度为len(msg.ID)的数据到msg.ID
	_, err := io.ReadFull(buf, msg.ID[:])
	if err != nil {
		return nil, err
	}
	// 从buf中读取剩余的全部数据
	msg.Body, err = ioutil.ReadAll(buf)
	if err != nil {
		return nil, err
	}

	return &msg, nil
}

// 将Message写入bytes.Buffer，然后再写入BackendQueue
func writeMessageToBackend(buf *bytes.Buffer, msg *Message, bq BackendQueue) error {
	buf.Reset()
	_, err := msg.WriteTo(buf)
	if err != nil {
		return err
	}
	return bq.Put(buf.Bytes())
}
