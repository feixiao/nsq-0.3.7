package nsqd

// the core algorithm here was borrowed from:
// Blake Mizerany's `noeqd` https://github.com/bmizerany/noeqd
// and indirectly:
// Twitter's `snowflake` https://github.com/twitter/snowflake

// only minor cleanup and changes to introduce a type, combine the concept
// of workerID + datacenterId into a single identifier, and modify the
// behavior when sequences rollover for our specific implementation needs

import (
	"encoding/hex"
	"errors"
	"time"
)

const (
	workerIDBits   = uint64(10)
	sequenceBits   = uint64(12)
	workerIDShift  = sequenceBits
	timestampShift = sequenceBits + workerIDBits
	sequenceMask   = int64(-1) ^ (int64(-1) << sequenceBits)

	// ( 2012-10-28 16:23:42 UTC ).UnixNano() >> 20
	twepoch = int64(1288834974288)
)

var ErrTimeBackwards = errors.New("time has gone backwards")
var ErrSequenceExpired = errors.New("sequence expired")
var ErrIDBackwards = errors.New("ID went backward")

type guid int64

type guidFactory struct {
	sequence      int64
	lastTimestamp int64
	lastID        guid
}

func (f *guidFactory) NewGUID(workerID int64) (guid, error) {
	// divide by 1048576, giving pseudo-milliseconds
	// ts 是 nano » 20, 也就是 1 ts = 1048576 nano, 而 1 milliseconds = 1000000 nano;
	// 这就是注释里说的 “giving pseudo-milliseconds” ,ts 近似等于 1毫秒
	ts := time.Now().UnixNano() >> 20

	if ts < f.lastTimestamp {
		return 0, ErrTimeBackwards
	}

	if f.lastTimestamp == ts {
		f.sequence = (f.sequence + 1) & sequenceMask
		if f.sequence == 0 {
			return 0, ErrSequenceExpired
		}
	} else {
		f.sequence = 0
	}

	f.lastTimestamp = ts
	// id = [ 37位ts + ?位 workerId + 12位 sequence ]
	// workerid 必须 小于 1024, 也就是10位; 而这个是靠启动nsqd的时候检查配置项实现的
	id := guid(((ts - twepoch) << timestampShift) |
		(workerID << workerIDShift) |
		f.sequence)

	if id <= f.lastID {
		return 0, ErrIDBackwards
	}

	f.lastID = id

	return id, nil
}

func (g guid) Hex() MessageID {
	var h MessageID
	var b [8]byte

	b[0] = byte(g >> 56)
	b[1] = byte(g >> 48)
	b[2] = byte(g >> 40)
	b[3] = byte(g >> 32)
	b[4] = byte(g >> 24)
	b[5] = byte(g >> 16)
	b[6] = byte(g >> 8)
	b[7] = byte(g)

	hex.Encode(h[:], b[:])
	return h
}
