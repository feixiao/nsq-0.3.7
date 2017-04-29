package nsqlookupd

import (
	"log"
	"os"
	"time"
)

// http://nsq.io/components/nsqlookupd.html

type Options struct {
	Verbose bool `flag:"verbose"` // enable verbose logging

	TCPAddress       string `flag:"tcp-address"`       //	TCP地址服务
	HTTPAddress      string `flag:"http-address"`      // 	HTTP地址服务
	BroadcastAddress string `flag:"broadcast-address"` // 	address of this lookupd node, (default to the OS hostname) (default "PROSNAKES.local")

	// 如果生产者最近一次ping距离现在的时间在InactiveProducerTimeout内，还是会被保存在活动队列中（FilterByActive中获取）
	InactiveProducerTimeout time.Duration `flag:"inactive-producer-timeout"` // duration of time a producer will remain in the active list since its last ping
	// 被标记为墓碑状态的生产者，如果registration存在，那么在这个时间内依然会存在
	TombstoneLifetime time.Duration `flag:"tombstone-lifetime"` // duration of time a producer will remain tombstoned if registration remains

	Logger logger
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	return &Options{
		TCPAddress:       "0.0.0.0:4160",
		HTTPAddress:      "0.0.0.0:4161",
		BroadcastAddress: hostname,

		InactiveProducerTimeout: 300 * time.Second,
		TombstoneLifetime:       45 * time.Second,

		Logger: log.New(os.Stderr, "[nsqlookupd] ", log.Ldate|log.Ltime|log.Lmicroseconds),
	}
}
