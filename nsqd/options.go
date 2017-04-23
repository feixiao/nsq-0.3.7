package nsqd

import (
	"crypto/md5"
	"crypto/tls"
	"hash/crc32"
	"io"
	"log"
	"os"
	"time"
)

// 参数信息
type Options struct {
	// basic options
	ID                     int64    `flag:"worker-id" cfg:"id"`
	Verbose                bool     `flag:"verbose"`		// 详细的日志输出
	TCPAddress             string   `flag:"tcp-address"`
	HTTPAddress            string   `flag:"http-address"`
	HTTPSAddress           string   `flag:"https-address"`
	BroadcastAddress       string   `flag:"broadcast-address"`
	NSQLookupdTCPAddresses []string `flag:"lookupd-tcp-address" cfg:"nsqlookupd_tcp_addresses"`	// lookupd的地址
	AuthHTTPAddresses      []string `flag:"auth-http-address" cfg:"auth_http_addresses"`		// 认证服务地址

	// diskqueue options
	DataPath        string        `flag:"data-path"`		// 持久化数据的路径
	MemQueueSize    int64         `flag:"mem-queue-size"`		// Message Channel的最大缓冲
	MaxBytesPerFile int64         `flag:"max-bytes-per-file"`	// 每个文件最大的字节数
	SyncEvery       int64         `flag:"sync-every"`
	SyncTimeout     time.Duration `flag:"sync-timeout"`

	QueueScanInterval        time.Duration
	QueueScanRefreshInterval time.Duration
	QueueScanSelectionCount  int
	QueueScanWorkerPoolMax   int
	QueueScanDirtyPercent    float64

	// msg and command options
	MsgTimeout    time.Duration `flag:"msg-timeout" arg:"1ms"`
	MaxMsgTimeout time.Duration `flag:"max-msg-timeout"`
	MaxMsgSize    int64         `flag:"max-msg-size" deprecated:"max-message-size" cfg:"max_msg_size"`	// 消息的最大长度
	MaxBodySize   int64         `flag:"max-body-size"`													// 消息体的最大长度
	MaxReqTimeout time.Duration `flag:"max-req-timeout"`
	ClientTimeout time.Duration

	// client overridable configuration options
	MaxHeartbeatInterval   time.Duration `flag:"max-heartbeat-interval"`					// 心跳超时
	MaxRdyCount            int64         `flag:"max-rdy-count"`								// 允许客户端一次最多接收的消息数量
	MaxOutputBufferSize    int64         `flag:"max-output-buffer-size"`					// tcp writer对象的缓存
	MaxOutputBufferTimeout time.Duration `flag:"max-output-buffer-timeout"`

	// statsd integration
	StatsdAddress  string        `flag:"statsd-address"`
	StatsdPrefix   string        `flag:"statsd-prefix"`
	StatsdInterval time.Duration `flag:"statsd-interval" arg:"1s"`
	StatsdMemStats bool          `flag:"statsd-mem-stats"`

	// e2e message latency
	E2EProcessingLatencyWindowTime  time.Duration `flag:"e2e-processing-latency-window-time"`
	E2EProcessingLatencyPercentiles []float64     `flag:"e2e-processing-latency-percentile" cfg:"e2e_processing_latency_percentiles"`

	// TLS config
	TLSCert             string `flag:"tls-cert"`
	TLSKey              string `flag:"tls-key"`
	TLSClientAuthPolicy string `flag:"tls-client-auth-policy"`
	TLSRootCAFile       string `flag:"tls-root-ca-file"`
	TLSRequired         int    `flag:"tls-required"`
	TLSMinVersion       uint16 `flag:"tls-min-version"`

	// compression
	DeflateEnabled  bool `flag:"deflate"`
	MaxDeflateLevel int  `flag:"max-deflate-level"`
	SnappyEnabled   bool `flag:"snappy"`

	Logger logger
}

func NewOptions() *Options {
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}

	h := md5.New()
	io.WriteString(h, hostname)
	defaultID := int64(crc32.ChecksumIEEE(h.Sum(nil)) % 1024)

	return &Options{
		ID: defaultID,

		TCPAddress:       "0.0.0.0:4150",
		HTTPAddress:      "0.0.0.0:4151",
		HTTPSAddress:     "0.0.0.0:4152",
		BroadcastAddress: hostname,

		NSQLookupdTCPAddresses: make([]string, 0),
		AuthHTTPAddresses:      make([]string, 0),

		MemQueueSize:    10000,
		MaxBytesPerFile: 100 * 1024 * 1024,
		SyncEvery:       2500,
		SyncTimeout:     2 * time.Second,

		QueueScanInterval:        100 * time.Millisecond,
		QueueScanRefreshInterval: 5 * time.Second,
		QueueScanSelectionCount:  20,
		QueueScanWorkerPoolMax:   4,
		QueueScanDirtyPercent:    0.25,

		MsgTimeout:    60 * time.Second,
		MaxMsgTimeout: 15 * time.Minute,
		MaxMsgSize:    1024 * 1024,
		MaxBodySize:   5 * 1024 * 1024,
		MaxReqTimeout: 1 * time.Hour,
		ClientTimeout: 60 * time.Second,

		MaxHeartbeatInterval:   60 * time.Second,
		MaxRdyCount:            2500,
		MaxOutputBufferSize:    64 * 1024,
		MaxOutputBufferTimeout: 1 * time.Second,

		StatsdPrefix:   "nsq.%s",
		StatsdInterval: 60 * time.Second,
		StatsdMemStats: true,

		E2EProcessingLatencyWindowTime: time.Duration(10 * time.Minute),

		DeflateEnabled:  true,
		MaxDeflateLevel: 6,
		SnappyEnabled:   true,

		TLSMinVersion: tls.VersionTLS10,

		Logger: log.New(os.Stderr, "[nsqd] ", log.Ldate|log.Ltime|log.Lmicroseconds),
	}
}
