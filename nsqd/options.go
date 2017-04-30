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
	ID                     int64    `flag:"worker-id" cfg:"id"` // 进程的唯一码(默认是主机名的哈希值%1024)
	Verbose                bool     `flag:"verbose"`            // 详细的日志输出
	TCPAddress             string   `flag:"tcp-address"`
	HTTPAddress            string   `flag:"http-address"`
	HTTPSAddress           string   `flag:"https-address"`
	BroadcastAddress       string   `flag:"broadcast-address"`                                  // 通过 lookupd  注册的地址（默认名是 OS）
	NSQLookupdTCPAddresses []string `flag:"lookupd-tcp-address" cfg:"nsqlookupd_tcp_addresses"` // lookupd的地址
	AuthHTTPAddresses      []string `flag:"auth-http-address" cfg:"auth_http_addresses"`        // 认证服务地址

	// diskqueue options
	DataPath        string        `flag:"data-path"`          // 持久化数据的路径
	MemQueueSize    int64         `flag:"mem-queue-size"`     // Message Channel的最大缓冲
	MaxBytesPerFile int64         `flag:"max-bytes-per-file"` // 每个文件最大的字节数
	SyncEvery       int64         `flag:"sync-every"`         // 磁盘队列 fsync 的消息数
	SyncTimeout     time.Duration `flag:"sync-timeout"`       // 每个磁盘队列 fsync 平均耗时

	QueueScanInterval        time.Duration // workTicker定时器时间（）
	QueueScanRefreshInterval time.Duration // refreshTicker定时器时间（更新Channel列表，并重新分配worker）
	QueueScanSelectionCount  int           // 每次扫描最多选择的Channel数量
	QueueScanWorkerPoolMax   int           // queueScanWorker的goroutines的最大数量
	QueueScanDirtyPercent    float64       // 消息投递的比例

	// msg and command options
	MsgTimeout    time.Duration `flag:"msg-timeout" arg:"1ms"`                                         // 自动重新队列消息前需要等待的时间
	MaxMsgTimeout time.Duration `flag:"max-msg-timeout"`                                               // 消息超时的最大时间间隔
	MaxMsgSize    int64         `flag:"max-msg-size" deprecated:"max-message-size" cfg:"max_msg_size"` // 消息的最大长度
	MaxBodySize   int64         `flag:"max-body-size"`                                                 // 消息体的最大长度
	MaxReqTimeout time.Duration `flag:"max-req-timeout"`                                               //  消息重新排队的超时时间
	ClientTimeout time.Duration

	// client overridable configuration options
	MaxHeartbeatInterval   time.Duration `flag:"max-heartbeat-interval"`    // 心跳超时
	MaxRdyCount            int64         `flag:"max-rdy-count"`             // 允许客户端一次最多接收的消息数量
	MaxOutputBufferSize    int64         `flag:"max-output-buffer-size"`    // tcp writer对象的缓存
	MaxOutputBufferTimeout time.Duration `flag:"max-output-buffer-timeout"` // 在 flushing 到客户端前，最长的配置时间间隔。

	// statsd integration
	StatsdAddress  string        `flag:"statsd-address"`           // 统计进程的 UDP <addr>:<port>
	StatsdPrefix   string        `flag:"statsd-prefix"`            // 发送给统计keys 的前缀(%s for host replacement)
	StatsdInterval time.Duration `flag:"statsd-interval" arg:"1s"` // 从推送到统计的时间间隔
	StatsdMemStats bool          `flag:"statsd-mem-stats"`         // 切换发送内存和 GC 统计数据

	// e2e message latency
	E2EProcessingLatencyWindowTime  time.Duration `flag:"e2e-processing-latency-window-time"`                                         // 算这段时间里，点对点时间延迟
	E2EProcessingLatencyPercentiles []float64     `flag:"e2e-processing-latency-percentile" cfg:"e2e_processing_latency_percentiles"` // 消息处理时间的百分比（通过逗号可以多次指定，默认为 none）

	// TLS config
	TLSCert             string `flag:"tls-cert"`               // 证书文件路径
	TLSKey              string `flag:"tls-key"`                // 私钥路径文件
	TLSClientAuthPolicy string `flag:"tls-client-auth-policy"` // 客户端证书授权策略 ('require' or 'require-verify')
	TLSRootCAFile       string `flag:"tls-root-ca-file"`       // 私钥证书授权 PEM 路径
	TLSRequired         int    `flag:"tls-required"`           // 客户端连接需求 TLS
	TLSMinVersion       uint16 `flag:"tls-min-version"`        //  ？？？

	// compression
	DeflateEnabled  bool `flag:"deflate"`           // 运行协商压缩特性（客户端压缩）
	MaxDeflateLevel int  `flag:"max-deflate-level"` // 最大的压缩比率等级（> values == > nsqd CPU usage)
	SnappyEnabled   bool `flag:"snappy"`            // 打开快速选项 (客户端压缩)

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
