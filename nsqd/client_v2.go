package nsqd

import (
	"bufio"
	"crypto/tls"
	"net"
	"sync"
	"sync/atomic"
	"time"
)

const defaultBufferSize = 16 * 1024

const (
	stateInit = iota
	stateDisconnected
	stateConnected
	stateSubscribed
	stateClosing
)

type identifyDataV2 struct {
	ClientID            string `json:"client_id"`
	Hostname            string `json:"hostname"`
	HeartbeatInterval   int    `json:"heartbeat_interval"`
	OutputBufferSize    int    `json:"output_buffer_size"`
	OutputBufferTimeout int    `json:"output_buffer_timeout"`
	FeatureNegotiation  bool   `json:"feature_negotiation"`
	TLSv1               bool   `json:"tls_v1"`
	Deflate             bool   `json:"deflate"`
	DeflateLevel        int    `json:"deflate_level"`
	Snappy              bool   `json:"snappy"`
	SampleRate          int32  `json:"sample_rate"`
	UserAgent           string `json:"user_agent"`
	MsgTimeout          int    `json:"msg_timeout"`
}

type clientV2 struct {
	// 64bit atomic vars need to be first for proper alignment on 32bit platforms
	ReadyCount    int64  //客户端能同时接收的消息数量
	InFlightCount int64  //处于发送中（已发送但客户端还未回复）的消息数量
	MessageCount  uint64  //已经发送的消息数量
	FinishCount   uint64  //客户端已经回复消费成功的消息数量
	RequeueCount  uint64  //客户端已经回复消费失败的消息数量

	pubCounts map[string]uint64   //统计每个topic收到的消息数量

	writeLock sync.RWMutex
	metaLock  sync.RWMutex

	ID        int64
	nsqd      *NSQD
	UserAgent string

	// original connection
	net.Conn

	// reading/writing interfaces
	Reader *bufio.Reader
	Writer *bufio.Writer

	OutputBufferSize    int
	OutputBufferTimeout time.Duration

	HeartbeatInterval time.Duration

	MsgTimeout time.Duration //一条消息的过时时间

	State          int32
	ConnectTime    time.Time  //该客户端的连接时间
	Channel        *Channel
	ReadyStateChan chan int
	ExitChan       chan int

	ClientID string
	Hostname string


	SubEventChan      chan *Channel //传递订阅事件

	lenSlice []byte

}

func newClientV2(id int64, conn net.Conn, nsqd *NSQD) *clientV2 {
	var identifier string
	if conn != nil {
		identifier, _, _ = net.SplitHostPort(conn.RemoteAddr().String())
	}

	c := &clientV2{
		ID:   id,
		nsqd: nsqd,
		Conn: conn,
		Reader: bufio.NewReaderSize(conn, defaultBufferSize),
		Writer: bufio.NewWriterSize(conn, defaultBufferSize),

		OutputBufferSize:    defaultBufferSize,
		OutputBufferTimeout: nsqd.getOpts().OutputBufferTimeout,

		MsgTimeout: nsqd.getOpts().MsgTimeout,

		// ReadyStateChan has a buffer of 1 to guarantee that in the event
		// there is a race the state update is not lost
		ReadyStateChan: make(chan int, 1),
		ExitChan:       make(chan int),
		ConnectTime:    time.Now(),
		State:          stateInit,

		ClientID: identifier,
		Hostname: identifier,

		SubEventChan:      make(chan *Channel, 1),

		// heartbeats are client configurable but default to 30s
		HeartbeatInterval: nsqd.getOpts().ClientTimeout / 2,

		pubCounts: make(map[string]uint64),
	}
	// re-usable buffer for reading the 4-byte lengths off the wire
	c.lenSlice = make([]byte,4)
	return c
}

func (c *clientV2) String() string {
	return c.RemoteAddr().String()
}

func (c *clientV2) Type() int {
	c.metaLock.RLock()
	hasPublished := len(c.pubCounts) > 0
	c.metaLock.RUnlock()
	if hasPublished {
		return typeProducer
	}
	return typeConsumer
}

// struct to convert from integers to the human readable strings
type prettyConnectionState struct {
	tls.ConnectionState
}


func (c *clientV2) IsReadyForMessages() bool {
	readyCount := atomic.LoadInt64(&c.ReadyCount)
	inFlightCount := atomic.LoadInt64(&c.InFlightCount)

	c.nsqd.logf(LOG_DEBUG, "[%s] state rdy: %4d inflt: %4d", c, readyCount, inFlightCount)

	if inFlightCount >= readyCount || readyCount <= 0 {
		return false
	}

	return true
}

func (c *clientV2) SetReadyCount(count int64) {
	oldCount := atomic.SwapInt64(&c.ReadyCount, count)
	//如果 ReadyCount 的值改变了，通知
	if oldCount != count {
		c.tryUpdateReadyState()
	}
}

func (c *clientV2) tryUpdateReadyState() {
	// you can always *try* to write to ReadyStateChan because in the cases
	// where you cannot the message pump loop would have iterated anyway.
	// the atomic integer operations guarantee correctness of the value.
	select {
	case c.ReadyStateChan <- 1:
	default:
	}
}

func (c *clientV2) FinishedMessage() {
	atomic.AddUint64(&c.FinishCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

func (c *clientV2) Empty() {
	atomic.StoreInt64(&c.InFlightCount, 0)
	c.tryUpdateReadyState()
}

func (c *clientV2) SendingMessage() {
	atomic.AddInt64(&c.InFlightCount, 1)
	atomic.AddUint64(&c.MessageCount, 1)
}

func (c *clientV2) PublishedMessage(topic string, count uint64) {
	c.metaLock.Lock()
	c.pubCounts[topic] += count
	c.metaLock.Unlock()
}

func (c *clientV2) TimedOutMessage() {
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

func (c *clientV2) RequeuedMessage() {
	atomic.AddUint64(&c.RequeueCount, 1)
	atomic.AddInt64(&c.InFlightCount, -1)
	c.tryUpdateReadyState()
}

func (c *clientV2) StartClose() {
	// Force the client into ready 0
	c.SetReadyCount(0)
	// mark this client as closing
	atomic.StoreInt32(&c.State, stateClosing)
}


func (c *clientV2) Flush() error {
	var zeroTime time.Time
	if c.HeartbeatInterval > 0 {
		c.SetWriteDeadline(time.Now().Add(c.HeartbeatInterval))
	} else {
		c.SetWriteDeadline(zeroTime)
	}

	err := c.Writer.Flush()
	if err != nil {
		return err
	}

	return nil
}


