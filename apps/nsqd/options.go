package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"strconv"
	"strings"
	"github.com/bowen/mynsq/internal/lg"
	"github.com/bowen/mynsq/nsqd"
)

type tlsRequiredOption int

func (t *tlsRequiredOption) Get() interface{} { return int(*t) }

func (t *tlsRequiredOption) String() string {
	return strconv.FormatInt(int64(*t), 10)
}

func (t *tlsRequiredOption) IsBoolFlag() bool { return true }

type tlsMinVersionOption uint16

func (t *tlsMinVersionOption) Set(s string) error {
	s = strings.ToLower(s)
	switch s {
	case "":
		return nil
	case "ssl3.0":
		*t = tls.VersionSSL30
	case "tls1.0":
		*t = tls.VersionTLS10
	case "tls1.1":
		*t = tls.VersionTLS11
	case "tls1.2":
		*t = tls.VersionTLS12
	default:
		return fmt.Errorf("unknown tlsVersionOption %q", s)
	}
	return nil
}

func (t *tlsMinVersionOption) Get() interface{} { return uint16(*t) }

func (t *tlsMinVersionOption) String() string {
	return strconv.FormatInt(int64(*t), 10)
}

type config map[string]interface{}

// Validate settings in the config file, and fatal on errors
func (cfg config) Validate() {
	if v, exists := cfg["tls_min_version"]; exists {
		var t tlsMinVersionOption
		err := t.Set(fmt.Sprintf("%v", v))
		if err == nil {
			newVal := fmt.Sprintf("%v", t.Get())
			if newVal != "0" {
				cfg["tls_min_version"] = newVal
			} else {
				delete(cfg, "tls_min_version")
			}
		} else {
			logFatal("failed parsing tls_min_version %+v", v)
		}
	}
	if v, exists := cfg["log_level"]; exists {
		var t lg.LogLevel
		err := t.Set(fmt.Sprintf("%v", v))
		if err == nil {
			cfg["log_level"] = t
		} else {
			logFatal("failed parsing log_level %+v", v)
		}
	}
}

func nsqdFlagSet(opts *nsqd.Options) *flag.FlagSet {
	flagSet := flag.NewFlagSet("nsqd", flag.ExitOnError)

	// basic options
	flagSet.Bool("version", false, "print version string")
	flagSet.String("config", "", "path to config file")

	logLevel := opts.LogLevel
	flagSet.Var(&logLevel, "log-level", "set log verbosity: debug, info, warn, error, or fatal")
	flagSet.String("log-prefix", "[nsqd] ", "log message prefix")
	flagSet.Bool("verbose", false, "[deprecated] has no effect, use --log-level")

	flagSet.Int64("node-id", opts.ID, "unique part for message IDs, (int) in range [0,1024) (default is hash of hostname)")
	flagSet.Bool("worker-id", false, "[deprecated] use --node-id")

	flagSet.String("tcp-address", opts.TCPAddress, "<addr>:<port> to listen on for TCP clients")
	flagSet.String("broadcast-address", opts.BroadcastAddress, "address that will be registered with lookupd (defaults to the OS hostname)")
	flagSet.Int("broadcast-tcp-port", opts.BroadcastTCPPort, "TCP port that will be registered with lookupd (defaults to the TCP port that this nsqd is listening on)")
	flagSet.Int("broadcast-http-port", opts.BroadcastHTTPPort, "HTTP port that will be registered with lookupd (defaults to the HTTP port that this nsqd is listening on)")
	flagSet.Duration("http-client-connect-timeout", opts.HTTPClientConnectTimeout, "timeout for HTTP connect")
	flagSet.Duration("http-client-request-timeout", opts.HTTPClientRequestTimeout, "timeout for HTTP request")

	// diskqueue options
	flagSet.String("data-path", opts.DataPath, "path to store disk-backed messages")
	flagSet.Int64("mem-queue-size", opts.MemQueueSize, "number of messages to keep in memory (per topic/channel)")
	flagSet.Int64("max-bytes-per-file", opts.MaxBytesPerFile, "number of bytes per diskqueue file before rolling")
	flagSet.Int64("sync-every", opts.SyncEvery, "number of messages per diskqueue fsync")
	flagSet.Duration("sync-timeout", opts.SyncTimeout, "duration of time per diskqueue fsync")

	flagSet.Int("queue-scan-worker-pool-max", opts.QueueScanWorkerPoolMax, "max concurrency for checking in-flight and deferred message timeouts")
	flagSet.Int("queue-scan-selection-count", opts.QueueScanSelectionCount, "number of channels to check per cycle (every 100ms) for in-flight and deferred timeouts")

	// msg and command options
	flagSet.Duration("msg-timeout", opts.MsgTimeout, "default duration to wait before auto-requeing a message")
	flagSet.Duration("max-msg-timeout", opts.MaxMsgTimeout, "maximum duration before a message will timeout")
	flagSet.Int64("max-msg-size", opts.MaxMsgSize, "maximum size of a single message in bytes")
	flagSet.Duration("max-req-timeout", opts.MaxReqTimeout, "maximum requeuing timeout for a message")
	flagSet.Int64("max-body-size", opts.MaxBodySize, "maximum size of a single command body")

	// client overridable configuration options
	flagSet.Duration("max-heartbeat-interval", opts.MaxHeartbeatInterval, "maximum client configurable duration of time between client heartbeats")
	flagSet.Int64("max-rdy-count", opts.MaxRdyCount, "maximum RDY count for a client")
	flagSet.Int64("max-output-buffer-size", opts.MaxOutputBufferSize, "maximum client configurable size (in bytes) for a client output buffer")
	flagSet.Duration("max-output-buffer-timeout", opts.MaxOutputBufferTimeout, "maximum client configurable duration of time between flushing to a client")
	flagSet.Duration("min-output-buffer-timeout", opts.MinOutputBufferTimeout, "minimum client configurable duration of time between flushing to a client")
	flagSet.Duration("output-buffer-timeout", opts.OutputBufferTimeout, "default duration of time between flushing data to clients")
	flagSet.Int("max-channel-consumers", opts.MaxChannelConsumers, "maximum channel consumer connection count per nsqd instance (default 0, i.e., unlimited)")


	return flagSet
}
