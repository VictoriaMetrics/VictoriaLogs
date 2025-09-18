package main

import (
	"bufio"
	"encoding/binary"
	"flag"
	"fmt"
	"io"
	"log"
	"log/syslog"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

var (
	logsPath     = flag.String("logsPath", "", "Path to logs directory")
	syslogAddr   = flag.String("syslog.addr", "logstash:12345", "Addr to send logs to")
	syslogAddr2  = flag.String("syslog.addr2", "logstash:12345", "Addr to send logs to")
	randomSuffix = flag.Bool("logs.randomSuffix", false, "Whether to add a random suffix to a log line")

	outputRateLimitItems  = flag.Int("outputRateLimitItems", 100, "Number of items to send per second")
	outputRateLimitPeriod = flag.Duration("outputRateLimitPeriod", time.Second, "Period of time to send items")

	backdateEnable = flag.Bool("logs.backdate.enable", false, "Enable backdating timestamps over a recent time window")
	backdateDays   = flag.Int("logs.backdate.days", 7, "Number of days in the past to spread logs over")
)

func main() {
	flag.Parse()
	startedAt := time.Now().Unix()

	logFiles, err := os.ReadDir(*logsPath)
	if err != nil {
		panic(fmt.Errorf("error reading directory %s:%w", *logsPath, err))
	}

	sourceFiles := make([]string, 0)

	for _, logFile := range logFiles {
		if strings.HasSuffix(logFile.Name(), ".log") {
			sourceFiles = append(sourceFiles, logFile.Name())
		}
	}
	log.Printf("sourceFiles: %v", sourceFiles)
	log.Printf("running with rate limit: %d items per %s", *outputRateLimitItems, *outputRateLimitPeriod)

	limitTicker := time.NewTicker(*outputRateLimitPeriod)
	limitItems := *outputRateLimitItems
	limiter := make(chan struct{}, limitItems)
	go func() {
		for {
			<-limitTicker.C
			for i := 0; i < limitItems; i++ {
				limiter <- struct{}{}
			}
		}
	}()

	// Calculate timestamp distribution for backdating if enabled
	var bucketStart time.Time
	var totalBuckets int
	var lineCounter int64
	var totalLines int64
	if *backdateEnable {
		// Pre-count total lines for even distribution
		for _, sourceFile := range sourceFiles {
			fc, err := countLines(*logsPath + "/" + sourceFile)
			if err != nil {
				panic(err)
			}
			totalLines += fc
		}
		totalBuckets = *backdateDays * 24
		bucketStart = time.Now().Add(-time.Duration(*backdateDays) * 24 * time.Hour).Truncate(time.Hour)
		log.Printf("backdating enabled: %d lines across %d hour buckets starting %s", totalLines, totalBuckets, bucketStart.Format(time.RFC3339))
	}

	// Setup connections for backdating or regular syslog writers
	var conn1, conn2 net.Conn
	var hostname string
	if *backdateEnable {
		hostname, _ = os.Hostname()
		var err error
		conn1, err = net.Dial("tcp", *syslogAddr)
		if err != nil {
			panic(fmt.Errorf("error dialing syslog tcp to %s: %w", *syslogAddr, err))
		}
		defer conn1.Close()
		conn2, err = net.Dial("tcp", *syslogAddr2)
		if err != nil {
			panic(fmt.Errorf("error dialing syslog tcp to %s: %w", *syslogAddr2, err))
		}
		defer conn2.Close()
	}

	for _, sourceFile := range sourceFiles {
		log.Printf("sourceFile: %s", sourceFile)
		f, err := os.Open(*logsPath + "/" + sourceFile)
		if err != nil {
			panic(err)
		}

		syslogTag := "logs-benchmark-" + sourceFile + "-" + strconv.FormatInt(startedAt, 10)

		// Loki uses RFC5424 syslog format, which has a 48 character limit on the tag.
		tagLen := len(syslogTag)
		if tagLen > 48 {
			truncate := tagLen - 48
			syslogTag = syslogTag[truncate:]
		}

		var logger, logger2 *syslog.Writer
		if !*backdateEnable {
			logger, err = syslog.Dial("tcp", *syslogAddr, syslog.LOG_INFO, syslogTag)
			if err != nil {
				panic(fmt.Errorf("error dialing syslog: %w", err))
			}
			logger2, err = syslog.Dial("tcp", *syslogAddr2, syslog.LOG_INFO, syslogTag)
			if err != nil {
				panic(fmt.Errorf("error dialing syslog: %w", err))
			}
		}

		scanner := bufio.NewScanner(f)
		for scanner.Scan() {
			<-limiter
			line := scanner.Text()
			if *randomSuffix {
				line = line + " " + randomString()
			}

			if *backdateEnable {
				// Calculate which hour bucket this line belongs to
				bucketIndex := int((lineCounter * int64(totalBuckets)) / totalLines)
				if bucketIndex >= totalBuckets {
					bucketIndex = totalBuckets - 1
				}
				ts := bucketStart.Add(time.Duration(bucketIndex) * time.Hour)
				msg := buildRFC5424(ts, hostname, syslogTag, line)
				// Debug log every 10000th message to verify timestamp
				if lineCounter%10000 == 0 {
					log.Printf("DEBUG: sending message with timestamp %s (bucket %d)", ts.Format(time.RFC3339), bucketIndex)
				}
				_, _ = io.WriteString(conn1, msg)
				_, _ = io.WriteString(conn2, msg)
				lineCounter++
			} else {
				_ = logger.Info(line)
				_ = logger2.Info(line)
			}
		}

		if !*backdateEnable {
			logger.Close()
			logger2.Close()
		}
		f.Close()
	}

}

func randomString() string {
	buf := make([]byte, 4)
	ip := rand.Uint32()

	binary.LittleEndian.PutUint32(buf, ip)
	return net.IP(buf).String()
}

// buildRFC5424 builds a minimal RFC5424 syslog message string.
// Example: <14>1 2024-01-02T15:04:05Z host app - - - message\n
func buildRFC5424(ts time.Time, host, app, msg string) string {
	pri := 14 // user-level info
	// Use RFC3339 timestamp; RFC5424 allows high precision
	tsStr := ts.UTC().Format(time.RFC3339)
	// PROCID, MSGID, and STRUCTURED-DATA all set to '-' (nilvalue)
	return fmt.Sprintf("<%d>1 %s %s %s - - - %s\n", pri, tsStr, host, app, msg)
}

// countLines returns the number of lines in a file by scanning.
func countLines(path string) (int64, error) {
	f, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer f.Close()
	var n int64
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		n++
	}
	return n, scanner.Err()
}
