package raft

import (
	"fmt"
	"log"
	"os"
	"reflect"
	"strconv"
	"time"
	"unsafe"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type LogTopic string

const (
	dClient  LogTopic = "CLNT"
	dCommit  LogTopic = "CMIT"
	dDrop    LogTopic = "DROP"
	dError   LogTopic = "ERRO"
	dInfo    LogTopic = "INFO"
	dLeader  LogTopic = "LEAD"
	dLog     LogTopic = "LOG1"
	dLog2    LogTopic = "LOG2"
	dPersist LogTopic = "PERS"
	dSnap    LogTopic = "SNAP"
	dTerm    LogTopic = "TERM"
	dTest    LogTopic = "TEST"
	dTimer   LogTopic = "TIMR"
	dTrace   LogTopic = "TRCE"
	dVote    LogTopic = "VOTE"
	dWarn    LogTopic = "WARN"
)

const (
	DEBUG = 1
	INFO  = 2
	WARN  = 3
	ERROR = 4
)

func getVerbosity() int {
	v := os.Getenv("VERBOSE")
	level := 0
	if v != "" {
		var err error
		level, err = strconv.Atoi(v)
		if err != nil {
			log.Fatalf("Invalid verbosity %v", v)
		}
	}
	return level
}

var debugStart time.Time
var debugVerbosity int

func init() {
	debugVerbosity = getVerbosity()
	debugStart = time.Now()

	log.SetFlags(log.Flags() &^ (log.Ldate | log.Ltime))
}

func GetLevelStr(level int) string {
	switch level {
	case DEBUG:
		return "DBUG"
	case INFO:
		return "INFO"
	case WARN:
		return "WARN"
	case ERROR:
		return "ERRO"
	}
	return "UNKNOWN"
}

func LogPrint(logLevel int, topic LogTopic, format string, a ...interface{}) {
	if debugVerbosity >= 1 && logLevel >= debugVerbosity {
		time := time.Since(debugStart).Microseconds()
		time /= 100
		prefix := fmt.Sprintf("%06d %s %v ", time, GetLevelStr(logLevel), string(topic))
		format = prefix + format
		log.Printf(format, a...)
	}
}

func entryByteSize(arr interface{}) uint64 {
	arrValue := reflect.ValueOf(arr)
	if arrValue.Kind() != reflect.String {
		return uint64(unsafe.Sizeof(arr))
	} else {
		return uint64(arrValue.Len())
	}
}

func logEntryByteSize(log []LogEntry) uint64 {
	var byteSize uint64
	byteSize = 0
	for _, entry := range log {
		byteSize += (uint64)(unsafe.Sizeof(entry.Term)) + entryByteSize(entry.Command)
	}
	return byteSize
}
