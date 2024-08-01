package raft

import "fmt"

const LogStartIndex = 1

type LogEntry struct {
	Term    int
	Command interface{}
}

func (logEntry *LogEntry) str() string {
	return fmt.Sprintf("{%d:%v}", logEntry.Term, logEntry.Command)
}

func logStr(log []LogEntry) string {
	str := "["
	for i := 0; i < len(log); i++ {
		// str += fmt.Sprintf("%d ", log[i].Term)
		str += log[i].str() + " "
	}
	if len(log) > 0 {
		str = str[:len(str)-1]
	}
	str += "]"
	return str
}

// LLI
func (rf *Raft) lastLogIndex() int {
	return (len(rf.log) - 1) + rf.lastIncludedIndex
}

// return log entry index
func (rf *Raft) logArrIndex(index int) int {
	arrIndex := 0
	if index >= rf.lastIncludedIndex {
		arrIndex = index - rf.lastIncludedIndex
	}
	return arrIndex
}

// return term of log index
func (rf *Raft) logEntryTerm(index int) int {
	arrIndex := rf.logArrIndex(index)

	if arrIndex == 0 && index != 0 { // lastIncludedIndex == 0
		return rf.lastIncludedTerm
	}

	return rf.log[arrIndex].Term
}

/*
	T => term

	CI => commit index
	MI => match index
	NI => next index

	ST => state

	LLI => last log entry index
	LLT => last log entry term

	LII => last include entry index
	LIT => last include entry term

	PLI => prev log entry index
	PLT => prev log entry term
*/
