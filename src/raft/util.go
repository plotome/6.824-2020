package raft

import "log"

// Debugging
const Debug = 0
const DebugA = 0
const DebugB = 1
const DebugC = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func DAPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugA > 0 {
		log.Printf(format, a...)
	}
	return
}

func DBPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugB > 0 {
		log.Printf(format, a...)
	}
	return
}

func DCPrintf(format string, a ...interface{}) (n int, err error) {
	if DebugC > 0 {
		log.Printf(format, a...)
	}
	return
}
