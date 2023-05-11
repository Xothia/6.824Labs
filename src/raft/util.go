package raft

import "log"

// Debugging
const Debug = false
const FDebug = false
const GDebug = false
const HDebug = true

func HPrintf(format string, a ...interface{}) (n int, err error) {
	if HDebug {
		log.Printf(format, a...)
	}
	return
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}
func FPrintf(format string, a ...interface{}) (n int, err error) {
	if FDebug {
		log.Printf(format, a...)
	}
	return
}
func GPrintf(format string, a ...interface{}) (n int, err error) {
	if GDebug {
		log.Printf(format, a...)
	}
	return
}

func Min(a int, b int) int {
	res := b
	if a < b {
		res = a
	}
	return res
}
