package raft

import "log"

// Debugging
const Debug = false
const FDebug = false

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
func Min(a int, b int) int {
	res := b
	if a < b {
		res = a
	}
	return res
}
