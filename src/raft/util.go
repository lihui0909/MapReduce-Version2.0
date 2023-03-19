package raft

import "log"

// Debugging
const Debug = true

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func min(num1 int, num2 int) (num int) {
	if num1 <= num2 {
		return num1
	} else {
		return num2
	}
}

func max(num1 int, num2 int) (num int) {
	if num1 > num2 {
		return num1
	} else {
		return num2
	}
}
