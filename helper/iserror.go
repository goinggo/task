package helper

import (
	"fmt"
	"github.com/goinggo/utilities/tracelog"
)

// IsErrorComplete abstract error handling
func IsError(err error, function string) bool {
	if err != nil {
		tracelog.LogSystemf("System", "ERROR", function, "ERROR : %s", err)
		return true
	}

	return false
}

// IsErrorComplete abstract error handling
func IsErrorComplete(err error, function string) bool {
	if err != nil {
		tracelog.LogSystemf("System", "ERROR", function, "Complete : ERROR : %s", err)
		return true
	}

	return false
}

// IsErrorCompleteR abstract error handling
func IsErrorCompleteR(err error, goRoutine string, function string) bool {
	if err != nil {
		tracelog.LogSystemf(goRoutine, "ERROR", function, "Complete : ERROR : %s", err)
		return true
	}

	return false
}

// IsErrorCompletef abstract error handling
func IsErrorCompletef(err error, function string, message string, a ...interface{}) bool {
	if err != nil {
		extMessage := fmt.Sprintf(message, a)
		tracelog.LogSystemf("System", "ERROR", function, "Complete : ERROR : %s : %s", extMessage, err)
		return true
	}

	return false
}

// IsErrorCompleteRf abstract error handling
func IsErrorCompleteRf(err error, goRoutine string, function string, message string, a ...interface{}) bool {
	if err != nil {
		extMessage := fmt.Sprintf(message, a)
		tracelog.LogSystemf(goRoutine, "ERROR", function, "Complete : ERROR : %s : %s", extMessage, err)
		return true
	}

	return false
}
