package helper

import (
	"fmt"
	"github.com/goinggo/utilities/tracelog"
	"runtime"
)

//** PUBLIC METHODS

// CatchPanicSystem is used to catch any Panic and log exceptions to Stdout. It will also write the stack trace
func CatchPanicSystem(err *error, goRoutine string, namespace string, functionName string) {
	if r := recover(); r != nil {
		// Capture the stack trace
		buf := make([]byte, 10000)
		runtime.Stack(buf, false)

		tracelog.LogSystemAlertf(EmailAlertSubject, goRoutine, namespace, functionName, "PANIC Defered [%v] : Stack Trace : %v", r, string(buf))

		if err != nil {
			*err = fmt.Errorf("%v", r)
		}
	}
}

// CatchPanic is used to catch any Panic and log exceptions to Stdout. It will also write the stack trace
func CatchPanic(err *error, logKey string, goRoutine string, namespace string, functionName string) {
	if r := recover(); r != nil {

		// Capture the stack trace
		buf := make([]byte, 10000)
		runtime.Stack(buf, false)

		tracelog.LogAlertf(EmailAlertSubject, logKey, goRoutine, namespace, functionName, "PANIC Defered [%v] : Stack Trace : %v", r, string(buf))

		if err != nil {
			*err = fmt.Errorf("%v", r)
		}
	}
}
