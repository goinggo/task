package helper

import (
	"fmt"
	"github.com/goinggo/tracelog"
	"runtime"
)

//** PUBLIC METHODS

// CatchPanic is used to catch any Panic and log exceptions to Stdout. It will also write the stack trace
func CatchPanic(err *error, goRoutine string, functionName string) {
	if r := recover(); r != nil {
		// Capture the stack trace
		buf := make([]byte, 10000)
		runtime.Stack(buf, false)

		err2 := fmt.Errorf("PANIC Defered [%v] : Stack Trace : %v", r, string(buf))
		tracelog.ALERT("Unhandled Exception", goRoutine, functionName, err2.Error())

		if err != nil {
			*err = err2
		}
	}
}
