package helper

import (
	"fmt"
	"github.com/goinggo/utilities/tracelog"
	"labix.org/v2/mgo"
	"strings"
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

// IsErrorMongoComplete abstract error handling
func IsErrorMongoComplete(mongoSession *mgo.Session, err error, function string) bool {
	if err != nil {
		tracelog.LogSystemf("System", "ERROR", function, "Complete : ERROR : %s", err)

		if strings.Contains(err.Error(), "i/o timeout") == true {
			mongoSession.Refresh()
		}
		return true
	}

	return false
}

// IsErrorMongoCompleteR abstract error handling
func IsErrorMongoCompleteR(mongoSession *mgo.Session, err error, goRoutine string, function string) bool {
	if err != nil {
		tracelog.LogSystemf(goRoutine, "ERROR", function, "Complete : ERROR : %s", err)

		if strings.Contains(err.Error(), "i/o timeout") == true {
			mongoSession.Refresh()
		}
		return true
	}

	return false
}

// IsErrorMongoCompletef abstract error handling for this package
func IsErrorMongoCompletef(mongoSession *mgo.Session, err error, function string, message string, a ...interface{}) bool {
	if err != nil {
		extMessage := fmt.Sprintf(message, a)
		tracelog.LogSystemf("System", "ERROR", function, "Complete : ERROR : %s : %s", extMessage, err)

		if strings.Contains(err.Error(), "i/o timeout") == true {
			mongoSession.Refresh()
		}
		return true
	}

	return false
}

// IsErrorMongoCompleteRf abstract error handling for this package
func IsErrorMongoCompleteRf(mongoSession *mgo.Session, err error, goRoutine string, function string, message string, a ...interface{}) bool {
	if err != nil {
		extMessage := fmt.Sprintf(message, a)
		tracelog.LogSystemf(goRoutine, "ERROR", function, "Complete : ERROR : %s : %s", extMessage, err)

		if strings.Contains(err.Error(), "i/o timeout") == true {
			mongoSession.Refresh()
		}
		return true
	}

	return false
}
