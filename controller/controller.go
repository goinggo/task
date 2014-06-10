package controller

import (
	"github.com/goinggo/straps"
	"github.com/goinggo/task/helper"
	"github.com/goinggo/tracelog"
	"log"
	"os"
	"os/signal"
	"sync/atomic"
	"time"
)

//** CONSTANTS

const (
	EmailAlertSubject = "Controller Exception"
)

//** PACKAGE VARIBLES

var (
	_This *controlManager
)

//** TYPES

type (
	// controlManager manages the starting and shutting down of the program
	controlManager struct {
		shutdown    int32
		userControl Controller
	}

	// Controller provides the functionality for the running application
	Controller interface {
		StrapEnv() (environment string, path string)
		Run() (err error)
	}
)

//** PUBLIC FUNCTIONS

// Run is the entry point for the controller
func Run(userControl Controller) (osExit int) {
	// Create the control manager
	_This = &controlManager{
		shutdown:    0,
		userControl: userControl,
	}

	// Init the program
	err := _This.init()
	if err != nil {
		os.Exit(1)
	}

	// Run the program
	err = _This.start()

	// Close the program
	_This.stop()

	// Did we error
	if err != nil {
		os.Exit(1)
	}

	return
}

// Isshutdown returns the value of the shutdown flag
func IsShutdown() bool {
	value := atomic.LoadInt32(&_This.shutdown)

	if value == 1 {
		return true
	}

	return false
}

//** MEMBER FUNCTIONS

// init is called to initialize the package
func (controlManager *controlManager) init() (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("main : init : Init Exceptions: %s\n", r)
			os.Exit(1)
		}
	}()

	// Capture the environment and path for the straps
	environment, path := controlManager.userControl.StrapEnv()

	if os.Getenv(environment) == "" {
		log.Fatalf("Environment %s Missing\n", environment)
	}

	// Load the straps file
	straps.Load(environment, path)

	// Capture the global email settings
	helper.EmailHost = straps.Strap("emailHost")
	helper.EmailPort = straps.StrapInt("emailPort")
	helper.EmailUserName = straps.Strap("emailUserName")
	helper.EmailPassword = straps.Strap("emailPassword")
	helper.EmailTo = straps.Strap("emailTo")
	helper.EmailAlertSubject = straps.Strap("emailAlertSubject")
	helper.TimeoutSeconds = straps.StrapInt("timeoutSeconds")

	consoleOnly := straps.StrapBool("consoleLogOnly")

	if consoleOnly == true {
		tracelog.Start(tracelog.LEVEL_TRACE)
	} else {
		tracelog.StartFile(tracelog.LEVEL_TRACE, straps.Strap("baseFilePath"), straps.StrapInt("daysToKeep"))
	}

	tracelog.ConfigureEmail(helper.EmailHost, helper.EmailPort, helper.EmailUserName, helper.EmailPassword, []string{helper.EmailTo})

	return err
}

// start gets the program running
func (controlManager *controlManager) start() (err error) {
	defer helper.CatchPanic(&err, "main", "start")

	tracelog.STARTED("main", "start")

	// Create a channel to talk with the OS
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	// Set the timeout channel
	timeout := time.After(time.Duration(helper.TimeoutSeconds) * time.Second)

	// Launch the process
	tracelog.TRACE("main", "start", "******> Launch Task")
	complete := make(chan error)
	go controlManager.launchProcessor(complete)

ControlLoop:
	for {
		select {
		case <-sigChan:
			tracelog.ALERT(helper.EmailAlertSubject, "main", "start", "OS INTERRUPT - Program Being Killed")

			// Set the flag to indicate the program should shutdown early
			atomic.StoreInt32(&_This.shutdown, 1)
			continue

		case <-timeout:
			tracelog.TRACE("main", "start", "Timeout - Killing Program")
			os.Exit(1)

		case err = <-complete:
			tracelog.TRACE("main", "start", "******> Task Complete")
			break ControlLoop
		}
	}

	// Program finished
	tracelog.COMPLETED("main", "start")
	return err
}

// stop releases all resource and prepares the program to terminate
func (controlManager *controlManager) stop() (err error) {
	defer helper.CatchPanic(&err, "main", "stop")

	// shutdown the log system
	tracelog.Stop()

	return err
}

// launchProcessor instanciates the specified inventory processor and runs the job
func (controlManager *controlManager) launchProcessor(complete chan error) {
	tracelog.STARTED("launch", "launchProcessor")

	var err error

	defer func() {
		// shutdown the program
		complete <- err
	}()

	// Run the user code
	err = controlManager.userControl.Run()

	tracelog.COMPLETED("launch", "launchProcessor")
}
