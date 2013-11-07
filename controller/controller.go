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

// Constants
const (
	EmailAlertSubject = "Controller Exception"
)

//** NEW TYPES

// controlManager manages the starting and shutting down of the program
type controlManager struct {
	Shutdown    int32
	UserControl Controller
}

// Controller provides the functionality for the running application
type Controller interface {
	StrapEnv() (environment string, path string)
	Run() (err error)
}

//** SINGLETON REFERENCE

// Reference to the singleton
var _This *controlManager

//** PUBLIC FUNCTIONS

// Run is the entry point for the controller
func Run(userControl Controller) (osExit int) {
	// Create the control manager
	_This = &controlManager{
		Shutdown:    0,
		UserControl: userControl,
	}

	// Init the program
	err := _This.Init()
	if err != nil {
		os.Exit(1)
	}

	// Run the program
	err = _This.Start()

	// Close the program
	_This.Close()

	// Did we error
	if err != nil {
		os.Exit(1)
	}

	return
}

// IsShutdown returns the value of the shutdown flag
func IsShutdown() bool {
	value := atomic.LoadInt32(&_This.Shutdown)

	if value == 1 {
		return true
	}

	return false
}

//** MEMBER FUNCTIONS

// Init is called to initialize the package
func (this *controlManager) Init() (err error) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("main : Init : Init Exceptions: %s\n", r)
			os.Exit(1)
		}
	}()

	// Capture the environment and path for the straps
	environment, path := this.UserControl.StrapEnv()

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

// Start gets the program running
func (this *controlManager) Start() (err error) {
	defer helper.CatchPanic(&err, "main", "Start")

	tracelog.STARTED("main", "Start")

	// Create a channel to talk with the OS
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	// Launch the process
	tracelog.TRACE("main", "Start", "******> Launch Task")
	complete := make(chan error)
	go this.LaunchProcessor(complete)

	for {
		select {
		case <-sigChan:
			tracelog.ALERT(helper.EmailAlertSubject, "main", "Start", "OS INTERRUPT - Program Being Killed")

			// Set the flag to indicate the program should shutdown early
			atomic.StoreInt32(&_This.Shutdown, 1)
			continue

		case <-time.After(time.Duration(helper.TimeoutSeconds) * time.Second):
			tracelog.ALERT(helper.EmailAlertSubject, "main", "Start", "Timeout - Killing Program")
			os.Exit(1)

		case err = <-complete:
			tracelog.TRACE("main", "Start", "******> Task Complete")
			break
		}

		// Break the loop
		break
	}

	// Program finished
	tracelog.COMPLETED("main", "Start")
	return err
}

// Close releases all resource and prepares the program to terminate
func (this *controlManager) Close() (err error) {
	defer helper.CatchPanic(&err, "main", "Close")

	// Shutdown the log system
	tracelog.Stop()

	return err
}

// LaunchProcessor instanciates the specified inventory processor and runs the job
func (this *controlManager) LaunchProcessor(complete chan error) {
	tracelog.STARTED("launch", "LaunchProcessor")

	var err error

	defer func() {
		// Shutdown the program
		complete <- err
	}()

	// Run the user code
	err = this.UserControl.Run()

	tracelog.COMPLETED("launch", "LaunchProcessor")
}
