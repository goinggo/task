package controller

import (
	"fmt"
	"github.com/goinggo/task/helper"
	"github.com/goinggo/utilities/straps"
	"github.com/goinggo/utilities/tracelog"
	"os"
	"os/signal"
	"sync/atomic"
	"time"
)

//** CONSTANTS

// Constants
const (
	_NAMESPACE        = "controller"
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
//  userControl: A pointer to the users program logic
func Run(userControl Controller) (osExit int) {
	// Create the control manager
	_This = &controlManager{
		Shutdown:    0,
		UserControl: userControl,
	}

	// Init the program
	if err := _This.Init(); err != nil {
		tracelog.LogSystemAlertf(EmailAlertSubject, "main", _NAMESPACE, "Run", "%s", err)
		os.Exit(1)
	}

	// Run the program
	err := _This.Start()

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
			fmt.Printf("Init Exceptions: %s\n", r)
			os.Exit(1)
		}
	}()

	// Capture the environment and path for the straps
	environment, path := this.UserControl.StrapEnv()

	if os.Getenv(environment) == "" {
		fmt.Printf("Environment %s Missing\n", environment)
		os.Exit(1)
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
		tracelog.StartupStdoutOnly(straps.Strap("machineName"))
	} else {
		tracelog.Startup(straps.Strap("baseFilePath"), straps.Strap("machineName"), straps.StrapInt("daysToKeep"))
	}

	tracelog.ConfigureEmailAlerts(helper.EmailHost, helper.EmailPort, helper.EmailUserName, helper.EmailPassword, []string{helper.EmailTo})

	return err
}

// Start gets the program running
func (this *controlManager) Start() (err error) {
	defer helper.CatchPanicSystem(&err, "main", _NAMESPACE, "Start")

	tracelog.LogSystem("main", _NAMESPACE, "Start", "Started")

	// Create a channel to talk with the OS
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	// Launch the process
	tracelog.LogSystem("main", _NAMESPACE, "Start", "******> Launch Task")
	complete := make(chan error)
	go this.LaunchProcessor(complete)

	for {
		select {
		case <-sigChan:
			tracelog.LogSystemf("main", _NAMESPACE, "Start", "******> Program Being Killed")
			helper.SendEmail("main", "main", helper.EmailAlertSubject, "OS INTERRUPT - Shutting Down Program")

			// Set the flag to indicate the program should shutdown early
			atomic.StoreInt32(&_This.Shutdown, 1)
			continue

		case <-time.After(time.Duration(helper.TimeoutSeconds) * time.Second):
			fmt.Printf("******> TIMEOUT\n")
			helper.SendEmail("main", "main", helper.EmailAlertSubject, "Timeout - Killing Program")
			os.Exit(1)

		case err = <-complete:
			tracelog.LogSystem("main", _NAMESPACE, "Start", "******> Task Complete")
			break
		}

		// Break the loop
		break
	}

	// Program finished
	tracelog.LogSystem("main", _NAMESPACE, "Start", "Completed")
	return err
}

// Close releases all resource and prepares the program to terminate
func (this *controlManager) Close() (err error) {
	defer helper.CatchPanicSystem(&err, "main", _NAMESPACE, "Close")

	// Shutdown the log system
	tracelog.Shutdown()

	return err
}

// LaunchProcessor instanciates the specified inventory processor and runs the job
//  complete: The channel to send result on when processing is complete
func (this *controlManager) LaunchProcessor(complete chan error) {
	tracelog.LogSystemf("launch", _NAMESPACE, "LaunchProcessor", "Started")

	var err error

	defer func() {
		// Shutdown the program
		complete <- err
	}()

	// Run the user code
	err = this.UserControl.Run()

	tracelog.LogSystemf("launch", _NAMESPACE, "LaunchProcessor", "Completed")
}
