package controller

import (
	"fmt"
	"github.com/goinggo/task/helper"
	"github.com/goinggo/utilities/straps"
	"github.com/goinggo/utilities/tracelog"
	"os"
	"os/signal"
	"strconv"
	"sync/atomic"
	"syscall"
)

//** CONSTANTS

// Constants
const (
	_NAMESPACE        = "controller"
	EmailAlertSubject = "Controller Exception"
)

//** NEW TYPES

// _Controller manages the starting and shutting down of the program
type _ControlManager struct {
	Shutdown    int32
	UserControl Controller
}

// Provides the functionality for the running application
type Controller interface {
	StrapEnv() (environment string, path string)
	Run() (err error)
}

//** SINGLETON REFERENCE

// Reference to the singleton
var _This *_ControlManager

//** PUBLIC FUNCTIONS

// Run is the entry point for the controller
//  userControl: A pointer to the users program logic
func Run(userControl Controller) (osExit int) {

	// Create the control manager
	_This = &_ControlManager{
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
func (this *_ControlManager) Init() (err error) {

	defer helper.CatchPanicSystem(&err, "main", _NAMESPACE, "Init")

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

	consoleOnly := straps.StrapBool("consoleLogOnly")

	if consoleOnly == true {

		tracelog.StartupStdoutOnly(straps.Strap("machineName"))
	} else {

		// Start the log and configure email alerts
		tracelog.Startup(straps.Strap("baseFilePath"), straps.Strap("machineName"), straps.StrapInt("daysToKeep"))
		tracelog.ConfigureEmailAlerts(helper.EmailHost, helper.EmailPort, helper.EmailUserName, helper.EmailPassword, []string{helper.EmailTo})
	}

	return err
}

// Start gets the program running
func (this *_ControlManager) Start() (err error) {

	defer helper.CatchPanicSystem(&err, "main", _NAMESPACE, "Start")

	tracelog.LogSystem("main", _NAMESPACE, "Start", "Started")

	// Create a channel to talk with the OS
	sigChan := make(chan os.Signal, 1)

	// Ask the OS to notify us about events
	signal.Notify(sigChan)

	// Launch the process
	tracelog.LogSystem("main", _NAMESPACE, "Start", "******> Launch Task")
	complete := make(chan error)
	go this.LaunchProcessor(complete)

	for {

		select {

		case whatSig := <-sigChan:

			// Convert the signal to an integer so we can display the hex number
			sigAsInt, _ := strconv.Atoi(fmt.Sprintf("%d", whatSig))

			tracelog.LogSystemf("main", _NAMESPACE, "Start", "******> OS Notification: %v : %#x", whatSig, sigAsInt)

			// Did we get any of these termination events
			if whatSig == syscall.SIGKILL {

				fmt.Printf("******> SIGNAL KILL REPORTED\n")
				os.Exit(1)
			} else if whatSig == os.Interrupt {

				tracelog.LogSystemf("main", _NAMESPACE, "Start", "******> Program Being Killed")

				// Set the flag to indicate the program should shutdown early
				atomic.StoreInt32(&_This.Shutdown, 1)
			}

			continue

		case err = <-complete:

			tracelog.LogSystem("main", _NAMESPACE, "Start", "******> Task Complete")

			// Break the case
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
func (this *_ControlManager) Close() (err error) {

	defer helper.CatchPanicSystem(&err, "main", _NAMESPACE, "Close")

	// Shutdown the log system
	tracelog.Shutdown()

	return err
}

// LaunchProcessor instanciates the specified inventory processor and runs the job
//  complete: The channel to send result on when processing is complete
func (this *_ControlManager) LaunchProcessor(complete chan error) {

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
