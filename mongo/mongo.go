// Copyright 2013 Ardan Studios. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
	This package provides mongo connectivity support
*/
package mongo

import (
	"github.com/goinggo/task/helper"
	"github.com/goinggo/utilities/straps"
	"github.com/goinggo/utilities/tracelog"
	"labix.org/v2/mgo"
	"strings"
	"time"
)

//** CONSTANTS

// Constants
const (
	_NAMESPACE     = "mongo"
	MASTER_SESSION = "master"
)

//** NEW TYPES

type _MongoSession struct {
	MongoDBDialInfo *mgo.DialInfo // The connection information
	MongoSession    *mgo.Session  // A master connection
}

// _MongoManager manages a connection and session
type _MongoManager struct {
	Sessions map[string]*_MongoSession // Map of available sessions
}

//** SINGLETON REFERENCE

var _This *_MongoManager // Reference to the singleton

//** PUBLIC FUNCTIONS

// Startup brings the manager to a running state
//  goRoutine: The name of the routine making the call
func Startup(goRoutine string) (err error) {

	defer helper.CatchPanicSystem(&err, goRoutine, _NAMESPACE, "Startup")

	tracelog.LogSystem(goRoutine, _NAMESPACE, "Startup", "Started")

	// Create the Mongo Manager
	_This = &_MongoManager{
		Sessions: map[string]*_MongoSession{},
	}

	// Log the mongodb connection straps
	tracelog.LogSystemf(goRoutine, _NAMESPACE, "Startup", "MongoDB : Addr[%s]", straps.Strap("mgo_host"))
	tracelog.LogSystemf(goRoutine, _NAMESPACE, "Startup", "MongoDB : Database[%s]", straps.Strap("mgo_database"))
	tracelog.LogSystemf(goRoutine, _NAMESPACE, "Startup", "MongoDB : Username[%s]", straps.Strap("mgo_username"))

	hosts := strings.Split(straps.Strap("mgo_host"), ",")

	// Create the master session
	err = CreateSession(goRoutine, MASTER_SESSION, hosts, straps.Strap("mgo_database"), straps.Strap("mgo_username"), straps.Strap("mgo_password"))

	tracelog.LogSystem(goRoutine, _NAMESPACE, "Startup", "Completed")

	return
}

// Shutdown systematically brings the manager down gracefully
//  goRoutine: The name of the routine making the call
func Shutdown(goRoutine string) (err error) {

	defer helper.CatchPanicSystem(&err, goRoutine, _NAMESPACE, "Shutdown")

	tracelog.LogSystem(goRoutine, _NAMESPACE, "Shutdown", "Started")

	// Close the databases
	for _, session := range _This.Sessions {

		CloseSession(goRoutine, session.MongoSession)
	}

	tracelog.LogSystem(goRoutine, _NAMESPACE, "Shutdown", "Completed")

	return
}

// CreateSession creates a connection pool for use
//  goRoutine: The name of the routine making the call
//  sessionName: A unique name for the session
//  host: The host and port to connect to
//  databaseName: The name of the database to use
//  username: The user name for authentication
//  password: The password for authentication
func CreateSession(goRoutine string, sessionName string, hosts []string, databaseName string, username string, password string) (err error) {

	defer helper.CatchPanicSystem(nil, goRoutine, _NAMESPACE, "CreateSession")

	tracelog.LogSystemf(goRoutine, _NAMESPACE, "CreateSession", "Started : SessionName[%s] Hosts[%s] DatabaseName[%s] Username[%s]", sessionName, hosts, databaseName, username)

	// Create the database object
	mongoSession := &_MongoSession{
		MongoDBDialInfo: &mgo.DialInfo{
			Addrs:    hosts,
			Timeout:  10 * time.Second,
			Database: databaseName,
			Username: username,
			Password: password,
		},
	}

	// Establish the master session
	mongoSession.MongoSession, err = mgo.DialWithInfo(mongoSession.MongoDBDialInfo)
	if err != nil {

		tracelog.LogSystemf(goRoutine, _NAMESPACE, "CreateSession", "ERROR : %s", err)
		return
	}

	// Reads and writes will always be made to the master server using a
	// unique connection so that reads and writes are fully consistent,
	// ordered, and observing the most up-to-date data.
	// http://godoc.org/labix.org/v2/mgo#Session.SetMode
	mongoSession.MongoSession.SetMode(mgo.Strong, true)

	// Have the session check for errors
	// http://godoc.org/labix.org/v2/mgo#Session.SetSafe
	mongoSession.MongoSession.SetSafe(&mgo.Safe{})

	// Don't want any longer than 10 second for an operation to complete
	mongoSession.MongoSession.SetSyncTimeout(10 * time.Second)

	// Add the database to the map
	_This.Sessions[sessionName] = mongoSession

	tracelog.LogSystem(goRoutine, _NAMESPACE, "CreateSession", "Completed")

	return
}

// CopySession get a new connection based on the master connection
//  goRoutine: The name of the routine making the call
func CopyMasterSession(goRoutine string) (mongoSession *mgo.Session, err error) {

	return CopySession(goRoutine, MASTER_SESSION)
}

// CopySession get a new connection based on the master connection
//  goRoutine: The name of the routine making the call
//  useSession: The name of the session to use
func CopySession(goRoutine string, useSession string) (mongoSession *mgo.Session, err error) {

	defer helper.CatchPanicSystem(nil, goRoutine, _NAMESPACE, "CopySession")

	tracelog.LogSystemf(goRoutine, _NAMESPACE, "CopySession", "Started : UseSession[%s]", useSession)

	// Find the session object
	session := _This.Sessions[useSession]

	if session == nil {

		tracelog.LogSystemf(goRoutine, _NAMESPACE, "CopySession", "Completed : ERROR : Unable To Locate Session %s", useSession)
		return
	}

	// Copy the master session
	mongoSession = session.MongoSession.Copy()

	tracelog.LogSystem(goRoutine, _NAMESPACE, "CopySession", "Completed")

	return
}

// CloseSession puts the connection back into the pool
//  goRoutine: The name of the routine making the call
func CloseSession(goRoutine string, mongoSession *mgo.Session) {

	defer helper.CatchPanicSystem(nil, goRoutine, _NAMESPACE, "CloseSession")

	tracelog.LogSystem(goRoutine, _NAMESPACE, "CloseSession", "Started")

	mongoSession.Close()

	tracelog.LogSystem(goRoutine, _NAMESPACE, "CloseSession", "Completed")
}

// GetCollection returns a reference to a collection for the specified database and collection name
//  goRoutine: The name of the routine making the call
//  mongoSession; The session to use to make the call
//  useDatabase: The name of the database that contains the collection
//  useCollection: The name of the collection to access
func GetCollection(goRoutine string, mongoSession *mgo.Session, useDatabase string, useCollection string) (collection *mgo.Collection, err error) {

	// Access the buoy_stations collection
	return mongoSession.DB(useDatabase).C(useCollection), err
}
