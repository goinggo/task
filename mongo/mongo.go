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

type mongoSession struct {
	MongoDBDialInfo *mgo.DialInfo // The connection information
	MongoSession    *mgo.Session  // A master connection
}

// mongoManager manages a connection and session
type mongoManager struct {
	Sessions map[string]*mongoSession // Map of available sessions
}

//** SINGLETON REFERENCE

var _This *mongoManager // Reference to the singleton

//** PUBLIC FUNCTIONS

// Startup brings the manager to a running state
func Startup(goRoutine string) (err error) {
	defer helper.CatchPanicSystem(&err, goRoutine, _NAMESPACE, "Startup")

	tracelog.LogSystemStarted(goRoutine, _NAMESPACE, "Startup")

	// Create the Mongo Manager
	_This = &mongoManager{
		Sessions: map[string]*mongoSession{},
	}

	// Log the mongodb connection straps
	tracelog.LogSystemf(goRoutine, _NAMESPACE, "Startup", "MongoDB : Addr[%s]", straps.Strap("mgo_host"))
	tracelog.LogSystemf(goRoutine, _NAMESPACE, "Startup", "MongoDB : Database[%s]", straps.Strap("mgo_database"))
	tracelog.LogSystemf(goRoutine, _NAMESPACE, "Startup", "MongoDB : Username[%s]", straps.Strap("mgo_username"))

	hosts := strings.Split(straps.Strap("mgo_host"), ",")

	// Create the master session
	err = CreateSession(goRoutine, MASTER_SESSION, hosts, straps.Strap("mgo_database"), straps.Strap("mgo_username"), straps.Strap("mgo_password"))

	tracelog.LogSystemCompleted(goRoutine, _NAMESPACE, "Startup")
	return
}

// Shutdown systematically brings the manager down gracefully
func Shutdown(goRoutine string) (err error) {
	defer helper.CatchPanicSystem(&err, goRoutine, _NAMESPACE, "Shutdown")

	tracelog.LogSystemStarted(goRoutine, _NAMESPACE, "Shutdown")

	// Close the databases
	for _, session := range _This.Sessions {
		CloseSession(goRoutine, session.MongoSession)
	}

	tracelog.LogSystemCompleted(goRoutine, _NAMESPACE, "Shutdown")
	return
}

// CreateSession creates a connection pool for use
func CreateSession(goRoutine string, sessionName string, hosts []string, databaseName string, username string, password string) (err error) {
	defer helper.CatchPanicSystem(nil, goRoutine, _NAMESPACE, "CreateSession")

	tracelog.LogSystemStartedf(goRoutine, _NAMESPACE, "CreateSession", "SessionName[%s] Hosts[%s] DatabaseName[%s] Username[%s]", sessionName, hosts, databaseName, username)

	// Create the database object
	mongoSession := &mongoSession{
		MongoDBDialInfo: &mgo.DialInfo{
			Addrs:    hosts,
			Timeout:  60 * time.Second,
			Database: databaseName,
			Username: username,
			Password: password,
		},
	}

	// Establish the master session
	mongoSession.MongoSession, err = mgo.DialWithInfo(mongoSession.MongoDBDialInfo)
	if err != nil {
		tracelog.LogSystemErrorCompleted(err, goRoutine, _NAMESPACE, "CreateSession")
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

	tracelog.LogSystemCompleted(goRoutine, _NAMESPACE, "CreateSession")
	return
}

// CopySession get a new connection based on the master connection
func CopyMasterSession(goRoutine string) (mongoSession *mgo.Session, err error) {
	return CopySession(goRoutine, MASTER_SESSION)
}

// CopySession get a new connection based on the master connection
func CopySession(goRoutine string, useSession string) (mongoSession *mgo.Session, err error) {
	defer helper.CatchPanicSystem(nil, goRoutine, _NAMESPACE, "CopySession")

	tracelog.LogSystemStartedf(goRoutine, _NAMESPACE, "CopySession", "UseSession[%s]", useSession)

	// Find the session object
	session := _This.Sessions[useSession]

	if session == nil {
		tracelog.LogSystemf(goRoutine, _NAMESPACE, "CopySession", "Completed : ERROR : Unable To Locate Session %s", useSession)
		return
	}

	// Copy the master session
	mongoSession = session.MongoSession.Copy()

	tracelog.LogSystemCompleted(goRoutine, _NAMESPACE, "CopySession")
	return
}

// CloseSession puts the connection back into the pool
func CloseSession(goRoutine string, mongoSession *mgo.Session) {
	defer helper.CatchPanicSystem(nil, goRoutine, _NAMESPACE, "CloseSession")

	tracelog.LogSystemStarted(goRoutine, _NAMESPACE, "CloseSession")

	mongoSession.Close()

	tracelog.LogSystemCompleted(goRoutine, _NAMESPACE, "CloseSession")
}

// GetCollection returns a reference to a collection for the specified database and collection name
func GetCollection(mongoSession *mgo.Session, useDatabase string, useCollection string) (collection *mgo.Collection, err error) {
	// Access the specified collection
	return mongoSession.DB(useDatabase).C(useCollection), err
}

// CollectionExists returns true if the collection name exists in the specified database
func CollectionExists(goRoutine string, mongoSession *mgo.Session, useDatabase string, useCollection string) bool {
	database := mongoSession.DB(useDatabase)
	collections, err := database.CollectionNames()

	if err != nil {
		return false
	}

	for _, collection := range collections {
		if collection == useCollection {
			return true
		}
	}

	return false
}
