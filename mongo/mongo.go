// Copyright 2013 Ardan Studios. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
	This package provides mongo connectivity support
*/
package mongo

import (
	"fmt"
	"github.com/goinggo/straps"
	"github.com/goinggo/task/helper"
	"github.com/goinggo/tracelog"
	"labix.org/v2/mgo"
	"strings"
	"time"
)

//** CONSTANTS

// Constants
const (
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
	defer helper.CatchPanic(&err, goRoutine, "Startup")

	tracelog.STARTED(goRoutine, "Startup")

	// Create the Mongo Manager
	_This = &mongoManager{
		Sessions: map[string]*mongoSession{},
	}

	// Log the mongodb connection straps
	tracelog.TRACE(goRoutine, "Startup", "MongoDB : Addr[%s]", straps.Strap("mgo_host"))
	tracelog.TRACE(goRoutine, "Startup", "MongoDB : Database[%s]", straps.Strap("mgo_database"))
	tracelog.TRACE(goRoutine, "Startup", "MongoDB : Username[%s]", straps.Strap("mgo_username"))

	hosts := strings.Split(straps.Strap("mgo_host"), ",")

	// Create the master session
	err = CreateSession(goRoutine, MASTER_SESSION, hosts, straps.Strap("mgo_database"), straps.Strap("mgo_username"), straps.Strap("mgo_password"))

	tracelog.COMPLETED(goRoutine, "Startup")
	return err
}

// Shutdown systematically brings the manager down gracefully
func Shutdown(goRoutine string) (err error) {
	defer helper.CatchPanic(&err, goRoutine, "Shutdown")

	tracelog.STARTED(goRoutine, "Shutdown")

	// Close the databases
	for _, session := range _This.Sessions {
		CloseSession(goRoutine, session.MongoSession)
	}

	tracelog.COMPLETED(goRoutine, "Shutdown")
	return err
}

// CreateSession creates a connection pool for use
func CreateSession(goRoutine string, sessionName string, hosts []string, databaseName string, username string, password string) (err error) {
	defer helper.CatchPanic(nil, goRoutine, "CreateSession")

	tracelog.STARTEDf(goRoutine, "CreateSession", "SessionName[%s] Hosts[%s] DatabaseName[%s] Username[%s]", sessionName, hosts, databaseName, username)

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
		tracelog.COMPLETED_ERROR(err, goRoutine, "CreateSession")
		return err
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

	tracelog.COMPLETED(goRoutine, "CreateSession")
	return err
}

// CopyMasterSession makes a copy of the master session for client use
func CopyMasterSession(goRoutine string) (*mgo.Session, error) {
	return CopySession(goRoutine, MASTER_SESSION)
}

// CopySession makes a copy of the specified session for client use
func CopySession(goRoutine string, useSession string) (mongoSession *mgo.Session, err error) {
	defer helper.CatchPanic(nil, goRoutine, "CopySession")

	tracelog.STARTEDf(goRoutine, "CopySession", "UseSession[%s]", useSession)

	// Find the session object
	session := _This.Sessions[useSession]

	if session == nil {
		err = fmt.Errorf("Unable To Locate Session %s", useSession)
		tracelog.COMPLETED_ERROR(err, goRoutine, "CopySession")
		return mongoSession, err
	}

	// Copy the master session
	mongoSession = session.MongoSession.Copy()

	tracelog.COMPLETED(goRoutine, "CopySession")
	return mongoSession, err
}

// CloneMasterSession makes a clone of the master session for client use
func CloneMasterSession(goRoutine string) (*mgo.Session, error) {
	return CloneSession(goRoutine, MASTER_SESSION)
}

// CopySession makes a clone of the specified session for client use
func CloneSession(goRoutine string, useSession string) (mongoSession *mgo.Session, err error) {
	defer helper.CatchPanic(nil, goRoutine, "CopySession")

	tracelog.STARTEDf(goRoutine, "CloneSession", "UseSession[%s]", useSession)

	// Find the session object
	session := _This.Sessions[useSession]

	if session == nil {
		err = fmt.Errorf("Unable To Locate Session %s", useSession)
		tracelog.COMPLETED_ERROR(err, goRoutine, "CloneSession")
		return mongoSession, err
	}

	// Clone the master session
	mongoSession = session.MongoSession.Clone()

	tracelog.COMPLETED(goRoutine, "CloneSession")
	return mongoSession, err
}

// CloseSession puts the connection back into the pool
func CloseSession(goRoutine string, mongoSession *mgo.Session) {
	defer helper.CatchPanic(nil, goRoutine, "CloseSession")

	tracelog.STARTED(goRoutine, "CloseSession")

	mongoSession.Close()

	tracelog.COMPLETED(goRoutine, "CloseSession")
}

// GetCollection returns a reference to a collection for the specified database and collection name
func GetCollection(mongoSession *mgo.Session, useDatabase string, useCollection string) (*mgo.Collection, error) {
	return mongoSession.DB(useDatabase).C(useCollection), nil
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
