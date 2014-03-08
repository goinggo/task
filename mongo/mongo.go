// Copyright 2013 Ardan Studios. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

/*
	This package provides mongo connectivity support
*/
package mongo

import (
	"encoding/json"
	"fmt"
	"github.com/goinggo/straps"
	"github.com/goinggo/task/helper"
	"github.com/goinggo/tracelog"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"strings"
	"time"
)

//** CONSTANTS

const (
	MASTER_SESSION = "master"
)

//** PACKAGE VARIABLES

var (
	singleton *mongoManager // Reference to the singleton
)

//** TYPES

type (
	// mongoManager contains dial and session information
	mongoSession struct {
		mongoDBDialInfo *mgo.DialInfo
		mongoSession    *mgo.Session
	}

	// mongoManager manages a map of session
	mongoManager struct {
		sessions map[string]*mongoSession
	}

	// MongoCall defines a type of function that can be used
	// to excecute code against MongoDB
	MongoCall func(*mgo.Collection) error
)

//** PUBLIC FUNCTIONS

// Startup brings the manager to a running state
func Startup(goRoutine string) (err error) {
	defer helper.CatchPanic(&err, goRoutine, "Startup")

	tracelog.STARTED(goRoutine, "Startup")

	// Create the Mongo Manager
	singleton = &mongoManager{
		sessions: map[string]*mongoSession{},
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
	for _, session := range singleton.sessions {
		CloseSession(goRoutine, session.mongoSession)
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
		mongoDBDialInfo: &mgo.DialInfo{
			Addrs:    hosts,
			Timeout:  60 * time.Second,
			Database: databaseName,
			Username: username,
			Password: password,
		},
	}

	// Establish the master session
	mongoSession.mongoSession, err = mgo.DialWithInfo(mongoSession.mongoDBDialInfo)
	if err != nil {
		tracelog.COMPLETED_ERROR(err, goRoutine, "CreateSession")
		return err
	}

	// Reads and writes will always be made to the master server using a
	// unique connection so that reads and writes are fully consistent,
	// ordered, and observing the most up-to-date data.
	// http://godoc.org/labix.org/v2/mgo#Session.SetMode
	mongoSession.mongoSession.SetMode(mgo.Strong, true)

	// Have the session check for errors
	// http://godoc.org/labix.org/v2/mgo#Session.SetSafe
	mongoSession.mongoSession.SetSafe(&mgo.Safe{})

	// Don't want any longer than 10 second for an operation to complete
	mongoSession.mongoSession.SetSyncTimeout(10 * time.Second)

	// Add the database to the map
	singleton.sessions[sessionName] = mongoSession

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
	session := singleton.sessions[useSession]

	if session == nil {
		err = fmt.Errorf("Unable To Locate Session %s", useSession)
		tracelog.COMPLETED_ERROR(err, goRoutine, "CopySession")
		return mongoSession, err
	}

	// Copy the master session
	mongoSession = session.mongoSession.Copy()

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
	session := singleton.sessions[useSession]

	if session == nil {
		err = fmt.Errorf("Unable To Locate Session %s", useSession)
		tracelog.COMPLETED_ERROR(err, goRoutine, "CloneSession")
		return mongoSession, err
	}

	// Clone the master session
	mongoSession = session.mongoSession.Clone()

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

// ToString converts the quer map to a string
func ToString(queryMap bson.M) string {
	json, err := json.Marshal(queryMap)
	if err != nil {
		return ""
	}
	return string(json)
}

// Execute the MongoDB literal function
func Execute(goRoutine string, mongoSession *mgo.Session, databaseName string, collectionName string, mongoCall MongoCall) (err error) {
	tracelog.STARTED(goRoutine, "Execute")

	// Capture the specified collection
	collection, err := GetCollection(mongoSession, databaseName, collectionName)
	if err != nil {

		tracelog.COMPLETED_ERROR(err, goRoutine, "Execute")
		return err
	}

	// Execute the mongo call
	err = mongoCall(collection)
	if err != nil {

		tracelog.COMPLETED_ERROR(err, goRoutine, "Execute")
		return err
	}

	tracelog.COMPLETED(goRoutine, "Execute")

	return err
}
