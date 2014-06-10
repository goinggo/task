package data

import (
	"github.com/goinggo/task/helper"
	"github.com/goinggo/task/mongo"
	"github.com/goinggo/tracelog"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"time"
)

//** CONSTANTS

const (
	JOBS_COLLECTION = "data_jobs"
)

//** TYPES

type (
	// JobDetail contains a detail for the job
	JobDetail struct {
		Task    string    `bson:"task"`
		Date    time.Time `bson:"date"`
		Details string    `bson:"details"`
	}

	// Job contains information about a new processor job
	Job struct {
		ObjectId  bson.ObjectId `bson:"_id"`
		Type      string        `bson:"type"`
		StartDate time.Time     `bson:"start_date"`
		Details   []JobDetail   `bson:"details"`
	}
)

//** PUBLIC FUNCTIONS

// CleanJobs removes old jobs from the jobs table
func CleanJobs(goRoutine string, useSession string, useDatabase string) (err error) {
	defer helper.CatchPanic(&err, goRoutine, "CleanJobs")

	tracelog.STARTEDf(goRoutine, "CleanJobs", "UseSession[%s] UseDatabase[%s]", useSession, useDatabase)

	// If it is between 12:00AM - 12:15AM remove old items
	currentTime := time.Now().UTC()

	if currentTime.Hour() == 0 && (currentTime.Minute() >= 0 && currentTime.Minute() <= 15) {
		tracelog.TRACE(goRoutine, "CleanJobs", "Info : Performing Clean Job : %v", currentTime)

		// Grab a mongo session
		mongoSession, err := mongo.CopySession(goRoutine, useSession)

		if err != nil {
			tracelog.COMPLETED_ERROR(err, goRoutine, "CleanJobs")
			return err
		}

		defer mongo.CloseSession(goRoutine, mongoSession)

		// Access the jobs collection
		collection, err := mongo.GetCollection(mongoSession, useDatabase, "jobs")

		if err != nil {
			tracelog.COMPLETED_ERROR(err, goRoutine, "CleanJobs")
			return err
		}

		removeDate := currentTime.AddDate(0, 0, -3)
		query := bson.M{"startDate": bson.M{"$lt": removeDate}}

		if _, err = collection.RemoveAll(query); err != nil {
			tracelog.COMPLETED_ERROR(err, goRoutine, "CleanJobs")
			return err
		}
	}

	tracelog.COMPLETED(goRoutine, "CleanJobs")
	return err
}

// StartJob inserts a new job record into mongodb
func StartJob(goRoutine string, useSession string, useDatabase string, jobType string) (job *Job, err error) {
	defer helper.CatchPanic(&err, goRoutine, "StartJob")

	tracelog.STARTEDf(goRoutine, "StartJob", "UseSession[%s] UseDatabase[%s] JobType[%s]", useSession, useDatabase, jobType)

	// Grab a mongo session
	mongoSession, err := mongo.CopySession(goRoutine, useSession)
	if err != nil {
		tracelog.COMPLETED_ERROR(err, goRoutine, "StartJob")
		return job, err
	}

	defer mongo.CloseSession(goRoutine, mongoSession)

	// Access the jobs collection
	collection, err := mongo.GetCollection(mongoSession, useDatabase, JOBS_COLLECTION)
	if err != nil {
		tracelog.COMPLETED_ERROR(err, goRoutine, "StartJob")
		return job, err
	}

	// Create a new job
	job = &Job{
		ObjectId:  bson.NewObjectId(),
		Type:      jobType,
		StartDate: time.Now(),
	}

	// Insert the job
	err = collection.Insert(job)
	if err != nil {
		tracelog.COMPLETED_ERROR(err, goRoutine, "StartJob")
		return job, err
	}

	tracelog.COMPLETED(goRoutine, "StartJob")
	return job, err
}

// EndJob updates the specified job document with end date and status
func EndJob(goRoutine string, useSession string, useDatabase string, result string, job *Job) (err error) {
	defer helper.CatchPanic(&err, goRoutine, "EndJob")

	tracelog.STARTEDf(goRoutine, "EndJob", "UseSession[%s] UseDatabase[%s] Id[%v] Result[%s]", useSession, useDatabase, job.ObjectId, result)

	// Grab a mongo session
	mongoSession, err := mongo.CopySession(goRoutine, useSession)
	if err != nil {
		tracelog.COMPLETED_ERROR(err, goRoutine, "EndJob")
		return err
	}

	defer mongo.CloseSession(goRoutine, mongoSession)

	// Access the jobs collection
	collection, err := mongo.GetCollection(mongoSession, useDatabase, JOBS_COLLECTION)
	if err != nil {
		tracelog.COMPLETED_ERROR(err, goRoutine, "EndJob")
		return err
	}

	// Create the update document
	update := bson.M{"$set": bson.M{"endDate": time.Now(), "result": result}}

	// Update the job
	err = collection.UpdateId(job.ObjectId, update)
	if err != nil {
		tracelog.COMPLETED_ERROR(err, goRoutine, "EndJob")
		return err
	}

	tracelog.COMPLETED(goRoutine, "EndJob")
	return err
}

// AddJobDetail captures a session and then writes a job detail record to the specifed job
func AddJobDetail(goRoutine string, useSession string, useDatabase string, job *Job, task string, details string) (err error) {
	defer helper.CatchPanic(&err, goRoutine, "AddJobDetail")

	// Grab a mongo session
	mongoSession, err := mongo.CopySession(goRoutine, useSession)
	if err != nil {
		tracelog.COMPLETED_ERROR(err, goRoutine, "AddJobDetail")
		return err
	}

	defer mongo.CloseSession(goRoutine, mongoSession)

	return AddJobDetailWithSession(goRoutine, mongoSession, useDatabase, job, task, details)
}

// AddJobDetailWithSession captures a session and then writes a job detail record to the specifed job
func AddJobDetailWithSession(goRoutine string, mongoSession *mgo.Session, useDatabase string, job *Job, task string, details string) (err error) {
	defer helper.CatchPanic(&err, goRoutine, "AddJobDetailWithSession")

	tracelog.STARTEDf(goRoutine, "AddJobDetailWithSession", "UseDatabase[%s] Id[%v] Task[%v] Details[%s]", useDatabase, job.ObjectId, task, details)

	// Access the jobs collection
	collection, err := mongo.GetCollection(mongoSession, useDatabase, JOBS_COLLECTION)
	if err != nil {
		tracelog.COMPLETED_ERROR(err, goRoutine, "AddJobDetailWithSession")
		return err
	}

	// Create a new job
	jobDetail := &JobDetail{
		Task:    task,
		Date:    time.Now(),
		Details: details,
	}

	// Create the update document
	update := bson.M{"$addToSet": bson.M{"details": jobDetail}}

	// Update the job
	_, err = collection.UpsertId(job.ObjectId, update)
	if err != nil {
		tracelog.COMPLETED_ERROR(err, goRoutine, "AddJobDetailWithSession")
		return err
	}

	tracelog.COMPLETED(goRoutine, "AddJobDetailWithSession")
	return err
}
