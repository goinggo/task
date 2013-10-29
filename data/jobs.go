package data

import (
	"github.com/goinggo/task/helper"
	"github.com/goinggo/task/mongo"
	"github.com/goinggo/utilities/tracelog"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
	"time"
)

//** CONSTANTS

const (
	JOBS_COLLECTION = "data_jobs"
)

//** NEW TYPES

type JobDetail struct {
	Task    string    `bson:"task"`
	Date    time.Time `bson:"date"`
	Details string    `bson:"details"`
}

// Job contains information about a new processor job
type Job struct {
	ObjectId  bson.ObjectId `bson:"_id"`
	Type      string        `bson:"type"`
	StartDate time.Time     `bson:"start_date"`
	Details   []JobDetail   `bson:"details"`
}

// CleanJobs removes old jobs from the jobs table
//  goRoutine: The name of the routine making the call
//  useSession: The mongo session to use
//  useDatabase: The name of the database to use
func CleanJobs(goRoutine string, useSession string, useDatabase string) (err error) {
	defer helper.CatchPanicSystem(&err, goRoutine, "data", "CleanJobs")

	tracelog.LogSystemf(goRoutine, "data", "CleanJobs", "Started : UseSession[%s] UseDatabase[%s]", useSession, useDatabase)

	// If it is between 12:00AM - 12:05AM remove old items
	currentTime := time.Now().UTC()

	if currentTime.Hour() == 0 && (currentTime.Minute() >= 0 && currentTime.Minute() <= 5) {
		tracelog.LogSystemf(goRoutine, "data", "CleanJobs", "Info : Performing Clean Job : %v", currentTime)

		// Grab a mongo session
		mongoSession, err := mongo.CopySession(goRoutine, useSession)

		if err != nil {
			tracelog.LogSystemf(goRoutine, "data", "CleanJobs", "Completed : ERROR : %s", err)
			return err
		}

		defer mongo.CloseSession(goRoutine, mongoSession)

		// Access the jobs collection
		collection, err := mongo.GetCollection(mongoSession, useDatabase, "jobs")

		if err != nil {
			tracelog.LogSystemf(goRoutine, "data", "CleanJobs", "Completed : ERROR : %s", err)
			return err
		}

		removeDate := currentTime.AddDate(0, 0, -3)
		query := bson.M{"startDate": bson.M{"$lt": removeDate}}

		if _, err = collection.RemoveAll(query); err != nil {
			tracelog.LogSystemf(goRoutine, "data", "CleanJobs", "Completed : ERROR : %s", err)
			return err
		}
	}

	tracelog.LogSystem(goRoutine, "data", "CleanJobs", "Completed")
	return err
}

// StartJob inserts a new job record into mongodb
//  goRoutine: The name of the routine making the call
//  useSession: The database used to create the session
//  useDatabase: The name of the database to use
//  jobType: The type of job being started
func StartJob(goRoutine string, useSession string, useDatabase string, jobType string) (job *Job, err error) {
	defer helper.CatchPanicSystem(&err, goRoutine, "data", "StartJob")

	tracelog.LogSystemf(goRoutine, "data", "StartJob", "Started : UseSession[%s] UseDatabase[%s] JobType[%s]", useSession, useDatabase, jobType)

	// Grab a mongo session
	mongoSession, err := mongo.CopySession(goRoutine, useSession)

	if err != nil {
		tracelog.LogSystemf(goRoutine, "data", "StartJob", "Completed : ERROR : %s", err)
		return job, err
	}

	defer mongo.CloseSession(goRoutine, mongoSession)

	// Access the jobs collection
	collection, err := mongo.GetCollection(mongoSession, useDatabase, JOBS_COLLECTION)

	if err != nil {
		tracelog.LogSystemf(goRoutine, "data", "StartJob", "Completed : ERROR : %s", err)
		return job, err
	}

	// Create a new job
	job = &Job{
		ObjectId:  bson.NewObjectId(),
		Type:      jobType,
		StartDate: time.Now(),
	}

	// Insert the job
	if err = collection.Insert(job); err != nil {
		tracelog.LogSystemf(goRoutine, "data", "StartJob", "Completed : ERROR : %s", err)
		return job, err
	}

	tracelog.LogSystem(goRoutine, "data", "StartJob", "Completed")
	return job, err
}

// EndJob updates the specified job document with end date and status
//  goRoutine: The name of the routine making the call
//  useSession: The database used to create the session
//  useDatabase: The name of the database to use
//  result: A message about the disposition of the job
//  job: The job to end
func EndJob(goRoutine string, useSession string, useDatabase string, result string, job *Job) (err error) {
	defer helper.CatchPanicSystem(&err, goRoutine, "data", "EndJob")

	tracelog.LogSystemf(goRoutine, "data", "EndJob", "Started : UseSession[%s] UseDatabase[%s] Id[%v] Result[%s]", useSession, useDatabase, job.ObjectId, result)

	// Grab a mongo session
	mongoSession, err := mongo.CopySession(goRoutine, useSession)

	if err != nil {
		tracelog.LogSystemf(goRoutine, "data", "EndJob", "Completed : ERROR : %s", err)
		return err
	}

	defer mongo.CloseSession(goRoutine, mongoSession)

	// Access the jobs collection
	collection, err := mongo.GetCollection(mongoSession, useDatabase, JOBS_COLLECTION)

	if err != nil {
		tracelog.LogSystemf(goRoutine, "data", "EndJob", "Completed : ERROR : %s", err)
		return err
	}

	// Create the update document
	update := bson.M{"$set": bson.M{"endDate": time.Now(), "result": result}}

	// Update the job
	err = collection.UpdateId(job.ObjectId, update)

	if err != nil {
		tracelog.LogSystemf(goRoutine, "data", "EndJob", "Completed : ERROR : %s", err)
		return err
	}

	tracelog.LogSystem(goRoutine, "data", "EndJob", "Completed")
	return err
}

// AddJobDetail captures a session and then writes a job detail record to the specifed job
//  goRoutine: The name of the routine making the call
//  useSession: The name of the session to use
//  useDatabase: The name of the database to use
//  job: The job to update
//  task: The task being performed
//  details: The details around the task
func AddJobDetail(goRoutine string, useSession string, useDatabase string, job *Job, task string, details string) (err error) {
	defer helper.CatchPanicSystem(&err, goRoutine, "data", "AddJobDetail")

	// Grab a mongo session
	mongoSession, err := mongo.CopySession(goRoutine, useSession)

	if err != nil {
		tracelog.LogSystemf(goRoutine, "data", "AddJobDetail", "Completed : ERROR : %s", err)
		return err
	}

	defer mongo.CloseSession(goRoutine, mongoSession)

	return AddJobDetailWithSession(goRoutine, mongoSession, useDatabase, job, task, details)
}

// AddJobDetailWithSession captures a session and then writes a job detail record to the specifed job
//  goRoutine: The name of the routine making the call
//  mongoSession: The database used to create the session
//  useDatabase: The name of the database to use
//  job: The job to update
//  task: The task being performed
//  details: The details around the task
func AddJobDetailWithSession(goRoutine string, mongoSession *mgo.Session, useDatabase string, job *Job, task string, details string) (err error) {
	defer helper.CatchPanicSystem(&err, goRoutine, "data", "AddJobDetailWithSession")

	tracelog.LogSystemf(goRoutine, "data", "AddJobDetailWithSession", "Started : UseDatabase[%s] Id[%v] Task[%v] Details[%s]", useDatabase, job.ObjectId, task, details)

	// Access the jobs collection
	collection, err := mongo.GetCollection(mongoSession, useDatabase, JOBS_COLLECTION)

	if err != nil {
		tracelog.LogSystemf(goRoutine, "data", "AddJobDetailWithSession", "Completed : ERROR : %s", err)
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
		tracelog.LogSystemf(goRoutine, "data", "AddJobDetailWithSession", "Completed : ERROR : %s", err)
		return err
	}

	tracelog.LogSystem(goRoutine, "data", "AddJobDetailWithSession", "Completed")
	return err
}
