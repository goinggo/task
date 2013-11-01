package helper

import (
	"bytes"
	"fmt"
	"github.com/goinggo/utilities/tracelog"
	"net/smtp"
	"text/template"
)

//** GLOBAL VARIABLES

var (
	emailTemplate *template.Template // A template for sending emails
)

// SendEmail will send an email
func SendEmail(goRoutine string, namespace string, subject string, message string) (err error) {
	defer CatchPanicSystem(&err, goRoutine, namespace, "SendEmail")

	tracelog.LogSystemStartedf(goRoutine, namespace, "SendEmail", "Subject[%s]", subject)

	if emailTemplate == nil {
		emailTemplate = template.Must(template.New("emailTemplate").Parse(emailScript()))
	}

	parameters := &struct {
		From    string
		To      string
		Subject string
		Message string
	}{
		EmailUserName,
		EmailTo,
		subject,
		message,
	}

	emailMessage := new(bytes.Buffer)
	emailTemplate.Execute(emailMessage, parameters)

	auth := smtp.PlainAuth("", EmailUserName, EmailPassword, EmailHost)

	err = smtp.SendMail(fmt.Sprintf("%s:%d", EmailHost, EmailPort), auth, EmailUserName, []string{EmailTo}, emailMessage.Bytes())

	if err != nil {
		tracelog.LogSystemErrorCompleted(err, goRoutine, namespace, "SendEmail")
		return err
	}

	tracelog.LogSystemCompleted(goRoutine, namespace, "SendEmail")
	return err
}

// emailScript returns a template for the email message to be sent
func emailScript() (script string) {
	return `From: {{.From}}
To: {{.To}}
Subject: {{.Subject}}
MIME-version: 1.0
Content-Type: text/html; charset="UTF-8"

<html><body>{{.Message}}</body></html>`
}

// SendProblemEmail sends an email with the slice of problems
func SendProblemEmail(goRoutine string, namespace string, subject string, problems []string) (err error) {
	defer CatchPanicSystem(&err, goRoutine, namespace, "SendProblemEmail")

	tracelog.LogSystemStarted(goRoutine, namespace, "SendProblemEmail")

	// Create a buffer to build the message
	message := new(bytes.Buffer)

	// Build the message
	for _, problem := range problems {
		message.WriteString(fmt.Sprintf("%s<br />", problem))
	}

	// Send the email
	SendEmail(goRoutine, namespace, subject, message.String())

	tracelog.LogSystemCompleted(goRoutine, namespace, "SendProblemEmail")
	return err
}
