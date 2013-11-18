package helper

import (
	"bytes"
	"fmt"
	"github.com/goinggo/tracelog"
	"net/smtp"
	"text/template"
)

//** PACKAGE VARIABLES

var (
	emailTemplate *template.Template // A template for sending emails
)

// SendEmail will send an email
func SendEmail(goRoutine string, subject string, message string) (err error) {
	tracelog.STARTEDf(goRoutine, "SendEmail", "Subject[%s]", subject)

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
		tracelog.COMPLETED_ERROR(err, goRoutine, "SendEmail")
		return err
	}

	tracelog.COMPLETED(goRoutine, "SendEmail")
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
func SendProblemEmail(goRoutine string, subject string, problems []string) (err error) {
	tracelog.STARTED(goRoutine, "SendProblemEmail")

	// Create a buffer to build the message
	message := new(bytes.Buffer)

	// Build the message
	for _, problem := range problems {
		message.WriteString(fmt.Sprintf("%s<br />", problem))
	}

	// Send the email
	SendEmail(goRoutine, subject, message.String())

	tracelog.COMPLETED(goRoutine, "SendProblemEmail")
	return err
}
