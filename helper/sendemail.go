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
	_EmailTemplate *template.Template // A template for sending emails
)

// SendEmail will send an email
//  goRoutine The Go routine making the call
//  namespace: The namespace the call is being made from
//  subject: The subject line for the email
//  message: The message to send in the email
func SendEmail(goRoutine string, namespace string, subject string, message string) (err error) {

	defer CatchPanicSystem(&err, goRoutine, namespace, "SendEmail")

	tracelog.LogSystemf(goRoutine, namespace, "SendEmail", "Started : Subject[%s]", subject)

	if _EmailTemplate == nil {

		_EmailTemplate = template.Must(template.New("emailTemplate").Parse(_EmailScript()))
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
	_EmailTemplate.Execute(emailMessage, parameters)

	auth := smtp.PlainAuth("", EmailUserName, EmailPassword, EmailHost)

	err = smtp.SendMail(fmt.Sprintf("%s:%d", EmailHost, EmailPort), auth, EmailUserName, []string{EmailTo}, emailMessage.Bytes())

	if err != nil {

		tracelog.LogSystemf(goRoutine, namespace, "SendEmail", "Completed : ERROR :  %v", err)
		return err
	}

	tracelog.LogSystem(goRoutine, namespace, "SendEmail", "Completed")

	return err
}

// _EmailScript returns a template for the email message to be sent
func _EmailScript() (script string) {

	return `From: {{.From}}
To: {{.To}}
Subject: {{.Subject}}
MIME-version: 1.0
Content-Type: text/html; charset="UTF-8"

<html><body>{{.Message}}</body></html>`
}
