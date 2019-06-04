package pg_stories

import (
	"fmt"
	"github.com/jackc/pgx/pgproto3"
	"reflect"
	"testing"
	"time"
)

type Step interface {
	pgproto3.Message
	Step()
}

type Command struct {
	pgproto3.FrontendMessage
}

func (c *Command) Step() {}

type Response struct {
	pgproto3.BackendMessage
}

func (r *Response) Step() {}

type Story struct {
	Frontend *pgproto3.Frontend
	Steps    []Step
}

func (s *Story) Run(t *testing.T, timeout time.Duration) (err error) {

	success := make(chan bool)
	errors := make(chan error)
	timer := time.NewTimer(timeout)

	go func() {
		for _, step := range s.Steps {
			var e error
			switch step.(type) {
			case *Command:
				msg := step.(*Command).FrontendMessage
				t.Logf("==>> %#v\n", msg)
				e = s.Frontend.Send(msg)
				if e != nil {
					break
				}
			case *Response:
				expected := step.(*Response).BackendMessage
				var msg pgproto3.BackendMessage
				msg, e = s.Frontend.Receive()
				t.Logf("<<== %#v\n", msg)
				if e != nil {
					break
				}
				if reflect.TypeOf(msg) != reflect.TypeOf(expected) {
					e = fmt.Errorf("wrong type of message. expected: %T. got %T", expected, msg)
					break
				}

				switch expected.(type) {
				case *pgproto3.ErrorResponse:
					expectedCode := expected.(*pgproto3.ErrorResponse).Code
					actualCode := msg.(*pgproto3.ErrorResponse).Code
					if expectedCode != "" && expectedCode != actualCode {
						e = fmt.Errorf("expected error response with code: %s. got %s", expectedCode, actualCode)
						break
					}
				}
			}
			if e != nil {
				errors <- e
				break
			}
		}
		success <- true
	}()

	select {
	case e := <-errors:
		err = e
		break
	case <-success:
		break
	case <-timer.C:
		err = fmt.Errorf("timeout reached")
		break
	}

	timer.Stop()
	return
}
