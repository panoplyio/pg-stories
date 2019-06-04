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

func (r *Response) Compare(msg pgproto3.BackendMessage) error {
	if reflect.TypeOf(msg) != reflect.TypeOf(r.BackendMessage) {
		return fmt.Errorf("wrong type of message. expected: %T. got %T", r.BackendMessage, msg)
	}

	switch r.BackendMessage.(type) {
	case *pgproto3.ErrorResponse:
		expectedCode := r.BackendMessage.(*pgproto3.ErrorResponse).Code
		actualCode := msg.(*pgproto3.ErrorResponse).Code
		if expectedCode != "" && expectedCode != actualCode {
			return fmt.Errorf("expected error response with code: %s. got %s", expectedCode, actualCode)
		}
	}

	return nil
}

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
				msg, e := s.Frontend.Receive()
				t.Logf("<<== %#v\n", msg)
				if e == nil {
					e = step.(*Response).Compare(msg)
				}
			}
			if e != nil {
				errors <- e
				return
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
