package pg_stories

import (
	"fmt"
	"github.com/jackc/pgx/pgproto3"
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
	expectedRaw := r.BackendMessage.Encode([]byte{})
	actualRaw := msg.Encode([]byte{})
	if len(expectedRaw) == 0 {
		return fmt.Errorf("invalid message expected")
	}
	if len(actualRaw) == 0 {
		return fmt.Errorf("invalid message received")
	}
	if expectedRaw[0] != actualRaw[0] {
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
	responseBuffer := make(chan pgproto3.BackendMessage, 100)
	timer := time.NewTimer(timeout)

	go func() {
		for {
			b, err := s.Frontend.Receive()
			if err != nil {
				errors <- err
				return
			}
			responseBuffer <- b
		}
	}()

	go func() {
		for _, step := range s.Steps {
			var e error
			switch step.(type) {
			case *Command:
				if len(responseBuffer) > 0 {
					e = fmt.Errorf("backend messages exist in buffer")
					break
				}
				msg := step.(*Command).FrontendMessage
				t.Logf("==>> %#v\n", msg)
				e = s.Frontend.Send(msg)
			case *Response:
				msg := <-responseBuffer
				t.Logf("<<== %#v\n", msg)
				e = step.(*Response).Compare(msg)
			}
			if e != nil {
				errors <- e
				return
			}
		}
		if len(responseBuffer) > 0 {
			errors <- fmt.Errorf("backend messages exist in buffer")
			return
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
