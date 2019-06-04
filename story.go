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

//func (s *Step) run(t *testing.T, frontend *pgproto3.Frontend) (err error) {
//
//	for _, m := range s.Messages {
//		t.Logf("==>> %#v\n", m)
//		err = frontend.Send(m)
//		if err != nil {
//			return
//		}
//	}
//
//	res := make(chan struct{})
//
//	timer := time.NewTimer(s.Timeout)
//
//	go func() {
//		for i := range s.Expect {
//			var msg pgproto3.BackendMessage
//			msg, err = frontend.Receive()
//			if err != nil {
//				break
//			}
//			t.Logf("<<== %#v\n", msg)
//			switch msg.(type) {
//			case *pgproto3.ErrorResponse:
//				switch s.Expect[i].(type) {
//				case *pgproto3.ErrorResponse:
//					s.Expect[i] = msg
//				default:
//					errResp := msg.(*pgproto3.ErrorResponse)
//					err = fmt.Errorf("error from server: %s (%s)", errResp.Message, errResp.Code)
//					res <- struct {}{}
//					return
//				}
//			default:
//				if reflect.TypeOf(msg) == reflect.TypeOf(s.Expect[i]) {
//					s.Expect[i] = msg
//				} else {
//					err = fmt.Errorf("wrong type of message. expected: %T. got %T", s.Expect[i], msg)
//					res <- struct {}{}
//					return
//				}
//			}
//		}
//		res <- struct {}{}
//		return
//	}()
//
//	select {
//	case <- res:
//		close(res)
//		timer.Stop()
//		break
//	case <-timer.C:
//		err = fmt.Errorf("timeout reached")
//		close(res)
//		break
//	}
//
//	if err != nil {
//		t.Error("Step Error:", err.Error())
//	} else {
//		t.Log("==== Step Completed")
//	}
//
//	return
//}

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
		break
	}

	timer.Stop()
	return
}
