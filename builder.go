package pg_stories

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/jackc/pgx/pgproto3"
	"io"
	"math"
	"strconv"
	"strings"
)

type UnknownMessageType struct {
	msgType byte
}

func (e *UnknownMessageType) Error() string {
	return fmt.Sprintf("unknown message type: %c", e.msgType)
}

type EmptyStoryError struct{}

func (e *EmptyStoryError) Error() string {
	return "story is empty"
}

type InvalidArgCountError struct {
	msgType byte
}

func (e *InvalidArgCountError) Error() string {
	return fmt.Sprintf("invalid argument count for message of type: %c", e.msgType)
}

type InvalidArgError struct {
	msgType byte
}

func (e *InvalidArgError) Error() string {
	return fmt.Sprintf("invalid argument in message of type: %c", e.msgType)
}

type UnexpectedTokenError struct {
	line     int
	expected []string
	actual   string
}

func (e *UnexpectedTokenError) Error() string {
	return fmt.Sprintf("unexpected token %s at line #%d. expected one of %s",
		e.actual, e.line, strings.Join(e.expected, "/"))
}

const (
	TokenBackendMessage      = "<-"
	TokenFrontendMessage     = "->"
	TokenStoryDelimiter      = "==="
	WhiteSpaceChars          = "\t "
	TokenDelimiterString     = '"'
	TokenDelimiterArrayStart = '['
	TokenDelimiterArrayEnd   = ']'
)

type tokenParser struct {
	r *bufio.Reader
}

func (t *tokenParser) readToken(start, end byte) (s string, e error) {
	if start != 0 {
		_, e = t.r.ReadString(start)
		if e != nil {
			return
		}
	}
	s, e = t.r.ReadString(end)
	if e != nil {
		return
	}
	s = strings.Trim(s, fmt.Sprintf("%c%c", start, end))
	return
}

func NewBuilder(r io.Reader, startupSeq ...Step) *Builder {
	return &Builder{r: bufio.NewReader(r), startupSeq: startupSeq}
}

type Builder struct {
	r          *bufio.Reader
	startupSeq []Step
}

func (b *Builder) parseResponse(msgType byte, parser *tokenParser) (res *Response, err error) {
	var msg pgproto3.BackendMessage
	switch msgType {
	case '1':
		msg = &pgproto3.ParseComplete{}
	case '2':
		msg = &pgproto3.BindComplete{}
	case '3':
		msg = &pgproto3.CloseComplete{}
	case 'A':
		msg = &pgproto3.NotificationResponse{}
	case 'c':
		msg = &pgproto3.CopyDone{}
	case 'f':
		msg = &pgproto3.CopyFail{}
	case 'C':
		msg = &pgproto3.CommandComplete{}
	case 'd':
		msg = &pgproto3.CopyData{}
	case 'D':
		msg = &pgproto3.DataRow{}
	case 'E':
		msg = &pgproto3.ErrorResponse{}
	case 'G':
		msg = &pgproto3.CopyInResponse{}
	case 'H':
		msg = &pgproto3.CopyOutResponse{}
	case 'I':
		msg = &pgproto3.EmptyQueryResponse{}
	case 'K':
		msg = &pgproto3.BackendKeyData{}
	case 'n':
		msg = &pgproto3.NoData{}
	case 'N':
		msg = &pgproto3.NoticeResponse{}
	case 'R':
		msg = &pgproto3.Authentication{}
	case 'S':
		msg = &pgproto3.ParameterStatus{}
	case 't':
		msg = &pgproto3.ParameterDescription{}
	case 'T':
		msg = &pgproto3.RowDescription{}
	case 'V':
		msg = &pgproto3.FunctionCallResponse{}
	case 'W':
		msg = &pgproto3.CopyBothResponse{}
	case 'Z':
		msg = &pgproto3.ReadyForQuery{}
	case 's':
		msg = &pgproto3.PortalSuspended{}
	default:
		err = &UnknownMessageType{msgType: msgType}
	}

	if err != nil {
		return
	}

	res = &Response{BackendMessage: msg}

	return
}

func (b *Builder) parseCommand(msgType byte, parser *tokenParser) (*Command, error) {
	var msg pgproto3.FrontendMessage
	switch msgType {
	case 'B':
		name, err := parser.readToken(TokenDelimiterString, TokenDelimiterString)
		if err != nil {
			return nil, err
		}
		stmt, err := parser.readToken(TokenDelimiterString, TokenDelimiterString)
		if err != nil {
			return nil, err
		}
		bind := pgproto3.Bind{DestinationPortal: name, PreparedStatement: stmt}
		params, err := parser.readToken(TokenDelimiterArrayStart, TokenDelimiterArrayEnd)
		if err != nil {
			return nil, err
		}
		if params != "" {
			for _, p := range strings.Split(params, ",") {
				i, err := strconv.ParseFloat(p, 64)
				if err != nil {
					bind.Parameters = append(bind.Parameters, []byte(p))
				} else {
					buf := make([]byte, 8)
					binary.LittleEndian.PutUint64(buf, math.Float64bits(i))
					bind.Parameters = append(bind.Parameters, buf)
				}
			}
		}
		msg = &bind
	case 'C':
		msg = &pgproto3.Close{}
	case 'D':
		t, err := parser.readToken(0, ' ')
		if err != nil {
			return nil, err
		}
		if t != "S" && t != "P" {
			return nil, &InvalidArgError{}
		}
		name, err := parser.readToken(TokenDelimiterString, TokenDelimiterString)
		if err != nil && err.Error() != "EOF" {
			return nil, err
		}
		msg = &pgproto3.Describe{Name: name, ObjectType: t[0]}
	case 'E':
		portal, err := parser.readToken(TokenDelimiterString, TokenDelimiterString)
		if err != nil {
			return nil, err
		}
		execute := pgproto3.Execute{Portal: portal}
		maxRows, err := parser.readToken(' ', ' ')
		if (err == nil || err.Error() == "EOF") && maxRows != "" {
			var i uint64
			i, err = strconv.ParseUint(maxRows, 10, 32)
			if err == nil {
				execute.MaxRows = uint32(i)
			}
		}
		if err != nil {
			return nil, err
		}
		msg = &execute
	case 'H':
		msg = &pgproto3.Flush{}
	case 'P':
		name, err := parser.readToken(TokenDelimiterString, TokenDelimiterString)
		if err != nil {
			return nil, err
		}
		query, err := parser.readToken(TokenDelimiterString, TokenDelimiterString)
		if err != nil {
			return nil, err
		}
		parse := pgproto3.Parse{Name: name, Query: query}
		params, err := parser.readToken(TokenDelimiterArrayStart, TokenDelimiterArrayEnd)
		if err != nil {
			return nil, err
		}
		if params != "" {
			for _, p := range strings.Split(params, ",") {
				i, err := strconv.ParseUint(p, 10, 32)
				if err != nil {
					return nil, &InvalidArgError{msgType: msgType}
				}
				parse.ParameterOIDs = append(parse.ParameterOIDs, uint32(i))
			}
		}
		msg = &parse
	case 'p':
		msg = &pgproto3.PasswordMessage{}
	case 'Q':
		query, err := parser.readToken(TokenDelimiterString, TokenDelimiterString)
		if err != nil {
			return nil, err
		}
		msg = &pgproto3.Query{String: query}
	case 'S':
		msg = &pgproto3.Sync{}
	case 'X':
		msg = &pgproto3.Terminate{}
	default:
		return nil, &UnknownMessageType{msgType: msgType}
	}

	return &Command{FrontendMessage: msg}, nil
}

func (b *Builder) ParseNextStep(txt string) (Step, error) {
	return b.parseStep(txt)
}

func (b *Builder) parseStep(txt string) (Step, error) {
	if len(txt) == 0 {
		return nil, fmt.Errorf("empty step definition")
	}
	if len(txt) < 4 {
		return nil, fmt.Errorf("invalid step definition")
	}
	parser := &tokenParser{bufio.NewReader(strings.NewReader(txt))}
	direction, err := parser.readToken(0, ' ')
	if err != nil {
		return nil, &UnexpectedTokenError{}
	}
	direction = strings.Trim(direction, WhiteSpaceChars)
	msgType, err := parser.readToken(0, ' ')
	if err != nil {
		if err.Error() != "EOF" || msgType == "" {
			return nil, err
		}
	}
	msgType = strings.Trim(msgType, WhiteSpaceChars)
	switch direction {
	case TokenBackendMessage:
		return b.parseResponse(msgType[0], parser)
	case TokenFrontendMessage:
		return b.parseCommand(msgType[0], parser)
	}
	return nil, fmt.Errorf("invalid diraction definition")
}

func (b *Builder) ParseNext() (story *Story, name string, err error) {
	i := 0
	for {
		var line string
		line, err = b.r.ReadString('\n')
		if err != nil {
			if err.Error() == "EOF" {
				err = nil
				break
			}
			return
		}
		i++
		line = strings.Trim(line, " \t\n")
		if line == "" {
			continue
		}
		if story == nil {
			if line[:3] != TokenStoryDelimiter {
				err = &UnexpectedTokenError{
					actual:   line[:3],
					line:     i,
					expected: []string{TokenStoryDelimiter},
				}
			}
			name = strings.Trim(line[3:], WhiteSpaceChars)
			story = &Story{Steps: b.startupSeq}
			continue
		}
		if len(line) == 3 && line == TokenStoryDelimiter {
			if len(story.Steps) == 0 {
				err = &EmptyStoryError{}
				return
			}
			break
		}
		var step Step
		step, err = b.parseStep(line)
		if err != nil {
			return
		}
		story.Steps = append(story.Steps, step)
	}
	return
}
