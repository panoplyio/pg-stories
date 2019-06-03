package pg_stories

import (
	"github.com/jackc/pgx/pgproto3"
	"net"
	"testing"
	"time"
)

const (
	NamedStmt   = "baa"
	NamedPortal = "baa"
)

func startupSeq() []Step {
	startupMsg := pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      make(map[string]string),
	}
	startupMsg.Parameters["user"] = "postgres"

	return []Step{
		&Command{&startupMsg},
		&Response{&pgproto3.Authentication{}},
		&Response{&pgproto3.ParameterStatus{}},
		&Response{&pgproto3.ParameterStatus{}},
		&Response{&pgproto3.ParameterStatus{}},
		&Response{&pgproto3.ParameterStatus{}},
		&Response{&pgproto3.ParameterStatus{}},
		&Response{&pgproto3.ParameterStatus{}},
		&Response{&pgproto3.ParameterStatus{}},
		&Response{&pgproto3.ParameterStatus{}},
		&Response{&pgproto3.ParameterStatus{}},
		&Response{&pgproto3.ParameterStatus{}},
		&Response{&pgproto3.ParameterStatus{}},
		&Response{&pgproto3.BackendKeyData{}},
		&Response{&pgproto3.ReadyForQuery{}},
	}
}

func initStory(steps []Step) (*Story, error) {
	conn, err := net.Dial("tcp", "127.0.0.1:5432")
	if err != nil {
		return nil, err
	}

	frontend, err := pgproto3.NewFrontend(conn, conn)
	if err != nil {
		return nil, err
	}
	return &Story{
		Steps:    steps,
		Frontend: frontend,
	}, nil
}

func TestExtendedSequences(t *testing.T) {

	_F_Q_query := &Command{&pgproto3.Query{String: "SELECT * FROM (VALUES('baa')) t;"}}

	_F_P_parse := &Command{&pgproto3.Parse{
		Name:          "",
		ParameterOIDs: []uint32{0},
		Query:         "SELECT * FROM (VALUES($1)) t;",
	}}

	_F_P_parseMultiRow := &Command{&pgproto3.Parse{
		Name:          "",
		ParameterOIDs: []uint32{0},
		Query:         "SELECT * FROM (VALUES($1), ($1)) t;",
	}}

	_F_P_parseNamed := &Command{&pgproto3.Parse{
		Name:          NamedStmt,
		ParameterOIDs: []uint32{0},
		Query:         "SELECT * FROM (VALUES($1)) t;",
	}}

	_F_B_bind := &Command{&pgproto3.Bind{
		DestinationPortal: "",
		Parameters:        [][]byte{[]byte("baa")},
		PreparedStatement: "",
	}}

	_F_B_bindNamedStmt := &Command{&pgproto3.Bind{
		DestinationPortal: "",
		Parameters:        [][]byte{[]byte("baa")},
		PreparedStatement: NamedStmt,
	}}

	_F_B_bindNamedPortal := &Command{&pgproto3.Bind{
		DestinationPortal: NamedPortal,
		Parameters:        [][]byte{[]byte("baa")},
		PreparedStatement: NamedStmt,
	}}

	_F_E_execute := &Command{&pgproto3.Execute{}}

	_F_E_executeSingleRow := &Command{&pgproto3.Execute{MaxRows: 1}}

	_F_E_executeNamed := &Command{&pgproto3.Execute{Portal: NamedPortal}}

	_B_1_parseComplete := &Response{&pgproto3.ParseComplete{}}

	_B_2_bindComplete := &Response{&pgproto3.BindComplete{}}

	_B_Z_readyForQuery := &Response{&pgproto3.ReadyForQuery{}}

	_B_T_rowDescription := &Response{&pgproto3.RowDescription{}}

	_B_D_dataRow := &Response{&pgproto3.DataRow{}}

	_B_s_portalSuspended := &Response{&pgproto3.PortalSuspended{}}

	_B_C_commandComplete := &Response{&pgproto3.CommandComplete{}}

	_B_E_errorResponse := &Response{&pgproto3.ErrorResponse{}}

	_F_S_sync := &Command{&pgproto3.Sync{}}

	_F_H_flush := &Command{&pgproto3.Flush{}}

	stories := []struct {
		Name  string
		Steps []Step
	}{
		{
			Name: "bind after parse",
			Steps: append(
				startupSeq(),
				_F_P_parse,
				_F_S_sync,
				_B_1_parseComplete,
				_B_Z_readyForQuery,
				_F_B_bind,
				_F_P_parse,
				_F_B_bind,
				_F_E_execute,
				_F_S_sync,
				_B_2_bindComplete,
				_B_1_parseComplete,
				_B_2_bindComplete,
				_B_D_dataRow,
				_B_C_commandComplete,
				_B_Z_readyForQuery,
			),
		},
		{
			Name: "bind without parse",
			Steps: append(
				startupSeq(),
				_F_B_bind,
				_F_E_execute,
				_F_S_sync,
				_B_E_errorResponse,
			),
		},
		{
			Name: "bind unnamed stmt after sync",
			Steps: append(
				startupSeq(),
				_F_P_parse,
				_F_S_sync,
				_B_1_parseComplete,
				_B_Z_readyForQuery,
				_F_B_bind,
				_F_E_execute,
				_F_S_sync,
				_B_2_bindComplete,
				_B_D_dataRow,
				_B_C_commandComplete,
				_B_Z_readyForQuery,
			),
		},
		{
			Name: "bind after simple query",
			Steps: append(
				startupSeq(),
				_F_P_parse,
				_F_Q_query,
				_B_1_parseComplete,
				_B_T_rowDescription,
				_B_D_dataRow,
				_B_C_commandComplete,
				_B_Z_readyForQuery,
				_F_B_bind,
				_F_E_execute,
				_F_S_sync,
				_B_E_errorResponse,
			),
		},
		{
			Name: "bind after sync then simple query",
			Steps: append(
				startupSeq(),
				_F_P_parse,
				_F_S_sync,
				_B_1_parseComplete,
				_F_Q_query,
				_F_B_bind,
				_F_S_sync,
				_B_E_errorResponse,
			),
		},
		{
			Name: "bind unnamed stmt after sync then simple query",
			Steps: append(
				startupSeq(),
				_F_P_parse,
				_F_Q_query,
				_B_1_parseComplete,
				_B_T_rowDescription,
				_B_D_dataRow,
				_B_C_commandComplete,
				_B_Z_readyForQuery,
				_F_B_bind,
				_F_E_execute,
				_F_S_sync,
				_B_E_errorResponse,
			),
		},
		{
			Name: "bind named stmt after simple query",
			Steps: append(
				startupSeq(),
				_F_P_parseNamed,
				_F_Q_query,
				_B_1_parseComplete,
				_B_C_commandComplete,
				_B_Z_readyForQuery,
				_F_B_bindNamedStmt,
				_F_E_execute,
				_F_S_sync,
				_B_2_bindComplete,
				_B_D_dataRow,
				_B_C_commandComplete,
				_B_Z_readyForQuery,
			),
		},
		{
			Name: "execute named portal",
			Steps: append(
				startupSeq(),
				_F_P_parseNamed,
				_F_S_sync,
				_B_1_parseComplete,
				_F_B_bindNamedPortal,
				_F_E_executeNamed,
				_F_S_sync,
				_B_2_bindComplete,
				_B_D_dataRow,
				_B_C_commandComplete,
				_B_Z_readyForQuery,
			),
		},
		{
			Name: "execute named portal after sync",
			Steps: append(
				startupSeq(),
				_F_P_parseNamed,
				_F_S_sync,
				_B_1_parseComplete,
				_B_Z_readyForQuery,
				_F_B_bindNamedPortal,
				_F_S_sync,
				_B_2_bindComplete,
				_B_Z_readyForQuery,
				_F_E_executeNamed,
				_F_S_sync,
				_B_E_errorResponse,
			),
		},
		{
			Name: "simple query after execute without sync",
			Steps: append(
				startupSeq(),
				_F_P_parse,
				_F_B_bind,
				_F_E_execute,
				_B_1_parseComplete,
				_B_2_bindComplete,
				_B_D_dataRow,
				_B_C_commandComplete,
				_B_C_commandComplete,
				_B_Z_readyForQuery,
			),
		},
		{
			Name: "multiple executes",
			Steps: append(
				startupSeq(),
				_F_P_parseMultiRow,
				_F_B_bind,
				_F_E_executeSingleRow,
				_F_E_executeSingleRow,
				_F_H_flush,
				_F_S_sync,
				_B_1_parseComplete,
				_B_2_bindComplete,
				_B_D_dataRow,
				_B_s_portalSuspended,
				_B_D_dataRow,
				_B_s_portalSuspended,
				_B_Z_readyForQuery,
			),
		},
	}

	for _, s := range stories {
		t.Run(s.Name, func(t *testing.T) {
			story, err := initStory(s.Steps)
			if err != nil {
				t.Fatal(err)
			}
			err = story.Run(t, time.Second*5)
			if err != nil {
				t.Fatal(err)
			}
		})
	}

}
