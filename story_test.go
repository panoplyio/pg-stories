package pg_stories

import (
	"github.com/jackc/pgx/pgproto3"
	"net"
	"testing"
	"time"
)

const (
	UnnamedStmt   = ""
	UnnamedPortal = ""
	NamedStmt     = "baa"
	NamedPortal   = "baa"

	ErrorInvalidSqlStatementName = "26000"
	ErrorInvalidCursorName       = "34000"

	SimpleQuery     = "SELECT * FROM (VALUES('baa')) t;"
	OneRowStatement = "SELECT * FROM (VALUES($1)) t;"
	TwoRowStatement = "SELECT * FROM (VALUES($1), ($1)) t;"
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
		&Response{&pgproto3.BackendKeyData{}},
		&Response{&pgproto3.ReadyForQuery{}},
	}
}

func filterStartupMessages(msg pgproto3.BackendMessage) bool {
	switch msg.(type) {
	case *pgproto3.ParameterStatus:
	case *pgproto3.BackendKeyData:
	case *pgproto3.NotificationResponse:
		return false
	}
	return true
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
		Filter:   filterStartupMessages,
	}, nil
}

func TestExtendedSequences(t *testing.T) {

	F_Q_Query := func(sql string) *Command {
		return &Command{&pgproto3.Query{String: sql}}
	}

	F_P_Parse := func(sql, name string, paramOIDs []uint32) *Command {
		return &Command{&pgproto3.Parse{
			Name:          name,
			ParameterOIDs: paramOIDs,
			Query:         sql,
		}}
	}

	F_B_Bind := func(stmtName, portalName string, values [][]byte) *Command {
		return &Command{&pgproto3.Bind{
			DestinationPortal: portalName,
			Parameters:        values,
			PreparedStatement: stmtName,
		}}
	}

	F_E_Execute := func(portalName string, maxRows uint32) *Command {
		return &Command{&pgproto3.Execute{
			Portal:  portalName,
			MaxRows: maxRows,
		}}
	}

	F_D_DescribeStmt := func(stmtName string) *Command {
		return &Command{&pgproto3.Describe{
			ObjectType: 'S',
			Name:       stmtName,
		}}
	}

	F_D_DescribePortal := func(portalName string) *Command {
		return &Command{&pgproto3.Describe{
			ObjectType: 'P',
			Name:       portalName,
		}}
	}

	B_1_ParseComplete := &Response{&pgproto3.ParseComplete{}}

	B_2_BindComplete := &Response{&pgproto3.BindComplete{}}

	B_Z_ReadyForQuery := &Response{&pgproto3.ReadyForQuery{}}

	B_T_RowDescription := &Response{&pgproto3.RowDescription{}}

	B_t_ParameterDescription := &Response{&pgproto3.ParameterDescription{}}

	B_D_DataRow := &Response{&pgproto3.DataRow{}}

	B_s_portalSuspended := &Response{&pgproto3.PortalSuspended{}}

	B_C_CommandComplete := &Response{&pgproto3.CommandComplete{}}

	B_E_ErrorResponse := func(code string) *Response {
		return &Response{&pgproto3.ErrorResponse{Code: code}}
	}

	F_S_Sync := &Command{&pgproto3.Sync{}}

	F_H_flush := &Command{&pgproto3.Flush{}}

	stories := []struct {
		Name  string
		Steps []Step
	}{
		{
			Name: "bind after parse",
			Steps: append(
				startupSeq(),
				F_P_Parse(OneRowStatement, UnnamedStmt, []uint32{0}),
				F_D_DescribeStmt(UnnamedStmt),
				F_S_Sync,
				B_1_ParseComplete,
				B_t_ParameterDescription,
				B_T_RowDescription,
				B_Z_ReadyForQuery,
				F_B_Bind(UnnamedStmt, UnnamedPortal, [][]byte{[]byte("baa")}),
				F_D_DescribePortal(UnnamedPortal),
				F_P_Parse(OneRowStatement, UnnamedStmt, []uint32{0}),
				F_B_Bind(UnnamedStmt, UnnamedPortal, [][]byte{[]byte("baa")}),
				F_E_Execute(UnnamedPortal, 0),
				F_S_Sync,
				B_2_BindComplete,
				B_T_RowDescription,
				B_1_ParseComplete,
				B_2_BindComplete,
				B_D_DataRow,
				B_C_CommandComplete,
				B_Z_ReadyForQuery,
			),
		},
		{
			Name: "parse after bind erases portal",
			Steps: append(
				startupSeq(),
				F_P_Parse(OneRowStatement, UnnamedStmt, []uint32{0}),
				F_D_DescribeStmt(UnnamedStmt),
				F_S_Sync,
				B_1_ParseComplete,
				B_t_ParameterDescription,
				B_T_RowDescription,
				B_Z_ReadyForQuery,
				F_B_Bind(UnnamedStmt, UnnamedPortal, [][]byte{[]byte("baa")}),
				F_D_DescribePortal(UnnamedPortal),
				F_S_Sync,
				B_2_BindComplete,
				B_T_RowDescription,
				B_Z_ReadyForQuery,
				F_P_Parse(TwoRowStatement, UnnamedStmt, []uint32{0}),
				F_S_Sync,
				B_1_ParseComplete,
				B_Z_ReadyForQuery,
				F_E_Execute(UnnamedPortal, 0),
				F_S_Sync,
				B_E_ErrorResponse("34000"),
			),
		},
		{
			Name: "parse after execute uses same ended portal",
			Steps: append(
				startupSeq(),

				F_P_Parse(OneRowStatement, UnnamedStmt, []uint32{0}),
				F_D_DescribeStmt(UnnamedStmt),
				F_S_Sync,
				B_1_ParseComplete,
				B_t_ParameterDescription,
				B_T_RowDescription,
				B_Z_ReadyForQuery,

				F_B_Bind(UnnamedStmt, UnnamedPortal, [][]byte{[]byte("baa")}),
				F_D_DescribePortal(UnnamedPortal),
				F_E_Execute(UnnamedPortal, 0),
				F_P_Parse(TwoRowStatement, UnnamedStmt, []uint32{0}),
				F_E_Execute(UnnamedPortal, 0),
				F_S_Sync,
				B_2_BindComplete,
				B_T_RowDescription,
				B_D_DataRow,
				B_C_CommandComplete,
				B_1_ParseComplete,
				B_C_CommandComplete,
				B_Z_ReadyForQuery,
			),
		},
		{
			Name: "re-bind after execute creates new portal",
			Steps: append(
				startupSeq(),

				F_P_Parse(OneRowStatement, UnnamedStmt, []uint32{0}),
				F_D_DescribeStmt(UnnamedStmt),
				F_S_Sync,
				B_1_ParseComplete,
				B_t_ParameterDescription,
				B_T_RowDescription,
				B_Z_ReadyForQuery,

				F_B_Bind(UnnamedStmt, UnnamedPortal, [][]byte{[]byte("baa")}),
				F_D_DescribePortal(UnnamedPortal),
				F_E_Execute(UnnamedPortal, 0),
				F_B_Bind(UnnamedStmt, UnnamedPortal, [][]byte{[]byte("baa")}),
				F_E_Execute(UnnamedPortal, 0),
				F_S_Sync,
				B_2_BindComplete,
				B_T_RowDescription,
				B_D_DataRow,
				B_C_CommandComplete,
				B_2_BindComplete,
				B_D_DataRow,
				B_C_CommandComplete,
				B_Z_ReadyForQuery,
			),
		},
		{
			Name: "bind without parse",
			Steps: append(
				startupSeq(),
				F_B_Bind(UnnamedStmt, UnnamedPortal, [][]byte{[]byte("baa")}),
				F_E_Execute(UnnamedPortal, 0),
				F_S_Sync,
				B_E_ErrorResponse(ErrorInvalidSqlStatementName),
			),
		},
		{
			Name: "bind unnamed stmt after sync",
			Steps: append(
				startupSeq(),
				F_P_Parse(OneRowStatement, UnnamedStmt, []uint32{0}),
				F_S_Sync,
				B_1_ParseComplete,
				B_Z_ReadyForQuery,
				F_B_Bind(UnnamedStmt, UnnamedPortal, [][]byte{[]byte("baa")}),
				F_E_Execute(UnnamedPortal, 0),
				F_S_Sync,
				B_2_BindComplete,
				B_D_DataRow,
				B_C_CommandComplete,
				B_Z_ReadyForQuery,
			),
		},
		{
			Name: "bind after simple query",
			Steps: append(
				startupSeq(),
				F_P_Parse(OneRowStatement, UnnamedStmt, []uint32{0}),
				F_Q_Query(SimpleQuery),
				B_1_ParseComplete,
				B_T_RowDescription,
				B_D_DataRow,
				B_C_CommandComplete,
				B_Z_ReadyForQuery,
				F_B_Bind(UnnamedStmt, UnnamedPortal, [][]byte{[]byte("baa")}),
				F_E_Execute(UnnamedPortal, 0),
				F_S_Sync,
				B_E_ErrorResponse(ErrorInvalidSqlStatementName),
			),
		},
		{
			Name: "bind after sync then simple query",
			Steps: append(
				startupSeq(),
				F_P_Parse(OneRowStatement, UnnamedStmt, []uint32{0}),
				F_S_Sync,
				B_1_ParseComplete,
				B_Z_ReadyForQuery,
				F_Q_Query(SimpleQuery),
				F_B_Bind(UnnamedStmt, UnnamedPortal, [][]byte{[]byte("baa")}),
				F_S_Sync,
				B_T_RowDescription,
				B_D_DataRow,
				B_C_CommandComplete,
				B_Z_ReadyForQuery,
			),
		},
		{
			Name: "bind unnamed stmt after sync then simple query",
			Steps: append(
				startupSeq(),
				F_P_Parse(OneRowStatement, UnnamedStmt, []uint32{0}),
				F_Q_Query(SimpleQuery),
				B_1_ParseComplete,
				B_T_RowDescription,
				B_D_DataRow,
				B_C_CommandComplete,
				B_Z_ReadyForQuery,
				F_B_Bind(UnnamedStmt, UnnamedPortal, [][]byte{[]byte("baa")}),
				F_E_Execute(UnnamedPortal, 0),
				F_S_Sync,
				B_E_ErrorResponse(ErrorInvalidSqlStatementName),
			),
		},
		{
			Name: "bind named stmt after simple query",
			Steps: append(
				startupSeq(),
				F_P_Parse(OneRowStatement, NamedStmt, []uint32{0}),
				F_Q_Query(SimpleQuery),
				B_1_ParseComplete,
				B_T_RowDescription,
				B_D_DataRow,
				B_C_CommandComplete,
				B_Z_ReadyForQuery,
				F_D_DescribeStmt(NamedStmt),
				F_B_Bind(NamedStmt, UnnamedPortal, [][]byte{[]byte("baa")}),
				F_E_Execute(UnnamedPortal, 0),
				F_S_Sync,
				B_t_ParameterDescription,
				B_T_RowDescription,
				B_2_BindComplete,
				B_D_DataRow,
				B_C_CommandComplete,
				B_Z_ReadyForQuery,
			),
		},
		{
			Name: "execute named portal",
			Steps: append(
				startupSeq(),
				F_P_Parse(OneRowStatement, NamedStmt, []uint32{0}),
				F_B_Bind(NamedStmt, NamedPortal, [][]byte{[]byte("baa")}),
				F_E_Execute(NamedPortal, 0),
				F_S_Sync,
				B_1_ParseComplete,
				B_2_BindComplete,
				B_D_DataRow,
				B_C_CommandComplete,
				B_Z_ReadyForQuery,
			),
		},
		{
			Name: "execute named portal after sync",
			Steps: append(
				startupSeq(),
				F_P_Parse(OneRowStatement, NamedStmt, []uint32{0}),
				F_S_Sync,
				B_1_ParseComplete,
				B_Z_ReadyForQuery,
				F_B_Bind(NamedStmt, NamedPortal, [][]byte{[]byte("baa")}),
				F_S_Sync,
				B_2_BindComplete,
				B_Z_ReadyForQuery,
				F_E_Execute(NamedPortal, 0),
				F_S_Sync,
				B_E_ErrorResponse(ErrorInvalidCursorName),
			),
		},
		{
			Name: "multiple executes",
			Steps: append(
				startupSeq(),
				F_P_Parse(TwoRowStatement, UnnamedStmt, []uint32{0}),
				F_B_Bind(UnnamedStmt, UnnamedPortal, [][]byte{[]byte("baa")}),
				F_E_Execute(UnnamedPortal, 1),
				F_E_Execute(UnnamedPortal, 1),
				F_H_flush,
				F_S_Sync,
				B_1_ParseComplete,
				B_2_BindComplete,
				B_D_DataRow,
				B_s_portalSuspended,
				B_D_DataRow,
				B_s_portalSuspended,
				B_Z_ReadyForQuery,
			),
		},
	}

	for _, s := range stories {
		t.Run(s.Name, func(t *testing.T) {
			story, err := initStory(s.Steps)
			if err != nil {
				t.Fatal(err)
			}
			err = story.Run(t, time.Second*2)
			if err != nil {
				t.Fatal(err)
			}
		})
	}

}
