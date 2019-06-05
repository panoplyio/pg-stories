package pg_stories

import (
	"encoding/binary"
	"fmt"
	"github.com/jackc/pgx/pgproto3"
	"math"
	"strings"
	"testing"
)

func createBuilder(name string, steps ...string) *Builder {
	sb := strings.Builder{}

	sb.WriteString(fmt.Sprintf("=== %s\n", name))
	for _, step := range steps {
		sb.WriteString(step + "\n")
	}
	sb.WriteString(`===` + "\n")

	return NewBuilder(strings.NewReader(sb.String()))
}

func TestBuilder_ParseNext(t *testing.T) {

	t.Run("test empty", func(t *testing.T) {
		builder := createBuilder(t.Name())
		_, _, err := builder.ParseNext()
		if err == nil {
			t.Fatalf("expected error")
		}
		switch err.(type) {
		case *EmptyStoryError:
			break
		default:
			t.Fatalf("expected: EmptyStoryError. got: %T", err)
		}
	})

	t.Run("test query", func(t *testing.T) {
		builder := createBuilder(t.Name(), `-> Q "baaa"`)
		story, _, err := builder.ParseNext()
		if err != nil {
			t.Fatal(err)
		}
		if story == nil {
			t.Fatal("story is nil")
		}
		switch story.Steps[0].(type) {
		case *Command:
			cmd := story.Steps[0].(*Command)
			switch cmd.FrontendMessage.(type) {
			case *pgproto3.Query:
				msg := cmd.FrontendMessage.(*pgproto3.Query)
				if msg.String != "baaa" {
					t.Fatalf("expected query content to be baaa. actual: %s", msg.String)
				}
			default:
				t.Fatalf("expected first step to be a command of type %T. actual: %T", &pgproto3.Query{}, cmd.FrontendMessage)
			}
		default:
			t.Fatalf("expected first step to be a command. actual: %T", story.Steps[0])
		}
	})

	t.Run("test unnamed parse", func(t *testing.T) {
		builder := createBuilder(t.Name(), `-> P "" "baaa" [1]`)
		story, _, err := builder.ParseNext()
		if err != nil {
			t.Fatal(err)
		}
		if story == nil {
			t.Fatal("story is nil")
		}
		switch story.Steps[0].(type) {
		case *Command:
			cmd := story.Steps[0].(*Command)
			switch cmd.FrontendMessage.(type) {
			case *pgproto3.Parse:
				msg := cmd.FrontendMessage.(*pgproto3.Parse)
				if msg.Query != "baaa" {
					t.Fatalf("expected query to be baaa. actual: %s", msg.Query)
				}
				if len(msg.ParameterOIDs) != 1 {
					t.Fatalf("expected ParameterOIDs to be 1 element long. actual: %d", len(msg.ParameterOIDs))
				}
				if msg.ParameterOIDs[0] != 1 {
					t.Fatalf("expected ParameterOIDs' first element to equal 1. actual: %d", msg.ParameterOIDs[0])
				}
			default:
				t.Fatalf("expected first step to be a command of type %T. actual: %T", &pgproto3.Query{}, cmd.FrontendMessage)
			}
		default:
			t.Fatalf("expected first step to be a command. actual: %T", story.Steps[0])
		}
	})

	t.Run("test named parse", func(t *testing.T) {
		builder := createBuilder(t.Name(), `-> P "stmt_name" "baaa" []`)
		story, _, err := builder.ParseNext()
		if err != nil {
			t.Fatal(err)
		}
		if story == nil {
			t.Fatal("story is nil")
		}
		switch story.Steps[0].(type) {
		case *Command:
			cmd := story.Steps[0].(*Command)
			switch cmd.FrontendMessage.(type) {
			case *pgproto3.Parse:
				msg := cmd.FrontendMessage.(*pgproto3.Parse)
				if msg.Name != "stmt_name" {
					t.Fatalf("expected stmt name to be 'stmt_name'. actual: %s", msg.Name)
				}
			default:
				t.Fatalf("expected first step to be a command of type %T. actual: %T", &pgproto3.Query{}, cmd.FrontendMessage)
			}
		default:
			t.Fatalf("expected first step to be a command. actual: %T", story.Steps[0])
		}
	})

	t.Run("test bind", func(t *testing.T) {
		builder := createBuilder(t.Name(), `-> B "" "stmt_name" [1,baa,1.2]`)
		story, _, err := builder.ParseNext()
		if err != nil {
			t.Fatal(err)
		}
		if story == nil {
			t.Fatal("story is nil")
		}
		switch story.Steps[0].(type) {
		case *Command:
			cmd := story.Steps[0].(*Command)
			switch cmd.FrontendMessage.(type) {
			case *pgproto3.Bind:
				msg := cmd.FrontendMessage.(*pgproto3.Bind)
				if msg.PreparedStatement != "stmt_name" {
					t.Fatalf("expected stmt name to be 'stmt_name'. actual: %s", msg.PreparedStatement)
				}
				i := math.Float64frombits(binary.LittleEndian.Uint64(msg.Parameters[0]))
				if i != 1 {
					t.Fatalf("expected parameter 0 to equal 1. actual %f", i)
				}
				s := string(msg.Parameters[1])
				if s != "baa" {
					t.Fatalf("expected parameter 1 to equal baa. actual %s", s)
				}
				f := math.Float64frombits(binary.LittleEndian.Uint64(msg.Parameters[2]))
				if f != 1.2 {
					t.Fatalf("expected parameter 2 to equal 1.2. actual %f", f)
				}
			default:
				t.Fatalf("expected first step to be a command of type %T. actual: %T", &pgproto3.Query{}, cmd.FrontendMessage)
			}
		default:
			t.Fatalf("expected first step to be a command. actual: %T", story.Steps[0])
		}
	})

	t.Run("test execute", func(t *testing.T) {
		builder := createBuilder(t.Name(), `-> E "portal_name" 10`)
		story, _, err := builder.ParseNext()
		if err != nil {
			t.Fatal(err)
		}
		if story == nil {
			t.Fatal("story is nil")
		}
		switch story.Steps[0].(type) {
		case *Command:
			cmd := story.Steps[0].(*Command)
			switch cmd.FrontendMessage.(type) {
			case *pgproto3.Execute:
				msg := cmd.FrontendMessage.(*pgproto3.Execute)
				if msg.Portal != "portal_name" {
					t.Fatalf("expected stmt name to be 'portal_name'. actual: %s", msg.Portal)
				}
				if msg.MaxRows != 10 {
					t.Fatalf("expected max rows to be 10. actual: %d", msg.MaxRows)
				}
			default:
				t.Fatalf("expected first step to be a command of type %T. actual: %T", &pgproto3.Query{}, cmd.FrontendMessage)
			}
		default:
			t.Fatalf("expected first step to be a command. actual: %T", story.Steps[0])
		}
	})

}
