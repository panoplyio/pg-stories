package main

import (
	"encoding/json"
	"fmt"
	"github.com/c-bata/go-prompt"
	"github.com/jackc/pgx/pgproto3"
	pgstories "github.com/panoplyio/pg-stories"
	"net"
	"regexp"
	"time"
)

var findWordsPattern = regexp.MustCompile(`"[^"]+"|[^\s]+|\[[^]]+\]`)

var storyBuilder *pgstories.Builder
var conn net.Conn

var commandCompletions = map[string]struct {
	Command     pgproto3.FrontendMessage
	suggestions [][]prompt.Suggest
}{
	"P": {Command: &pgproto3.Parse{}, suggestions: [][]prompt.Suggest{
		{{Text: `""`, Description: "Destination Statement Name"}},
		{{Text: `""`, Description: "Query Text"}},
		{{Text: "[]", Description: "Parameters"}},
	}},
	"B": {Command: &pgproto3.Parse{}, suggestions: [][]prompt.Suggest{
		{{Text: `""`, Description: "Destination Portal Name"}},
		{{Text: `""`, Description: "Source Statement Name"}},
		{{Text: "[]", Description: "Parameters"}},
	}},
	"Q": {Command: &pgproto3.Parse{}, suggestions: [][]prompt.Suggest{
		{{Text: `""`, Description: "Query Text"}},
	}},
	"D": {Command: &pgproto3.Parse{}, suggestions: [][]prompt.Suggest{
		{{Text: "S", Description: "Statement"}, {Text: "P", Description: "Portal"}},
	}},
}

func completer(in prompt.Document) []prompt.Suggest {

	if len(in.Text) > 1 && in.Text[1] == ' ' {
		if v, ok := commandCompletions[in.Text[0:1]]; ok {
			matches := findWordsPattern.FindAllStringSubmatch(in.Text, -1)
			if len(v.suggestions) >= len(matches) {
				return v.suggestions[len(matches)-1]
			}
		}
		return nil
	}

	s := []prompt.Suggest{
		{Text: "B", Description: "Bind"},
		{Text: "P", Description: "Parse"},
		{Text: "D", Description: "Describe"},
		{Text: "Q", Description: "Query"},
		{Text: "S", Description: "Sync"},
	}

	return prompt.FilterHasPrefix(s, in.GetWordBeforeCursor(), true)
}

func startupSeq() []pgstories.Step {
	startupMsg := pgproto3.StartupMessage{
		ProtocolVersion: pgproto3.ProtocolVersionNumber,
		Parameters:      make(map[string]string),
	}
	startupMsg.Parameters["user"] = "postgres"

	return []pgstories.Step{
		&pgstories.Command{FrontendMessage: &startupMsg},
		&pgstories.Response{BackendMessage: &pgproto3.Authentication{}},
		&pgstories.Response{BackendMessage: &pgproto3.ReadyForQuery{}},
	}
}

func log(txt string) {
	fmt.Print(txt)
}

func executor(in string) {
	step, err := storyBuilder.ParseNextStep("-> " + in)
	if err != nil {
		fmt.Println(err)
		prompt.NewStderrWriter().Write([]byte(err.Error()))
		return
	}

	sigKill := make(chan interface{})
	timer := time.NewTimer(time.Second * 2)
	go func() {
		t := <-timer.C
		sigKill <- t
	}()

	steps := []pgstories.Step{step}
	switch step.(type) {
	case *pgstories.Command:
		switch step.(*pgstories.Command).FrontendMessage.(type) {
		case *pgproto3.Parse, *pgproto3.Bind, *pgproto3.Describe:
		default:
			steps = append(steps, &pgstories.Response{BackendMessage: &pgproto3.ReadyForQuery{}})
		}
	}
	err = (&pgstories.Story{
		Steps: steps,
		Filter: func(message pgproto3.BackendMessage) bool {
			res, err := json.Marshal(message)
			if err == nil && res != nil {
				fmt.Println(string(res))
			} else {
				fmt.Printf("%v\n", message)
			}
			switch message.(type) {
			case *pgproto3.ReadyForQuery:
				return true
			}
			return false
		},
	}).Run(conn, conn, log, sigKill)
	if err != nil {
		fmt.Println(err.Error())
	}
}

func main() {
	var err error
	conn, err = net.Dial("tcp", "127.0.0.1:5432")
	if err != nil {
		panic(err)
	}

	fmt.Println("connecting...")

	err = (&pgstories.Story{
		Steps: startupSeq(),
		Filter: func(message pgproto3.BackendMessage) bool {
			switch message.(type) {
			case *pgproto3.ReadyForQuery, *pgproto3.Authentication:
				return true
			}
			return false
		},
	}).Run(conn, conn, log, make(chan interface{}))

	fmt.Println("connected")

	if err != nil {
		panic(err)
	}
	storyBuilder = pgstories.NewBuilder(nil)

	p := prompt.New(executor, completer)
	p.Run()

}
