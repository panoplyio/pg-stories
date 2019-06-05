# pg-stories

I created this package to test backend implementation postgres protocol.  
It is heavily dependant on the great [jackc/pgx](https://github.com/jackc/pgx) package.

### Main Concepts

#### `Step`
A `Step` can be one of:
 - `*Command`  
 Consists of `FrontendMessage` and will cause the test to send this message to backend.  
**Example**:
   ```go
   &Command{&pgproto3.Query{String: "SELECT 1;"}}
   ```
 - `*Response`  
 Consists of `BackendMessage` and will cause the test to wait for message from backend 
 and compare between the provided and received messages.  
**Example**:
   ```go
   &Response{&pgproto3.ReadyForQuery{}}
   ```


#### `*Story`
Story contains a sequence of `Step` and requires a `Frontend` to run the steps upon.  
Each step will be either sent to backend or be compared to received message from backend, 
according to it's type.
##### Example
```go
func TestExample(t *testing.T) {
    story := &Story{
        Steps: []Step{
            &Command{&pgproto3.Query{String: "SELECT 1;"}},
            &Response{&pgproto3.RowDescription{}},
            &Response{&pgproto3.DataRow{}},
            &Response{&pgproto3.CommandComplete{}},
            &Response{&pgproto3.ReadyForQuery{}},
        },
        Frontend: f,
    }
    sigKill := make(chan interface{})
    timer := time.NewTimer(time.Second * 2)
    go func() {
        <-timer.C
        sigKill <- fmt.Errorf("timeout")
    }()
    err = story.Run(t, sigKill)
    if err != nil {
    	timer.Stop()
        t.Fatal(err)
    }
}
```

### Story Transcript (WIP)
You can also define stories using simple text files. Currently all backend and frontend messages
that don't require parameters are working and some are supporting parameters.
##### Notes  
- Each line of the file can contain only one step, start order or end order
#### DSL
##### Story
- `=== $1`  
   Signals start of story.  
   **Params**
   1. The name / description of the story. Will be returned from the parser.
- `===`  
   Signals end of story.
##### Step
Each line that define a step **must** start with either `->` for command (frontend message) 
or `<-` for response (backend message)  
Responses not yet accepting values so you can define them as expected response 
and they will be evaluated without checking returned values.
  
__Commands__:
- `-> Q "$1"` - (Query)  
   **Params**  
   1. Query string.  
   **Example**  
   `Q "SELECT 1;"`
- `-> P "$1" "$2" [$3]` - (Parse)    
   **Params**  
   1. Destination prepared statement name. Empty string defines unnamed statement.
   2. Query string.
   3. Comma separated parameter OIDs  
   **Example**  
   `-> P "stmt1" "SELECT * FROM (VALUES($1),($2)) t;" [0,2]`
- `-> B "$1" "$2" [$3]` - (Bind)  
  **Params**
  1. Destination portal name. Empty string defines unnamed portal.
  2. Source prepared statement. Empty string targets unnamed statement.
  3. Comma separated parameter values  
  **Example**  
  `-> B "portal1" "stmt1" [1,foo]`
- `-> D $1 "$2"` - (Describe)  
    **Params**
    1. Object type. Can be either `S` for statement or `P` for portal.
    2. Name of the Object  
    **Example**
    `-> D S "stmt1"`  
 - `-> E "$1" $2` - (Execute)  
    **Params**  
    1. Portal name. Empty string targets unnamed portal.
    2. Max rows. 0 for unlimited.
 - `-> S` - (Sync)
 - `-> H` - (Flush)
 
 __Responses__:  
 - `<- 1` - (ParseComplete)
 - `<- 2` - (BindComplete)
 - `<- C` - (CommandComplete)
 - `<- T` - (RowDescription)
 - `<- t` - (ParameterDescription)
 - `<- D` - (DataRow)
 - `<- E` - (ErrorResponse)
 - `<- Z` - (ReadyForQuery)
 
 __Full Example__:
 ```
 === execute named portal
-> P "stmt_name" "SELECT * FROM (VALUES($1))" [0]
-> B "portal_name" "stmt_name" [foo]
-> E "portal_name" 0
-> S
<- 1
<- 2
<- D
<- C
<- Z
===
```
