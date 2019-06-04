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
&Story{
	Steps: []Step{
		&Command{&pgproto3.Query{String: "SELECT 1;"}},
		&Response{&pgproto3.RowDescription{}},
		&Response{&pgproto3.DataRow{}},
		&Response{&pgproto3.CommandComplete{}},
		&Response{&pgproto3.ReadyForQuery{}},
	},
	Frontend: f,
}
```