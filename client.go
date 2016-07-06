package recongo

import (
  "fmt"
  "errors"
  r "gopkg.in/dancannon/gorethink.v2"
)

// Connection holds info for connecting to a rethinkdb cluster
type Connection struct {
  Addr string
  DB string
}

// Client manages a rethinkdb connection (scoped to a particular database)
type Client struct {
  session *r.Session
  term r.Term
  db string
  table string
  LogOutput bool
  indexListCache map[string][]string
  tableListCache []string
}

// NewClient creates a new Client from a Connection
func NewClient(conn Connection) (*Client, error) {
  session, err := r.Connect(r.ConnectOpts{
    Address: conn.Addr,
    Database: conn.DB,
    MaxOpen:  40,
  })
  if (err != nil) { return nil, errors.New("Couldn't connect to rethinkdb at "+conn.Addr)}
  res, _ := r.DBList().Run(session)
  dbs := []string{}
  res.All(&dbs)
  if (!stringInSlice(conn.DB, dbs)) {
    return nil, errors.New(conn.DB+" is wrong db name. Please type correct db name")
  }
  return &Client{
    session: session,
    term: r.DB(conn.DB),
    db: conn.DB,
    LogOutput: false,
    indexListCache: map[string][]string{},
    tableListCache: []string{},
  }, nil
}

// Log conditionally prints to the standard-out if client.LogOutput is true
func (c *Client) Log(f string) {
  if c.LogOutput {
    fmt.Println(f)
  }
}

// Close db session
func (c *Client) Table(name string) *Client {
  if c.TableTest(name) != nil {
    c.table = ""
  } else {
    c.table = name
  }
  return c
}

// Close db session
func (c *Client) Close(optArgs ...r.CloseOpts) error {
  return c.session.Close()
}

// TableTree returns a map with all the databases and tabels
func (c *Client) DBTableTree() map[string][]string {
  result := make(map[string][]string)

  for _, database := range c.DBList() {
    tables, _ := c.TableLs(database)
    result[database] = append(result[database], tables...)
  }

  return result
}