package recongo

import (
  "errors"
  "fmt"
  r "gopkg.in/dancannon/gorethink.v2"
)

// DBPresent creates a database in the rethink cluster if it doesn't exist already
func (c Client) DBPresent() error {
  if (!stringInSlice(c.db, c.DBList())) {

    _, err := r.DBCreate(c.db).RunWrite(c.session)
    if (err != nil) {
      c.Log(fmt.Sprintf("%v ... create failed", c.db))
      return err
    }
  }
  c.Log(fmt.Sprintf("+ %v", c.db))
  return nil
}

// DBList returns a slice of cluster database names
func (c Client) DBList() []string {
  res, _ := r.DBList().Run(c.session)
  dbs := []string{}
  res.All(&dbs)
  return dbs
}

func (c Client) DB() string {
  return c.db
}

// TableTest tests a table on client database
func (c *Client) DBTest(name string) error {
  if name == "" {
    return errors.New("Type in db name")
  }
  dbs := c.DBList()
  
  if (!stringInSlice(name, dbs)) {
    return errors.New("Incorrect db name")
  }
  return nil
}