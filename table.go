package recongo

import (
  "fmt"
  "errors"
  r "gopkg.in/dancannon/gorethink.v2"
)

// TableTest tests a table on client database
func (c *Client) TableTest(name string) error {
  if name == "" {
    return errors.New("Type in table name")
  }
  tables, err := c.TableList()
  if (err != nil) { return err }

  if (!stringInSlice(name, tables)) {
    return errors.New("Incorrect table name")
  }
  return nil
}

// TablePresent creates a table on client databae if it doesn't exist already
func (c *Client) TablePresent(name string) error {
  if name == "" {
    return errors.New("Type in table name")
  }
  tables, err := c.TableList()
  if (err != nil) { return err }

  if (!stringInSlice(name, tables)) {
    _, err := r.DB(c.db).TableCreate(name).RunWrite(c.session)
    if (err != nil) {
      c.Log(fmt.Sprintf("  + %v ... create failed", name))
      return err
    }
  }
  c.Log(fmt.Sprintf("  + %v", name))

  return nil
}

// TableList returns a slice of table names on the Client database
func (c *Client) TableList() ([]string, error) {
  res, err := r.DB(c.db).TableList().Run(c.session)
  if (err != nil) { return nil, err }

  tableList := []string{}
  res.All(&tableList)

  return tableList, nil
}

// LsTables returns the names of the tables in a database, and panics if the db doesn't exist
func (c *Client) TableLs(db string) ([]string, error) {
  var tables []string
  term, err := r.DB(db).TableList().Run(c.session)
  if err != nil {
    fmt.Println("TableLs failed on db", db)
    return nil, err
  }
  err = term.All(&tables)
  if err != nil {
    fmt.Println("TableLs couldn't unfold a term when processing db", db)
    return nil, err
  }
  return tables, nil
}

// PrimKey returns table's primary key
func (c *Client) PrimKey() (string, error) {
  if c.table == "" {
    return "", errors.New("Table wasn't selected")
  }
  res, err := r.DB(c.db).Table(c.table).Info().Run(c.session)
  if (err != nil) { return "", err }

  var primKeyStr struct {
    PrimKey string `gorethink:"primary_key"`
  }
  res.One(&primKeyStr)

  return primKeyStr.PrimKey, nil
}
