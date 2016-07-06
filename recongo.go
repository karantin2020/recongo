package recongo

// RESTful go binding functions for rethinkdb
// database using gorethink.v2 driver


// v.0.1.0

import (
  "errors"
  // "fmt"
  r "gopkg.in/dancannon/gorethink.v2"
)

// Model function

// Get function
func (c *Client) Get(id, result interface{}) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Insert the new item into the database
  query := r.DB(c.db).Table(c.table).Get(id)
  res, err := query.Run(c.session)
  defer res.Close()
  if err != nil {
    return errors.New("Item with that id doesn't exist")
  }
  // fmt.Printf("%+v\n",res)
  // fmt.Println(res.)
  err = res.One(result)
  if err != nil {
    return errors.New("Incorrect result")
  }
  return nil

}

// // Get function
func (c *Client) GetRaw(id interface{}, result *[]byte) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Insert the new item into the database
  query := r.DB(c.db).Table(c.table).Get(id)
  res, err := query.Run(c.session)
  defer res.Close()
  if err != nil {
    return errors.New("Item with that id doesn't exist")
  }
  // fmt.Printf("%+v\n",res)
  // fmt.Println(res.)
  // var ok bool
  *result, _ = res.NextResponse()
  return nil

}

// GetAll function
func (c *Client) GetAll(result interface{}) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Insert the new item into the database
  query := r.DB(c.db).Table(c.table)
  res, err := query.Run(c.session)
  defer res.Close()
  if err != nil {
    return errors.New("Item with that id doesn't exist")
  }
  err = res.All(result)
  if err != nil {
    return errors.New("Incorrect result")
  }
  return nil

}

// Create function
func (c *Client) Create(name interface{}, optArgs ...r.TableOpts) (r.WriteResponse, error) {

  var response r.WriteResponse
  if c.table == "" {
    return response, errors.New("Table wasn't selected")
  }
  // Insert the new item into the database
  return r.DB(c.db).Table(c.table).Insert(name, r.InsertOpts{ReturnChanges: true}).RunWrite(c.session)

}

func (c *Client) Find(field, val, result interface{}) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Find query
  query := r.DB(c.db).Table(c.table).Filter(r.Row.Field(field).Eq(val))
  res, err := query.Run(c.session)
  defer res.Close()
  if err != nil {
    return err
  }
  err = res.All(result)
  return err

}

func (c *Client) FindCond(f interface{}, result interface{}) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Find query
  query := r.DB(c.db).Table(c.table).Filter(f)
  res, err := query.Run(c.session)
  defer res.Close()
  if err != nil {
    return err
  }
  err = res.All(result)
  return err

}

func (c *Client) FindCount(field, val interface{}) (int,error) {

  var result int
  if c.table == "" {
    return 0, errors.New("Table wasn't selected")
  }
  // Find query
  query := r.DB(c.db).Table(c.table).Filter(r.Row.Field(field).Eq(val)).Count()
  res, err := query.Run(c.session)
  defer res.Close()
  if err != nil {
    return 0, err
  }
  err = res.One(&result)
  return result, err

}

func (c *Client) Populate(id interface{}, child string, result interface{}) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Populate query
  // prKey, _ := c.PrimKey()
  query := r.DB(c.db).Table(c.table).Get(id).Merge(func(row r.Term) r.Term {
    return r.Object(child+"s", r.DB(c.db).Table(child).GetAllByIndex(c.table+"_id", id).CoerceTo("ARRAY"))
  })
  res, err := query.Run(c.session)
  defer res.Close()
  if err != nil {
    return err
  }
  err = res.One(result)
  return err

}

func (c *Client) Update(id, arg interface{}, optArgs ...r.UpdateOpts) (r.WriteResponse, error) {
  
  var response r.WriteResponse
  if c.table == "" {
    return response, errors.New("Table wasn't selected")
  }
  // Update query
  return r.DB(c.db).Table(c.table).Get(id).Update(arg, r.UpdateOpts{ReturnChanges: true}).RunWrite(c.session)

}

func (c *Client) Delete(id interface{}) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Delete query
  _, err := r.DB(c.db).Table(c.table).Get(id).Delete().Run(c.session)
  return err
  
}

