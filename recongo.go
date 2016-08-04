package recongo

// RESTful CRUD go binding functions for rethinkdb
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
  res, err := query.Run(c.Session)
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

// Get function
func (c *Client) GetRaw(id interface{}, result *[]byte) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Insert the new item into the database
  query := r.DB(c.db).Table(c.table).Get(id)
  res, err := query.Run(c.Session)
  if err != nil {
    return errors.New("Item with that id doesn't exist")
  }
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
  res, err := query.Run(c.Session)
  if err != nil {
    return errors.New("Item with that id doesn't exist")
  }
  err = res.All(result)
  if err != nil {
    return errors.New("Incorrect result")
  }
  return nil

}

// GetAll function
func (c *Client) GetAllRaw(result *[]byte) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Insert the new item into the database
  query := r.DB(c.db).Table(c.table)
  res, err := query.Run(c.Session)
  if err != nil {
    return errors.New("Item with that id doesn't exist")
  }
  *result, _ = res.NextResponse()
  return nil

}

// Create function
func (c *Client) Create(name interface{}, optArgs ...r.TableOpts) (r.WriteResponse, error) {

  var response r.WriteResponse
  if c.table == "" {
    return response, errors.New("Table wasn't selected")
  }
  // Insert the new item into the database
  return r.DB(c.db).Table(c.table).Insert(name, r.InsertOpts{Durability: "hard", ReturnChanges: true}).RunWrite(c.Session)

}

func (c *Client) Add(idName string, id interface{}, 
    childTable string, child interface{}, 
    optArgs ...r.TableOpts) (r.WriteResponse, error) {

  var response r.WriteResponse
  if c.table == "" {
    return response, errors.New("Table wasn't selected")
  }
  // Add query
  return r.Table(c.table).Get(id).Branch(
    r.DB(c.db).Table(childTable).Insert(r.Object(idName + "_id", id).Merge(child), 
        r.InsertOpts{Durability: "hard", ReturnChanges: true}),
    r.Error("Didn't find such id")).RunWrite(c.Session)

}

func (c *Client) Find(field, val, result interface{}) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Find query
  query := r.DB(c.db).Table(c.table).Filter(r.Row.Field(field).Eq(val))
  res, err := query.Run(c.Session)
  if err != nil {
    return err
  }
  err = res.All(result)
  return err

}

func getListRaw(res *r.Cursor, result *[]byte) {
  var (
    ok bool
    next []byte
    stlen int = 0
  )

  *result = append(*result,'[')
  for {
    if next, ok = res.NextResponse(); ok != false {
      if stlen != 0 {
        *result = append(*result,',')
      } else {
        stlen += 1
      }
      *result = append(*result,next...)
    } else {
      break
    }
  }
  *result = append(*result,']')
}

func (c *Client) FindRaw(field, val interface{}, result *[]byte) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Find query
  query := r.DB(c.db).Table(c.table).Filter(r.Row.Field(field).Eq(val))
  res, err := query.Run(c.Session)
  if err != nil {
    return err
  }
  getListRaw(res, result)
  return nil

}


func (c *Client) FindCond(f interface{}, result interface{}) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Find query
  query := r.DB(c.db).Table(c.table).Filter(f)
  res, err := query.Run(c.Session)
  if err != nil {
    return err
  }
  err = res.All(result)
  return err

}

func (c *Client) FindCondRaw(f interface{}, result *[]byte) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Find query
  query := r.DB(c.db).Table(c.table).Filter(f)
  res, err := query.Run(c.Session)
  if err != nil {
    return err
  }
  getListRaw(res, result)
  return nil

}

func (c *Client) FindOne(f interface{}, nth int, result interface{}) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Find query
  query := r.DB(c.db).Table(c.table).Filter(f).Nth(nth)
  res, err := query.Run(c.Session)
  if err != nil {
    return err
  }
  err = res.One(result)
  if err != nil {
    return errors.New("Incorrect result")
  }
  return err

}

func (c *Client) FindOneField(f interface{}, nth int, field string, result interface{}) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Find query
  query := r.DB(c.db).Table(c.table).Filter(f).Nth(nth).Field(field)
  res, err := query.Run(c.Session)
  if err != nil {
    return err
  }
  err = res.One(result)
  if err != nil {
    return errors.New("Incorrect result")
  }
  return err

}

func (c *Client) FindCount(field, val interface{}) (int,error) {

  var result int
  if c.table == "" {
    return 0, errors.New("Table wasn't selected")
  }
  // Find query
  query := r.DB(c.db).Table(c.table).Filter(r.Row.Field(field).Eq(val)).Count()
  res, err := query.Run(c.Session)
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
  query := r.DB(c.db).Table(c.table).Get(id).Merge(func(row r.Term) r.Term {
    return r.Object(child+"s", r.DB(c.db).Table(child).Filter(
      func(subr r.Term) r.Term { 
        return subr.Field(c.table+"_id").Eq(id) 
      }).CoerceTo("ARRAY"))
  })
  res, err := query.Run(c.Session)
  if err != nil {
    return err
  }
  err = res.One(result)
  return err

}

func (c *Client) PopulateRaw(id interface{}, child string, result *[]byte) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Populate query
  query := r.DB(c.db).Table(c.table).Get(id).Merge(func(row r.Term) r.Term {
    return r.Object(child+"s", r.DB(c.db).Table(child).Filter(
      func(subr r.Term) r.Term { 
        return subr.Field(c.table+"_id").Eq(id) 
      }).CoerceTo("ARRAY"))
  })
  res, err := query.Run(c.Session)
  if err != nil {
    return err
  }
  *result, _ = res.NextResponse()
  return nil

}

func (c *Client) PopulateAllRaw(f interface{}, child string, result *[]byte) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Populate query
  query := r.DB(c.db).Table(c.table).Filter(f).ConcatMap( func(rowOne r.Term) r.Term {
      return r.DB(c.db).Table(child).Filter(
        func(row r.Term) r.Term {
          return row.Field(c.table + "_id").Eq(rowOne.Field("id"))
        }).CoerceTo("ARRAY").Do(
        func(childArray r.Term) r.Term {
          return r.Expr([]interface{}{}).Append(rowOne.Merge(r.Object(child+"s",childArray)))
        })
  })
  res, err := query.Run(c.Session)
  if err != nil {
    return err
  }
  getListRaw(res, result)
  return nil

}

func (c *Client) Update(id, arg interface{}, optArgs ...r.UpdateOpts) (r.WriteResponse, error) {
  
  var response r.WriteResponse
  if c.table == "" {
    return response, errors.New("Table wasn't selected")
  }
  // Update query
  return r.DB(c.db).Table(c.table).Get(id).Update(arg, r.UpdateOpts{Durability: "hard", ReturnChanges: true}).RunWrite(c.Session)

}

func (c *Client) Delete(id interface{}) error {

  if c.table == "" {
    return errors.New("Table wasn't selected")
  }
  // Delete query
  _, err := r.DB(c.db).Table(c.table).Get(id).Delete(r.DeleteOpts{Durability: "hard"}).Run(c.Session)
  return err
  
}

