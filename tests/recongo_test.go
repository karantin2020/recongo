package tests

import (
  "testing"
  "reflect"
  "fmt"
  r "gopkg.in/dancannon/gorethink.v2"
  re "github.com/karantin2020/recongo"
  "time"
  // "encoding/json"
)

var (
  session *r.Session
  host = "172.17.0.2:28015"
  db = "recongo_test"
  client *re.Client
  table = "testTable"
)

func rebuildDB() {
  r.DBDrop(db).Exec(session)
  r.DBCreate(db).Exec(session)
}

func init() {
  s, err := r.Connect(r.ConnectOpts{
    Address: host,
    Database: db,
  })
  if (err != nil) { panic(err) }
  session = s
  rebuildDB()

  client, _ = re.NewClient(re.Connection{host, db})
  // client = c
  client.TablePresent(table)
  newTable := "newtable"
  client.TablePresent(newTable)
}

func expect(t *testing.T, a interface{}, b interface{}) {
  if a != b {
    t.Errorf("Expected %v (type %v) - Got %v (type %v)", b, reflect.TypeOf(b), a, reflect.TypeOf(a))
  }
}

func refute(t *testing.T, a interface{}, b interface{}) {
  if a == b {
    t.Errorf("Did not expect %v (type %v) - Got %v (type %v)", b, reflect.TypeOf(b), a, reflect.TypeOf(a))
  }
}

func Test_NewClient_Error(t *testing.T) {
  _, err := re.NewClient(re.Connection{"cheese:8080", db})
  refute(t, err, nil)
}

func Test_NewClient_BadDB(t *testing.T) {
  _, err := re.NewClient(re.Connection{host, "bad_db"})
  refute(t, err, nil)
}

func Test_NewClient_EmptyDB(t *testing.T) {
  _, err := re.NewClient(re.Connection{host, ""})
  refute(t, err, nil)
}

func Test_NewClient(t *testing.T) {
  c, err := re.NewClient(re.Connection{host, db})
  expect(t, err, nil)
  expect(t, c.DB(), "recongo_test")
}

func Test_ClientLog(t *testing.T) {
  c := re.Client{LogOutput: true}
  c.Log("Client logging...")
}

func Test_CreateEmpty(t *testing.T) {
  ts := struct{Test string `gorethink:"test"`}{"create"}
  c, _ := client.Table("").Create(ts)
  // fmt.Printf("%+v\n",c)
  expect(t, c.Inserted, 0)
  expect(t, len(c.Changes), 0)
}

func Test_CreateIncorrect(t *testing.T) {
  ts := struct{Test string `gorethink:"test"`}{"create"}
  c, _ := client.Table("incorrect").Create(ts)
  // fmt.Printf("%+v\n",c)
  expect(t, c.Inserted, 0)
  expect(t, len(c.Changes), 0)
}

func Test_CreateCorrect(t *testing.T) {
  ts := struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    }{"create", "thinker"}
  c, err := client.Table(table).Create(ts)
  // fmt.Println(c.Changes[0])
  expect(t, err, nil)
  expect(t, c.Inserted, 1)
  expect(t, len(c.Changes), 1)
  expect(t, len(c.GeneratedKeys), 1)
}

func Test_CreatePreTable(t *testing.T) {
  ts := struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    }{"create", "thinker"}
  client.Table(table)
  c, err := client.Create(ts)
  // fmt.Println(c.Changes[0])
  expect(t, err, nil)
  expect(t, c.Inserted, 1)
  expect(t, len(c.Changes), 1)
  expect(t, len(c.GeneratedKeys), 1)
}

func Test_Find(t *testing.T) {
  var ts []map[string]interface{}
  err := client.Table(table).Find("test", "create", &ts)
  expect(t, err, nil)
  expect(t, len(ts), 2)
  expect(t, ts[0]["test"], "create")
  fmt.Printf("%+v\n",ts[0])
}

func Test_FindRaw(t *testing.T) {
  var ts []byte
  err := client.Table(table).FindRaw("test", "create", &ts)
  expect(t, err, nil)
  // expect(t, len(ts), 2)
  // expect(t, ts[0].Test, "create")
  fmt.Println(string(ts))
}

func Test_Get(t *testing.T) {
  ts := struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    }{"createGet", "thinkerGet"}
  var tsr struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    }
  c, err := client.Create(&ts)
  expect(t, err, nil)
  // fmt.Println(c.GeneratedKeys[0])
  err = client.Get(c.GeneratedKeys[0], &tsr)
  expect(t, err, nil)
  expect(t, tsr.Test, "createGet")
  expect(t, tsr.Abc, "thinkerGet")
}

func Test_GetByArray(t *testing.T) {
  var tsr struct{
    Id []string `gorethink:"id"`
    Permissions map[string]int `gorethink:"permissions"`
  }
  err := client.SetDB("rethinkdb").Table("permissions").Get(
    [1]string{"admin"}, &tsr)
  expect(t, err, nil)
  fmt.Printf("%v\n",tsr)
  // expect(t, tsr.Test, "createGet")
  // expect(t, tsr.Abc, "thinkerGet")
  client.SetDB("recongo_test").Table("testTable")
}

func Test_GetNull(t *testing.T) {
  var tsr struct{
    Id []string `gorethink:"id"`
    Permissions map[string]int `gorethink:"permissions"`
  }
  err := client.SetDB("rethinkdb").Table("permissions").Get(
    [1]string{"admin123"}, &tsr)
  refute(t, err, nil)
  expect(t, err.Error(), "Incorrect result")
  fmt.Printf("%v\n",tsr)
  // expect(t, tsr.Test, "createGet")
  // expect(t, tsr.Abc, "thinkerGet")
  client.SetDB("recongo_test").Table("testTable")
}

func Test_GetAll(t *testing.T) {
  ts := []struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    }{}
  err := client.Table(table).GetAll(&ts)
  expect(t, err, nil)
  expect(t, len(ts), 3)
  // fmt.Printf("%+v\n",ts)
}


func Test_FindCount(t *testing.T) {
  n, err := client.FindCount("test", "create")
  expect(t, err, nil)
  expect(t, n, 2)
}

func Test_FindCond(t *testing.T) {
  ts := []struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    }{}
  err := client.FindCond(r.Row.Field("test").Eq("create"), &ts)
  expect(t, err, nil)
  expect(t, len(ts), 2)
  expect(t, ts[0].Test, "create")
  fmt.Printf("%+v\n",ts[0])
}

func Test_FindCondRaw(t *testing.T) {
  var ts []byte
  err := client.FindCondRaw(r.Row.Field("test").Eq("create"), &ts)
  expect(t, err, nil)
  // expect(t, len(ts), 2)
  // expect(t, ts[0].Test, "create")
  fmt.Println(string(ts))
}

func Test_Update(t *testing.T) {
  ts := []struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    Id string `gorethink:"id"`
  }{}
  tsn := struct{
    Test string `gorethink:"test"`
  }{"createNew"}
  err := client.FindCond(r.Row.Field("test").Eq("createGet"), &ts)
  expect(t, err, nil)
  fmt.Println("Id is:", ts[0].Id)
  c, errn := client.Update(ts[0].Id,tsn)
  // fmt.Println(c.Changes[0].NewValue["test"])
  expect(t, errn, nil)
  expect(t, c.Replaced, 1)
  expect(t, len(c.Changes), 1)
}

func Test_UpdateTwo(t *testing.T) {
  ts := []struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    Id string `gorethink:"id"`
  }{}
  tsn := struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
  }{"createNewNew", "thinkerNew"}
  err := client.FindCond(r.Row.Field("test").Eq("createNew"), &ts)
  expect(t, err, nil)
  fmt.Println("Id is:", ts[0].Id)
  c, errn := client.Update(ts[0].Id,tsn)
  fmt.Println(c)
  expect(t, errn, nil)
  expect(t, c.Replaced, 1)
  expect(t, len(c.Changes), 1)
  var tsr struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
  }
  err = client.Get(ts[0].Id, &tsr)
  expect(t, err, nil)
  expect(t, tsr.Test, "createNewNew")
  expect(t, tsr.Abc, "thinkerNew")
}

func Test_PrimKey(t *testing.T) {
  str, err := client.PrimKey()
  expect(t, err, nil)
  expect(t, str, "id")
  fmt.Printf("%s\n",str)
}

func Test_PopulatePrepare(t *testing.T) {
  newTable := "newtable"
  ts := []struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    Id string `gorethink:"id"`
  }{}
  err := client.Table(table).FindCond(r.Row.Field("test").Eq("createNewNew"), &ts)
  c, _ := client.Table(newTable).Create(r.Object(table+"_id", ts[0].Id, "wow", "yowww"))
  fmt.Println(c.GeneratedKeys[0])
  expect(t, err, nil)
}

func Test_Populate(t *testing.T) {
  newTable := "newtable"
  ts := []struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    Id string `gorethink:"id"`
  }{}
  err := client.Table(table).FindCond(r.Row.Field("test").Eq("createNewNew"), &ts)
  expect(t, err, nil)
  
  tsr := struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    Id string `gorethink:"id"`
    Newtables []struct {
      Wow string `gorethink:"wow"`
      TestTable_id string `gorethink:"testTable_id"`
      Id string `gorethink:"id"`
    } `gorethink:"newtables"`
  }{}
  err = client.Table(table).Populate(ts[0].Id, newTable, &tsr)
  expect(t, err, nil)
  expect(t, tsr.Newtables[0].Wow, "yowww")
  fmt.Printf("%+v\n",tsr)
}

func Test_Delete(t *testing.T) {
  ts := []struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    Id string `gorethink:"id"`
  }{}
  err := client.FindCond(r.Row.Field("test").Eq("create"), &ts)
  expect(t, err, nil)
  fmt.Println("Id to delete is:", ts[0].Id)
  err = client.Table(table).Delete(ts[0].Id)
  expect(t, err, nil)
}

func Test_DBTableTree(t *testing.T) {
  ll := client.DBTableTree()
  fmt.Println(ll)
  expect(t, len(ll["recongo_test"]), 2)
  expect(t, ll["recongo_test"][0], "newtable")
}

func Test_GetRaw(t *testing.T) {
  ts := []struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    Id string `gorethink:"id"`
  }{}
  err := client.FindCond(r.Row.Field("test").Eq("createNewNew"), &ts)
  expect(t, err, nil)
  var result []byte
  ok := client.GetRaw(ts[0].Id, &result)
  expect(t, ok, nil)
  fmt.Println(reflect.TypeOf(string(result[:])))
  fmt.Println(string(result[:]))
}

func Test_CreateCorrectList(t *testing.T) {
  ts := []struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    }{{"createFoo", "thinkerFoo"},
      {"createMoo", "thinkerMoo"},
      {"createDoo", "thinkerDoo"},
      {"createWoo", "thinkerWoo"},
    }
  c, err := client.Table(table).Create(ts)
  // fmt.Println(c.Changes[0])
  expect(t, err, nil)
  expect(t, c.Inserted, 4)
  expect(t, len(c.Changes), 4)
  expect(t, len(c.GeneratedKeys), 4)
}

func Test_PopulateRaw(t *testing.T) {
  newTable := "newtable"
  ts := []struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    Id string `gorethink:"id"`
  }{}
  err := client.Table(table).FindCond(r.Row.Field("test").Eq("createNewNew"), &ts)
  expect(t, err, nil)
  
  var tsr []byte
  err = client.Table(table).PopulateRaw(ts[0].Id, newTable, &tsr)
  expect(t, err, nil)
  // expect(t, tsr.Newtables[0].Wow, "yowww")
  fmt.Println(string(tsr))
}

func Test_PopulateAllRaw(t *testing.T) {
  newTable := "newtable"
  ts := []struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    Id string `gorethink:"id"`
  }{}
  err := client.Table(table).FindCond(r.Row.Field("test").Eq("createNewNew"), &ts)
  expect(t, err, nil)
  
  _, err = client.Table(newTable).Create(r.Object(table+"_id", ts[0].Id, "wow", "rowww"))
  expect(t, err, nil)
  var tsr []byte
  err = client.Table(table).PopulateAllRaw(
    r.Row.Field("test").Eq("createNewNew"), 
    newTable, 
    &tsr)
  expect(t, err, nil)
  // expect(t, tsr.Newtables[0].Wow, "yowww")
  fmt.Println(string(tsr))
}

func Test_AddCorrect(t *testing.T) {
  childTable := "newtable"
  ts := struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    }{"create", "thinker"}
  tso := []struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    Id string `gorethink:"id"`
    }{}
  err := client.FindCond(r.Row.Field("test").Eq("create"), &tso)
  c, err := client.Table(table).Add(table, tso[0].Id, childTable, ts)
  fmt.Println(c.Changes)
  expect(t, err, nil)
  expect(t, c.Inserted, 1)
  expect(t, len(c.Changes), 1)
  expect(t, len(c.GeneratedKeys), 1)
}

func Test_AddWithIncorrectId(t *testing.T) {
  childTable := "newtable"
  ts := struct{
    Test string `gorethink:"test"`
    Abc string `gorethink:"abc"`
    }{"create", "thinker"}
  
  c, err := client.Table(table).Add(table, 123, childTable, ts)
  // fmt.Println(err)
  refute(t, err, nil)
  expect(t, c.Inserted, 0)
  expect(t, len(c.Changes), 0)
  expect(t, len(c.GeneratedKeys), 0)
}

func Test_SetDB(t *testing.T) {
  defer func() {
        if rc := recover(); rc != nil {
            fmt.Println("Recovered in Test_SetDB: '", rc, "'")
            refute(t, rc, nil)
        }
        client.SetDB("recongo_test")
    }()
  client.SetDB("ewq")
  // fmt.Println(err)
}

func Test_GetWithTimeField(t *testing.T) {
  ts := struct{
    Test string `gorethink:"test" json:"test"`
    Abc string `gorethink:"abc" json:"abc"`
    Time time.Time `gorethink:"time" json:"time"`
  }{"createTime", "thinkerTime", time.Now().UTC()}
  c, err := client.Table(table).Create(ts)
  // fmt.Println(c.Changes[0])
  expect(t, err, nil)
  expect(t, c.Inserted, 1)
  expect(t, len(c.Changes), 1)
  expect(t, len(c.GeneratedKeys), 1)

  var tsr struct{
    Test string `gorethink:"test" json:"test"`
    Abc string `gorethink:"abc" json:"abc"`
    Time time.Time `gorethink:"time" json:"time"`
  }
  err = client.Get(c.GeneratedKeys[0], &tsr)
  expect(t, err, nil)
  expect(t, tsr.Test, "createTime")
  expect(t, tsr.Abc, "thinkerTime")
  // fmt.Printf("%+v\n", tsr)
  // fmt.Println(tsr)
  // res2B, errm := json.Marshal(tsr)
  // if errm == nil {
  //   fmt.Println(string(res2B))
  //   } else {
  //     fmt.Println("Bad Marshal result")
  //   }
}

func Test_GetOne(t *testing.T) {
  var tsr struct{
    Test string `gorethink:"test" json:"test"`
    Abc string `gorethink:"abc" json:"abc"`
    Time time.Time `gorethink:"time" json:"time"`
  }
  err := client.FindOne(r.Row.Field("abc").Eq("thinkerTime"), 0, &tsr)
  expect(t, err, nil)
  fmt.Printf("%+v\n", tsr)
}

type Foo struct {
  R int
}

type Zoo int

func Test_GetOneCustomType(t *testing.T) {
  ts := struct{F Foo; R Foo}{Foo{321},Foo{432}}
  fmt.Println(ts)
  c, err := client.Table(table).Create(ts)
  fmt.Println(c.Changes[0])
  expect(t, err, nil)
  expect(t, c.Inserted, 1)
  expect(t, len(c.Changes), 1)
  expect(t, len(c.GeneratedKeys), 1)

  var tsr struct{F Foo}
  err = client.Get(c.GeneratedKeys[0], &tsr)
  expect(t, err, nil)

  fmt.Printf("%+v\n", tsr)

  tsz := struct {
    Z Zoo
  }{987}
  fmt.Println(tsz)
  c, err = client.Table(table).Create(tsz)
  fmt.Println(c.Changes[0])
  expect(t, err, nil)
  expect(t, c.Inserted, 1)
  expect(t, len(c.Changes), 1)
  expect(t, len(c.GeneratedKeys), 1)

  var tsrz struct{Z Zoo}
  err = client.Get(c.GeneratedKeys[0], &tsrz)
  expect(t, err, nil)

  fmt.Printf("%+v\n", tsrz)
}

func Test_GetOneField(t *testing.T) {
  var tsr time.Time
  err := client.FindOneField(r.Row.Field("abc").Eq("thinkerTime"), 0, "time", &tsr)
  expect(t, err, nil)
  fmt.Println(tsr)
}


