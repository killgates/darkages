import org.scalatest.funsuite.AnyFunSuite

import rk.darkages.{Util, DBAgent, DBApi, ConfigProvider}
import DBApi.{MEntity, MFactory, PKSingleField, PKStr}
import rk.darkages.implement.{TableImpl, FieldImpl, ActionImpl}
import rk.darkages.implement.Dialect

class DBSchemaTestSuite extends AnyFunSuite:

  lazy val sDBFile: String =
    val dbDir = System.getProperty("java.io.tmpdir")
    s"${dbDir}/test_db"

  lazy val db: DBAgent =
    DBAgent(url = s"jdbc:h2:$sDBFile", username="", password="", poolSize = 1)

  lazy val testDb: DBAgent =
    db.query("CREATE SCHEMA IF NOT EXISTS s1;")
    db.query("CREATE MEMORY TABLE IF NOT EXISTS s1.t1(tf1 VARCHAR(10) PRIMARY KEY , tf2 INTEGER);")
    db.query("CREATE MEMORY TABLE IF NOT EXISTS s1.t2(xf1 VARCHAR(10) PRIMARY KEY, xf2 DOUBLE PRECISION);")
    db.query("DELETE FROM s1.t1;")
    db.query("DELETE FROM s1.t2;")
    db.query("INSERT INTO s1.t1(tf1, tf2) VALUES('xyz1', 100),('xyz2', 200);")
    db

  test("Database operation from entities test") {
    val s =
      """ {"dbs": [{"name": "ak", "hint": 1,
        | "tables":[
        |{"uniName": "u1", "tableName": "t1", "schema": "s1",
        |"fields": [{"virtName": "mf1", "name": "tf1", "dataType": "char", "defaultVal": "abc", "pk": true},
        |           {"virtName": "mf2", "name": "tf2", "dataType": "Int", "defaultVal": "3", "pk": false}],
        |"foreignKeys":[]},
        |{"uniName": "u2", "tableName": "t2", "schema": "s1",
        |"fields": [{"name": "xf1", "dataType": "char", "defaultVal": "xyz", "pk": true},
        |           {"name": "xf2", "dataType": "Float", "defaultVal": "9.0", "pk": false}],
        |"foreignKeys": [{"name": "xxx", "revName": "yyy", "target": "u1", "localFields": ["xf1"], "remoteFields": ["mf1"]}]
        |}]
        |}]} """.stripMargin
    val cfg = ConfigProvider(s)
    TableImpl.setSqlDialectInferer({_ => Dialect.H2})
    DBApi.bootstrap(TableImpl, FieldImpl, ActionImpl, Some(cfg))
    DBApi.configDBs((1, db))

    val f1 = MFactory("u1")
    assert(f1.storage.canonicalName == "s1.t1")
    assert(f1.storage.signature == "s1_t1")
    assert(f1.storage.hierFields.length == 2)

    val f2 = MFactory("u2")
    assert(f2.storage.foreignKeys.length == 1)
    val fk = f2.storage.foreignKeys.head
    assert(fk.lTable.name == "t2")
    assert(fk.rTable.name == "t1")
    assert(fk.upName == "xxx")
    assert(fk.downName == "yyy")
  }

  test("Entity operation test"){
    val f1 = MFactory("u1")
    val t1 = f1()
    f1 setDBElector {x=>
      testDb
    }
    t1.mf1 = "abc"
    t1.mf2 = 99
    assert(t1.mf1 == "abc")
    assert(t1.mf2 == 99)

    val t2 = f1.fetchByKey(PKSingleField("xyz1", "mf1"))
    assert(t2.nonEmpty)
    val tx2 = t2.get
    assert(tx2.mf1 == "xyz1")
    assert(tx2.mf2 == 100)

    val t3 = f1()
    t3.mf1 = "zzz3"
    t3.mf2 = 121
    val xv3 = f1.insert(t3)
    assert(xv3.mf1 == "zzz3")
    assert(xv3.mf2 == 121)

    val tAll = f1.fetch(None)
    assert(tAll.length == 3)

    val t4 = f1()
    t4.mf1 = "zzz3"
    t4.mf2 = 886
    f1.save(t4)

    val t5 = f1.fetchByKey(new PKStr("zzz3", "mf1"))
    assert(t5.nonEmpty)
    assert(t5.get.mf2 == 886)
  }

  

