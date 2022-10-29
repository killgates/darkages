import org.scalatest.funsuite.AnyFunSuite
import rk.darkages.{Util, DBConfig}


class JsonTestSuite extends AnyFunSuite:

  test("ConfigMap test") {
    val s = """ {"abc": 123, "kafka_server": "10.194.148.55", "web_name": "oops", "err": false } """
    val result = Util.Json.read[Util.ConfigMap](s)
    assert(result.getInt("abc").get == 123)
    assert(!result.getBoolean("err"))
    assert(result.getString("kafka_server").get == "10.194.148.55")
  }

  test("DBConfig test") {
    val s =
      """ {"dbs": [{"name": "ak", "hint": 2, "tables":[
        |{"uniName": "u1", "tableName": "t1", "schema": "s1", "cacheHint": 0,
        |"fields": [{"virtName": "mf1", "name": "tf1", "dataType": "char", "defaultVal": "abc", "pk": true}, {"name": "tf2", "dataType": "Int", "defaultVal": "3", "pk": false}],
        |"foreignKeys":[]},
        |{"uniName": "u2", "tableName": "t2", "schema": "s2",
        |"fields": [{"name": "xf1", "dataType": "char", "defaultVal": "xyz", "pk": true}, {"name": "xf2", "dataType": "Float", "defaultVal": "9.0", "pk": false}]
        |}]
        |}]} """.stripMargin
    val result = Util.Json.read[DBConfig](s)
    assert(result.dbs.length==1)
    assert(result.dbs.head.hint == 2)
    assert(result.dbs.head.name == "ak")
    assert(result.dbs.head.tables.length == 2)
    assert(result.dbs.head.tables.head.uniName == "u1")
    assert(result.dbs.head.tables.head.tableName == "t1")
    assert(result.dbs.head.tables.head.fields.length == 2)
    assert(result.dbs.head.tables.head.fields.head.name == "tf1")
    assert(result.dbs.head.tables.head.fields.head.dataType == "char")
    assert(result.dbs.head.tables.head.fields.head.pk)

  }


