package rk.darkages

import DBApi.MFactory

object DBConfig:
  case class DBFieldConfig(virtName: String, name: String, dataType: String, defaultVal: String, pk: Boolean, auto: Boolean)
  case class DBForeignKeyConfig(name: String, revName: String, target: String, localFields: Seq[String], remoteFields: Seq[String])

  case class DBTableConfig(uniName: String, tableName: String, schema: String, fields: Seq[DBConfig.DBFieldConfig], foreignKeys: Seq[DBConfig.DBForeignKeyConfig], cacheHint: Int=0)

  case class DBServerConfig(name: String, hint: Int, tables: Seq[DBTableConfig])

  lazy val empty: DBConfig =
    apply(Nil)

  given Util.Json.JFactory[DBConfig] with
    override def build(jv: Util.Json.JVal): DBConfig =
      jv match
        case xRoot: Util.Json.JObj =>
          xRoot("dbs") match
            case xDbs: Util.Json.JArr =>
              val dbs = xDbs.v map {x=>
                x match
                  case xSvr: Util.Json.JObj =>
                    val svrName = xSvr("name").strVal
                    val svrHint = xSvr("hint").numVal.toInt
                    val svrTables = xSvr("tables") match
                      case xTables: Util.Json.JArr =>
                        xTables.v map {_parseTable}
                      case _ => throw RuntimeException("Illegal config file: table definition")
                    DBConfig.DBServerConfig(svrName, svrHint, svrTables)
                  case _ => throw RuntimeException("Illegal config file: dbs definition")
              }
              DBConfig(dbs = dbs)
            case _ => throw RuntimeException("Illegal config file ")
        case _ => throw RuntimeException("Illegal config file")

    private def _parseTable(jv: Util.Json.JVal): DBConfig.DBTableConfig =
      jv match
        case xTable: Util.Json.JObj =>
          val uniName = Util.Json.readNodeAsStr(xTable("uniName"))
          val tableName = Util.Json.readNodeAsStr(xTable("tableName"))
          val schema = Util.Json.readNodeAsStr(xTable("schema"))
          val cacheHint = Util.Json.readNodeAsInt(xTable("cacheHint"))
          val fields = xTable("fields") match
            case fs: Util.Json.JArr =>
              fs.v map { x =>
                val (virtName, fName, fDataType, fDefaultVal, fPk, fAuto) = x match
                  case foo: Util.Json.JObj =>
                    val realName = Util.Json.readNodeAsStr(foo("name"), RuntimeException("Database settings error，a field MUST has a name"))
                    val virtName = Util.Json.readNodeAsStr(foo("virtName"), realName)
                    val dataType = Util.Json.readNodeAsStr(foo("dataType"))
                    val defaultVal = Util.Json.readNodeAsStr(foo("defaultVal"))
                    val pk = Util.Json.readNodeAsBool(foo("pk"))
                    val autoGen = Util.Json.readNodeAsBool(foo("auto"))
                    (virtName, realName, dataType, defaultVal, pk, autoGen)
                  case _ => ("", "", "", "", false, false)
                DBConfig.DBFieldConfig(MFactory.normalizeFldName(virtName), fName, fDataType, fDefaultVal, fPk, fAuto)
              }
            case _ => Nil
          val foreignKeys = xTable("foreignKeys") match
            case fks: Util.Json.JArr =>
              fks.v map { x =>
                val (name, revName, target, locFlds, rFLds) = x match
                  case koo: Util.Json.JObj =>
                    val lxFlds = koo("localFields") match
                      case z: Util.Json.JArr => z.v map {
                        _.strVal
                      }
                      case _ => Nil
                    val rxFlds = koo("remoteFields") match
                      case z: Util.Json.JArr => z.v map {
                        _.strVal
                      }
                      case _ => Nil
                    (koo("name").strVal, koo("revName").strVal, koo("target").strVal, lxFlds, rxFlds)
                  case _ => ("", "", "", Nil, Nil)
                DBConfig.DBForeignKeyConfig(name = name, revName = revName, target = target, localFields = locFlds, remoteFields = rFLds)
              }
            case _ => Nil
          DBConfig.DBTableConfig(uniName = uniName, tableName = tableName, schema = schema, fields = fields, foreignKeys = foreignKeys, cacheHint = cacheHint)
        case _ => throw RuntimeException("config file format is illegal")

case class DBConfig(dbs: Seq[DBConfig.DBServerConfig]):
  def findTable(uniName: String): Option[(DBConfig.DBServerConfig, DBConfig.DBTableConfig)] =
    for svr <- dbs
        t <- svr.tables do
      if t.uniName == uniName then
        return Some((svr, t))
    None


object ConfigProvider:
  def apply(s: String): ConfigProvider = new StrConfigProvider(s)

  def apply(): ConfigProvider =
    new ConfigProvider:
      override def config: DBConfig = DBConfig(Nil)

trait ConfigProvider:
  def config: DBConfig

class StrConfigProvider(val txt: String) extends ConfigProvider:
  override def config: DBConfig =
    Util.Json.read[DBConfig](txt)

