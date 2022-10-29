package rk.darkages.implement

import scala.collection.mutable
import java.time.{Instant, LocalDate}
import rk.darkages.{Util, DBApi, DBConfig, ConfigProvider}
import DBApi.{DBAction, DBField, DBTable, MCriteria, MEntity, MFactory, PKMultiFields}
import com.typesafe.scalalogging.Logger

object TableImpl extends DBTable.Factory:
  private def _log: Logger = Util.getLogger("TableImpl")
  private val _instances = mutable.HashMap[String, DBTable]()

  private var _dialectInferer: DBTable => DBAction.Dialect = { (t: DBTable) =>
    Dialect.Postgres
  }

  def register(uniName: String, table: DBTable): Unit =
    if !_instances.contains(uniName) then
      _instances(uniName) = table
      MFactory registerMeta {case entKind if entKind == uniName =>
        val r = new FactoryImpl(table, _cache = table.cacheAdvice)
        r.activate()
        r
      }

  override def setSqlDialectInferer(fx: DBTable => DBAction.Dialect): Unit = _dialectInferer = fx

  override def apply(name: String, schema: String, fldNames: (String, Any)*)
                    (dbHint: Int = 2, uniName: String="", accFuns: Map[String, (String => String, String => String)] = Map.empty): DBTable =
    val uniNameX = if uniName.nonEmpty then uniName else name
    if !_instances.contains(uniNameX) then
      val flds = fldNames map { case (s, v) =>
        val (fn, pk, auto) = if s.endsWith("_pk") then
          (s.trim.substring(0, s.length - 3), true, false)
        else if s.endsWith("_apk") then
          (s.trim.substring(0, s.length - 4), true, true)
        else
          (s.trim, false, false)
        val accFun = accFuns find { case (k, _) => k == fn } map {
          _._2
        }
        val fld = DBField(MFactory.normalizeFldName(fn), fn, DBField.inferDataType(v), v, accFun)
        if pk then
          fld.setPK()
          fld.setAuto(auto)
        fld
      }
      val tabResult = new TableImpl(name, schema, flds, dbHint)
      register(uniNameX, tabResult)

    _instances(uniNameX)

  def apply(uniName: String): DBTable =
    _instances(uniName)

  override def buildDBSchema(cp: ConfigProvider): Unit =
    cp.config.dbs foreach {svr=>
      svr.tables foreach {tab=>
        val fields = tab.fields map { fx =>
          val fldName = fx.name.trim.toLowerCase
          val fldVirtName = MFactory.normalizeFldName(fx.virtName)
          val (dt, defVal, accFun) = fx.dataType.toUpperCase() match
            case s if s.contains("CHAR") || s.contains("VARCHAR") => (DBField.DataType.STR, Util.fixStr(fx.defaultVal, ""), None)
            case s if s == "INT" || s == "INTEGER" => (DBField.DataType.INT, Util.fixVal(fx.defaultVal.toInt, 0), None)
            case s if s.contains("TIMESTAMP") || s.contains("DATETIME") =>
              if fx.defaultVal.nonEmpty then
                (DBField.DataType.TM, Util.fixVal(Instant.parse(fx.defaultVal), Instant.MIN), None)
              else
                (DBField.DataType.TM, Instant.MIN, None)
            case s if s.contains("DATE") =>
              if fx.defaultVal.nonEmpty then
                (DBField.DataType.DT, LocalDate.parse(fx.defaultVal), None)
              else
                (DBField.DataType.DT, LocalDate.MIN, None)
            case s if s.contains("BOOL") || s.contains("BOOLEAN") => (DBField.DataType.BOOL, fx.defaultVal.toBoolean, None)
            case s if s.contains("FLOAT") || s.contains("DOUBLE") || s.contains("DOUBLE PRECISION") => (DBField.DataType.FLT, Util.fixVal(fx.defaultVal.toDouble, 0.0d), None)
            case s if s.contains("BLOB") || s.contains("BYTEA") => (DBField.DataType.LOB, Array[Byte](), None)
            case s if s.contains("GEOMETRY") || s.contains("GEOGRAPHY") =>
              val fOut = (s: String) => s" ST_AsText(ST_Transform(ST_SetSRID($s, 4326), 3857)) "
              val fIn = (s: String) => s" ST_Transform(ST_SetSRID(ST_GeomFromText($s), 3857),4326) "
              (DBField.DataType.GEOM, fx.defaultVal, Some((fOut, fIn)))
            case s if s=="TENSOR" => (DBField.DataType.TENSOR_F, Seq[Double](0.0), None)
            case s if s=="TENSOR_INT" => (DBField.DataType.TENSOR_I, Seq[Int](0), None)
            case s => throw RuntimeException(s"Database does not support：${s}")

          val fld = DBField(fldVirtName, fldName, dt, defVal, accFun)
          if fx.pk then
            fld.setPK()
          if fx.auto then
            fld.setAuto(true)
          fld
        }
        val cache = tab.cacheHint match
          case 0 => None
          case 100 => Some(new MFactory.FullCache)
          case 1 => Some(new MFactory.SimpleCache)
          case _ => None
        val tabResult = new TableImpl(tab.tableName, tab.schema, fields, svr.hint, cache)
        register(tab.uniName, tabResult)

        val fkr: DBTable.FKRetriever = new DBTable.FKRetriever:
          val _raw: Seq[DBConfig.DBForeignKeyConfig] = tab.foreignKeys
          var _all: Seq[DBTable.DBForeignKey] = Nil
          var _completed = false

          override def retrieve(): Seq[DBTable.DBForeignKey] =
            if !_completed then
              try
                _all = _raw map { fk =>
                  val rt = _instances(fk.target)
                  val klflds = fk.localFields map {tabResult.field(_).get}
                  val krflds = fk.remoteFields map {rt.field(_).get}
                  DBTable.DBForeignKey(tabResult, rt, klflds.zip(krflds), fk.name, fk.revName)
                }
                _completed = true
              catch
                case _: Exception => _log.error(s"Some table has not been built，foreign key from [${tab.tableName}] is unavailable")
            _all

        tabResult.setForeignKeysRetriever(fkr)
      }
    }

  override def findRevFK(t: DBTable, name: String="__sub__"): Option[DBTable.DBForeignKey] =
    val a = for x <- _instances.values
                k <- x.foreignKeys if k.rTable == t && k.downName == name yield k
    a.headOption

  def inferSqlDialect(t: TableImpl): DBAction.Dialect = _dialectInferer(t)

class TableImpl(override val name: String, override val schema: String, override val selfFields: Seq[DBField],
                override val dbHint: Int=2, override val cacheAdvice: Option[MFactory.Cache]=None) extends DBTable:
  private lazy val _foreignKeys = mutable.ArrayBuffer[DBTable.DBForeignKey]()
  private var _foreignKeysRetriever: Option[DBTable.FKRetriever] = None
  private var _boundWith: Option[MFactory] = None

  selfFields foreach {_.owner = Some(this)}

  lazy val inheritedHierarchy: Seq[DBTable] =
    var result = Seq[DBTable](this)
    var t = this
    var fk = inheritedFK
    while fk.nonEmpty do
      result = fk.get.rTable +: result
      fk = fk.get.rTable.inheritedFK
    result

  override def signature: String =
    if schema.nonEmpty then
      s"${schema}_${name}"
    else
      name

  override def canonicalName: String =
    if schema.nonEmpty then
      s"${schema}.${name}"
    else
      name

  override def bind(f: MFactory): Unit =
    _boundWith = Some(f)

  override def boundWith: Option[MFactory] = _boundWith

  override def field(fldName: String): Option[DBField] =
    selfFields find {fx => fx.virtName==fldName || fx.ingestName(false) == fldName}

  override def hierFields: Seq[DBField.Ref] =
    inheritedHierarchy flatMap {t =>
      t.selfFields map {f =>
        if t != this then
          DBField.Ref(f, f.ingestName(false))
        else
          DBField.Ref(f, f.ingestName())
      }
    }

  override def hierForeignKeys: Seq[DBTable.DBForeignKey] =
    val fks = foreignKeys
    val sup = _findInheritFK(fks) map {_.rTable} match
      case Some(t) => t.hierForeignKeys
      case _ => Nil
    fks ++ sup

  override def pkFields: Seq[DBField] =
    selfFields filter {_.isPK}

  override def parent: Option[DBTable] =
    inheritedFK match
      case Some(fk) => Some(fk.rTable)
      case _ => None

  override def addForeignKey(target: DBTable,  lFlds: Seq[(DBField | String)], rFlds: Seq[(DBField | String)], kName: String, revName: String=""): Unit =
    val lfs = lFlds map {fx=>
      fx match
        case f: DBField => f
        case s: String => field(s).getOrElse(DBField.fake)
    }
    val rfs = rFlds map { fx =>
      fx match
        case f: DBField => f
        case s: String => target.field(s).getOrElse(DBField.fake)
    }
    _foreignKeys += DBTable.DBForeignKey(this, target, lfs.zip(rfs), kName, revName)

  override def foreignKeys: Seq[DBTable.DBForeignKey] =
    _foreignKeysRetriever match
      case Some(fr) => fr.retrieve()
      case _ => _foreignKeys.toSeq

  override def selectSql(crit: MCriteria): DBAction =
    given DBAction.Dialect = TableImpl.inferSqlDialect(this)
    DBAction.buildQuery(this, Some(crit))

  override def deleteSql(crit: MCriteria): DBAction =
    given DBAction.Dialect = TableImpl.inferSqlDialect(this)
    DBAction.buildDelete(this, Some(crit))

  override def insertSql(v: MEntity, excludedFlds: Seq[String]=Nil): DBAction =
    given DBAction.Dialect = TableImpl.inferSqlDialect(this)
    DBAction.buildInsert(this, v, excludedFlds)

  override def updateSql(v: MEntity, excludedFlds: Seq[String]=Nil): DBAction =
    given DBAction.Dialect = TableImpl.inferSqlDialect(this)
    DBAction.buildUpdate(this, v, excludedFlds)

  override def mergeSql(v: MEntity, excludedFlds: Seq[String]=Nil): DBAction =
    given sqlDialect: DBAction.Dialect = TableImpl.inferSqlDialect(this)
    DBAction.buildMerge(this, v, excludedFlds)

  override def substituteSql(crit: MCriteria, vs: MEntity *): DBAction =
    val result = deleteSql(crit)
    vs foreach  { x =>
      result.attachTask(insertSql(x).apply())
    }
    result

  override def inheritedFK: Option[DBTable.DBForeignKey] =
    _findInheritFK(foreignKeys)

  override def asHPrefix: String = s"H_$name"

  private def setForeignKeysRetriever(v: DBTable.FKRetriever): Unit =
    _foreignKeysRetriever = Some(v)

  private def _findInheritFK(fks: Seq[DBTable.DBForeignKey]): Option[DBTable.DBForeignKey] =
    fks find {_.upName == "__inherit__"}