package rk.darkages

import ch.qos.logback.core.rolling.DefaultTimeBasedFileNamingAndTriggeringPolicy

import scala.concurrent.Future
import scala.collection.mutable
import java.time.{Instant, LocalDate}
import java.sql.ResultSet
import scala.language.{dynamics, postfixOps}
import java.util.concurrent.locks.ReentrantLock
import java.util.UUID
import com.typesafe.scalalogging.Logger

object DBApi:
  private val _feasibleDbs: mutable.Map[Int, DBAgent] = mutable.TreeMap()
  private def _log: Logger = Util.getLogger("DBAgent")

  def configDBs(dbs: (Int, DBAgent)*): Unit =
    dbs foreach {_feasibleDbs += _}

  private[darkages] def adapt(hint: Int) = _feasibleDbs(hint)

  trait MQContext:
    def interpolateField(fld: String, v: Any): DBAction.SqlLiteral

    def fieldNameInDB(tableAlias: String, fld: DBField): String

    def dialect: DBAction.Dialect

  object MCriteria:
    object trivial extends MCriteria:
      override def toSql(ctx: MQContext): DBAction.SqlLiteral = ctx.dialect.composeTrivialCriteria()

    def apply(fldNames: Seq[String], vals: Seq[Any]): MCriteria =
      BaseCriteria(fldNames, vals)

    def apply(fldName: String, v: Any): MCriteria =
      BaseCriteria(Seq(fldName), Seq(v))

    def apply(fvs: (String, Any)*): MCriteria =
      val (fldNames, vals) = fvs.unzip
      BaseCriteria(fldNames, vals)

  trait MCriteria:
    def toSql(ctx: MQContext): DBAction.SqlLiteral //(String, Seq[Any])

  object DBField:
    enum DataType(val usedId: Int):
      override def toString: String =
        usedId match
          case -1 => "Unknown(nothing)"
          case 2 => "Integer"
          case 1 => "Float"
          case 3 => "String"
          case 4 => "Timestamp"
          case 44 => "Datetime"
          case 5 => "LOB"
          case 6 => "JSON"
          case 8 => "Tensor of float"
          case 9 => "Tensor of integer"
          case 45 => "Boolean"
          case 64 => "GIS Geometry"
      //case => ""

      case NOTHING extends DataType(-1)
      case INT extends DataType(2)
      case FLT extends DataType(1)
      case STR extends DataType(3)
      case TM extends DataType(4) //datetime or timestamp
      case DT extends DataType(44) //date
      case LOB extends DataType(5)
      case JSON extends DataType(6)
      case TENSOR_F extends DataType(8)
      case TENSOR_I extends DataType(9)
      case BOOL extends DataType(45)
      case GEOM extends DataType(64)
    //    case  extends DataType()


    trait Owner:
      def name: String
      def schema: String
      def signature: String
      def asHPrefix: String = s"H_${name}"

    class NoOwner extends Owner:
      override def name: String = ""

      override def signature: String = ""

      override def schema: String = ""

    case class Ref(principal: DBField, accKey: String)

    trait Factory:
      def apply[F](virtName: String, name: String, dataType: DBField.DataType, dVal: F, fldAccFun: Option[(String => String, String => String)] = None): DBField
      def inferDataType(v: Any): DataType

    private var _fac: Option[Factory] = None

    lazy val fake: DBField = apply("__fake__", "__fake__", DataType.NOTHING, ())

    def registerFactory(fac: Factory): Unit = _fac = Some(fac)

    def apply[F](virtName: String, name: String, dataType: DBField.DataType, dVal: F, fldAccFun: Option[(String => String, String => String)] = None): DBField =
      _fac match
        case Some(fx) => fx(virtName, name, dataType, dVal, fldAccFun)
        case _ => throw RuntimeException(s"Can't create DBField: $virtName")

    def inferDataType(v: Any): DataType =
      _fac match
        case Some(fx) => fx.inferDataType(v)
        case _ => throw RuntimeException(s"Can't infer the data type：$v")

  trait DBField():
    type T
    val virtName: String
    val name: String
    val dataType: DBField.DataType
    val defaultVal: T

    def accTransform: Option[(String=>String, String=>String)] = None

    def accTransformDown: Option[String => String] =
      accTransform map {case(d, _) => d}

    def accTransformUp: Option[String => String] =
      accTransform map { case (_, u) => u }

    def accTransform_=(v: Option[(String=>String, String=>String)]): Unit

    def owner: Option[DBField.Owner]
    def owner_=(v: Option[DBField.Owner]): Unit

    def setPK(): DBField
    def isPK: Boolean
    def setAuto(v: Boolean): DBField

    def willAutoGen(v: Any): Boolean

    def typedDefaultVal: T = defaultVal.asInstanceOf[T]

    def extractData(q: MQContext, rs: ResultSet, partAlias: String = "", idx: Int = -1): T

    def extractData(rs: ResultSet, idx: Int): T

    def extractData(rs: ResultSet, asName: String): T

    def buildPK(v: MEntity): PKSingleField[T] =
      new PKSingleField[T](v(virtName).asInstanceOf[T], virtName)

    def signatue: String =
      owner match
        case Some(ox) => s"${ox.signature}->${virtName}"
        case _ => s"${virtName}"

    def ingestName(asLeaf: Boolean=true): String =
      if asLeaf then
        virtName
      else
        owner match
          case Some(t) => s"H_${t.asHPrefix}_${virtName}"
          case _ => throw RuntimeException(s"Field can't obtain right name（$virtName）for accessing, no owner setting")

    def refAs(s: String = ""): DBField.Ref =
      if s.nonEmpty then
        DBField.Ref(this, s)
      else
        DBField.Ref(this, this.ingestName())

    override def toString: String = signatue

    override def hashCode(): Int = signatue.hashCode

  object DBTable:

    case class DBForeignKey(lTable: DBTable, rTable: DBTable, linkFlds: Seq[(DBField, DBField)], upName: String="__head__", downName: String="__sub__")

    trait FKRetriever:
      def retrieve(): Seq[DBForeignKey]

    trait Factory:
      def apply(name: String, schema: String, fldNames: (String, Any)*)
               (dbHint: Int = 2, uniName: String="", accFuns: Map[String, (String => String, String => String)] = Map.empty): DBTable
      def apply(uniName: String): DBTable
      def findRevFK(t: DBTable, name: String="__sub__"): Option[DBTable.DBForeignKey]
      def buildDBSchema(cp: ConfigProvider): Unit
      def setSqlDialectInferer(fx: DBTable=>DBAction.Dialect): Unit

    private var _fac: Option[Factory] = None

    def registerFactory(fac: Factory): Unit = _fac = Some(fac)

    def setSqlDialectInferer(inf: DBTable=>DBAction.Dialect): Unit =
      _fac match
        case Some(fx) => fx.setSqlDialectInferer(inf)
        case _ => throw RuntimeException(s"Can't handle DBTable, it has not initiated")

    def apply(name: String, schema: String, fldNames: (String, Any)*)
             (dbHint: Int = 2, uniName: String="", accFuns: Map[String, (String => String, String => String)] = Map.empty): DBTable =
      _fac match
        case Some(fx) => fx(name, schema, fldNames: _*)(dbHint, uniName, accFuns)
        case _ => throw RuntimeException(s"Can't create DBTable: no implementation: $_fac")

    def apply(uniName: String): DBTable =
      _fac match
        case Some(fx) => fx(uniName)
        case _ => throw RuntimeException(s"Can't create DBTable: no implementation $_fac")

    def findRevFK(t: DBTable, name: String="__sub__"): Option[DBTable.DBForeignKey] =
      _fac match
        case Some(fx) => fx.findRevFK(t, name)
        case _ => throw RuntimeException("Can't perform findRevFK, no implementation")

  trait DBTable extends DBField.Owner:
    val selfFields: Seq[DBField]
    val dbHint: Int
    val cacheAdvice: Option[MFactory.Cache]
    def inheritedHierarchy: Seq[DBTable]
    def canonicalName: String
    def bind(f: MFactory): Unit
    def boundWith: Option[MFactory]
    def hierFields: Seq[DBField.Ref]
    def hierForeignKeys: Seq[DBTable.DBForeignKey]
    def field(fldName: String): Option[DBField]
    def pkFields: Seq[DBField]
    def parent: Option[DBTable]
    def addForeignKey(target: DBTable,  lFlds: Seq[(DBField | String)], rFlds: Seq[(DBField | String)], kName: String, revName: String=""): Unit
    def foreignKeys: Seq[DBTable.DBForeignKey]
    def selectSql(crit: MCriteria): DBAction
    def deleteSql(crit: MCriteria): DBAction
    def insertSql(v: MEntity, excludedFlds: Seq[String]=Nil): DBAction
    def updateSql(v: MEntity, excludedFlds: Seq[String]=Nil): DBAction
    def mergeSql(v: MEntity, excludedFlds: Seq[String]=Nil): DBAction
    def substituteSql(crit: MCriteria, vs: MEntity *): DBAction
    def inheritedFK: Option[DBTable.DBForeignKey]


  object DBAction:
    private var _serNum = 0
    private var _fac: Option[Factory] = None

    def uniqueSerNum: Int =
      _serNum += 1
      _serNum

    trait Factory:
      def buildQuery(t: DBTable, crit: Option[MCriteria], selFields: Seq[DBField], asSub: Boolean)(using sqlDialect: DBAction.Dialect): DBAction
      def buildDelete(t: DBTable, crit: Option[MCriteria])(using sqlDialect: DBAction.Dialect): DBAction
      def buildInsert(t: DBTable, v: MEntity, excludedFlds: Seq[String])(using sqlDialect: DBAction.Dialect): DBAction
      def buildUpdate(t: DBTable, v: MEntity, excludedFlds: Seq[String])(using sqlDialect: DBAction.Dialect): DBAction
      def buildMerge(t: DBTable, v: MEntity, excludedFlds: Seq[String])(using sqlDialect: DBAction.Dialect): DBAction

    case class TableInstance(principal: DBTable, alias: String)

    enum JoinDirection:
      case J_IN, J_OUT

    enum JoinKind(val s: String):
      override def toString: String = s

      case INNER extends JoinKind("INNER")
      case LEFT_OUTER extends JoinKind("LEFT OUTER")
      case RIGHT_OUTER extends JoinKind("RIGHT OUTER")
      case FULL_OUTER extends JoinKind("FULL OUTER")
      case CROSS extends JoinKind("CROSS")

    case class JoinAnchor(peer: TableInstance, joinKey: DBTable.DBForeignKey, dir: JoinDirection = JoinDirection.J_OUT, kind: JoinKind = JoinKind.INNER)

    case class Participant(tabRef: TableInstance, joinInfo: Option[JoinAnchor] = None):
      def substance: DBTable = tabRef.principal

      def alias: String = tabRef.alias

    case class FieldInstance(principal: DBField, engagedBy: Option[TableInstance], asHeritage: Boolean=false):
      def quoteName: String =
        engagedBy match
          case Some(ti) =>
            if ti.alias.nonEmpty then
              s"X_${ti.alias}_${principal.virtName}"
            else
              principal.ingestName(asHeritage)
          case _ =>
            principal.ingestName(asHeritage)

      def extractData(rs: ResultSet): Any =
        principal.extractData(rs, asName = quoteName)

    given Conversion[DBTable, TableInstance] = new TableInstance(_, "")
    given Conversion[DBField, FieldInstance] = new FieldInstance(_, None)

    given Conversion[TableInstance, Participant] = new Participant(_, None)

    given Conversion[Participant, TableInstance] = _.tabRef

    def mkFieldInstance(fld: DBField, t: DBTable | TableInstance | Participant | Option[Nothing]): FieldInstance =
      t match
        case t: DBTable => FieldInstance(fld, Some(TableInstance(t, "")))
        case t: TableInstance => FieldInstance(fld, Some(t))
        case t: Participant => FieldInstance(fld, Some(t.tabRef))
        case _ => FieldInstance(fld, None)

    object SqlLiteral:
      val NULL = new SqlLiteral("NULL")
      val VOID: SqlLiteral = new SqlLiteral("__VOID__") :
        override def isActive: Boolean = false

      def fromTuple(v: Tuple2[String, Seq[Any]]): SqlLiteral = new SqlLiteral(v._1, v._2)

      given Conversion[(String, Seq[Any]), SqlLiteral] with
        override def apply(x: (String, Seq[Any])): SqlLiteral = new SqlLiteral(x._1, x._2)

      given Conversion[SqlLiteral, (String, Seq[Any])] with
        override def apply(x: SqlLiteral): (String, Seq[Any]) = x.toTuple

    case class SqlLiteral(statement: String, params: Seq[Any] = Nil):
      override def toString: String = statement

      def toTuple: Tuple2[String, Seq[Any]] = (statement, params)

      def isActive: Boolean = statement.nonEmpty

      def appendParams(p: Seq[Any]): SqlLiteral =
        new SqlLiteral(statement, params ++ p)

      def +(x: SqlLiteral): SqlLiteral =
        SqlLiteral(statement + " " + x.statement, params ++ x.params)

    object SqlSpecial:
      val fieldSpaceHolder = new SqlSpecial("?")
      val nothing = new SqlSpecial("")

    case class SqlSpecial(tag: String = ""):
      override def toString: String = tag

    given Conversion[String, SqlLiteral] with
      override def apply(x: String): SqlLiteral = SqlLiteral(x)

    given Conversion[SqlLiteral, String] with
      override def apply(x: SqlLiteral): String =
        x.statement

    trait PendingVal:
      def apply(): Any

    case class EntityPendingVal(fld: FieldInstance, v: MEntity) extends PendingVal:
      def apply(): Any =
        v(fld.quoteName)

    object QTask:
      case class Effect(outcome: Seq[Map[DBField.Ref, Any]], tag: String = "")

      lazy val empty: QTask =
        new AbsQTask(TrivialSqlDialect):
          override def apply(): SqlLiteral = SqlLiteral.VOID

    trait QTask extends MQContext:
      def id: UUID
      def apply(): SqlLiteral  //its returned ResultSet as argument for invoking confirm，then invoke nextChoice to get the next QTask

      def setRouter(rx: QTask.Effect => Option[QTask]): Unit

      def confirm(rs: Option[ResultSet]): QTask.Effect

      def nextChoice: Option[QTask] = None

      def concatenate(oth: QTask): QTask

      def append(oth: QTask): Unit

      def +(x: QTask): QTask = this concatenate x

      def +=(x: QTask): Unit = append(x)

      def relThing: Option[Any]

      def relThing_=(x: Any): Unit

      def effect: Option[QTask.Effect]


    abstract class AbsQTask(private val _dialect: Dialect) extends QTask:
      private val _id: UUID = UUID.randomUUID()
      private var _internalNext: Option[QTask] = None
      private var _extra: Option[QTask] = None
      private var _router: QTask.Effect =>Option[QTask] = {_ => None}
      protected var _pendingFields: Seq[DBField.Ref] = Nil
      private var _relThing: Option[Any] = None
      private var _effect: Option[QTask.Effect] = None

      protected def _objMembers: Seq[TableInstance] = Nil

      protected def _internalNextChoice: Option[QTask] = _internalNext

      override def id: UUID = _id

      override def interpolateField(fld: String, v: Any): SqlLiteral =
        val (t, fldName) = fld.split("\\.", 2).toList match
          case List(f) =>
            val ts = _objMembers filter {
              _.substance.field(f).nonEmpty
            }
            if ts.length == 1 then
              (ts.head, f)
            else
              throw RuntimeException(s"Field $fld is absent or ambiguous")
          case List(table, field) =>
            val pt = _objMembers find {
              _.alias == table
            } getOrElse {
              throw RuntimeException(s"Table：$table is absent in query")
            }
            (pt, field)
          case _ => throw RuntimeException(s"Field $fld is ambiguous in query")

        val f = t.substance.field(fldName) getOrElse {
          throw RuntimeException(s"Field $fld is absent in the related tables")
        }
        _dialect.interpolateField(DBAction.FieldInstance(f, Some(t)), v)

      override def fieldNameInDB(tableAlias: String, fld: DBField): String =
        if tableAlias.isEmpty then
          _objMembers find {
            _.substance.signature == fld.owner.get.signature
          } match
            case Some(p) => _dialect.referenceField(DBAction.mkFieldInstance(fld, p))
            case _ => throw RuntimeException(s"Invalid field: $fld")
        else
          _objMembers find {
            _.alias == tableAlias
          } match
            case Some(p) => _dialect.referenceField(DBAction.mkFieldInstance(fld, p))
            case _ => throw RuntimeException(s"Invalid field: $fld @ $tableAlias")

      override def setRouter(rx: QTask.Effect => Option[QTask]): Unit =
        _router = rx

      override def confirm(rs: Option[ResultSet]): QTask.Effect =
        val result = _amendWithResult(rs)
        _effect = Some(result)
        _router(result) match
          case Some(nextTask) =>
            _internalNext match
              case Some(sx) => sx += nextTask
              case _ => _internalNext = Some(nextTask)
          case _ =>
        result

      override def concatenate(oth: QTask): QTask =
        _extra match
          case Some(extraX) => extraX.concatenate(oth)
          case _ => _extra = Some(oth)
        this

      override def append(oth: QTask): Unit =
        concatenate(oth)

      override def nextChoice: Option[QTask] =
        _internalNextChoice match
          case Some(cx) => Some(cx)
          case _ =>
            _extra match
              case Some(cxx) => Some(cxx)
              case _ => None

      override def dialect: Dialect = _dialect

      override def relThing: Option[Any] = _relThing

      override def relThing_=(x: Any): Unit =
        _relThing = x match
          case None | null => None
          case xx => Some(xx)

      override def effect: Option[QTask.Effect] = _effect

      protected def _amendWithResult(rs: Option[ResultSet]): QTask.Effect =
        rs match
          case Some(xrs) =>
            val result = mutable.ArrayBuffer[Map[DBField.Ref, Any]]()
            while xrs.next() do
              val row = _pendingFields.zipWithIndex map {case (fx, i) => (fx, fx.principal.extractData(xrs, i))}
              result += row.toMap
            QTask.Effect(result.toSeq)
          case _ => QTask.Effect(Nil)


    abstract class SimpleQTask(dx: Dialect, val objTable: DBTable) extends AbsQTask(dx):
      override protected def _objMembers: Seq[TableInstance] = Seq(objTable)

    trait Dialect:
      def customize(settings: Map[String, Any]): Unit = {}

      def composeQuery(fields: Seq[FieldInstance], partiTables: Seq[Participant], whereSpec: SqlLiteral | MCriteria, asSub: Boolean = false): QTask

      def composeInsert(t: DBTable, fields: Seq[DBField], returningFields: Seq[DBField], vs: Seq[Any] = Nil): QTask

      def composeUpdate(t: DBTable, fields: Seq[DBField], vs: Seq[Any], whereCrit: MCriteria): QTask

      def composeDelete(t: DBTable, crit: MCriteria, returnningFields: Seq[DBField]): QTask

      def interpolateField(fld: DBAction.FieldInstance, v: Any): SqlLiteral

      def introduceField(fld: FieldInstance, usingAlias: Boolean = true): SqlLiteral

      def referenceField(fld: FieldInstance): SqlLiteral

      def introduceTable(t: TableInstance): SqlLiteral

      def introduceNull(): SqlLiteral

      def introducePlaceHolder(): SqlLiteral

      def joinWithFields(f1: FieldInstance, f2: FieldInstance): SqlLiteral

      def composeJoinTable(kind: JoinKind, t: DBAction.Participant, linkStr: Seq[SqlLiteral]): SqlLiteral

      def composeFieldLiteralCriteria(fld: FieldInstance, rel: SqlLiteral, sqlExpr: Any): SqlLiteral

      def completeStatement(s: SqlLiteral): SqlLiteral

      def composeTrivialCriteria(): SqlLiteral = SqlLiteral(" 1=1 ")


    object TrivialSqlDialect extends Dialect :
      override def composeQuery(fields: Seq[FieldInstance], partiTables: Seq[Participant], whereSpec: SqlLiteral | MCriteria, asSub: Boolean): QTask = ???

      override def composeInsert(t: DBTable, fields: Seq[DBField], returningFields: Seq[DBField], vs: Seq[Any]): QTask = ???

      override def composeUpdate(t: DBTable, fields: Seq[DBField], vs: Seq[Any], whereCrit: MCriteria): QTask = ???

      override def composeDelete(t: DBTable, crit: MCriteria, returnningFields: Seq[DBField]): QTask = ???

      override def interpolateField(fld: FieldInstance, v: Any): SqlLiteral = ???

      override def introduceField(fld: FieldInstance, usingAlias: Boolean): SqlLiteral = ???

      override def referenceField(fld: FieldInstance): SqlLiteral = ???

      override def introduceTable(t: TableInstance): SqlLiteral = ???

      override def introduceNull(): SqlLiteral = ???

      override def introducePlaceHolder(): SqlLiteral = ???

      override def joinWithFields(f1: FieldInstance, f2: FieldInstance): SqlLiteral = ???

      override def composeJoinTable(kind: JoinKind, t: Participant, linkStr: Seq[SqlLiteral]): SqlLiteral = ???

      override def composeFieldLiteralCriteria(fld: FieldInstance, rel: SqlLiteral, sqlExpr: Any): SqlLiteral = ???

      override def completeStatement(s: SqlLiteral): SqlLiteral = ???


    def registerFactory(fac: Factory): Unit =
      _fac = Some(fac)

    def buildQuery(t: DBTable, crit: Option[MCriteria], selFields: Seq[DBField] = Nil, asSub: Boolean = false)(using sqlDialect: Dialect): DBAction =
      _fac match
        case Some(fx) => fx.buildQuery(t, crit, selFields, asSub)
        case _ => throw RuntimeException(s"Can't create a query, no implementation: ${t.name} && $crit")

    def buildDelete(t: DBTable, crit: Option[MCriteria])(using sqlDialect: Dialect): DBAction =
      _fac match
        case Some(fx) => fx.buildDelete(t, crit)
        case _ => throw RuntimeException(s"Can't create a DELETE sql, no implementation: ${t.name} && $crit")

    def buildInsert(t: DBTable, v: MEntity, excludedFlds: Seq[String])(using sqlDialect: Dialect): DBAction =
      _fac match
        case Some(fx) => fx.buildInsert(t, v, excludedFlds)
        case _ => throw RuntimeException(s"Can't create a INSERT sql, no implementation: ${t.name} && $v")

    def buildUpdate(t: DBTable, v: MEntity, excludedFlds: Seq[String])(using sqlDialect: Dialect): DBAction =
      _fac match
        case Some(fx) => fx.buildUpdate(t, v, excludedFlds)
        case _ => throw RuntimeException(s"Can't create a UPDATE sql, no implementation: ${t.name} && $v")

    def buildMerge(t: DBTable, v: MEntity, excludedFlds: Seq[String])(using sqlDialect: Dialect): DBAction =
      _fac match
        case Some(fx) => fx.buildMerge(t, v, excludedFlds)
        case _ => throw RuntimeException(s"Can't create a merge sql, no implementation")


  trait DBAction:
    def mainTable: DBAction.Participant

    def participants: Seq[DBAction.Participant]

    def apply(): DBAction.QTask

    def attachTask(t: DBAction.QTask): Unit

    def join(t: DBTable, joinAs: String = "", gateway: Option[DBAction.TableInstance]=None, kind: DBAction.JoinKind = DBAction.JoinKind.INNER): DBAction.Participant

    def setCriteria(crit: MCriteria): Unit

    def setInvolvedFields(flds: DBAction.FieldInstance *): Unit

    def dialect: DBAction.Dialect


  object MFactory:
    private val _metaFacs: mutable.Buffer[PartialFunction[String, MFactory]] = mutable.ArrayBuffer[PartialFunction[String, MFactory]]()
    private val _instances: mutable.Map[String, MFactory] = mutable.TreeMap[String, MFactory]()

    trait Cache:
      def find(crit: MKeyCriteria): Option[MEntity]
      def acquireWhenLosing(fac: MFactory, crit: MKeyCriteria): Seq[MEntity]
      def cacheItems(items: Seq[MEntity]): Unit

      def purge(ks: MKeyCriteria*): Unit

    abstract class BaseCache extends Cache:
      private val _lock = ReentrantLock()
      private val _depot: mutable.Buffer[MEntity] = mutable.ArrayBuffer[MEntity]()

      override def find(crit: MKeyCriteria): Option[MEntity] =
        _depot find { crit.agreeWith }

      override def cacheItems(items: Seq[MEntity]): Unit =
        _lock.lock()
        try
          items foreach { x =>
            if !_depot.contains(x) then
              _depot += x
          }
        finally
          _lock.unlock()

      override def purge(ks: MKeyCriteria*): Unit =
        _lock.lock()
        try
          if ks.nonEmpty then
            _depot --= _depot.toSeq filter {x=> ks exists {k=>k.agreeWith(x)}}
          else
            _depot.clear()
        finally
          _lock.unlock()

    class SimpleCache extends BaseCache:
      override def acquireWhenLosing(fac: MFactory, crit: MKeyCriteria): Seq[MEntity] =
        fac.fetch(crit)

    class FullCache extends BaseCache:
      override def acquireWhenLosing(fac: MFactory, crit: MKeyCriteria): Seq[MEntity] =
        fac.fetch(EmptyCriteria())

    def normalizeFldName(fld: String): String =
      fld.split("_").zipWithIndex map { case (s, i) =>
        if i > 0 then
          s.capitalize
        else
          s
      } mkString ("")

    def apply(entKind: String): MFactory =
      if ! _instances.contains(entKind) then
        _metaFacs find {_.isDefinedAt(entKind)} match
          case Some(fx) => _instances(entKind) = fx(entKind)
          case _ => throw RuntimeException(s"Can't manipulate this kind of entity: $entKind")
      _instances(entKind)

    def registerMeta(m: PartialFunction[String, MFactory]): Unit =
      _metaFacs += m

  trait MFactory:
    def activate(): Unit = {
    }

    def setDBElector(v: Int=>DBAgent): Unit

    def storage: DBTable

    def apply(): MEntity

    def fetch(crit: MCriteria): Seq[MEntity]

    def fetchByKey(crit: MKeyCriteria): Option[MEntity]

    def parse(data: Array[Byte]): MEntity

    def insert(v: MEntity): MEntity

    def update(v: MEntity, excludedFlds: Seq[String] = Nil): MEntity

    def save(v: MEntity, excludedFlds: Seq[String] = Nil): MEntity

    def saveAll(v: Seq[MEntity], excludedFlds: Seq[String] = Nil): Future[Seq[MEntity]]

    def substituteAll(crit: MCriteria, vs: MEntity *): Future[Seq[MEntity]]

    def resolveForeign(flds: String*): Option[(MFactory, Seq[DBField])]

    def resolveSub(relName: String): Option[(MFactory, Seq[(DBField, DBField)])]


  trait MEntity extends Dynamic:
    def supervisor: MFactory

    def pk: MKeyCriteria

    def toJSON: String = ""

    def selectDynamic(attrName: String): Any

    def updateDynamic(attrName: String)(v: Any): Unit

    def apply(fld: String): Any

    def update(fld: String, v: Any): Unit =
      val t = this.getClass
      throw RuntimeException(s"Try to change a immutable Entity: ${t.getName}")

    def attrDetail(flds: String*): Option[MEntity]

    def subItems(relName: String = "__sub__"): Seq[MEntity]


  trait MKeyCriteria extends MCriteria:
    def agreeWith(x: MEntity): Boolean

  case class BaseCriteria(flds: Seq[String], vals: Seq[Any]) extends MCriteria:
    override def toSql(ctx: MQContext): DBAction.SqlLiteral =
      val result = flds zip vals map {case(fx, vx)=>
        ctx.interpolateField(fx, vx)
      }
      val (sql, params) = result.unzip
      DBAction.SqlLiteral(sql.mkString(" AND "), params flatMap { x => x })

  case class PKSingleField[T](v: T, fldName: String="id") extends MKeyCriteria:
    override def toSql(ctx: MQContext): DBAction.SqlLiteral =
      ctx.interpolateField(fldName, v)

    override def agreeWith(x: MEntity): Boolean =
      v match
        case vx: v.type => x(fldName) == v
        case _ => false

  type PKInt = PKSingleField[Int]
  type PKStr = PKSingleField[String]


  class PKMultiFields(flds: Seq[String], vals: Seq[Any]) extends BaseCriteria(flds, vals) with MKeyCriteria:
    override def agreeWith(x: MEntity): Boolean =
      flds zip vals exists {case (fx, v)=> x(fx) != v }

  class EmptyCriteria extends MCriteria:
    override def toSql(ctx: MQContext): DBAction.SqlLiteral = ctx.dialect.composeTrivialCriteria()

  given Conversion[Int, MKeyCriteria]  = new PKInt(_)
  given Conversion[String, MKeyCriteria] = new PKStr(_)
  given Conversion[Option[Nothing], MCriteria] = {
    _ => new EmptyCriteria()
  }

  def bootstrap(tableFac: DBTable.Factory, fldFac: DBField.Factory, qFac: DBAction.Factory, dbCfg: Option[ConfigProvider] = None): Unit =
    DBTable.registerFactory(tableFac)
    DBField.registerFactory(fldFac)
    DBAction.registerFactory(qFac)
    dbCfg match
      case Some(cfg) => tableFac.buildDBSchema(cfg)
      case _ =>

