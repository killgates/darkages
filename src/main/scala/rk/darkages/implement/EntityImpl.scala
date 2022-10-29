package rk.darkages.implement

import java.sql.{Statement, ResultSet}
import java.time.{Instant, LocalDate}
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import com.typesafe.scalalogging.Logger

import rk.darkages.{DBAgent, Util, DBApi}
import DBApi.{DBAction, DBField, DBTable, MCriteria, MEntity, MFactory, MKeyCriteria, PKMultiFields, PKSingleField}
import DBAction.SqlLiteral
import DBAgent.SqlTask.Guide

import scala.language.dynamics

class FactoryImpl(val table: DBTable, private val _cache: Option[MFactory.Cache]=None) extends MFactory:
  private var _dbElector: Int=> DBAgent = { DBApi.adapt }
  table.bind(this)

  override def storage: DBTable = table

  override def setDBElector(v: Int => DBAgent): Unit =
    _dbElector = v

  override def apply(): MEntity =
    new EntityImpl(this)

  override def fetch(crit: MCriteria): Seq[MEntity] =
    val q = table.selectSql(crit)
    val d = q()
    val DBAction.SqlLiteral(sql, params) = d()
    val rs = _matchedDB.query(sql, params: _*).singleReturning
    val result = mutable.ArrayBuffer[MEntity]()
    while rs.next() do
      val x = apply()
      table.hierFields foreach { f =>
        x.update(f.accKey, f.principal.extractData(d, rs))
      }
      result += x
    result.toSeq

  override def fetchByKey(crit: MKeyCriteria): Option[MEntity] =
    _cache match
      case Some(sx) =>
        sx.find(crit) match
          case Some(r) => Some(r)
          case _ =>
            val newItems = sx.acquireWhenLosing(this, crit)
            sx.cacheItems(newItems)
            newItems find {
              crit.agreeWith
            }
      case _ =>
        fetch(crit).headOption

  override def parse(data: Array[Byte]): MEntity =
    throw RuntimeException("No implementation")

  override def insert(v: MEntity): MEntity =
    val q = table.insertSql(v)
    val r = _saveWithAction(q, Some(v))
    r.getOrElse(v)


  override def update(v: MEntity, excludedFlds: Seq[String]): MEntity =
    val q = table.updateSql(v, excludedFlds)
    val r = _saveWithAction(q, Some(v))
    r.getOrElse(v)

  override def save(v: MEntity, excludedFlds: Seq[String] = Nil): MEntity =
    val q = table.mergeSql(v, excludedFlds)
    val r = _saveWithAction(q, Some(v))
    r.getOrElse(v)

  override def saveAll(v: Seq[MEntity], excludedFlds: Seq[String] = Nil): Future[Seq[MEntity]] =
    given ExecutionContext = ExecutionContext.global

    val t = table.mergeSql(v.head, excludedFlds)()
    t.relThing = v.head

    v.tail foreach { x =>
      val q = table.mergeSql(x, excludedFlds)
      val tx = q()
      tx.relThing = x
      t += tx
    }

    val sx: DBAgent.SqlScheme = new DBAgent.SqlScheme() :
      private val _qTask: DBAction.QTask = t
      private var _execSer = 1

      override def guide: Option[Guide] = Some(_execGuide)

      def _execGuide(st: Statement, rs: Option[ResultSet], relObj: Option[Any]): Int =
        val m = _qTask.confirm(rs)
        _qTask.relThing match
          case Some(target) if target.isInstanceOf[MEntity] =>
            if m.outcome.size == 1 then
              var attrs = m.outcome.head map {case(k, v)=> (k.accKey, v)}
              _amend(target.asInstanceOf[MEntity], attrs)
            else
              throw RuntimeException(s"Invalid entity effect: ${m}")
          case _ =>
        _qTask.nextChoice match
          case Some(tNext) =>
            _execSer += 1
            val DBAction.SqlLiteral(sql, params) = tNext()
            addTask(_execSer, DBAgent.SqlTask(sql, params))
            _execSer
          case _ => -1

    _matchedDB.execScheme(sx) map { _ => v }

  override def substituteAll(crit: MCriteria, vs: MEntity*): Future[Seq[MEntity]] =
    given ExecutionContext = ExecutionContext.global

    _cache match
      case Some(cx) => cx.purge()
      case _ =>
    val q = table.substituteSql(crit, vs: _*)
    Future {
      _saveWithAction(q, None)
      vs
    }

  override def resolveForeign(flds: String*): Option[(MFactory, Seq[DBField])] =
    table.foreignKeys find { fk => fk.linkFlds.map({
      _._1
    }) == flds
    } map { x =>
      (x.rTable, x.linkFlds.map({
        _._2
      }))
    } match
      case Some((t, rFlds)) =>
        Some((t.boundWith.get, rFlds))
      case _ => None

  override def resolveSub(relName: String = "__sub__"): Option[(MFactory, Seq[(DBField, DBField)])] =
    DBTable.findRevFK(table, relName) match
      case Some(fk) =>
        Some(fk.lTable.boundWith.get, fk.linkFlds)
      case _ => None

  protected def _matchedDB: DBAgent =
    _dbElector(table.dbHint)

  protected def _saveWithAction(q: DBAction, relEnt: Option[MEntity]): Option[MEntity] =
    var xt = q()
    var running = true
    val db = _matchedDB
    val pId = db.preserve()
    try
      while running do
        val sqlSpec = xt()
        val rs = if sqlSpec.isActive then
          val DBAction.SqlLiteral(sql, params) = sqlSpec
          val xParams = params map {x =>
            x match
              case xx: DBAction.PendingVal => xx()
              case xx: DBAction.FieldInstance =>
                relEnt match
                  case Some(v) => v(xx.quoteName)
                  case _ => throw RuntimeException("Invalid field value, NO relative object to get attr")
              case xx => xx
          }
          db.queryEx(pId, false, sql, xParams: _*).singleReturningOpt
        else
          None
        val chgAttrs = xt.confirm(rs)
        relEnt match
          case Some(v) =>
            if chgAttrs.outcome.isEmpty then
            //_log.debug("")
              ()
            else if chgAttrs.outcome.size == 1 then
              _amend(v, chgAttrs.outcome.head map {case (k, v) => (k.accKey, v)})
            else
              throw RuntimeException(s"Unknown returnning values from sql, abount ->: $v")
          case _ =>
        xt = xt.nextChoice.getOrElse(DBAction.QTask.empty)
        running = xt != DBAction.QTask.empty
      db.queryEx(pId, true, "")
      relEnt
    finally
      db.unchain(pId)

  private def _amend(v: MEntity, attrs: Map[String, Any]): MEntity =
    attrs foreach {case (k, x)=>
      v(k) = x
    }
    v

class EntityImpl(override val supervisor: MFactory) extends MEntity with Dynamic:
  private val _attrs = mutable.HashMap[String, Any]()

  override def selectDynamic(attrName: String): Any =
    apply(attrName)

  override def updateDynamic(attrName: String)(v: Any): Unit =
    update(attrName, v)

  override def apply(fld: String): Any =
    val fldName = MFactory.normalizeFldName(fld)
    _findValidFieldRef(fldName) match
      case Some(fx) => _attrs(fx.accKey)
      case _ => throw RuntimeException(s"Illegal attribute name: $fld")

  override def update(fld: String, v: Any): Unit =
    val fldName = MFactory.normalizeFldName(fld)
    if _checkValidField(fldName, v) then
      val fx = _findValidFieldRef(fldName)
      _attrs(fx.get.accKey) = v
    else
      throw RuntimeException(s"Illegal name or value: $fld=$v")

  override def pk: MKeyCriteria =
    val flds = supervisor.storage.pkFields
    if flds.length == 1 then
      flds.head.buildPK(this)
    else
      val vs = flds map {fx => apply(fx.virtName)}
      new PKMultiFields(flds map {_.virtName}, vs)

  override def attrDetail(flds: String*): Option[MEntity] =
    supervisor.resolveForeign(flds: _*) match
      case Some((fac, kFlds)) =>
        val lFlds = flds map {
          supervisor.storage.field(_)
        }
        val fldVals = lFlds map {
          case Some(fx) =>
            apply(MFactory.normalizeFldName(fx.virtName))
          case _ => None
        }
        val k = PKMultiFields(kFlds map {_.virtName}, fldVals)
        fac.fetchByKey(k)
      case _ => None

  override def subItems(relName: String): Seq[MEntity] =
    supervisor.resolveSub(relName) match
      case Some((fac, linkFlds)) =>
        val fldVals = linkFlds map { case (_, fx) => apply(fx.name) }
        val kFlds = linkFlds map { case (fx, _) => fx }
        val k = PKMultiFields(kFlds map {_.virtName}, fldVals)
        fac.fetch(k)
      case _ => Nil

  private def _findValidFieldRef(fld: String): Option[DBField.Ref] =
    val avFlds = supervisor.storage.hierFields filter { x => x.principal.virtName == fld || x.accKey == fld }
    avFlds.size match
      case 1 => Some(avFlds.head)
      case n if n > 0 => avFlds find {x=> x.accKey == x.principal.virtName}
      case _ => None



  private def _checkValidField(fld: String, v: Any): Boolean =
    _findValidFieldRef(fld) match
      case Some(fx) =>
        fx.principal.dataType match
          case DBField.DataType.INT => v.isInstanceOf[Int]
          case DBField.DataType.FLT => v.isInstanceOf[Double | Float]
          case DBField.DataType.STR => v.isInstanceOf[String]
          case DBField.DataType.TM => v.isInstanceOf[Instant]
          case DBField.DataType.DT => v.isInstanceOf[LocalDate]
          case DBField.DataType.BOOL => v.isInstanceOf[Boolean]
          case DBField.DataType.GEOM => v.isInstanceOf[String]
          //case _: DBField.DataType.JSON => v.isInstanceOf[String]
          case _ => false
      case _ => false

