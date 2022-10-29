package rk.darkages.implement

import scala.collection.mutable

import java.sql.ResultSet
import scala.language.postfixOps

import rk.darkages.DBApi
import DBApi.{DBAction, DBField, DBTable, MCriteria, MEntity, MFactory}
import DBAction.{QTask, SqlLiteral, FieldInstance}

object ActionImpl extends DBAction.Factory:
  override def buildQuery(t: DBTable, crit: Option[MCriteria], selFields: Seq[DBField], asSub: Boolean)(using sqlDialect: DBAction.Dialect): DBAction =
    new QueryImpl(t, crit, selFields.filter({_.owner == t}), asSub)


  override def buildDelete(t: DBTable, crit: Option[MCriteria])(using sqlDialect: DBAction.Dialect): DBAction =
    new DeleteImpl(t, crit)

  override def buildInsert(t: DBTable, v: MEntity, excludedFlds: Seq[String])(using sqlDialect: DBAction.Dialect): DBAction =
    new InsertImpl(t, v, excludedFlds)

  override def buildUpdate(t: DBTable, v: MEntity, excludedFlds: Seq[String])(using sqlDialect: DBAction.Dialect): DBAction =
    new UpdateImpl(t, v, excludedFlds)

  override def buildMerge(t: DBTable, v: MEntity, excludedFlds: Seq[String])(using sqlDialect: DBAction.Dialect): DBAction =
    new MergeImpl(t, v, excludedFlds)

abstract class ActionImpl(protected val _dialect: DBAction.Dialect) extends DBAction:
  private val _participants: mutable.Buffer[DBAction.Participant] = mutable.ArrayBuffer()
  private var _involvedFields: Seq[DBAction.FieldInstance] = Nil
  private var _criteria: Option[MCriteria] = None
  private var _extraTask: Option[DBAction.QTask] = None

  override def participants: Seq[DBAction.Participant] = _participants.toSeq

  override def mainTable: DBAction.Participant = participants.head

  override def setCriteria(crit: MCriteria): Unit = _criteria = Some(crit)

  override def setInvolvedFields(flds: FieldInstance*): Unit = _involvedFields = flds

  override def attachTask(t: QTask): Unit =
    _extraTask match
      case Some(tx) => tx += t
      case _ => _extraTask = Some(t)

  override def apply(): QTask =
    val result = _buildTask()
    if _extraTask.nonEmpty then
      result += _extraTask.get
    result

  override def join(t: DBTable, joinAs: String="", gateway: Option[DBAction.TableInstance]=None, kind: DBAction.JoinKind = DBAction.JoinKind.INNER): DBAction.Participant =
    val alias = joinAs match
      case s if s.isEmpty => if _participants.exists(_.substance == t) then s"$t.name_${DBAction.uniqueSerNum}" else ""
      case s => s

    if _participants.isEmpty then
      val tx = DBAction.TableInstance(t, alias)
      val px = DBAction.Participant(tx, None)
      _participants += px
      px
    else
      _participants find { px=>
        px.substance.foreignKeys exists {_.rTable == t && (gateway.isEmpty || gateway.get==px.tabRef)}
      } match
        case Some(p) =>
          val tx = DBAction.TableInstance(t, alias)
          val fk = p.substance.foreignKeys.find({_.rTable==t}).get
          val joinInfo = DBAction.JoinAnchor(p.tabRef, fk, DBAction.JoinDirection.J_OUT, kind)
          val px = DBAction.Participant(tx, Some(joinInfo))
          _participants += px
          px
        case _ =>
          _participants find {px=>
            t.foreignKeys exists {_.rTable == px.substance && (gateway.isEmpty || gateway.get == px.tabRef)}
          } match
            case Some(p) =>
              val tx = DBAction.TableInstance(t, alias)
              val fk = t.foreignKeys.find({_.rTable == p.substance }).get
              val joinInfo = DBAction.JoinAnchor(p.tabRef, fk, DBAction.JoinDirection.J_IN, kind)
              val px = DBAction.Participant(tx, Some(joinInfo))
              _participants += px
              px
            case _ =>
              throw RuntimeException(s"Can't join table: $t")

  override def dialect: DBAction.Dialect = _dialect

  protected def uSerNum: Int = DBAction.uniqueSerNum

  protected def _currCriteria: MCriteria = _criteria.getOrElse(MCriteria.trivial)

  protected def _currInvolvedFields: Seq[DBAction.FieldInstance] = _involvedFields

  protected def _buildTask(): DBAction.QTask = DBAction.QTask.empty


class QueryImpl(t: DBTable, crit: Option[MCriteria] = None, selFields: Seq[DBField] = Nil, asSub: Boolean = false)
               (using sqlDialect: DBAction.Dialect) extends ActionImpl(sqlDialect):
  private val _asSub = asSub
  val tp = join(t)
  if crit.nonEmpty then
    setCriteria(crit.get)
  setInvolvedFields(selFields map {FieldInstance(_, Some(tp.tabRef))}: _*)

  override protected def _buildTask(): DBAction.QTask =
    dialect.composeQuery(_currInvolvedFields, participants, _currCriteria, _asSub)


abstract class DBManipulate(t: DBTable, crit: Option[MCriteria], sqlDialect: DBAction.Dialect) extends ActionImpl(sqlDialect):
  join(t)
  if crit.nonEmpty then
    setCriteria(crit.get)

class DeleteImpl(t: DBTable, crit: Option[MCriteria] = None)(using sqlDialect: DBAction.Dialect) extends DBManipulate(t, crit, sqlDialect):
  override protected def _buildTask(): DBAction.QTask =
    dialect.composeDelete(mainTable.substance, _currCriteria, Nil)

class InsertImpl(t: DBTable, val v: MEntity, val excludedFlds: Seq[String]=Nil)(using sqlDialect: DBAction.Dialect) extends DBManipulate(t, None, sqlDialect):
  override protected def _buildTask(): DBAction.QTask =
    val h = mainTable.substance.inheritedHierarchy
    val top = h.head
    var derived = h.tail

    val necessaryFlds = top.selfFields filterNot { x =>
      val fName = x.virtName
      val fVal = v(fName)
      x.willAutoGen(fVal) || excludedFlds.contains(x.virtName)
    }
    val params = necessaryFlds map { x => v(x.virtName) }
    val returnningFlds = top.pkFields filter {x=> x.willAutoGen(v(x.virtName))}
    val result = dialect.composeInsert(top, necessaryFlds, returnningFlds, params)

    if derived.nonEmpty then
      while derived.nonEmpty do
        val t = derived.head
        val ti = DBAction.TableInstance(t, "")
        derived = derived.tail

        val pkFlds = t.pkFields
        val otherFlds = t.selfFields filterNot { x =>
          excludedFlds.contains(x.virtName) || x.isPK
        }
        val necessaryFlds = pkFlds ++ otherFlds
        val pkVals = top.pkFields map {x => DBAction.FieldInstance(x, Some(ti), true)}
        val otherVals = otherFlds map {DBAction.FieldInstance(_, Some(ti), true)} map {x => v(x.quoteName)}
        val params10 = pkVals ++ otherVals
        val sql10 = dialect.composeInsert(t, necessaryFlds, Nil, params10)
        result += sql10

    result


class UpdateImpl(t: DBTable, val v: MEntity, val excludedFlds: Seq[String]=Nil)(using sqlDialect: DBAction.Dialect) extends DBManipulate(t, None, sqlDialect):
  override protected def _buildTask(): DBAction.QTask =
    val h = mainTable.substance.inheritedHierarchy.reverse
    val bottom = h.head
    var derived = h.tail
    val result = _buildForSingleTable(bottom, true)

    while derived.nonEmpty do
      val tx = derived.head
      derived = derived.tail
      result += _buildForSingleTable(tx, false)
    result

  private def _buildForSingleTable(tx: DBTable, isLeaf: Boolean): QTask =
    val (uptFlds, uptVals) = tx.selfFields filterNot { x =>
      x.isPK || excludedFlds.contains(x.virtName)
    } map { x =>
      val fldName = x.ingestName(isLeaf)
      (x, v(fldName)) } unzip
    val wh = MCriteria(tx.pkFields map {_.virtName}, tx.pkFields map {x=> v(x.ingestName(isLeaf))} )
    dialect.composeUpdate(tx, uptFlds , uptVals, wh)

class MergeImpl(t: DBTable, val v: MEntity, val excludedFlds: Seq[String]=Nil)(using sqlDialect: DBAction.Dialect) extends DBManipulate(t, None, sqlDialect):
  override protected def _buildTask(): DBAction.QTask =
    val ins = new InsertImpl(t, v, excludedFlds)
    if t.hierFields exists { fld => (fld.principal.isPK && fld.principal.willAutoGen(v(fld.accKey))) } then
      ins.apply()
    else
      val upd = new UpdateImpl(t, v, excludedFlds)
      val aCheck = new QueryImpl(t, Some(v.pk))
      val tCheck = aCheck()
      tCheck.setRouter({ m =>
        if m.outcome.isEmpty then
          Some(ins())
        else
          Some(upd())
      })
      tCheck



