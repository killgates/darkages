package rk.darkages.implement

import java.sql.ResultSet
import java.util.UUID
import scala.language.postfixOps

import rk.darkages.DBApi
import DBApi.{DBAction, DBField, DBTable, MCriteria, MEntity}
import DBAction.{Dialect, FieldInstance, Participant, QTask, SqlLiteral, SqlSpecial, TableInstance}


object Dialect:

  object Postgres extends Dialect :
    override def composeQuery(fields: Seq[FieldInstance], partiTables: Seq[Participant], whereSpec: SqlLiteral | MCriteria, asSub: Boolean = false): QTask =
      new DBAction.AbsQTask(this) :
        private lazy val _selParts =
          _introduceFields()
        private lazy val _fromParts =
          _introduceTables()
        private lazy val _wherePart =
          whereSpec match
            case lx: SqlLiteral => lx
            case cx: MCriteria => SqlLiteral.fromTuple(cx.toSql(this))
            case _ => throw RuntimeException(s"Unknown query criteria：$whereSpec")

        override protected def _objMembers: Seq[TableInstance] = partiTables map {
          _.tabRef
        }

        override def apply(): SqlLiteral =
          val fldParams = _selParts flatMap {
            _.params
          }
          val tabParams = _fromParts flatMap {
            _.params
          }
          val whParams = _wherePart.params
          val params = fldParams ++ tabParams ++ whParams
          val sqlFlds = _selParts map {
            _.statement
          } mkString ","
          val sqlTables = _fromParts map {
            _.statement
          } mkString "\n"
          SqlLiteral(s"SELECT ${sqlFlds} FROM ${sqlTables} WHERE ${_wherePart.statement} ${if asSub then " " else "; "}", params)

        private def _introduceTables(): Seq[SqlLiteral] =
          val mainTable = partiTables.head
          var result = introduceTable(mainTable.tabRef) :: Nil
          partiTables.tail foreach { p =>
            p.joinInfo match
              case Some(j) =>
                if j.dir == DBAction.JoinDirection.J_OUT then
                  val ft = j.peer
                  val tt = p.tabRef
                  val sql = _mkJoinSql(ft, tt, j.joinKey, j.kind)
                  result = sql :: result
                else
                  val ft = p.tabRef
                  val tt = j.peer
                  val sql = _mkJoinSql(ft, tt, j.joinKey, j.kind)
                  result = sql :: result
              case _ =>
                throw RuntimeException(s"SQL JOIN error: join criteria is absenc!")
          }
          result

        private def _mkJoinSql(ft: TableInstance, tt: Participant, fk: DBTable.DBForeignKey, kind: DBAction.JoinKind = DBAction.JoinKind.INNER): DBAction.SqlLiteral =
          val link = fk.linkFlds map { case (x, y) =>
            joinWithFields(FieldInstance(x, Some(ft)), FieldInstance(y, Some(tt)))
          }
          composeJoinTable(kind, tt, link)

        private def _introduceFields(exclude: Seq[String] = Nil): Seq[DBAction.SqlLiteral] =
          if fields.length == 1 && fields.head.principal == DBField.fake then
            Seq(SqlLiteral(" * "))
          else if fields.isEmpty then
            _objMembers flatMap { p =>
              p.substance.selfFields filterNot { x => exclude.contains(x.virtName) } map { f =>
                introduceField(DBAction.mkFieldInstance(f, p))
              }
            }
          else
            fields map {
              introduceField(_)
            }

    override def composeInsert(t: DBTable, fields: Seq[DBField], returnningFields: Seq[DBField], vs: Seq[Any] = Nil): QTask =
      new DBAction.SimpleQTask(this, t) :

        override def apply(): SqlLiteral =
          val fldNames = fields map { x => referenceField(x) } map {
            _.statement
          }
          val fldVals = if vs.isEmpty then
            fields map { _ => introduceNull() }
          else
            vs
          val fldValSpaceholders = fields.zip(fldVals) map { case (fx, vx) =>
            val raw = vx match
              case x: DBAction.SqlLiteral => x
              case _ => introducePlaceHolder()
            fx.accTransformUp match
              case Some(fo) => SqlLiteral(fo(raw.statement), raw.params)
              case _ => raw
          }
          val sql = if returnningFields.nonEmpty then
            val returningStr = returnningFields map { x =>
              introduceField(DBAction.mkFieldInstance(x, t), false)
            }
            s"INSERT INTO ${introduceTable(t)}(${fldNames.mkString(",")}) VALUES(${fldValSpaceholders.mkString(",")}) RETURNING ${returningStr.mkString(",")};"
          else
            s"INSERT INTO ${introduceTable(t)}(${fldNames.mkString(",")}) VALUES(${fldValSpaceholders.mkString(",")});"
          _pendingFields = returnningFields map {
            _.refAs()
          }
          SqlLiteral(sql, fldVals filterNot { x => x.isInstanceOf[SqlLiteral] || x.isInstanceOf[SqlSpecial] })

    override def composeUpdate(t: DBTable, fields: Seq[DBField], vs: Seq[Any], whereCrit: MCriteria): QTask =
      new DBAction.SimpleQTask(this, t) :
        override def apply(): SqlLiteral =
          val ti = TableInstance(t, "")
          val uptFlds = fields zip vs map { case (fx, vx) =>
            vx match
              case SqlLiteral(s, _) => s"${introduceField(DBAction.mkFieldInstance(fx, ti), false)} = ${s}"
              case _ => s"${introduceField(DBAction.mkFieldInstance(fx, ti), false)} = ${introducePlaceHolder()}"
          }
          val wh = whereCrit.toSql(this)

          val rSql = s"UPDATE ${introduceTable(ti)} SET ${uptFlds.mkString(",")} WHERE ${wh.statement};"
          val fldParams = vs filterNot { x => x.isInstanceOf[SqlLiteral] || x.isInstanceOf[SqlSpecial] }
          SqlLiteral(rSql, fldParams ++ wh.params)

    override def composeDelete(t: DBTable, crit: MCriteria, returnningFields: Seq[DBField]): QTask =
      new DBAction.SimpleQTask(this, t) :
        private val _target = TableInstance(t, "")
        private val _crit = crit
        private val _returnningFields = returnningFields

        override def apply(): SqlLiteral =
          val critSql = crit.toSql(this)
          val sql = if _returnningFields.nonEmpty then
            val returnningSql = _returnningFields map { x => introduceField(DBAction.mkFieldInstance(x, _target)) } mkString ", "
            s"DELETE FROM ${introduceTable(_target)} WHERE ${critSql.statement} RETURNING ${returnningSql};"
          else
            s"DELETE FROM ${introduceTable(_target)} WHERE ${critSql.statement};"
          SqlLiteral(sql, critSql.params)

    override def interpolateField(fld: DBAction.FieldInstance, v: Any): SqlLiteral =
      val ph = fld.principal.accTransformUp match
        case Some(f) => f(introducePlaceHolder())
        case _ => introducePlaceHolder()

      fld.engagedBy match
        case Some(t) =>
          if t.alias.nonEmpty then
            SqlLiteral(s"${t.alias}.${fld.principal.name} = ${ph}", Seq(v))
          else
            SqlLiteral(s"${fld.principal.name} = ${ph}", Seq(v))
        case _ =>
          SqlLiteral(s"${fld.principal.name} = ${ph}", Seq(v))

    override def introduceField(fld: DBAction.FieldInstance, usingAlias: Boolean = true): SqlLiteral =
      if usingAlias then
        fld.engagedBy match
          case Some(t) =>
            if t.alias.isEmpty then
              fld.principal.accTransformDown match
                case Some(f) => s"${f(fld.principal.name)} AS ${fld.principal.name}"
                case _ => s"${fld.principal.name}"
            else
              fld.principal.accTransformDown match
                case Some(f) =>
                  val s = s"${t.alias}.${fld.principal.name}"
                  s"${f(s)} AS ${t.alias}_${fld.principal.name}"
                case _ => s"${t.alias}.${fld.principal.name} AS ${t.alias}_${fld.principal.name}"
          case _ => s"${fld.principal.name}"
      else
        fld.principal.accTransformDown match
          case Some(f) => f(fld.principal.name)
          case _ => fld.principal.name

    override def referenceField(fld: FieldInstance): SqlLiteral =
      fld.engagedBy match
        case Some(t) =>
          if t.alias.isEmpty then
            fld.principal.name
          else
            s"${t.alias}_${fld.principal.name}"
        case _ => fld.principal.name

    override def introduceTable(t: TableInstance): SqlLiteral =
      if t.alias.nonEmpty then
        if t.principal.schema.nonEmpty then
          s" ${t.principal.schema}.${t.principal.name} AS ${t.alias} "
        else
          s" ${t.principal.name} AS ${t.alias} "
      else if t.principal.schema.nonEmpty then
        s" ${t.principal.schema}.${t.principal.name} "
      else
        s" ${t.principal.name} "

    override def introduceNull(): SqlLiteral = SqlLiteral(" NULL ")

    override def introducePlaceHolder(): SqlLiteral = SqlLiteral("?")

    override def joinWithFields(f1: FieldInstance, f2: FieldInstance): SqlLiteral =
      s" ${introduceField(f1, false)} = ${introduceField(f2, false)} "

    override def composeJoinTable(kind: DBAction.JoinKind, t: DBAction.Participant, linkExprs: Seq[SqlLiteral]): SqlLiteral =
      val sql = if t.alias.nonEmpty then
        if t.substance.schema.nonEmpty then
          s" ${kind.s} JOIN ${t.substance.schema}.${t.substance.name} AS ${t.alias} ON (${linkExprs.mkString(" AND ")}) "
        else
          s" ${kind.s} JOIN ${t.substance.name} AS ${t.alias} ON (${linkExprs.mkString(" AND ")}) "
      else if t.substance.schema.nonEmpty then
        s" ${kind.s} JOIN ${t.substance.schema}.${t.substance.name} AS ${t.substance.name} ON (${linkExprs.mkString(" AND ")}) "
      else
        s" ${kind.s} JOIN ${t.substance.name} AS ${t.substance.name} ON (${linkExprs.mkString(" AND ")}) "
      SqlLiteral(sql, linkExprs flatMap {
        _.params
      })

    override def composeFieldLiteralCriteria(fld: FieldInstance, rel: SqlLiteral, sqlExpr: Any): SqlLiteral =
      val literalExpr = sqlExpr match
        case x: SqlLiteral => x
        case x: SqlSpecial => SqlLiteral(x.toString)
        case elVal => SqlLiteral(introducePlaceHolder(), Seq(elVal))

      rel.statement match
        case "=" | "<" | "<=" | ">" | ">=" | "LIKE" =>
          SqlLiteral(s"${introduceField(fld, false)} ${rel.statement} ${literalExpr.statement}", literalExpr.params)
        case _ => ""

    override def completeStatement(s: SqlLiteral): SqlLiteral =
      if !s.statement.endsWith(";") then
        SqlLiteral(s"$s;", s.params)
      else
        s


  object H2 extends Dialect :
    override def composeQuery(fields: Seq[FieldInstance], partiTables: Seq[Participant], whereSpec: SqlLiteral | MCriteria, asSub: Boolean = false): QTask =
      new DBAction.AbsQTask(this) :
        private lazy val _selParts =
          _introduceFields()
        private lazy val _fromParts =
          _introduceTables()
        private lazy val _wherePart =
          whereSpec match
            case lx: SqlLiteral => lx
            case cx: MCriteria => SqlLiteral.fromTuple(cx.toSql(this))
            case _ => throw RuntimeException(s"Unknown query criteria：$whereSpec")

        override protected def _objMembers: Seq[TableInstance] = partiTables map {
          _.tabRef
        }

        override def apply(): SqlLiteral =
          val fldParams = _selParts flatMap {
            _.params
          }
          val tabParams = _fromParts flatMap {
            _.params
          }
          val whParams = _wherePart.params
          val params = fldParams ++ tabParams ++ whParams
          val sqlFlds = _selParts map {
            _.statement
          } mkString ","
          val sqlTables = _fromParts map {
            _.statement
          } mkString "\n"
          SqlLiteral(s"SELECT ${sqlFlds} FROM ${sqlTables} WHERE ${_wherePart.statement} ${if asSub then " " else "; "}", params)

        private def _introduceTables(): Seq[SqlLiteral] =
          val mainTable = partiTables.head
          var result = introduceTable(mainTable.tabRef) :: Nil
          partiTables.tail foreach { p =>
            p.joinInfo match
              case Some(j) =>
                if j.dir == DBAction.JoinDirection.J_OUT then
                  val ft = j.peer
                  val tt = p.tabRef
                  val sql = _mkJoinSql(ft, tt, j.joinKey, j.kind)
                  result = sql :: result
                else
                  val ft = p.tabRef
                  val tt = j.peer
                  val sql = _mkJoinSql(ft, tt, j.joinKey, j.kind)
                  result = sql :: result
              case _ =>
                throw RuntimeException(s"SQL JOIN error: join criteria is absenc!")
          }
          result

        private def _mkJoinSql(ft: TableInstance, tt: Participant, fk: DBTable.DBForeignKey, kind: DBAction.JoinKind = DBAction.JoinKind.INNER): DBAction.SqlLiteral =
          val link = fk.linkFlds map { case (x, y) =>
            joinWithFields(FieldInstance(x, Some(ft)), FieldInstance(y, Some(tt)))
          }
          composeJoinTable(kind, tt, link)

        private def _introduceFields(exclude: Seq[String] = Nil): Seq[DBAction.SqlLiteral] =
          if fields.length == 1 && fields.head.principal == DBField.fake then
            Seq(SqlLiteral(" * "))
          else if fields.isEmpty then
            _objMembers flatMap { p =>
              p.substance.selfFields filterNot { x => exclude.contains(x.virtName) } map { f =>
                introduceField(DBAction.mkFieldInstance(f, p))
              }
            }
          else
            fields map {
              introduceField(_)
            }

    override def composeInsert(t: DBTable, fields: Seq[DBField], returnningFields: Seq[DBField], vs: Seq[Any] = Nil): QTask =
      if returnningFields.nonEmpty then
        val result = _buildAutoGenFieldsQTask(t, returnningFields)
        result += _buildDoInsertQTask(t, fields, returnningFields, vs, result.effect.getOrElse(QTask.Effect(Nil)))
        result += _buildReturnningQtask(t, returnningFields)
        result
      else
        _buildDoInsertQTask(t, fields, returnningFields, vs, QTask.Effect(Nil))


    private def _buildAutoGenFieldsQTask(t: DBTable, returnningFields: Seq[DBField]): QTask =
      new DBAction.SimpleQTask(this, t) :
        override def apply(): SqlLiteral =
          if returnningFields.isEmpty then
            SqlLiteral.VOID
          else
            _pendingFields = returnningFields map {
              _.refAs()
            }
            val seqInfos = returnningFields map { x =>
              x.owner match
                case Some(t) =>
                  if t.schema.nonEmpty then
                    (t.schema, s"${x.name}_seq", x.dataType)
                  else
                    ("", s"${x.name}_seq", x.dataType)
                case _ => ("", s"${x.name}_seq", x.dataType)
            }
            val seqGens = seqInfos map { case (schema, sx, dataType) =>
              dataType match
                case DBField.DataType.INT =>
                  if schema.nonEmpty then
                    s"NETXVAL($schema, $sx)"
                  else
                    s"NETXVAL($sx)"
                case DBField.DataType.STR =>
                  s"'X_${UUID.randomUUID().toString.replaceAll("-", "_")}'"
                case _ =>
                  throw RuntimeException(s"Can't generate a unique value for field: ${dataType}")
            }
            s"SELECT ${seqGens.mkString(",")};"

    private def _buildReturnningQtask(t: DBTable, returnningFields: Seq[DBField]): QTask =
      val p = DBAction.Participant(t, None)
      val flds = returnningFields map {
        DBAction.FieldInstance(_, Some(p.tabRef))
      }
      composeQuery(flds, Seq(p), MCriteria(returnningFields map {
        _.ingestName(true)
      }, returnningFields map {
        DBAction.FieldInstance(_, None)
      }))

    private def _buildDoInsertQTask(t: DBTable, fields: Seq[DBField], returnningFields: Seq[DBField], vs: Seq[Any], autoInfo: => QTask.Effect): QTask =
      new DBAction.SimpleQTask(this, t) :
        override def apply(): SqlLiteral =
          val (ultFlds, ultVals) = if returnningFields.nonEmpty then
            autoInfo.outcome match
              case Seq(autoVals) =>
                val extFileds = returnningFields filterNot {
                  fields.contains(_)
                }
                val extVals = extFileds map { _ => None }

                val allFields = fields ++ extFileds
                val mVals = if vs.isEmpty then
                  val xFields = fields map { _ => introduceNull() }
                  xFields ++ extVals
                else
                  vs ++ extVals
                val allVals = allFields zip mVals map { case (f, xv) =>
                  val fr = f.refAs()
                  if autoVals.contains(fr) then
                    autoVals(fr)
                  else
                    xv
                }
                (allFields, allVals)
              case _ => throw RuntimeException(s"Invalid auto generation while insert to $t")
          else
            (fields, vs)

          val fldNames = ultFlds map { x => introduceField(x, false) } map {
            _.statement
          }
          val fldValSpaceholders = ultVals map { vx =>
            vx match
              case x: DBAction.SqlLiteral => x
              case _ => introducePlaceHolder()
          }
          val sql = s"INSERT INTO ${introduceTable(t)}(${fldNames.mkString(",")}) VALUES(${fldValSpaceholders.mkString(",")});"

          SqlLiteral(sql, ultVals filterNot { x => x.isInstanceOf[SqlLiteral] || x.isInstanceOf[SqlSpecial] })


    override def composeUpdate(t: DBTable, fields: Seq[DBField], vs: Seq[Any], whereCrit: MCriteria): QTask =
      new DBAction.SimpleQTask(this, t) :
        override def apply(): SqlLiteral =
          val ti = TableInstance(t, "")
          val uptFlds = fields zip vs map { case (fx, vx) =>
            vx match
              case SqlLiteral(s, _) => s"${introduceField(DBAction.mkFieldInstance(fx, ti), false)} = ${s}"
              case _ => s"${introduceField(DBAction.mkFieldInstance(fx, ti), false)} = ${introducePlaceHolder()}"
          }
          val wh = whereCrit.toSql(this)

          val rSql = s"UPDATE ${introduceTable(ti)} SET ${uptFlds.mkString(",")} WHERE ${wh.statement};"
          val fldParams = vs filterNot { x => x.isInstanceOf[SqlLiteral] || x.isInstanceOf[SqlSpecial] }
          SqlLiteral(rSql, fldParams ++ wh.params)

    override def composeDelete(t: DBTable, crit: MCriteria, returnningFields: Seq[DBField]): QTask =
      new DBAction.AbsQTask(this) :
        private val _target = TableInstance(t, "")
        private val _crit = crit
        private val _returnningFields = returnningFields

        override def apply(): SqlLiteral =
          val critSql = crit.toSql(this)
          val sql = if _returnningFields.nonEmpty then
            val returnningSql = _returnningFields map { x => introduceField(DBAction.mkFieldInstance(x, _target)) } mkString ", "
            s"DELETE FROM ${introduceTable(_target)} WHERE ${critSql.statement} RETURNING ${returnningSql};"
          else
            s"DELETE FROM ${introduceTable(_target)} WHERE ${critSql.statement};"
          SqlLiteral(sql, critSql.params)

    override def interpolateField(fld: DBAction.FieldInstance, v: Any): SqlLiteral =
      fld.engagedBy match
        case Some(t) =>
          if t.alias.nonEmpty then
            SqlLiteral(s"${t.alias}.${fld.principal.name} = ${introducePlaceHolder()}", Seq(v))
          else
            SqlLiteral(s"${fld.principal.name} = ${introducePlaceHolder()}", Seq(v))
        case _ =>
          SqlLiteral(s"${fld.principal.name} = ${introducePlaceHolder()}", Seq(v))

    override def introduceField(fld: DBAction.FieldInstance, usingAlias: Boolean = true): SqlLiteral =
      if usingAlias then
        fld.engagedBy match
          case Some(t) =>
            if t.alias.isEmpty then
              s"${fld.principal.name}"
            else
              s"${t.alias}.${fld.principal.name} AS ${t.alias}_${fld.principal.name}"
          case _ => s"${fld.principal.name}"
      else
        s"${fld.principal.name}"

    override def referenceField(fld: FieldInstance): SqlLiteral =
      fld.engagedBy match
        case Some(t) =>
          if t.alias.isEmpty then
            fld.principal.name
          else
            s"${t.alias}_${fld.principal.name}"
        case _ => fld.principal.name

    override def introduceTable(t: TableInstance): SqlLiteral =
      if t.alias.nonEmpty then
        if t.principal.schema.nonEmpty then
          s" ${t.principal.schema}.${t.principal.name} AS ${t.alias} "
        else
          s" ${t.principal.name} AS ${t.alias} "
      else if t.principal.schema.nonEmpty then
        s" ${t.principal.schema}.${t.principal.name} "
      else
        s" ${t.principal.name} "

    override def introduceNull(): SqlLiteral = SqlLiteral(" NULL ")

    override def introducePlaceHolder(): SqlLiteral = SqlLiteral("?")

    override def joinWithFields(f1: FieldInstance, f2: FieldInstance): SqlLiteral =
      s" ${introduceField(f1, false)} = ${introduceField(f2, false)} "

    override def composeJoinTable(kind: DBAction.JoinKind, t: DBAction.Participant, linkExprs: Seq[SqlLiteral]): SqlLiteral =
      val sql = if t.alias.nonEmpty then
        if t.substance.schema.nonEmpty then
          s" ${kind.s} JOIN ${t.substance.schema}.${t.substance.name} AS ${t.alias} ON (${linkExprs.mkString(" AND ")}) "
        else
          s" ${kind.s} JOIN ${t.substance.name} AS ${t.alias} ON (${linkExprs.mkString(" AND ")}) "
      else if t.substance.schema.nonEmpty then
        s" ${kind.s} JOIN ${t.substance.schema}.${t.substance.name} AS ${t.substance.name} ON (${linkExprs.mkString(" AND ")}) "
      else
        s" ${kind.s} JOIN ${t.substance.name} AS ${t.substance.name} ON (${linkExprs.mkString(" AND ")}) "
      SqlLiteral(sql, linkExprs flatMap {
        _.params
      })

    override def composeFieldLiteralCriteria(fld: FieldInstance, rel: SqlLiteral, sqlExpr: Any): SqlLiteral =
      val literalExpr = sqlExpr match
        case x: SqlLiteral => x
        case x: SqlSpecial => SqlLiteral(x.toString)
        case elVal => SqlLiteral(introducePlaceHolder(), Seq(elVal))

      rel.statement match
        case "=" | "<" | "<=" | ">" | ">=" | "LIKE" =>
          SqlLiteral(s"${introduceField(fld, false)} ${rel.statement} ${literalExpr.statement}", literalExpr.params)
        case _ => ""

    override def completeStatement(s: SqlLiteral): SqlLiteral =
      if !s.statement.endsWith(";") then
        SqlLiteral(s"$s;", s.params)
      else
        s


