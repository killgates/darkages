package rk.darkages.implement

import java.sql.ResultSet
import java.time.{Instant, LocalDate}
import rk.darkages.{Util, DBApi}
import DBApi.{DBAction, DBField}
import DBApi.DBField.{DataType, Factory}

object FieldImpl extends Factory:

  override def inferDataType(v: Any): DataType =
    v match
      case _: Int => DataType.INT
      case _: Double | Float => DataType.FLT
      case _: String => DataType.STR
      case _: Instant => DataType.TM
      case _: LocalDate => DataType.DT
      case _: Boolean => DataType.BOOL
      case _: Array[Byte] => DataType.LOB
      case _ => throw RuntimeException(s"Can't infer the data type of the field: $v")

  override def apply[F](virtName: String, name: String, dt: DBField.DataType, dVal: F, fldAccFun: Option[(String => String, String => String)] = None): DBField =
    new FieldImpl(virtName, name, dt, dVal) :
      override type T = F
      accTransform = fldAccFun

      override def extractData(q: DBApi.MQContext, rs: ResultSet, partAlias: String = "", idx: Int = -1): T =
        if idx > 0 then
          extractData(rs, idx)
        else
          val fldName = q.fieldNameInDB(partAlias, this)
          val vx = dataType match
            case DataType.INT => Util.fixVal[Int](rs.getInt(fldName), typedDefaultVal.asInstanceOf[Int])
            case DataType.STR => Util.fixStr(rs.getString(fldName), typedDefaultVal.asInstanceOf[String])
            case DataType.TM => Util.fixVal(rs.getTimestamp(fldName).toInstant, typedDefaultVal.asInstanceOf[Instant])
            case DataType.DT => Util.fixVal(rs.getDate(fldName).toLocalDate, typedDefaultVal.asInstanceOf[LocalDate])
            case DataType.FLT => Util.fixVal(rs.getDouble(fldName), typedDefaultVal.asInstanceOf[Double])
            case DataType.BOOL => Util.fixVal(rs.getBoolean(fldName), typedDefaultVal.asInstanceOf[Boolean])
            case DataType.LOB => Util.fixVal(rs.getBytes(fldName), typedDefaultVal.asInstanceOf[Array[Byte]])
            case _ => throw RuntimeException(s"Unknown data type of field: ${this.dataType}")
          vx.asInstanceOf[T]


      override def extractData(rs: ResultSet, idx: Int): F =
        val result = dataType match
          case DataType.INT => Util.fixVal[Int](rs.getInt(idx), typedDefaultVal.asInstanceOf[Int])
          case DataType.STR => Util.fixStr(rs.getString(idx), typedDefaultVal.asInstanceOf[String])
          case DataType.TM => Util.fixVal(rs.getTimestamp(idx).toInstant, typedDefaultVal.asInstanceOf[Instant])
          case DataType.DT => Util.fixVal(rs.getDate(idx).toLocalDate, typedDefaultVal.asInstanceOf[LocalDate])
          case DataType.FLT => Util.fixVal(rs.getDouble(idx), typedDefaultVal.asInstanceOf[Double])
          case DataType.BOOL => Util.fixVal(rs.getBoolean(idx), typedDefaultVal.asInstanceOf[Boolean])
          case DataType.LOB => Util.fixVal(rs.getBytes(idx), typedDefaultVal.asInstanceOf[Array[Byte]])
          case _ => throw RuntimeException(s"Unknown data type of field: ${dataType}")
        result.asInstanceOf[T]

      override def extractData(rs: ResultSet, asName: String): F =
        val result = dataType match
          case DataType.INT => Util.fixVal[Int](rs.getInt(asName).asInstanceOf[Int], typedDefaultVal.asInstanceOf[Int])
          case DataType.STR => Util.fixStr(rs.getString(asName), typedDefaultVal.asInstanceOf[String])
          case DataType.TM => Util.fixVal(rs.getTimestamp(asName).toInstant, typedDefaultVal.asInstanceOf[Instant])
          case DataType.DT => Util.fixVal(rs.getDate(asName).toLocalDate, typedDefaultVal.asInstanceOf[LocalDate])
          case DataType.FLT => Util.fixVal(rs.getDouble(asName), typedDefaultVal.asInstanceOf[Double])
          case DataType.BOOL => Util.fixVal(rs.getBoolean(asName), typedDefaultVal.asInstanceOf[Boolean])
          case DataType.LOB => Util.fixVal(rs.getBytes(asName), typedDefaultVal.asInstanceOf[Array[Byte]])
          case _ => throw RuntimeException(s"Unknown data type of field: ${this.dataType}")
        result.asInstanceOf[T]

abstract class FieldImpl(override val virtName: String, override val name: String, override val dataType: DBField.DataType, dVal: Any) extends DBField:
  private var _pk = false
  private var _auto = false
  private var _owner: Option[DBField.Owner] = None
  private var _accFun: Option[(String=>String, String=>String)] = None
  override val defaultVal: T = dVal.asInstanceOf[T]

  override def owner: Option[DBField.Owner] = _owner

  override def owner_=(v: Option[DBField.Owner]): Unit =
    _owner = v

  override def accTransform: Option[(String => String, String => String)] = _accFun

  override def accTransform_=(v: Option[(String => String, String => String)]): Unit =
    _accFun = v

  override def setPK(): DBField =
    _pk = true
    this

  override def isPK: Boolean = _pk

  override def setAuto(v: Boolean): DBField =
    _auto = v
    this

  override def willAutoGen(v: Any): Boolean =
    _pk && _auto && v == defaultVal
