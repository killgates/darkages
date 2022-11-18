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
      case _ => throw RuntimeException(s"不能推断出字段的数据类型: $v")

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
            case DataType.INT => Util.readColumnInt2(rs, fldName, typedDefaultVal.asInstanceOf[Int])
            case DataType.STR => Util.readColumnString2(rs, fldName, fallback = typedDefaultVal.asInstanceOf[String])
            case DataType.TM => Util.readColumnTimestamp2(rs, fldName, typedDefaultVal.asInstanceOf[Instant])
            case DataType.DT => Util.readColumnDate2(rs, fldName, typedDefaultVal.asInstanceOf[LocalDate])
            case DataType.FLT => Util.readColumnDouble2(rs, fldName, typedDefaultVal.asInstanceOf[Double])
            case DataType.BOOL => Util.readColumnBool2(rs, fldName, typedDefaultVal.asInstanceOf[Boolean])
            case DataType.LOB => Util.readColumnBin2(rs, fldName, typedDefaultVal.asInstanceOf[Array[Byte]])
            case _ => throw RuntimeException(s"不支持的字段: ${this.dataType}")
          vx.asInstanceOf[T]


      override def extractData(rs: ResultSet, idx: Int): F =
        val result = dataType match
          case DataType.INT => Util.readColumnInt2(rs, idx, typedDefaultVal.asInstanceOf[Int])
          case DataType.STR => Util.readColumnString2(rs, idx, fallback = typedDefaultVal.asInstanceOf[String])
          case DataType.TM => Util.readColumnTimestamp2(rs, idx, typedDefaultVal.asInstanceOf[Instant])
          case DataType.DT => Util.readColumnDate2(rs, idx, typedDefaultVal.asInstanceOf[LocalDate])
          case DataType.FLT => Util.readColumnDouble2(rs, idx, typedDefaultVal.asInstanceOf[Double])
          case DataType.BOOL => Util.readColumnBool2(rs, idx, typedDefaultVal.asInstanceOf[Boolean])
          case DataType.LOB => Util.readColumnBin2(rs, idx, typedDefaultVal.asInstanceOf[Array[Byte]])
          case _ => throw RuntimeException(s"不支持的字段: ${dataType}")
        result.asInstanceOf[T]

      override def extractData(rs: ResultSet, asName: String): F =
        val result = dataType match
          case DataType.INT => Util.readColumnInt2(rs, asName, typedDefaultVal.asInstanceOf[Int])
          case DataType.STR => Util.readColumnString2(rs, asName, fallback = typedDefaultVal.asInstanceOf[String])
          case DataType.TM => Util.readColumnTimestamp2(rs, asName, typedDefaultVal.asInstanceOf[Instant])
          case DataType.DT => Util.readColumnDate2(rs, asName, typedDefaultVal.asInstanceOf[LocalDate])
          case DataType.FLT => Util.readColumnDouble2(rs, asName, typedDefaultVal.asInstanceOf[Double])
          case DataType.BOOL => Util.readColumnBool2(rs, asName, typedDefaultVal.asInstanceOf[Boolean])
          case DataType.LOB => Util.readColumnBin2(rs, asName, typedDefaultVal.asInstanceOf[Array[Byte]])
          case _ => throw RuntimeException(s"不支持的字段: ${this.dataType}")
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
