package rk.darkages

import java.io.{File, InputStream}
import scala.io.Source
import java.security.{KeyFactory, PrivateKey, PublicKey, Signature}
import java.security.spec.{PKCS8EncodedKeySpec, X509EncodedKeySpec}
import java.time.Instant
import com.typesafe.scalalogging.Logger
import scala.collection.mutable
import scala.reflect.ClassTag

import upickle.default.{macroRW, ReadWriter as RW}
import ujson.Value.Value

object Util:
  def getLogger(logName: String): Logger = Logger(logName)

  def fixVal[T](v: =>T, defaultVal: T, trimer: Option[T=>T]=None): T = {
    try
      val vx = v
      if(vx == null) defaultVal else trimer match
        case Some(t) => t(vx)
        case None => vx
    catch
      case _: Exception => defaultVal
  }

  def fixStr(s: String, defaultVal: String=""): String = {
    fixVal(s, defaultVal, Some({_.trim()}))
  }

//  def readInitRes(resName: String): Map[String, Map[String, String]] =
//    val res = Source.fromResource(resName)
//    _readInitSrc(res)
//
//  def readInitFile(f: File): Map[String, Map[String, String]] =
//    val res = Source.fromFile(f)
//    _readInitSrc(res)

//  def signPayload(bs: Array[Byte], prvKey: PrivateKey, algo: String="SHA256withRSA"): Array[Byte] = {
//    val s2 = Signature.getInstance(algo)
//    s2.initSign(prvKey)
//    s2.update(bs)
//    s2.sign
//  }
//
//  def verifyPayload(bs: Array[Byte], sig: Array[Byte], publicKey: PublicKey, algo: String="SHA256withRSA"): Boolean =
//    val s2 = Signature.getInstance(algo)
//    s2.initVerify(publicKey)
//    s2.update(bs)
//    s2.verify(sig)

//  def getPrivateKey(data: InputStream | Array[Byte], algo: String="RSA"): PrivateKey =
//    val keyBytes = data match
//      case ins: InputStream => ins.readAllBytes()
//      case ba: Array[Byte] => ba
//    val spec = new PKCS8EncodedKeySpec(keyBytes)
//    val kf = KeyFactory.getInstance(algo)
//    kf.generatePrivate(spec)

//  def getPublicKey(data: InputStream | Array[Byte], algo: String="RSA"): PublicKey =
//    val keyBytes = data match
//      case ins: InputStream => ins.readAllBytes()
//      case ba: Array[Byte] => ba
//    val spec = new X509EncodedKeySpec(keyBytes)
//    val kf = KeyFactory.getInstance(algo)
//    kf.generatePublic(spec)

//  private def _readInitSrc(res: Source): Map[String, Map[String, String]] =
//    val result = collection.mutable.HashMap[String, Map[String, String]]()
//
//    var currSec = ""
//    val currEntries = collection.mutable.ArrayBuffer[(String, String)]()
//
//    res.getLines() foreach { lx =>
//      lx.trim match
//        case s if s.startsWith("#") =>
//        case s if s.startsWith("[") && s.endsWith("]") =>
//          if currEntries.nonEmpty then
//            result.update(currSec, Map.from(currEntries))
//
//          currSec = s.substring(1, s.length - 1).trim.toUpperCase()
//          currEntries.clear()
//        case s if s.contains('=') =>
//          val ps = s.split("=", 2)
//          val entName = ps(0).trim
//          val entVal = if ps.length > 1 then
//            ps(1).trim
//          else
//            ""
//          currEntries.append((entName, entVal))
//        case s =>
//          s.trim match
//            case sx if sx.nonEmpty =>
//              currEntries.append((sx, ""))
//            case _ =>
//    }
//    if currEntries.nonEmpty then
//      result.update(currSec, Map.from(currEntries))
//    result.toMap

  case class ConfigMap(entries: Map[String, Any]):
    def foreachStr(act: (String, String)=>Unit): Unit =
      entries foreach {(k, v)=>
        v match
          case vt: String => act(k, vt)
          case _ =>
      }

    def getString(k: String): Option[String] =
      if entries.contains(k) then
        entries(k) match
          case s: String => Some(s)
          case x => Some(x.toString)
      else
        None

    def getInt(k: String): Option[Int] =
      if entries.contains(k) then
        entries(k) match
          case n: Double => Some(n.toInt)
          case _ => None
      else
        None

    def getDouble(k: String): Option[Double] =
      if entries.contains(k) then
        entries(k) match
          case n: Double => Some(n)
          case _ => None
      else
        None

    def getBoolean(k: String): Boolean =
      if entries.contains(k) then
        entries(k) match
          case b: Boolean => b
          case _ => false
      else
        false


  object Json:

    trait JVal:
      def isNull: Boolean = false
      def strVal: String = throw RuntimeException("Invalid json data")
      def numVal: Double = throw RuntimeException("Invalid json data")
      def boolVal: Boolean = throw RuntimeException("Invalid json data")

    case class JStr(v: String) extends JVal:
      override def strVal: String = v

    case class JNum(v: Double) extends JVal:
      override def numVal: Double = v

    case class JBool(v: Boolean) extends JVal:
      override def boolVal: Boolean = v

    case class JNull() extends JVal:
      override def isNull: Boolean = true

      override def strVal: String = ""

      override def boolVal: Boolean = false

      override def numVal: Double = 0.0

    case class JObj(v: Map[String, JVal]) extends JVal:
      def apply(p: String): JVal =
        if contains(p) then
          v(p)
        else
          JNull()

      def contains(p: String): Boolean =
        v contains p

    case class JArr(v: Seq[JVal]) extends JVal:
      def apply(p: Int): JVal = v(p)

    trait JFactory[T]:
      def build(jv: JVal): T

    def read[T: JFactory](s: String): T =
      val fac = summon[JFactory[T]]
      val m = ujson.read(s)
      fac.build(m)

    def read2(s: String): JVal =
      ujson.read(s)

    def readNodeAsStr(n: JVal, fallback: =>String | Exception = ""): String =
      n match
        case x if x.isNull => _extractFallback(fallback)
        case x: JStr => x.strVal
        case _ => _extractFallback(fallback)

    def readNodeAsInt(n: JVal, fallback: =>Int | Exception = 0): Int =
      n match
        case x if x.isNull => _extractFallback(fallback)
        case x: JNum => x.numVal.toInt
        case x: JStr => x.strVal.toInt
        case _ => _extractFallback(fallback)

    def readNodeAsFlt(n: JVal, fallback: =>Double | Exception = 0.0): Double =
      n match
        case x if x.isNull => _extractFallback(fallback)
        case x: JNum => x.numVal
        case x: JStr => x.strVal.toDouble
        case _ => _extractFallback(fallback)

    def readNodeAsBool(n: JVal, fallback: =>Boolean | Exception = false): Boolean =
      n match
        case x if x.isNull => _extractFallback(fallback)
        case x: JBool => x.boolVal
        case x: JNum => x.numVal.toInt != 0
        case x: JStr =>
          val hx = x.strVal.trim.head
          hx == 'Y' || hx == 'y' || hx == 'T' || hx == 't'
        case _ => _extractFallback(fallback)

    private def _extractFallback[T](v: T | Exception): T =
      v match
        case ex: Exception => throw ex
        case vt => vt.asInstanceOf[T]

    def write[T](x: T): String =
      throw RuntimeException("Json export has no been implemented!")

    given Conversion[ujson.Value, JVal] with
      override def apply(x: Value): JVal =
        x match
          case ujson.Null => JNull()
          case s: ujson.Str => JStr(s.value)
          case n: ujson.Num => JNum(n.value)
          case b: ujson.Bool => JBool(b.value)
          case arr: ujson.Arr =>
            val r = arr.value.toSeq map { vx=>
              val j: JVal = vx
              j
            }
            JArr(r)
          case obj: ujson.Obj =>
            val r = obj.value map {case (k, v)=>
              val j: JVal = v
              (k, j)
            }
            JObj(r.toMap)

    given JFactory[ConfigMap] with
      override def build(jv: JVal): ConfigMap =
        jv match
          case x: JObj =>
            ConfigMap(x.v map {case(k, v)=>
              (k, v match
                case vn: JNum => vn.v
                case vs: JStr => vs.v
                case vb: JBool => vb.v
                case n: JNull => None
              )
            })
          case _ =>
            throw RuntimeException("Configuration file is illegal")

