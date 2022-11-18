package rk.darkages

import ujson.Value.Value

import java.io.{File, InputStream}
import scala.io.Source
import java.security.{KeyFactory, PrivateKey, PublicKey, Signature}
import java.security.spec.{PKCS8EncodedKeySpec, X509EncodedKeySpec}
import upickle.default.{macroRW, ReadWriter as RW}

import java.time.{Instant, LocalDate}
import com.typesafe.scalalogging.Logger

import java.nio.charset.StandardCharsets
import scala.collection.mutable
import scala.reflect.ClassTag

import java.sql.ResultSet

object Util:
  private val _logers: mutable.Map[String, (Instant, Logger)] = mutable.TreeMap[String, (Instant, Logger)]()

  def getLogger(logName: String): Logger =
    _logers.get(logName) match
      case Some((t, lx)) =>
        Instant.now().compareTo(t.plusSeconds(900)) match
          case v if v <= 0 => lx
          case _ =>
            val lx = Logger(logName)
            _logers(logName) = (Instant.now(), lx)
            lx
      case _ =>
        val lx = Logger(logName)
        _logers(logName) = (Instant.now(), lx)
        lx

  def readColumnInt(rs: ResultSet, col: String | Int): Option[Int] =
    val v = col match
      case i: Int => rs.getInt(i)
      case s: String => rs.getInt(s)
    if rs.wasNull() then
      None
    else
      Some(v)

  def readColumnInt2(rs: ResultSet, col: String | Int, fallback: Int = -1): Int =
    readColumnInt(rs, col) match
      case Some(x) => x
      case _ => fallback

  def readColumnDouble(rs: ResultSet, col: String | Int): Option[Double] =
    val v = col match
      case i: Int => rs.getDouble(i)
      case s: String => rs.getDouble(s)
    if rs.wasNull() then
      None
    else
      Some(v)

  def readColumnDouble2(rs: ResultSet, col: String | Int, fallback: Double = Double.NaN): Double =
    readColumnDouble(rs, col) match
      case Some(x) => x
      case _ => fallback

  def readColumnBool(rs: ResultSet, col: String | Int): Option[Boolean] =
    val v = col match
      case i: Int => rs.getBoolean(i)
      case s: String => rs.getBoolean(s)
    if rs.wasNull() then
      None
    else
      Some(v)

  def readColumnBool2(rs: ResultSet, col: String | Int, fallback: Boolean = false): Boolean =
    readColumnBool(rs, col) match
      case Some(x) => x
      case _ => fallback

  def readColumnString(rs: ResultSet, col: String | Int, trim: Boolean = true): Option[String] =
    val v = col match
      case i: Int => rs.getString(i)
      case s: String => rs.getString(s)
    if rs.wasNull() then
      None
    else
      if trim then
        Some(v.trim)
      else
        Some(v)

  def readColumnString2(rs: ResultSet, col: String | Int, trim: Boolean = true, fallback: String=""): String =
    readColumnString(rs, col, trim) match
      case Some(x) => x
      case _ => fallback

  def readColumnDate(rs: ResultSet, col: String | Int): Option[LocalDate] =
    val v = col match
      case i: Int => rs.getDate(i)
      case s: String => rs.getDate(s)
    if rs.wasNull() then
      None
    else
      Some(v.toLocalDate)

  def readColumnDate2(rs: ResultSet, col: String | Int, fallback: LocalDate = LocalDate.MIN): LocalDate =
    readColumnDate(rs, col) match
      case Some(x) => x
      case _ => fallback

  def readColumnTimestamp(rs: ResultSet, col: String | Int): Option[Instant] =
    val v = col match
      case i: Int => rs.getTimestamp(i)
      case s: String => rs.getTimestamp(s)
    if rs.wasNull() then
      None
    else
      Some(v.toInstant)

  def readColumnTimestamp2(rs: ResultSet, col: String | Int, fallback: Instant = Instant.MIN): Instant =
    readColumnTimestamp(rs, col) match
      case Some(x) => x
      case _ => fallback

  def readColumnBin(rs: ResultSet, col: String | Int): Option[Array[Byte]] =
    val v = col match
      case i: Int => rs.getBytes(i)
      case s: String => rs.getBytes(s)
    if rs.wasNull() then
      None
    else
      Some(v)

  def readColumnBin2(rs: ResultSet, col: String | Int, fallback: Array[Byte] = Array.empty): Array[Byte] =
    readColumnBin(rs, col) match
      case Some(x) => x
      case _ => fallback

  def fixVal[T: ClassTag](v: => T, fallback: T): T =
    try
      Option(v) match
        case Some(x) => x
        case _ => fallback
    catch
      case ex: Exception => fallback

  def encodeStr(s: String, encoding: String=StandardCharsets.UTF_8.name()): Array[Byte] =
    s.getBytes(encoding)

  def decodeStr(b: Array[Byte], encoding: String=StandardCharsets.UTF_8.name()): String =
    new String(b, encoding)

  def readInitRes(resName: String): Map[String, Map[String, String]] =
    val res = Source.fromResource(resName)
    _readInitSrc(res)

  def readInitFile(f: File): Map[String, Map[String, String]] =
    val res = Source.fromFile(f)
    _readInitSrc(res)

  def signPayload(bs: Array[Byte], prvKey: PrivateKey, algo: String="SHA256withRSA"): Array[Byte] = {
    val s2 = Signature.getInstance(algo)
    s2.initSign(prvKey)
    s2.update(bs)
    s2.sign
  }

  def verifyPayload(bs: Array[Byte], sig: Array[Byte], publicKey: PublicKey, algo: String="SHA256withRSA"): Boolean =
    val s2 = Signature.getInstance(algo)
    s2.initVerify(publicKey)
    s2.update(bs)
    s2.verify(sig)

  def getPrivateKey(data: InputStream | Array[Byte], algo: String="RSA"): PrivateKey =
    val keyBytes = data match
      case ins: InputStream => ins.readAllBytes()
      case ba: Array[Byte] => ba
    val spec = new PKCS8EncodedKeySpec(keyBytes)
    val kf = KeyFactory.getInstance(algo)
    kf.generatePrivate(spec)

  def getPublicKey(data: InputStream | Array[Byte], algo: String="RSA"): PublicKey =
    val keyBytes = data match
      case ins: InputStream => ins.readAllBytes()
      case ba: Array[Byte] => ba
    val spec = new X509EncodedKeySpec(keyBytes)
    val kf = KeyFactory.getInstance(algo)
    kf.generatePublic(spec)

  private def _readInitSrc(res: Source): Map[String, Map[String, String]] =
    val result = collection.mutable.HashMap[String, Map[String, String]]()

    var currSec = ""
    val currEntries = collection.mutable.ArrayBuffer[(String, String)]()

    res.getLines() foreach { lx =>
      lx.trim match
        case s if s.startsWith("#") =>
        case s if s.startsWith("[") && s.endsWith("]") =>
          if currEntries.nonEmpty then
            result.update(currSec, Map.from(currEntries))

          currSec = s.substring(1, s.length - 1).trim.toUpperCase()
          currEntries.clear()
        case s if s.contains('=') =>
          val ps = s.split("=", 2)
          val entName = ps(0).trim
          val entVal = if ps.length > 1 then
            ps(1).trim
          else
            ""
          currEntries.append((entName, entVal))
        case s =>
          s.trim match
            case sx if sx.nonEmpty =>
              currEntries.append((sx, ""))
            case _ =>
    }
    if currEntries.nonEmpty then
      result.update(currSec, Map.from(currEntries))
    result.toMap

  object ConfigBundle:
    lazy val empty: ConfigBundle = ConfigBundle()
    def apply(s: String="{}"): ConfigBundle =
      Json.read2(s) match
        case x: Json.JObj => new ConfigBundle(x)
        case _ => throw RuntimeException(s"JSON配置文件格式錯誤: $s")

  case class ConfigBundle(entRoot: Json.JObj):
    def inject(k: String, subConfig: ConfigBundle): ConfigBundle =
      val j = entRoot.addAttr(k, subConfig.entRoot)
      ConfigBundle(j)

    def inject[T: ClassTag](k: String, attr: T): ConfigBundle =
      val j = attr match
        case x: Json.JVal => entRoot.addAttr(k, x)
        case x: String => entRoot.addAttr(k, Json.JStr(x))
        case x: Int => entRoot.addAttr(k, Json.JNum(x.toDouble))
        case x: Long => entRoot.addAttr(k, Json.JNum(x.toDouble))
        case x: Double => entRoot.addAttr(k, Json.JNum(x))
        case x: Float => entRoot.addAttr(k, Json.JNum(x.toDouble))
        case x: Boolean => entRoot.addAttr(k, Json.JBool(x))
        case _ => entRoot
      ConfigBundle(j)

    def contains(k: String): Boolean =
      entRoot.contains(k)

    def getItem[T: ClassTag](k: String): Option[T] =
      if entRoot.contains(k) then
        val r = entRoot(k) match
          case Json.JStr(s) => s
          case Json.JNum(n) => n
          case Json.JBool(b) => b
          case a: Json.JObj => ConfigBundle(a)
          case a: Json.JArr => _mkArrItem(a)
          case _ => None
        summon[ClassTag[T]].unapply(r)
      else
        None

    private def _mkArrItem(j: Json.JArr): Seq[Any] =
      val Json.JArr(a) = j
      a map { x =>
        x match
          case Json.JStr(ss) => ss
          case Json.JNum(nn) => nn
          case Json.JBool(bb) => bb
          case aa: Json.JObj => ConfigBundle(aa)
          case ar: Json.JArr => _mkArrItem(ar)
      }

    def getString(k: String): Option[String] =
      if entRoot.contains(k) then
        entRoot(k) match
          case Json.JStr(s) => Some(s)
          case _ => None
      else
        None

    def getInt(k: String): Option[Int] =
      if entRoot.contains(k) then
        entRoot(k) match
          case Json.JNum(x) => Some(x.toInt)
          case _ => None
      else
        None

    def getDouble(k: String): Option[Double] =
      if entRoot.contains(k) then
        entRoot(k) match
          case Json.JNum(x) => Some(x)
          case _ => None
      else
        None

    def getTimeStamp(k: String): Option[Instant] =
      getDouble(k) map {x=>Instant.ofEpochSecond(x.toLong)}

    def getDate(k: String): Option[LocalDate] =
      getString(k) map {x=> LocalDate.parse(x)}

    def check(k: String): Boolean =
      if entRoot.contains(k) then
        entRoot(k) match
          case Json.JBool(x) => x
          case _ => false
      else
        false

    def getJson(k: String): Json.JVal =
      entRoot(k)

    def asFlatDict: Map[String, Any] =
      entRoot.v map {case (k, v)=>
        v match
          case Json.JStr(vx) => (k, vx)
          case Json.JNum(vx) => (k, vx)
          case Json.JBool(vx) => (k, vx)
      }

    def getSub(ps: Seq[String]): ConfigBundle =
      if ps.nonEmpty then
        var j: Json.JVal = entRoot
        for xp <- ps do
          j match
            case jx: Json.JObj =>
              j = j(xp)
            case _ => throw RuntimeException(s"JSON配置文件格式錯誤, 找不到屬性：${ps.mkString(",")}")
        j match
          case r: Json.JObj => ConfigBundle(r)
          case _ => throw RuntimeException(s"JSON配置文件格式錯誤, 找不到屬性：${ps.mkString(",")}")
      else
        this

    def getSub(path: String, sep: String = "/"): ConfigBundle =
      val ps = path.split(sep)
      getSub(ps)

    def getDeepString(path: String, sep: String="/"): Option[String] =
      val ps = path.split(sep)
      val k = ps.last
      val sub = getSub(ps.dropRight(1))
      sub.getString(k)

    def getDeepInt(path: String, sep: String = "/"): Option[Int] =
      val ps = path.split(sep)
      val k = ps.last
      val sub = getSub(ps.dropRight(1))
      sub.getInt(k)

    def getDeepDouble(path: String, sep: String = "/"): Option[Double] =
      val ps = path.split(sep)
      val k = ps.last
      val sub = getSub(ps.dropRight(1))
      sub.getDouble(k)

    def deepCheek(path: String, sep: String = "/"): Boolean =
      val ps = path.split(sep)
      val k = ps.last
      val sub = getSub(ps.dropRight(1))
      sub.check(k)

  object Json:

    trait JVal:
      def isNull: Boolean = false
      def strVal: String = throw RuntimeException("Json數據類型錯誤")
      def numVal: Double = throw RuntimeException("Json數據類型錯誤")
      def boolVal: Boolean = throw RuntimeException("Json數據類型錯誤")

    case class JStr(v: String) extends JVal:
      override def strVal: String = v

      override def toString: String = s"JSON=> $v"

    case class JNum(v: Double) extends JVal:
      override def numVal: Double = v

      override def toString: String = s"JSON=> $v"

    case class JBool(v: Boolean) extends JVal:
      override def boolVal: Boolean = v

      override def toString: String = s"JSON=> ${if v then "true" else "false"}"

    case class JNull() extends JVal:
      override def isNull: Boolean = true

      override def strVal: String = ""

      override def boolVal: Boolean = false

      override def numVal: Double = 0.0

      override def toString: String = s"JSON=> null"

    object JObj:
      def apply(attrs: (String, JVal)*): JObj =
        new JObj(attrs.toMap)

      def apply(s: String): JObj =
        Json.read2(s) match
          case x: JObj => x
          case _ => throw RuntimeException(s"非法JSON: $s")

    case class JObj(v: Map[String, JVal]) extends JVal:
      def apply(p: String): JVal =
        if contains(p) then
          v(p)
        else
          JNull()

      def contains(p: String): Boolean =
        v contains p

      def addAttr(attrName: String, attrVal: JVal): JObj =
        JObj(v + ((attrName, attrVal)))

      override def toString: String = s"JSON=>{ ${v.mkString(",")} }"

    case class JArr(v: Seq[JVal]) extends JVal:
      def apply(p: Int): JVal = v(p)

      override def toString: String = s"JSON=> [ ${v.mkString(",")} ]"

    trait JFactory[T]:
      def build(jv: JVal): T

    trait JView:
      def toJson: JVal

    def read[T: JFactory](s: String): T =
      val fac = summon[JFactory[T]]
      val m = ujson.read(s)
      fac.build(m)

    def read2(s: String): JVal =
      ujson.read(s)

    def write(x: Any): String =
      import upickle.default.{write => uWrite, Writer}
      x match
        case xx: JView => ujson.write(xx.toJson)
        case xx: String => xx
        case null => "null"
        case xx: (Int | Long | Float | Double) => s"$xx"
        case xx: Boolean => if xx then "true" else "false"
        case xx: Iterable[Any] =>
          s"[${xx map write mkString "," }]"
        case xx: Array[Any] =>
          s"[${xx map write mkString "," }]"
        case _ => ""

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

    given Conversion[JVal, ujson.Value] with
      override def apply(x: JVal): Value =
        x match
          case _: JNull => ujson.Null
          case JStr(xx) => ujson.Str(xx)
          case JNum(xx) => ujson.Num(xx)
          case JBool(xx) => ujson.Bool(xx)
          case JArr(xx) =>
            val items = xx map {_.convert}
            ujson.Arr(items: _*)
          case JObj(xx) =>
            val attrs = xx.toSeq map {case (k, v) => (k, v.convert)}
            ujson.Obj(attrs.head, attrs.tail: _*)


    given JFactory[ConfigBundle] with
      override def build(jv: JVal): ConfigBundle =
        jv match
          case x: JObj =>
            ConfigBundle(x)
          case _ =>
            throw RuntimeException("配置文檔格式錯誤")


    given Conversion[Double, JNum] = JNum(_)
    given Conversion[Float, JNum] = JNum(_)
    given Conversion[Int, JNum] = JNum(_)
    given Conversion[Long, JNum] = JNum(_)
    given Conversion[String, JStr] = JStr(_)
    given Conversion[Boolean, JBool] = JBool(_)


