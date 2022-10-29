package rk.darkages

import java.time.{Duration, Instant, LocalDate, LocalDateTime, LocalTime}
import java.time.temporal.TemporalAmount
import java.sql.{Connection, Date, DriverManager, PreparedStatement, ResultSet, SQLException, Statement, Time, Timestamp}
import java.util.UUID
import java.util.concurrent.locks.{Condition, Lock, ReentrantLock}
import scala.annotation.targetName
import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import com.typesafe.scalalogging.Logger


object DBAgent:
  private def _log: Logger = Util.getLogger("DBAgent")

  case class TimeOut(v: Duration=Duration.ofSeconds(30))

  given TimeOut = TimeOut()

  object DBWorker:
    trait DBContext:
      def conn: Connection

    def setParamsForSql(st: PreparedStatement, params: Any*): PreparedStatement =
      for (n <- 1 to params.length) {
        params(n - 1) match {
          case x: Int => st.setInt(n, x)
          case x: Long => st.setLong(n, x)
          case x: Double => st.setDouble(n, x)
          case x: Float => st.setFloat(n, x)
          case x: String => st.setString(n, x)
          case x: Array[Byte] => st.setBytes(n, x)
          case x: Instant => st.setTimestamp(n, Timestamp.from(x))
          case x: LocalTime => st.setTime(n, Time.valueOf(x))
          case x: LocalDate => st.setDate(n, Date.valueOf(x))
          case x: LocalDateTime => st.setTimestamp(n, Timestamp.valueOf(x))
        }
      }
      st

  trait DBWorker:
    protected var _startTime: Instant = Instant.MAX
    protected var _finishTime: Instant = Instant.MAX
    private var _timeOut = Duration.ofSeconds(30)

    val id: UUID = UUID.randomUUID()

    def connFingerprint: UUID

    def currContext: DBWorker.DBContext

    def runTask(t: SqlTask, timeOut: Duration=Duration.ofSeconds(10), ownKey: Option[UUID]=None): Unit

    def isAlive: Boolean =
      val currTime = Instant.now()
      currTime.isBefore(_startTime) || _finishTime.isBefore(currTime) || Duration.between(_startTime, currTime).compareTo(_timeOut) < 0

    def isIdle: Boolean

    def check(): Unit

    def start(): Unit

    def stop(timeOutMS: Long = 30000): Unit

    def setTimeOut(t: Duration): Unit =
      _timeOut = t

    def engage(x: UUID): Unit

    def giveUp(): Unit

    def engagedBy: Option[UUID]

  enum SqlTaskStatus:
    case Fresh, Running, Succ, Err

  object SqlResult:
    val zero: SqlResult = new SqlResult(None, None, "__ZERO__")

    def apply(st: Statement, rs: Option[ResultSet], affectedRows: Int = -1, info: String = "", decision: Int = -1): SqlResult =
      new SqlResult(Some(st), rs, info, affectedRows)

    def apply(info: String): SqlResult =
      new SqlResult(None, None, info)

    def unapply(v: SqlResult): Option[(Option[ResultSet], Int, String)] =
      Some((v._firstRS, v.affectedRows, v.info))

  class SqlResult(private val _statement: Option[Statement], private var _firstRS: Option[ResultSet], val info: String,
                  val affectedRows: Int = -1, protected[DBAgent] var _decision: Int = -1):
    _firstRS match
      case None =>
        _statement match
          case Some(st) =>
            _firstRS = Option(st.getResultSet)
          case _ =>
      case _ =>

    def singleReturning: ResultSet =
      _firstRS.get

    def singleReturningOpt: Option[ResultSet] =
      _firstRS

    def nextResultSet: Option[ResultSet] =
      _statement match
        case Some(st) =>
          if st.getMoreResults then
            Some(st.getResultSet)
          else
            None
        case _ => None

    def decision: Int = _decision


  object SqlTask:
    type Guide = (Statement, Option[ResultSet], Option[Any]) => Int

    def apply(sql: String, params: Any*): SqlTask =
      new SqlTask(sql, params: _*)


  class SqlTask(val sql: String, pars: Any*):
    private var _execTag = 1
    private var _params: Seq[Any] = pars

    private var _affinitiveWorker: Option[DBWorker] = None
    private var _connFingerprint: Option[UUID] = None
    private var _st: Option[PreparedStatement] = None
    var relative: Option[Any] = None
    var procGuide: Option[SqlTask.Guide] = None

    private lazy val _stKind: Int =
      if sql.isEmpty then
        0
      else
        val uSql = sql.toUpperCase()
        uSql match
          case s if s.contains("SELECT") => 1
          case s if s.contains("RETURNING") => 10
          case s if s.contains("INSERT") || s.contains("UPDATE") => 2
          case s if s.contains("CREATE") || s.contains("DELETE") => 3
          case _ => -1
      end if

    private var _prevCmd: Option[DBWorker.DBContext=>Unit] = None
    private var _postCmd: Option[DBWorker.DBContext=>Unit] = None

    var label: String = ""
    var status: SqlTaskStatus=SqlTaskStatus.Fresh
    private var _result: Option[SqlResult] = None
    var affectedRows: Int = -1
    var info: String = ""

    def result: SqlResult =
      _result match
        case Some(x) => x
        case _ => throw RuntimeException(s"No result, sql statement is running")

    protected[darkages] def result_=(v: SqlResult): Unit =
      _result = Some(v)

    def affinitiveInfo: Option[(DBWorker, UUID)] =
      _affinitiveWorker match
        case  Some(w) => Some((w, _connFingerprint.get))
        case _ => None

    def chgParams(params: Any*): Unit =
      _params = params
      _result = None
      _execTag += 1
      _st match
        case Some(stx) =>
          DBWorker.setParamsForSql(stx, _params: _*)
        case _ =>

    def execTag: Int = _execTag

    def setCommit(): Unit =
      _log.debug(s"SqlTask(${sql}) is set to commit")
      _postCmd = Some({ctx =>
        if !ctx.conn.getAutoCommit then
          _log.debug("Commit...")
          ctx.conn.commit()
      })

    def setRollBack(): Unit =
      _postCmd = Some({ ctx =>
        if !ctx.conn.getAutoCommit then
          _log.debug("Roll back...")
          ctx.conn.rollback()
      })

    def attachToWorker(worker: DBWorker): Unit =
      _affinitiveWorker match
        case Some(w) if w == worker =>
          _connFingerprint match
            case Some(fp) if fp == w.connFingerprint =>
            case _ => _initForWorker(worker)
        case _ =>
          _initForWorker(worker)

    def runner: DBWorker.DBContext => SqlResult =
      ctx => {
        _prevCmd match
          case Some(cmd) =>
            _log.debug("Perform PRE command...")
            cmd(ctx)
          case _ =>

        val runResult = _st match
          case Some(stx) =>
            DBWorker.setParamsForSql(stx, _params: _*)
            _stKind match
              case 0 => SqlResult(stx, None, 0, "EMPTY_SQL")
              case 1 =>
                val rs = stx.executeQuery()
                SqlResult(stx, Some(rs), -1, "SUCCESS", decision = _checkResult(stx, Some(rs)))
              case 2 =>
                val ar = stx.executeUpdate()
                SqlResult(stx, None, ar, "SUCCESS", decision = _checkResult(stx, None))
              case 3 =>
                val ar = stx.executeUpdate()
                SqlResult(stx, None, ar, "SUCCESS", decision = _checkResult(stx, None))
              case 10 =>
                stx.execute()
                val rs = stx.getResultSet
                SqlResult(stx, Some(rs), -1, "SUCCESS", decision = _checkResult(stx, Some(rs)))
              case _ => SqlResult(s"Unknown sql statement: $stx")

          case _ =>
            SqlResult("Illegal sql statement")

        _postCmd match
          case Some(cmd) =>
            _log.debug("Perform POST command...")
            cmd(ctx)
          case _ =>
        runResult
      }

    override def toString: String = sql

    private def _checkResult(st: Statement, rs: Option[ResultSet]): Int =
      procGuide match
        case Some(gx) => gx(st, rs, relative)
        case _ => -1

    private def _initForWorker(w: DBWorker): Unit =
      _affinitiveWorker = Some(w)
      _connFingerprint = Some(w.connFingerprint)
      val st = _mkStatment(w.currContext)
      _st = Some(st)

    private def _mkStatment(ctx: DBWorker.DBContext): PreparedStatement =
      ctx.conn.prepareStatement(sql)

  class SqlJob(val wholly: Boolean=true):
    var id: UUID = UUID.randomUUID()
    private val _taskMap = mutable.TreeMap[String, SqlTask]()
    private val _paramsLst = mutable.Queue[(String, Seq[Any], Option[Any])]()

    def tasks(): Iterable[() => SqlTask] =
      _paramsLst map { case (sql, params, relative) =>
        () =>
          val t = _taskMap(sql)
          t.chgParams(params: _*)
          t.relative = relative
          if !wholly then
            t.setCommit()
          t
      }

    def addSql(sql: String, params: Seq[Any], relative: Option[Any]=None): Unit =
      if !_taskMap.contains(sql) then
        val t = SqlTask(sql)
        t.label = s"Job_${id.toString}_${sql.take(16)}"
        _taskMap(sql) = t
      _paramsLst.append((sql, params, relative))


  abstract class SqlScheme(val wholly: Boolean=true):
    var id: UUID = UUID.randomUUID()

    protected var _tasks: mutable.Map[Int, SqlTask] = mutable.TreeMap()

    private var _nextKey : Int = 0

    def guide: Option[SqlTask.Guide]

    def addTask(k: Int, t: SqlTask): Unit =
      _tasks(k) = t

    def nextTask: Option[SqlTask] =
      if _tasks contains _nextKey then
        val t = _tasks(_nextKey)
        t.procGuide = guide
        Some(t)
      else
        None

    def review(r: SqlResult): Unit =
      _nextKey = r.decision


  class NaDBWorker(val url: String, val username: String, val password: String, private val _autoCommit: Boolean = false) extends DBWorker:
    private var _conn: Option[Connection] = None
    private var _connFingerprint: UUID = UUID.randomUUID()
    private var _task: Option[SqlTask] = None
    private var _execTask: Option[SqlTask] = None
    private var _active = true
    private val _lock = ReentrantLock()
    private val _hasTask = _lock.newCondition()
    private val _noTask = _lock.newCondition()
    private val _taskCompleted = _lock.newCondition()

    private var _engagedBy: Option[UUID] = None
    private val _released = _lock.newCondition()

    private var _idleTime = Instant.now()
    private val _self = this

    private val _th = new Thread({()=>
      _log.debug(s"NaDBWorker[${_self.id}] starting...")
      while _active do
        try
          _startTime = Instant.now()
          _log.debug(s"[Worker: ${id}]: Fetch next Task...")
          val task = _execTask match
            case Some(tx) =>
              tx
            case _ =>
              //              _log.debug(s"[Worker: ${id}]: 等待SqlTask來到...")
              val tx = _waitForTask()
              _execTask = Some(tx)
              tx
          _log.debug(s"[Worker: ${id}]: SqlTask(${task}) has been obtained, run it...")
          task.status = SqlTaskStatus.Running
          task.attachToWorker(this)
          task.result = task.runner(currContext)
          //          _log.debug(s"[Worker: ${id}]: SqlTask completed")
          task.status = SqlTaskStatus.Succ
          _finishTime = Instant.now()
          //          _log.debug(s"[Worker: ${id}]: Notify SqlTask completion...")
          _notifyComplete()
          _execTask = None
          //          _log.debug(s"[Worker: ${id}]: The running turn is ok, next...")
        catch
          case _: InterruptedException =>
            _handleEx(None, "Database access thread is break, try again...", 0, stopTask = false)
          case ex: Exception =>
            val sErr = _execTask match
              case Some(task) => s"sql= ${task.toString}"
              case _ => ""
            _execTask = None
            _handleEx(Some(ex), s"Database operation is failed! ${sErr}")
      end while
    }, s"DBWorker#${_self.id}")

    override def currContext: DBWorker.DBContext =
      new DBWorker.DBContext:
        override def conn: Connection = _connect()

    override def connFingerprint: UUID = _connFingerprint

    override def start(): Unit =
      _active = true
      _th.start()

    override def stop(timeOutMS: Long = 30000): Unit =
      _active = false
      _th.interrupt()
      _th.join(timeOutMS)

    override def engage(x: UUID): Unit =
      syncDo(_lock){()=>
        while _engagedBy.nonEmpty do
          _released.await()

        _engagedBy = Some(x)
        _conn match
          case Some(cx) => cx.rollback()
          case _ =>
      }

    override def giveUp(): Unit =
      syncDo(_lock){
        ()=>
          _conn match
            case Some(cx) => cx.rollback()
            case _ =>
          _engagedBy = None
          _released.signalAll()
      }

    def engagedBy: Option[UUID] = _engagedBy

    override def check(): Unit =
      _log.debug(s"Checking database connection[${id}]:...")
      _lock.lock()
      try
        val tn = Instant.now()
        if !isAlive then
          _th.interrupt()
          _idleTime = tn
        end if
        if Duration.between(_idleTime, tn).getSeconds > 600 && _conn.nonEmpty then
          _log.debug(s"Database connection is idling, break it！")
          _disconnect()
      finally
        _lock.unlock()

    override def isIdle: Boolean =
      syncDo(_lock) { ()=>
        _task.isEmpty && _execTask.isEmpty && _engagedBy.isEmpty
      }

    override def runTask(t: SqlTask, timeOut: Duration=Duration.ofSeconds(10), ownKey: Option[UUID]=None): Unit =
      _log.debug(s"[Thread: ${Thread.currentThread().getId}]@<$id> runTask: running SqlTask => ${t.label}#${t.execTag}... ")
      _setTask(t, ownKey)
      setTimeOut(timeOut)
      //      _log.debug(s"[Worker: ${id}] runTask: Wait for task completion...")
      _waitForResult(t)

    private def _setTask(t: SqlTask, ownKey: Option[UUID]=None): Unit =
      _lock.lock()
      try
        var succ = false
        while !succ do
          succ = ownKey match
            case Some(k) =>
              val h = {() => {_engagedBy.nonEmpty && k != _engagedBy.get}}
              while h() do
                _log.debug(s"Worker is engaged，waiting for its releasing...")
                _released.await()
              _installTask(t, () => !h())
            case _ =>
              val h = {() => {_engagedBy.nonEmpty}}
              while h() do
                _released.await()
              _installTask(t, () => !h())
      finally
        _lock.unlock()

    private def _installTask(t: SqlTask, instCri: ()=> Boolean): Boolean =
      //      _log.debug("Installing SqlTask...")
      while _task.nonEmpty do
        _log.debug(s"Worker is running other task, waiting ...")
        _noTask.await()
        _log.debug(s"Worker has idled...")
      if instCri() then
        t.status = SqlTaskStatus.Fresh
        _task = Some(t)
        _hasTask.signalAll()
        true
      else
        _log.debug(s"Can't to install the task，maybe because of (${_engagedBy})")
        false

    private def _waitForTask(): SqlTask =
      _lock.lock()
      try
        while _task.isEmpty do
          _hasTask.await()
        val t = _task.get
        _task = None
        _noTask.signalAll()
        t
      catch
        case ex: Exception =>
          _log.debug(s"************ Error: $ex ***************")
          throw ex
      finally
        _lock.unlock()

    private def _notifyComplete(): Unit =
      _lock.lock()
      try
        _idleTime = Instant.now()
        _taskCompleted.signalAll()
      finally
        _lock.unlock()

    private def _waitForResult(t: SqlTask): Boolean =
      _lock.lock()
      try
        //        _log.debug("Waiting for database operation result...")
        while t.status != SqlTaskStatus.Succ && t.status != SqlTaskStatus.Err do
          _taskCompleted.await()
        //        _log.debug("Database operation ok")
        true
      catch
        case ex: Exception =>
          _log.error(s"Database operation error: ${ex.toString}")
          false
      finally
        _lock.unlock()

    private def _waitForFree(): Unit =
      syncDo(_lock){()=>
        while _engagedBy.nonEmpty do
          _released.await()
      }

    private def _connect(): Connection =
      if (_conn.isEmpty) then
        syncDo(_lock) {()=>
          val cx = DriverManager.getConnection(url, username, password)
          cx.setAutoCommit(_autoCommit)
          _conn = Some(cx)
          _connFingerprint = UUID.randomUUID()
        }
      end if
      _conn.get

    def _disconnect(): Unit =
      _conn match
        case Some(cx) =>
          syncDo(_lock) { ()=>
            cx.close()
            _conn = None
            _connFingerprint = UUID.randomUUID()
          }
        case _ =>

    private def _handleEx(ex: Option[Exception], prompt: String="", sleepMS: Int=100000, stopTask: Boolean=true): Unit =
      val sEx = ex match
        case Some(e) => e.toString
        case _ => ""
      if sEx.nonEmpty then
        _log.error(s"${prompt}: ${sEx}")
      _disconnect()
      syncDo(_lock) {
        _execTask match
          case Some(task) => ()=>
            task.result = SqlResult.zero
            task.status = SqlTaskStatus.Err
            task.info = ex.toString
            task.affectedRows = -1
          case _ => ()=>
      }
      _finishTime = Instant.now()
      if stopTask then
        _execTask = None
        _notifyComplete()
      if sleepMS > 0 then
        Thread.sleep(sleepMS)


  def syncDo[T](lx: Lock)(f: () => T): T =
    lx.lock()
    try
      f()
    finally
      lx.unlock()


class DBAgent(val url: String, private val username: String, private val password: String, private val _autoCommit: Boolean=false, poolSize: Int=2):
  private lazy val _workers = {
    val t = mutable.ArrayBuffer[DBAgent.DBWorker]()
    for i <- 1 to poolSize do
      val wx = DBAgent.NaDBWorker(url, username, password, _autoCommit)
      t.append(wx)
    t.toSeq
  }
  private val _lock = ReentrantLock()
  private val _thInspector = new Thread ({()=>
    while true do
      try
        Thread.sleep(180_000)
        _workers foreach { _.check() }
      catch
        case _: Exception =>
  })
  private val _preservedWorkers: mutable.Map[UUID, DBAgent.DBWorker] = mutable.TreeMap()

  _workers foreach {_.start()}
  _thInspector.setDaemon(true)
  _thInspector.setName("DBInspector")
  _thInspector.start()


  def query(sql: String, args: Any*)(using timeOut: DBAgent.TimeOut): DBAgent.SqlResult =
    //    _log.debug("Choose a worker...")
    val worker = _electWorker()
    //    _log.debug(s"Worker has been chose：${worker.id}, creating a SqlTask...")
    val t = DBAgent.SqlTask(sql, args: _*)
    t.setCommit()
    //    _log.debug(s"SqlTask is created，run it...")
    worker.runTask(t, timeOut = timeOut.v)
    t.result

  def update(sql: String, args: Any*)(using timeOut: DBAgent.TimeOut): Unit =
    val worker = _electWorker()
    val t = DBAgent.SqlTask(sql, args: _*)
    t.setCommit()
    worker.runTask(t, timeOut = timeOut.v)

  def preserve(): UUID =
    _lock.lock()
    try
      val xId = UUID.randomUUID
      val w = _electWorker()
      w.engage(xId)
      _preservedWorkers(xId) = w
      xId
    finally
      _lock.unlock()

  def unchain(xId: UUID): Unit =
    _lock.lock()
    try
      val w = _preservedWorkers(xId)
      w.giveUp()
      _preservedWorkers.remove(xId)
    finally
      _lock.unlock()

  def queryEx(pId: UUID, commit: Boolean, sql: String, args: Any*)(using timeOut: DBAgent.TimeOut): DBAgent.SqlResult =
    val worker = _retrieveWorker(pId)
    worker.setTimeOut(timeOut.v)
    val t = DBAgent.SqlTask(sql, args: _*)
    if commit then
      t.setCommit()
    worker.runTask(t, timeOut = timeOut.v, ownKey = Some(pId))
    t.result

  def execJob[T](job: DBAgent.SqlJob, onTaskComplete: Option[DBAgent.SqlTask=>Option[T]]=None)
                (using timeOut: DBAgent.TimeOut, executionContext: ExecutionContext): Future[Seq[T]] =
    Future {
      var worker: Option[DBAgent.DBWorker] = None
      try
        val result = mutable.ArrayBuffer[T]()
        job.tasks() foreach { tg =>
          //          _log.debug(s"SqlJob(${job.id.toString}): generating next Task...")
          val t = tg()
          //          _log.debug(s"SqlJob(${job.id.toString}): Task=>${t}")
          val wx = worker match
            case Some(w) => w
            case _ =>
              val w = _electWorker()
              w.engage(job.id)
              w.setTimeOut(timeOut.v)
              worker = Some(w)
              w
          //          _log.debug(s"SqlJob(${job.id.toString}): Obtain a worker: ${wx.id}")
          wx.runTask(t, ownKey = Some(job.id))
          if t.status == DBAgent.SqlTaskStatus.Err then
            throw RuntimeException(s"Running sql statement Error: ${t.info}")
          onTaskComplete match
            case Some(f) =>
              f(t) match
                case Some(x) => result += x
                case _ =>
            case _ =>
        }
        if job.wholly then
          val tx = DBAgent.SqlTask("")
          tx.setCommit()
          worker.get.runTask(tx, ownKey = Some(job.id))
        result.toSeq
      finally
        DBAgent._log.debug(s"SqlJob(${job.id.toString}): SQLJob OK！")
        worker match
          case Some(w) => w.giveUp()
          case _ =>
    }

  def execScheme(sch: DBAgent.SqlScheme)(using timeOut: DBAgent.TimeOut, executionContext: ExecutionContext): Future[Unit] =
    Future {
      val wx = _electWorker()
      try
        wx.engage(sch.id)
        wx.setTimeOut(timeOut.v)

        var running = true
        while running do
          sch.nextTask match
            case Some(t) =>
              wx.runTask(t, ownKey = Some(sch.id))
              if t.status == DBAgent.SqlTaskStatus.Err then
                throw RuntimeException(s"Run Sql scheme error: ${t.info}")
              sch.review(t.result)
            case _ => running = false
        if sch.wholly then
          val tx = DBAgent.SqlTask("")
          tx.setCommit()
          wx.runTask(tx, ownKey = Some(sch.id))
      finally
        DBAgent._log.debug(s"SqlScheme(${sch.id.toString}): SQL scheme complete！")
        wx.giveUp()
    }

  private def _electWorker(): DBAgent.DBWorker =
    DBAgent.syncDo[DBAgent.DBWorker](_lock){()=>
      _workers.find { _.isIdle } match
        case Some(w) => w
        case _ => _workers.head
    }

  private def _retrieveWorker(pId: UUID): DBAgent.DBWorker =
    DBAgent.syncDo[DBAgent.DBWorker](_lock) { () =>
      _preservedWorkers(pId)
    }