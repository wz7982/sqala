package sqala.test

import sqala.jdbc.*
import sqala.static.dsl.*
import sqala.test.JdbcFixture.*

import java.sql.SQLException

/**
 * Logger abstraction: success events, failure events, operation kinds,
 * level filtering, and the built-in logger factories.
 */
abstract class J6_LoggerBase extends munit.FunSuite:
    def db: JdbcContext

    def reset(): Unit

    override def beforeAll(): Unit =
        reset()
        db.insertBatch(List(JUser(1, "Alice", 30), JUser(2, "Bob", 25)))

    private class Recorder extends Logger:
        val events: scala.collection.mutable.ListBuffer[LogEvent] = scala.collection.mutable.ListBuffer()
        def enabled(level: LogLevel): Boolean = true
        def log(event: LogEvent): Unit = events += event

    test("successful update logs a debug event with args and row count"):
        val rec = new Recorder
        given Logger = rec
        db.deleteByPrimaryKey[JUser](7)
        db.insert(JUser(7, "Log", 3))
        val event = rec.events.last
        assertEquals(event.level, LogLevel.Debug)
        assertEquals(event.operation, LogOperation.Update)
        assertEquals(event.rowCount, Some(1))
        assert(event.statement.contains("?"))
        assertEquals(event.args.toList, List(7, "Log", 3))
        assertEquals(event.error, None)
        db.deleteByPrimaryKey[JUser](7)

    test("successful query logs a query event with fetched rows"):
        val rec = new Recorder
        given Logger = rec
        val rows = db.fetch(from(JUser).filter(u => u.age > 20))
        val event = rec.events.last
        assertEquals(event.level, LogLevel.Debug)
        assertEquals(event.operation, LogOperation.Query)
        assertEquals(event.rowCount, Some(rows.length))
        assertEquals(event.error, None)

    test("failed statement logs an error event"):
        val rec = new Recorder
        given Logger = rec
        intercept[SQLException](db.execute(sql"select * from no_such_table_x"))
        val event = rec.events.last
        assertEquals(event.level, LogLevel.Error)
        // the native `execute` entry point always logs as an update operation
        assertEquals(event.operation, LogOperation.Update)
        assertEquals(event.rowCount, None)
        assert(event.error.exists(_.getMessage.nn.nonEmpty))

    test("cursor fetch logs a cursor event"):
        val rec = new Recorder
        given Logger = rec
        db.cursorFetch(from(JUser).sortBy(u => u.id), 1)(_ => ())
        val event = rec.events.last
        assertEquals(event.operation, LogOperation.Cursor)
        assertEquals(event.rowCount, Some(2))

    test("disabled logger receives no events"):
        val rec = new Recorder:
            override def enabled(level: LogLevel): Boolean = false
        given Logger = rec
        assertEquals(db.fetch(from(JUser)).length, 2)
        assertEquals(rec.events.length, 0)

    test("logLevel override changes the success event level"):
        val rec = new Recorder:
            override def logLevel: LogLevel = LogLevel.Info
        given Logger = rec
        db.fetch(from(JUser))
        assertEquals(rec.events.last.level, LogLevel.Info)

    test("console logger filters by threshold"):
        val logger = Logger.console(LogLevel.Warn)
        assert(!logger.enabled(LogLevel.Debug))
        assert(!logger.enabled(LogLevel.Info))
        assert(logger.enabled(LogLevel.Warn))
        assert(logger.enabled(LogLevel.Error))
        assertEquals(logger.logLevel, LogLevel.Warn)

    test("noop logger never enables any level"):
        assert(!Logger.noop.enabled(LogLevel.Error))

    test("jdk logger factory respects its level argument"):
        val logger = JdkLogger("sqala-test-logger", LogLevel.Info)
        assert(!logger.enabled(LogLevel.Debug))
        assert(logger.enabled(LogLevel.Error))


class J6_Logger_Postgresql extends J6_LoggerBase:
    override def db: JdbcContext =
        pgContext

    override def reset(): Unit =
        runPgDdl(userDdl*)


class J6_Logger_Mysql extends J6_LoggerBase:
    override def db: JdbcContext =
        mysqlContext

    override def reset(): Unit =
        runMysqlDdl(userDdl*)
