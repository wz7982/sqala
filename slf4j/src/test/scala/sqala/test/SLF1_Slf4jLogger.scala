package sqala.test

import scala.language.unsafeNulls
import sqala.jdbc.{LogEvent, LogLevel, LogOperation}
import sqala.slf4j.Slf4jLogger

import java.lang.reflect.Proxy
import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable.ListBuffer

object Slf4jStub:
    final class Recorder:
        val calls = ListBuffer.empty[(String, List[Any])]
        val enabled = new AtomicReference[LogLevel => Boolean](_ => true)

        def isAt(level: LogLevel): Boolean = enabled.get.apply(level)

        def last: (String, List[Any]) = calls.last

        def clear(): Unit = calls.clear()

        /** The message argument of the last routing call, if any. */
        def lastMessage: String = last._2.collectFirst { case s: String => s }.getOrElse("")

        def lastThrowable: Option[Throwable] = last._2.collectFirst { case t: Throwable => t }

        val logger: org.slf4j.Logger =
            Proxy
                .newProxyInstance(
                    classOf[org.slf4j.Logger].getClassLoader,
                    Array(classOf[org.slf4j.Logger]),
                    (_, method, args) =>
                        val name = method.getName
                        val list = if args == null then Nil else args.toList
                        name match
                            case "getName" => "sqala-stub"
                            case n if n.length > 9 && n.startsWith("is") && n.endsWith("Enabled") =>
                                val level = LogLevel.valueOf(n.substring(2, n.length - 7))
                                java.lang.Boolean.valueOf(isAt(level))
                            case _ =>
                                calls += (name -> list)
                                null
                )
                .asInstanceOf[org.slf4j.Logger]

    def event(
        level: LogLevel = LogLevel.Debug,
        error: Option[Throwable] = None,
        args: Array[Any] = Array.empty,
        rowCount: Option[Int] = None
    ): LogEvent =
        LogEvent(level, LogOperation.Query, "select 1", args, 1_500_000L, rowCount, error)

class SLF1_Slf4jLogger extends munit.FunSuite:
    import Slf4jStub.*

    private def adapter(stub: Recorder, logArgs: Boolean = true, maxLen: Int = 200, level: LogLevel = LogLevel.Debug) =
        Slf4jLogger(stub.logger, logArgs, maxLen, level)

    test("enabled delegates to the slf4j logger per level"):
        val stub = new Recorder
        stub.enabled.set(l => Set(LogLevel.Debug, LogLevel.Info).contains(l))
        val a = adapter(stub)
        assertEquals(a.enabled(LogLevel.Trace), false)
        assertEquals(a.enabled(LogLevel.Debug), true)
        assertEquals(a.enabled(LogLevel.Info), true)
        assertEquals(a.enabled(LogLevel.Warn), false)
        assertEquals(a.enabled(LogLevel.Error), false)

    test("logLevel reports the configured success level"):
        assertEquals(adapter(new Recorder, level = LogLevel.Info).logLevel, LogLevel.Info)
        assertEquals(Slf4jLogger().logLevel, LogLevel.Debug)

    test("each event level routes to the matching slf4j method"):
        for level <- List(LogLevel.Trace, LogLevel.Debug, LogLevel.Info, LogLevel.Warn, LogLevel.Error) do
            val stub = new Recorder
            adapter(stub).log(event(level = level))
            assertEquals(stub.calls.map(_._1).toList, List(level.toString.toLowerCase))

    test("an event error is passed as the throwable argument"):
        for level <- List(LogLevel.Trace, LogLevel.Debug, LogLevel.Info, LogLevel.Warn, LogLevel.Error) do
            val stub = new Recorder
            val boom = new RuntimeException("boom")
            adapter(stub).log(event(level = level, error = Some(boom)))
            val (method, args) = stub.last
            assertEquals(method, level.toString.toLowerCase)
            assert(args.exists(a => a.asInstanceOf[AnyRef] eq boom), s"throwable not forwarded for $level")

    test("a disabled level produces no call and no message formatting"):
        val stub = new Recorder
        stub.enabled.set(_ => false)
        adapter(stub).log(event(error = Some(new RuntimeException("x"))))
        assertEquals(stub.calls.toList, Nil)

    test("the message is Logger.format of the event"):
        val stub = new Recorder
        val boom = new IllegalStateException("cause")
        adapter(stub).log(event(level = LogLevel.Warn, args = Array(1, "abc"), rowCount = Some(7), error = Some(boom)))
        val message = stub.lastMessage
        assert(message.startsWith("[Warn] Query 1.50ms, rows=7"), message)
        assert(message.contains("select 1"), message)
        assert(message.contains("args=[1, abc]"), message)

    test("logArgs=false omits the args line"):
        val stub = new Recorder
        adapter(stub, logArgs = false).log(event(args = Array("hidden")))
        assert(!stub.lastMessage.contains("args="), stub.lastMessage)

    test("maxArgumentLength truncates rendered arguments"):
        val stub = new Recorder
        val long = "x" * 50
        adapter(stub, maxLen = 10).log(event(args = Array(long)))
        assert(stub.lastMessage.contains("xxxxxxxxxx..."), stub.lastMessage)
        assert(!stub.lastMessage.contains("xxxxxxxxxxx"), stub.lastMessage)

    test("the named factory builds a working logger over the slf4j backend"):
        val a = Slf4jLogger(name = "sqala-test-suite", logArgs = true, maxArgumentLength = 200, level = LogLevel.Error)
        assertEquals(a.logLevel, LogLevel.Error)
        a.log(event(level = LogLevel.Error))

    test("the module default given logger exists and reports Debug"):
        import sqala.slf4j.given
        val defaultLogger = summon[sqala.jdbc.Logger]
        assertEquals(defaultLogger.logLevel, LogLevel.Debug)
        defaultLogger.log(event(level = LogLevel.Info))
