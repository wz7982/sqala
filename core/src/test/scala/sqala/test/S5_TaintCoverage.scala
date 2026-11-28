package sqala.test

import scala.language.unsafeNulls
import sqala.printer.*

import java.lang.reflect.{InvocationTargetException, Method}
import java.nio.file.{Files, Path, Paths}

/**
 * I1 whole-AST taint sweep.
 *
 * Every sqala.ast enum case is constructed reflectively with unique taint
 * markers in all String fields, printed with six dialects and both
 * standardEscapeStrings values, and the output is stripped of string and
 * quoted-identifier regions: a marker surviving in the skeleton means a
 * field reached the SQL text without quoting. The only allowlisted sink is
 * SqlExpr.IdentFunc, whose name is a bare keyword identifier by contract
 * (printFuncName, covered by S1 for function names generally).
 */
class S5_TaintCoverage extends munit.FunSuite:
    import TaintFactory.*

    private def astRoot: Path =
        var root = Paths.get(System.getProperty("user.dir")).toAbsolutePath
        while !Files.exists(root.resolve("build.sbt")) && root.getParent != null do root = root.getParent
        root.resolve("core/src/main/scala/sqala/ast")

    private val dialects: List[(String, Boolean => StandardSqlPrinter, Char, Char)] = List(
        ("postgresql", e => new PostgresqlPrinter(e), '"', '"'),
        ("mysql", e => new MysqlPrinter(e), '`', '`'),
        ("sqlite", e => new SqlitePrinter(e), '"', '"'),
        ("oracle", e => new OraclePrinter(e), '"', '"'),
        ("h2", e => new H2Printer(e), '"', '"'),
        ("sqlserver", e => new SqlserverPrinter(e), '[', ']')
    )

    /** Blank out string and quoted-identifier regions, keeping the skeleton. */
    private def skeleton(sql: String, lq: Char, rq: Char): String =
        val sb = new StringBuilder
        var i = 0
        while i < sql.length do
            val c = sql.charAt(i)
            if c == '\'' then
                sb.append('S')
                i += 1
                var done = false
                while i < sql.length && !done do
                    if sql.charAt(i) == '\'' then
                        if i + 1 < sql.length && sql.charAt(i + 1) == '\'' then i += 2
                        else { done = true; i += 1 }
                    else i += 1
            else if c == lq then
                sb.append('I')
                i += 1
                var done = false
                while i < sql.length && !done do
                    if sql.charAt(i) == rq then
                        if i + 1 < sql.length && sql.charAt(i + 1) == rq then i += 2
                        else { done = true; i += 1 }
                    else i += 1
            else
                sb.append(c)
                i += 1
        sb.toString

    private def sinkFor(cls: Class[?]): Option[Method] =
        classOf[StandardSqlPrinter].getMethods
            .filter(m => m.getName.startsWith("print") && m.getParameterCount == 1 && m.getParameterTypes()(0) == cls)
            .sortBy(_.getName)
            .headOption

    test("the enum scanner and factory cover the whole ast package"):
        val classes = enumClasses(astRoot)
        assert(classes.size >= 70, s"expected at least 70 ast enums, found ${classes.size}")
        assert(classes.exists(_.getSimpleName == "SqlExpr"), "the scanner lost SqlExpr")

        var built = 0
        for cls <- classes do
            if !cls.getSimpleName.contains("UnsafeCustom") then built += allInstances(cls, 3).size
        assert(built >= 300, s"reflective construction built only $built cases; the factory is broken")

    test("the skeleton detector flags bare markers and passes quoted ones"):
        assert(skeleton("SELECT SLT9 FROM x", '"', '"').contains("SLT9"))
        assertEquals(skeleton("'SLT9'", '"', '"'), "S")
        assertEquals(skeleton("\"SLT9\"", '"', '"'), "I")
        assertEquals(skeleton("`SLT9`", '`', '`'), "I")
        assertEquals(skeleton("[SLT9]", '[', ']'), "I")
        assertEquals(skeleton("'a''SLT9'''", '"', '"'), "S")

    test("no taint marker reaches the sql text outside a quoted region"):
        val classes = enumClasses(astRoot)
        var checkedPrints = 0
        var dbgBuilt = 0
        var dbgMarkers = 0
        val leaks = List.newBuilder[String]

        for cls <- classes do
            val sink =
                if cls.getSimpleName.contains("UnsafeCustom") then None
                else sinkFor(cls)
            if sink.isDefined then
                for built <- allInstances(cls, 3) do
                    dbgBuilt += 1
                    val isBareAllowed = s"${cls.getSimpleName}.${built.caseName}" == "SqlExpr.IdentFunc"
                    val markers = (built.markerFrom + 1 to built.markerTo).map(i => s"SLT$i").toList
                    if markers.nonEmpty then dbgMarkers += 1
                    if markers.nonEmpty && !isBareAllowed then
                        for (dtag, mkPrinter, lq, rq) <- dialects; esc <- List(true, false) do
                            val p = mkPrinter(esc)
                            try
                                sink.get.invoke(p, built.instance)
                                checkedPrints += 1
                                val skel = skeleton(p.sql, lq, rq)
                                val hit = markers.filter(m => skel.contains(m))
                                if hit.nonEmpty then
                                    leaks += s"${cls.getSimpleName}.${built.caseName}|$dtag|esc=$esc|markers=${hit.mkString(",")}|${p.sql.replace("\n", "\\n").take(200)}"
                            catch
                                case it: InvocationTargetException => ()
                                case _: Throwable => ()

        assert(dbgMarkers >= 40, s"only $dbgBuilt cases were built and $dbgMarkers carried String taint; the factory lost the string sink signal")
        assert(checkedPrints >= 150, s"only $checkedPrints marker-carrying prints were checkable; the sweep lost its signal")
        assertEquals(leaks.result(), Nil)
