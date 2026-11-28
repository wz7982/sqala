package sqala.test

import java.io.File
import scala.collection.mutable

/**
 * Structural scan of `using` clauses in the static DSL sources.
 *
 * Guards against the wiring mistake where two argument slots reference
 * the same `AsExpr`/`ValidateArgType` result (the historical `overlaps`
 * bug: `da..dd` all pointing at `aa.R`), which per-slot type tests
 * cannot detect.
 */
class A10_ArgSlotScan extends munit.FunSuite:
    private def repoRoot: File =
        var dir = new File(sys.props("user.dir")).getAbsoluteFile
        while dir != null && !(new File(dir, "build.sbt").exists() && new File(dir, "static").isDirectory) do
            dir = dir.getParentFile
        if dir == null then fail("cannot locate repository root from " + sys.props("user.dir"))
        dir

    private def walk(root: File, out: mutable.ArrayBuffer[File]): Unit =
        if root.isDirectory then Option(root.listFiles).foreach(_.foreach(walk(_, out)))
        else if root.getName.endsWith(".scala") then out += root

    private def sources: List[File] =
        val out = mutable.ArrayBuffer.empty[File]
        walk(new File(repoRoot, "static/src/main"), out)
        walk(new File(repoRoot, "static-ext/src/main"), out)
        out.toList.sortBy(_.getAbsolutePath)

    private def matching(s: String, open: Int, ob: Char, cb: Char): Int =
        var depth = 0
        var i = open
        while i < s.length do
            val c = s.charAt(i)
            if c == ob then depth += 1
            else if c == cb then
                if depth == 1 then return i
                depth -= 1
            i += 1
        -1

    private def splitParams(body: String): List[String] =
        val out = mutable.ArrayBuffer.empty[String]
        val cur = new StringBuilder
        var round = 0
        var square = 0
        var brace = 0
        var quote: Char = 0
        body.foreach: c =>
            if quote != 0 then
                cur += c
                if c == quote then quote = 0
            else if c == '"' || c == '\'' then
                quote = c
                cur += c
            else
                c match
                    case '(' => round += 1
                    case ')' => round -= 1
                    case '[' => square += 1
                    case ']' => square -= 1
                    case '{' => brace += 1
                    case '}' => brace -= 1
                    case ',' if round == 0 && square == 0 && brace == 0 =>
                        out += cur.toString
                        cur.clear()
                    case _ => cur += c
        out += cur.toString
        out.map(_.trim).filter(_.nonEmpty).toList

    private def defNameBefore(s: String, usingIdx: Int): String =
        val sig = s.lastIndexOf("def ", usingIdx)
        if sig < 0 then "<header>"
        else
            val rest = s.substring(sig + 4)
            val end = rest.indexWhere(c => !(c.isLetterOrDigit || c == '_' || "+-*/%<>=!~&|^@:".contains(c)))
            rest.take(if end < 0 then 24 else math.min(end, 24))

    private def scanClause(file: String, s: String, usingIdx: Int, body: String): List[String] =
        val found = mutable.ArrayBuffer.empty[(String, Int, String, Int, String, String)]
        var i = 0
        while i < body.length do
            if body.startsWith("ValidateArgType", i) && (i == 0 || !(body.charAt(i - 1).isLetterOrDigit || body.charAt(i - 1) == '.')) then
                val open = body.indexOf('[', i)
                val close = if open < 0 then -1 else matching(body, open, '[', ']')
                if close > 0 then
                    splitParams(body.substring(open + 1, close)) match
                        case f :: x :: _ => found += (("va", i, f.trim, 0, x.trim, ""))
                        case _ =>
                    i = close
            else if body.startsWith("AsExpr", i) && (i == 0 || !(body.charAt(i - 1).isLetterOrDigit || body.charAt(i - 1) == '.')) then
                val open = body.indexOf('[', i)
                val close = if open < 0 then -1 else matching(body, open, '[', ']')
                if close > 0 then
                    splitParams(body.substring(open + 1, close)) match
                        case x :: _ => found += (("as", i, "", 0, x.trim, ""))
                        case _ =>
                    i = close
            i += 1

        val problems = mutable.ArrayBuffer.empty[String]
        val defn = defNameBefore(s, usingIdx)
        val line = s.take(usingIdx).count(_ == '\n') + 1

        found.groupBy { case (kind, _, _, _, x, _) => (kind, x) }.foreach:
            case ((kind, x), xs) =>
                if xs.size > 1 then
                    val label = if kind == "va" then "ValidateArgType" else "AsExpr"
                    problems += s"$file:$line def $defn: $label on `$x` referenced ${xs.size} times in one using clause"

        found.groupBy { case (kind, _, f, _, x, _) => (kind, f, x) }.foreach:
            case (key, xs) =>
                key match
                    case ("va", f, x) if xs.size > 1 =>
                        problems += s"$file:$line def $defn: duplicate ValidateArgType[$f, $x, _]"
                    case _ =>

        problems.toList

    private def fileText(f: File): String =
        new String(java.nio.file.Files.readAllBytes(f.toPath), java.nio.charset.StandardCharsets.UTF_8)

    private def scanText(rel: String, text: String): (List[String], Int) =
        val out = mutable.ArrayBuffer.empty[String]
        var clauses = 0
        var i = 0
        while i < text.length do
            val j = text.indexOf("using", i)
            if j < 0 then i = text.length
            else
                val prevOk = j == 0 || !(text.charAt(j - 1).isLetterOrDigit || text.charAt(j - 1) == '_')
                var p = j - 1
                while p >= 0 && (text.charAt(p) == ' ' || text.charAt(p) == '\t' || text.charAt(p) == '\n' || text.charAt(p) == '\r') do p -= 1
                if prevOk && p >= 0 && text.charAt(p) == '(' then
                    val close = matching(text, p, '(', ')')
                    if close < 0 then
                        out += s"$rel: unterminated using( near offset $j"
                        i = text.length
                    else
                        clauses += 1
                        out ++= scanClause(rel, text, p, text.slice(p + 1, close))
                        i = close
                else i = j + 5
        (out.toList, clauses)

    test("scan detects the historical overlaps-style wiring bug"):
        val fixture =
            """
              |extension [A](self: A)
              |    def overlaps[C, D](that: (C, D))(using
              |        ac: AsExpr[C, CL],
              |        da: ValidateArgType[SqlDateTime, aa.R, "datetime"],
              |        db: ValidateArgType[SqlDateTime, aa.R, "datetime"],
              |        dc: ValidateArgType[SqlDateTime, ac.R, "datetime"]
              |    ): Expr[Int, Value] = ???
              |""".stripMargin
        val (violations, clauses) = scanText("fixture.scala", fixture)
        assertEquals(clauses, 1)
        assert(violations.exists(_.contains("ValidateArgType on `aa.R` referenced 2 times")), violations.toString())
        assert(violations.exists(_.contains("duplicate ValidateArgType[SqlDateTime, aa.R, _]")), violations.toString())
        assertEquals(violations.size, 2)

    test("no using clause reuses the same AsExpr/ValidateArgType slot"):
        val scanned = sources.map: f =>
            scanText(
                f.getAbsolutePath.substring(repoRoot.getAbsolutePath.length + 1).replace('\\', '/'),
                fileText(f)
            )
        val violations = scanned.flatMap(_._1)
        val clauseCount = scanned.map(_._2).sum

        if violations.nonEmpty then
            println("== arg-slot scan findings ==")
            violations.foreach(println)
        println(s"scanned ${sources.size} files, $clauseCount using clauses")
        assert(clauseCount > 600, s"expected the scan to see the bulk of the DSL using clauses, got $clauseCount")
        assertEquals(violations, Nil)
