package sqala.test

import sqala.ast.expr.*
import sqala.ast.table.*
import sqala.test.Coverage.assertCoverage

class L1_Printer_RowPattern extends munit.FunSuite:
    import QueryFixture.*

    def lines(xs: String*): String = xs.mkString("\n")

    def pat(n: String): SqlRowPatternTerm = SqlRowPatternTerm.Pattern(n, None)

    def rowPattern(term: SqlRowPatternTerm): SqlRowPattern =
        SqlRowPattern(None, None, term, Nil, nel(SqlRowPatternDefineItem("A", SqlExpr.BooleanLiteral(true))))

    val quantifiers: Map[String, (SqlRowPatternQuantifier, String)] = Map(
        "Asterisk" -> (SqlRowPatternQuantifier.Asterisk(false), "*"),
        "AsteriskReluctant" -> (SqlRowPatternQuantifier.Asterisk(true), "*?"),
        "Plus" -> (SqlRowPatternQuantifier.Plus(false), "+"),
        "PlusReluctant" -> (SqlRowPatternQuantifier.Plus(true), "+?"),
        "Question" -> (SqlRowPatternQuantifier.Question(false), "?"),
        "QuestionReluctant" -> (SqlRowPatternQuantifier.Question(true), "??"),
        "BetweenBoth" -> (SqlRowPatternQuantifier.Between(Some(num(1)), Some(num(3)), false), "{1,3}"),
        "BetweenStart" -> (SqlRowPatternQuantifier.Between(Some(num(1)), None, false), "{1,}"),
        "BetweenEnd" -> (SqlRowPatternQuantifier.Between(None, Some(num(3)), false), "{,3}"),
        "BetweenReluctant" -> (SqlRowPatternQuantifier.Between(Some(num(1)), Some(num(3)), true), "{1,3}?"),
        "Quantity" -> (SqlRowPatternQuantifier.Quantity(num(2)), "{2}")
    )

    test("row pattern coverage"):
        assertCoverage[SqlRowPatternQuantifier]("Asterisk", "Plus", "Question", "Between", "Quantity")
        assertCoverage[SqlRowPatternTerm]("Pattern", "Circumflex", "Dollar", "Exclusion", "Permute", "Then", "Or")
        assertCoverage[SqlRowPatternSkipMode]("ToNextRow", "PastLastRow", "ToFirst", "ToLast", "To")
        assertCoverage[SqlRowPatternStrategy]("Initial", "Seek")
        assertCoverage[SqlRecognizePatternRowsMode]("OneRow", "AllRows")
        assertCoverage[SqlRecognizePatternEmptyMatchMode]("ShowEmptyMatches", "OmitEmptyMatches", "WithUnmatchedRows")

    test("quantifier tokens"):
        for (name, (q, expected)) <- quantifiers.toList.sortBy(_._1) do
            assertEquals(createSql(_.printRowPatternQuantifier(q)), expected, name)

    test("simple terms"):
        assertEquals(createSql(_.printRowPatternTerm(pat("A"))), """"A"""")
        assertEquals(createSql(_.printRowPatternTerm(SqlRowPatternTerm.Circumflex(None))), "^")
        assertEquals(createSql(_.printRowPatternTerm(SqlRowPatternTerm.Dollar(None))), "$")
        assertEquals(
            createSql(_.printRowPatternTerm(SqlRowPatternTerm.Exclusion(pat("A"), None))),
            """{-"A"-}"""
        )
        assertEquals(
            createSql(_.printRowPatternTerm(SqlRowPatternTerm.Permute(nel(pat("A"), pat("B")), None))),
            """PERMUTE("A", "B")"""
        )

    test("term quantifier is attached without a space"):
        assertEquals(
            createSql(_.printRowPatternTerm(SqlRowPatternTerm.Pattern("A", Some(SqlRowPatternQuantifier.Plus(false))))),
            """"A"+"""
        )
        assertEquals(
            createSql(_.printRowPatternTerm(SqlRowPatternTerm.Circumflex(Some(SqlRowPatternQuantifier.Question(false))))),
            "^?"
        )

    test("then and or"):
        assertEquals(
            createSql(_.printRowPatternTerm(SqlRowPatternTerm.Then(pat("A"), pat("B"), None))),
            """"A" "B""""
        )
        assertEquals(
            createSql(_.printRowPatternTerm(SqlRowPatternTerm.Or(pat("A"), pat("B"), None))),
            """"A" | "B""""
        )

    test("alternation nested in a sequence is parenthesized"):
        val or = SqlRowPatternTerm.Or(pat("A"), pat("B"), None)
        assertEquals(
            createSql(_.printRowPatternTerm(SqlRowPatternTerm.Then(or, pat("C"), None))),
            """("A" | "B") "C""""
        )
        assertEquals(
            createSql(_.printRowPatternTerm(SqlRowPatternTerm.Then(pat("C"), or, None))),
            """"C" ("A" | "B")"""
        )

    test("quantified compound terms are parenthesized"):
        val q = Some(SqlRowPatternQuantifier.Plus(false))
        assertEquals(
            createSql(_.printRowPatternTerm(SqlRowPatternTerm.Then(pat("A"), pat("B"), q))),
            """("A" "B")+"""
        )
        assertEquals(
            createSql(_.printRowPatternTerm(SqlRowPatternTerm.Or(pat("A"), pat("B"), q))),
            """("A" | "B")+"""
        )

    test("skip mode tokens"):
        assertEquals(createSql(_.printRowPatternSkipMode(SqlRowPatternSkipMode.ToNextRow)), "SKIP TO NEXT ROW")
        assertEquals(createSql(_.printRowPatternSkipMode(SqlRowPatternSkipMode.PastLastRow)), "SKIP PAST LAST ROW")
        assertEquals(createSql(_.printRowPatternSkipMode(SqlRowPatternSkipMode.ToFirst("A"))), """SKIP TO FIRST "A"""")
        assertEquals(createSql(_.printRowPatternSkipMode(SqlRowPatternSkipMode.ToLast("A"))), """SKIP TO LAST "A"""")
        assertEquals(createSql(_.printRowPatternSkipMode(SqlRowPatternSkipMode.To("A"))), """SKIP TO "A"""")

    test("strategy tokens"):
        assertEquals(createSql(_.printRowPatternStrategy(SqlRowPatternStrategy.Initial)), "INITIAL")
        assertEquals(createSql(_.printRowPatternStrategy(SqlRowPatternStrategy.Seek)), "SEEK")

    test("rows per match modes"):
        assertEquals(createSql(_.printRecognizePatternRowsMode(SqlRecognizePatternRowsMode.OneRow)), "ONE ROW PER MATCH")
        assertEquals(
            createSql(_.printRecognizePatternRowsMode(SqlRecognizePatternRowsMode.AllRows(None))),
            "ALL ROWS PER MATCH"
        )
        assertEquals(
            createSql(_.printRecognizePatternRowsMode(
                SqlRecognizePatternRowsMode.AllRows(Some(SqlRecognizePatternEmptyMatchMode.ShowEmptyMatches))
            )),
            "ALL ROWS PER MATCH SHOW EMPTY MATCHES"
        )
        assertEquals(
            createSql(_.printRecognizePatternEmptyMatchMode(SqlRecognizePatternEmptyMatchMode.OmitEmptyMatches)),
            "OMIT EMPTY MATCHES"
        )
        assertEquals(
            createSql(_.printRecognizePatternEmptyMatchMode(SqlRecognizePatternEmptyMatchMode.WithUnmatchedRows)),
            "WITH UNMATCHED ROWS"
        )

    test("subset and define items"):
        assertEquals(
            createSql(_.printRowPatternSubsetItem(SqlRowPatternSubsetItem("S", nel("A", "B")))),
            """"S" = ("A", "B")"""
        )
        assertEquals(
            createSql(_.printRowPatternDefineItem(SqlRowPatternDefineItem("A", gt(col("x"), 0)))),
            """"A" AS "x" > 0"""
        )
        assertEquals(
            createSql(_.printRowPatternMeasureItem(SqlRowPatternMeasureItem(col("x"), "m"))),
            """"x" AS "m""""
        )

    test("row pattern"):
        assertEquals(
            createSql(_.printRowPattern(rowPattern(pat("A")))),
            lines("""    PATTERN ("A")""", "    DEFINE", """        "A" AS TRUE""")
        )

    test("row pattern with skip mode strategy and subset"):
        val p = rowPattern(pat("A")).copy(
            afterMatchMode = Some(SqlRowPatternSkipMode.PastLastRow),
            strategy = Some(SqlRowPatternStrategy.Initial),
            subset = List(SqlRowPatternSubsetItem("S", nel("A")))
        )
        assertEquals(
            createSql(_.printRowPattern(p)),
            lines(
                "    AFTER MATCH SKIP PAST LAST ROW",
                """    INITIAL PATTERN ("A")""",
                "    SUBSET",
                """        "S" = ("A")""",
                "    DEFINE",
                """        "A" AS TRUE"""
            )
        )

    test("match recognize"):
        val mr = SqlMatchRecognize(
            Nil,
            Nil,
            nel(SqlRowPatternMeasureItem(col("a"), "m")),
            None,
            rowPattern(pat("A")),
            None
        )
        assertEquals(
            createSql(_.printMatchRecognize(mr)),
            lines(
                "",
                "MATCH_RECOGNIZE(",
                "    MEASURES",
                """        "a" AS "m"""",
                """    PATTERN ("A")""",
                "    DEFINE",
                """        "A" AS TRUE""",
                ")"
            )
        )

    test("match recognize with all clauses"):
        val mr = SqlMatchRecognize(
            List(col("p")),
            List(ord(col("o"))),
            nel(SqlRowPatternMeasureItem(col("a"), "m")),
            Some(SqlRecognizePatternRowsMode.AllRows(Some(SqlRecognizePatternEmptyMatchMode.OmitEmptyMatches))),
            rowPattern(pat("A")),
            Some(SqlTableAlias("x", Nil))
        )
        assertEquals(
            createSql(_.printMatchRecognize(mr)),
            lines(
                "",
                "MATCH_RECOGNIZE(",
                "    PARTITION BY",
                """        "p"""",
                "    ORDER BY",
                """        "o" ASC""",
                "    MEASURES",
                """        "a" AS "m"""",
                "    ALL ROWS PER MATCH OMIT EMPTY MATCHES",
                """    PATTERN ("A")""",
                "    DEFINE",
                """        "A" AS TRUE""",
                """) AS "x""""
            )
        )