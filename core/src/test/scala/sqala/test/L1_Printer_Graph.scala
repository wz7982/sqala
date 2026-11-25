package sqala.test

import sqala.ast.table.*
import sqala.test.Coverage.assertCoverage

class L1_Printer_Graph extends munit.FunSuite:
    import QueryFixture.*

    def lines(xs: String*): String = xs.mkString("\n")

    def vertex(n: String): SqlGraphPatternTerm = SqlGraphPatternTerm.Vertex(Some(n), None, None)

    val symbols: Map[String, (SqlGraphSymbol, String)] = Map(
        "Dash" -> (SqlGraphSymbol.Dash, "-"),
        "Tilde" -> (SqlGraphSymbol.Tilde, "~"),
        "LeftArrow" -> (SqlGraphSymbol.LeftArrow, "<-"),
        "RightArrow" -> (SqlGraphSymbol.RightArrow, "->"),
        "LeftTildeArrow" -> (SqlGraphSymbol.LeftTildeArrow, "<~"),
        "RightTildeArrow" -> (SqlGraphSymbol.RightTildeArrow, "~>")
    )

    def graph(term: SqlGraphPatternTerm): SqlTable.Graph =
        SqlTable.Graph(
            false, "g", None, nel(SqlGraphPattern(None, term)), None, None, nel(item(col("x"))), None, None, None
        )

    test("graph coverage"):
        assertCoverage[SqlGraphSymbol]("Dash", "Tilde", "LeftArrow", "RightArrow", "LeftTildeArrow", "RightTildeArrow")
        assertCoverage[SqlGraphQuantifier]("Asterisk", "Question", "Plus", "Between", "Quantity")
        assertCoverage[SqlGraphLabel]("Label", "Percent", "Not", "And", "Or")
        assertCoverage[SqlGraphPatternTerm]("Quantified", "Vertex", "Edge", "And", "Or", "Alternation")
        assertCoverage[SqlGraphMatchMode]("Repeatable", "Different")
        assertCoverage[SqlGraphRepeatableMode]("Element", "ElementBindings", "Elements")
        assertCoverage[SqlGraphDifferentMode]("Edge", "EdgeBindings", "Edges")
        assertCoverage[SqlGraphRowsMode]("Match", "Vertex", "Step")
        assertCoverage[SqlGraphExportMode]("AllSingletons", "Singletons", "NoSingletons")
        assertEquals(symbols.keySet, Coverage.casesOf[SqlGraphSymbol])

    test("edge symbol tokens"):
        for (name, (s, expected)) <- symbols.toList.sortBy(_._1) do
            assertEquals(createSql(_.printGraphSymbol(s)), expected, name)

    test("graph quantifier tokens"):
        assertEquals(createSql(_.printGraphQuantifier(SqlGraphQuantifier.Asterisk)), "*")
        assertEquals(createSql(_.printGraphQuantifier(SqlGraphQuantifier.Question)), "?")
        assertEquals(createSql(_.printGraphQuantifier(SqlGraphQuantifier.Plus)), "+")
        assertEquals(createSql(_.printGraphQuantifier(SqlGraphQuantifier.Quantity(num(2)))), "{2}")
        assertEquals(
            createSql(_.printGraphQuantifier(SqlGraphQuantifier.Between(Some(num(1)), Some(num(3))))),
            "{1,3}"
        )
        assertEquals(createSql(_.printGraphQuantifier(SqlGraphQuantifier.Between(Some(num(1)), None))), "{1,}")
        assertEquals(createSql(_.printGraphQuantifier(SqlGraphQuantifier.Between(None, Some(num(3))))), "{,3}")

    test("graph label tokens"):
        assertEquals(createSql(_.printGraphLabel(SqlGraphLabel.Label("L"))), """"L"""")
        assertEquals(createSql(_.printGraphLabel(SqlGraphLabel.Percent)), "%")
        assertEquals(createSql(_.printGraphLabel(SqlGraphLabel.Not(SqlGraphLabel.Label("L")))), """!("L")""")
        assertEquals(
            createSql(_.printGraphLabel(SqlGraphLabel.And(SqlGraphLabel.Label("A"), SqlGraphLabel.Label("B")))),
            """"A" & "B""""
        )
        assertEquals(
            createSql(_.printGraphLabel(SqlGraphLabel.Or(SqlGraphLabel.Label("A"), SqlGraphLabel.Label("B")))),
            """"A" | "B""""
        )

    test("label alternation inside a conjunction is parenthesized"):
        val or = SqlGraphLabel.Or(SqlGraphLabel.Label("A"), SqlGraphLabel.Label("B"))
        assertEquals(
            createSql(_.printGraphLabel(SqlGraphLabel.And(or, SqlGraphLabel.Label("C")))),
            """("A" | "B") & "C""""
        )
        assertEquals(
            createSql(_.printGraphLabel(SqlGraphLabel.And(SqlGraphLabel.Label("C"), or))),
            """"C" & ("A" | "B")"""
        )

    test("vertex pattern combinations"):
        assertEquals(createSql(_.printGraphPatternTerm(SqlGraphPatternTerm.Vertex(None, None, None))), "()")
        assertEquals(createSql(_.printGraphPatternTerm(vertex("a"))), """("a")""")
        assertEquals(
            createSql(_.printGraphPatternTerm(
                SqlGraphPatternTerm.Vertex(None, Some(SqlGraphLabel.Label("L")), None)
            )),
            """(IS "L")"""
        )
        assertEquals(
            createSql(_.printGraphPatternTerm(
                SqlGraphPatternTerm.Vertex(Some("a"), Some(SqlGraphLabel.Percent), Some(gt(col("x"), 0)))
            )),
            """("a" IS % WHERE "x" > 0)"""
        )

    test("edge pattern combinations"):
        val base: SqlGraphPatternTerm.Edge =
            SqlGraphPatternTerm.Edge(SqlGraphSymbol.Dash, None, None, None, SqlGraphSymbol.RightArrow)
        assertEquals(createSql(_.printGraphPatternTerm(base)), "-[]->")
        assertEquals(
            createSql(_.printGraphPatternTerm(base.copy(
                leftSymbol = SqlGraphSymbol.LeftTildeArrow,
                name = Some("e"),
                label = Some(SqlGraphLabel.Label("L")),
                where = Some(gt(col("x"), 0)),
                rightSymbol = SqlGraphSymbol.RightTildeArrow
            ))),
            """<~["e" IS "L" WHERE "x" > 0]~>"""
        )

    test("quantified pattern term"):
        assertEquals(
            createSql(_.printGraphPatternTerm(
                SqlGraphPatternTerm.Quantified(vertex("a"), SqlGraphQuantifier.Plus)
            )),
            """("a")+"""
        )
        val and = SqlGraphPatternTerm.And(vertex("a"), vertex("b"))
        assertEquals(
            createSql(_.printGraphPatternTerm(SqlGraphPatternTerm.Quantified(and, SqlGraphQuantifier.Asterisk))),
            """(("a") ("b"))*"""
        )

    test("pattern term operators and precedence"):
        val a = vertex("a")
        val b = vertex("b")
        val c = vertex("c")
        assertEquals(createSql(_.printGraphPatternTerm(SqlGraphPatternTerm.And(a, b))), """("a") ("b")""")
        assertEquals(createSql(_.printGraphPatternTerm(SqlGraphPatternTerm.Or(a, b))), """("a") | ("b")""")
        assertEquals(createSql(_.printGraphPatternTerm(SqlGraphPatternTerm.Alternation(a, b))), """("a") |+| ("b")""")

        val or = SqlGraphPatternTerm.Or(a, b)
        assertEquals(
            createSql(_.printGraphPatternTerm(SqlGraphPatternTerm.And(or, c))),
            """(("a") | ("b")) ("c")"""
        )
        val alternation = SqlGraphPatternTerm.Alternation(a, b)
        assertEquals(
            createSql(_.printGraphPatternTerm(SqlGraphPatternTerm.Or(alternation, c))),
            """(("a") |+| ("b")) | ("c")"""
        )
        assertEquals(
            createSql(_.printGraphPatternTerm(SqlGraphPatternTerm.Alternation(or, c))),
            """(("a") | ("b")) |+| ("c")"""
        )

    test("named graph pattern"):
        assertEquals(
            createSql(_.printGraphPattern(SqlGraphPattern(Some("p"), vertex("a")))),
            """"p" = ("a")"""
        )

    test("graph match mode"):
        assertEquals(
            createSql(_.printGraphMatchMode(SqlGraphMatchMode.Repeatable(SqlGraphRepeatableMode.Element))),
            "REPEATABLE ELEMENT"
        )
        assertEquals(
            createSql(_.printGraphMatchMode(SqlGraphMatchMode.Repeatable(SqlGraphRepeatableMode.ElementBindings))),
            "REPEATABLE ELEMENT BINDINGS"
        )
        assertEquals(
            createSql(_.printGraphMatchMode(SqlGraphMatchMode.Repeatable(SqlGraphRepeatableMode.Elements))),
            "REPEATABLE ELEMENTS"
        )
        assertEquals(
            createSql(_.printGraphMatchMode(SqlGraphMatchMode.Different(SqlGraphDifferentMode.Edge))),
            "DIFFERENT EDGE"
        )
        assertEquals(
            createSql(_.printGraphMatchMode(SqlGraphMatchMode.Different(SqlGraphDifferentMode.EdgeBindings))),
            "DIFFERENT EDGE BINDINGS"
        )
        assertEquals(
            createSql(_.printGraphMatchMode(SqlGraphMatchMode.Different(SqlGraphDifferentMode.Edges))),
            "DIFFERENT EDGES"
        )

    test("graph rows mode"):
        assertEquals(createSql(_.printGraphRowsMode(SqlGraphRowsMode.Match)), "ONE ROW PER MATCH")
        assertEquals(
            createSql(_.printGraphRowsMode(SqlGraphRowsMode.Vertex("a", Nil))),
            """ONE ROW PER VERTEX ("a")"""
        )
        assertEquals(
            createSql(_.printGraphRowsMode(SqlGraphRowsMode.Vertex("a", List("p", "q")))),
            """ONE ROW PER VERTEX ("a") IN ("p", "q")"""
        )
        assertEquals(
            createSql(_.printGraphRowsMode(SqlGraphRowsMode.Step("a", "e", "b", Nil))),
            """ONE ROW PER STEP ("a", "e", "b")"""
        )
        assertEquals(
            createSql(_.printGraphRowsMode(SqlGraphRowsMode.Step("a", "e", "b", List("p")))),
            """ONE ROW PER STEP ("a", "e", "b") IN ("p")"""
        )

    test("graph export mode"):
        assertEquals(
            createSql(_.printGraphExportMode(SqlGraphExportMode.AllSingletons(nel("p", "q")))),
            """EXPORT ALL SINGLETONS EXCEPT ("p", "q")"""
        )
        assertEquals(
            createSql(_.printGraphExportMode(SqlGraphExportMode.Singletons(nel("p")))),
            """EXPORT SINGLETONS ("p")"""
        )
        assertEquals(createSql(_.printGraphExportMode(SqlGraphExportMode.NoSingletons)), "EXPORT NO SINGLETONS")

    test("graph table"):
        assertEquals(
            createSql(_.printTable(graph(vertex("a")))),
            lines(
                "GRAPH_TABLE(",
                """    "g"""",
                "    MATCH",
                """        ("a")""",
                "    COLUMNS(",
                """        "x"""",
                "    )",
                ")"
            )
        )

    test("graph table with all clauses"):
        val g = graph(vertex("a")).copy(
            withLateral = true,
            matchMode = Some(SqlGraphMatchMode.Different(SqlGraphDifferentMode.Edges)),
            where = Some(gt(col("x"), 0)),
            rowsMode = Some(SqlGraphRowsMode.Match),
            exportMode = Some(SqlGraphExportMode.NoSingletons),
            alias = Some(SqlTableAlias("t", Nil))
        )
        assertEquals(
            createSql(_.printTable(g)),
            lines(
                "LATERAL GRAPH_TABLE(",
                """    "g"""",
                "    MATCH DIFFERENT EDGES",
                """        ("a")""",
                "    WHERE",
                """        "x" > 0""",
                "    ONE ROW PER MATCH",
                "    COLUMNS(",
                """        "x"""",
                "    )",
                "    EXPORT NO SINGLETONS",
                """) AS "t""""
            )
        )