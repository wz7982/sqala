package sqala.test

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr, SqlMatchPhase, SqlType}
import sqala.ast.order.SqlOrdering
import sqala.ast.table.{SqlGraphExportMode, SqlGraphLabel, SqlGraphRowsMode, SqlGraphSymbol, SqlJoinType, SqlRecognizePatternEmptyMatchMode, SqlRecognizePatternRowsMode, SqlRowPatternSkipMode, SqlRowPatternStrategy, SqlRowPatternSubsetItem}
import sqala.ir.*
import sqala.test.IrFixture.*
import sqala.util.NonEmptyList

/**
 * Table source lowering: identifiers with column aliases, joins,
 * function tables, JSON tables, graph tables, and MATCH_RECOGNIZE.
 */
class I3_Tables extends munit.FunSuite:
    private val user: IrTable.Ident = IrTable.Ident("user", Nil)

    test("identifier tables carry column aliases"):
        assertEquals(
            pg(sel(List(IrSelectItem(col(user, "id"), "id")), Some(IrTable.Ident("user", List("c1", "c2"))))),
            lines(
                "SELECT",
                """    "t2"."id" AS "id"""",
                "FROM",
                """    "user" AS "t1"("c1", "c2")"""
            )
        )

    test("join types and cross joins lower with and without on"):
        assertEquals(
            pg(sel(List(IrSelectItem(n(1), "v")), Some(IrTable.Join(user, SqlJoinType.Left, IrTable.Ident("post", Nil), Some(IrExpr.Binary(col(user, "id"), SqlBinaryOperator.Equal, col(IrTable.Ident("post", Nil), "user_id"))))))),
            lines(
                "SELECT",
                """    1 AS "v"""",
                "FROM",
                """    "user" AS "t1"""",
                """    LEFT OUTER JOIN "post" AS "t2" ON "t1"."id" = "t3"."user_id""""
            )
        )
        assertEquals(
            pg(sel(List(IrSelectItem(n(1), "v")), Some(IrTable.Join(user, SqlJoinType.Cross, IrTable.Ident("role", Nil), None)))),
            lines(
                "SELECT",
                """    1 AS "v"""",
                "FROM",
                """    "user" AS "t1"""",
                """    CROSS JOIN "role" AS "t2""""
            )
        )

    test("function tables render arguments lateral and ordinality"):
        assertEquals(
            pg(sel(List(IrSelectItem(n(1), "v")), Some(IrTable.Func(false, "GENERATE_SERIES", List(n(1), n(3)), false, Nil)))),
            lines(
                "SELECT",
                """    1 AS "v"""",
                "FROM",
                """    GENERATE_SERIES(1, 3) AS "t1""""
            )
        )
        assertEquals(
            pg(sel(List(IrSelectItem(n(1), "v")), Some(IrTable.Func(true, "f", Nil, true, List("x"))))),
            lines(
                "SELECT",
                """    1 AS "v"""",
                "FROM",
                """    LATERAL f() WITH ORDINALITY AS "t1"("x")"""
            )
        )

    test("json tables lower all column kinds"):
        assertEquals(
            pg(sel(List(IrSelectItem(col(IrTable.Ident("jt", Nil), "jv"), "jv")), Some(IrTable.Json(
                false,
                IrExpr.Native(SqlExpr.StringLiteral("[1,2]")),
                s("$[*]"),
                Some("pa"),
                NonEmptyList(
                    IrJsonColumn.Ordinality("ord"),
                    List(
                        IrJsonColumn.Column("jv", SqlType.Int, Some(s("$"))),
                        IrJsonColumn.Exists("has", SqlType.Boolean, None),
                        IrJsonColumn.Nested(s("$.arr"), Some("na"), NonEmptyList(IrJsonColumn.Column("nv", SqlType.Int, None), Nil))
                    )
                ),
                Nil
            )))),
            lines(
                "SELECT",
                """    "t2"."jv" AS "jv"""",
                "FROM",
                "    JSON_TABLE(",
                "        '[1,2]', '$[*]' AS \"pa\" COLUMNS(",
                "            \"ord\" FOR ORDINALITY,",
                "            \"jv\" INTEGER PATH '$',",
                "            \"has\" BOOLEAN EXISTS,",
                "            NESTED PATH '$.arr' AS \"na\" COLUMNS(",
                """                "nv" INTEGER""",
                "            )",
                "        )",
                """    ) AS "t1""""
            )
        )

    test("graph tables lower patterns vertices edges and quantifiers"):
        assertEquals(
            pg(sel(List(IrSelectItem(n(1), "v")), Some(IrTable.Graph(
                false,
                "g",
                None,
                NonEmptyList(
                    IrGraphPattern(Some("p1"), IrGraphPatternTerm.And(
                        IrGraphPatternTerm.Vertex(IrTable.PatternVariable("a"), Some(SqlGraphLabel.Label("person")), None),
                        IrGraphPatternTerm.Edge(SqlGraphSymbol.LeftArrow, IrTable.PatternVariable("e"), None, None, SqlGraphSymbol.RightArrow)
                    )),
                    List(
                        IrGraphPattern(None, IrGraphPatternTerm.Quantified(
                            IrGraphPatternTerm.Edge(SqlGraphSymbol.Dash, IrTable.PatternVariable("f"), Some(SqlGraphLabel.Not(SqlGraphLabel.Percent)), Some(IrExpr.Binary(col(IrTable.PatternVariable("f"), "k"), SqlBinaryOperator.Equal, n(1))), SqlGraphSymbol.Tilde),
                            IrGraphQuantifier.Between(Some(n(1)), Some(n(2)))
                        )),
                        IrGraphPattern(None, IrGraphPatternTerm.Or(
                            IrGraphPatternTerm.Alternation(
                                IrGraphPatternTerm.Vertex(IrTable.PatternVariable("x"), None, None),
                                IrGraphPatternTerm.Vertex(IrTable.PatternVariable("y"), None, None)
                            ),
                            IrGraphPatternTerm.Quantified(
                                IrGraphPatternTerm.Vertex(IrTable.PatternVariable("z"), None, None),
                                IrGraphQuantifier.Quantity(n(2))
                            )
                        ))
                    )
                ),
                Some(IrExpr.Binary(col(IrTable.PatternVariable("p1"), "k"), SqlBinaryOperator.Equal, n(1))),
                Some(SqlGraphRowsMode.Vertex("pv", List("p1"))),
                NonEmptyList(IrSelectItem(col(IrTable.PatternVariable("a"), "name"), "n"), Nil),
                Some(SqlGraphExportMode.NoSingletons),
                Nil
            )))),
            lines(
                "SELECT",
                """    1 AS "v"""",
                "FROM",
                "    GRAPH_TABLE(",
                """        "g"""",
                "        MATCH",
                """            "p1" = ("t1" IS "person") <-["t2"]->,""",
                """            -["t3" IS !(%) WHERE "f"."k" = 1]~{1,2},""",
                """            (("t4") |+| ("t5")) | ("t6"){2}""",
                "        WHERE",
                """            "p1"."k" = 1""",
                """        ONE ROW PER VERTEX ("pv") IN ("p1")""",
                "        COLUMNS(",
                """            "a"."name" AS "n"""",
                "        )",
                "        EXPORT NO SINGLETONS",
                """    ) AS "t7""""
            )
        )

    test("match_recognize on an identifier lowers every clause"):
        assertEquals(
            pg(IrQuery.Select(
                None,
                NonEmptyList(IrSelectItem(IrExpr.MatchPhase(SqlMatchPhase.Final, col(IrTable.PatternVariable("A"), "v")), "mv"), Nil),
                Some(IrTable.MatchRecognize(
                    user,
                    List(col(user, "cat")),
                    List(IrOrderingItem(col(user, "ts"), SqlOrdering.Asc, None)),
                    NonEmptyList(IrSelectItem(col(IrTable.PatternVariable("A"), "price"), "p"), Nil),
                    Some(SqlRecognizePatternRowsMode.AllRows(Some(SqlRecognizePatternEmptyMatchMode.ShowEmptyMatches))),
                    IrRowPattern(
                        Some(SqlRowPatternSkipMode.ToFirst("A")),
                        Some(SqlRowPatternStrategy.Seek),
                        IrRowPatternTerm.Then(
                            IrRowPatternTerm.Exclusion(IrRowPatternTerm.Circumflex(None), None),
                            IrRowPatternTerm.Or(
                                IrRowPatternTerm.Pattern("A", Some(IrRowPatternQuantifier.Asterisk(true))),
                                IrRowPatternTerm.Pattern("B", Some(IrRowPatternQuantifier.Between(Some(n(1)), None, false))),
                                None
                            ),
                            None
                        ),
                        List(SqlRowPatternSubsetItem("s", NonEmptyList("A", List("B")))),
                        NonEmptyList(IrRowPatternDefineItem("B", IrExpr.Binary(col(IrTable.PatternVariable("B"), "x"), SqlBinaryOperator.GreaterThan, n(1))), Nil)
                    ),
                    Nil
                )),
                None, None, None, Nil, None, None
            )),
            lines(
                "SELECT",
                """    FINAL "A"."v" AS "mv"""",
                "FROM",
                """    "user" AS "t1"""",
                "    MATCH_RECOGNIZE(",
                "        PARTITION BY",
                """            "t1"."cat"""",
                "        ORDER BY",
                """            "t1"."ts" ASC""",
                "        MEASURES",
                """            "A"."price" AS "p"""",
                "        ALL ROWS PER MATCH SHOW EMPTY MATCHES",
                """        AFTER MATCH SKIP TO FIRST "A"""",
                """        SEEK PATTERN ({-^-} ("A"*? | "B"{1,}))""",
                "        SUBSET",
                """            "s" = ("A", "B")""",
                "        DEFINE",
                """            "B" AS "B"."x" > 1""",
                """    ) AS "t2""""
            )
        )

    test("match_recognize attaches to derived table sources"):
        assertEquals(
            pg(IrQuery.Select(
                None,
                NonEmptyList(IrSelectItem(col(IrTable.PatternVariable("A"), "v"), "mv"), Nil),
                Some(IrTable.MatchRecognize(
                    IrTable.Subquery(false, sel(List(IrSelectItem(n(1), "v"))), Nil),
                    Nil,
                    Nil,
                    NonEmptyList(IrSelectItem(col(IrTable.PatternVariable("A"), "v"), "m"), Nil),
                    None,
                    IrRowPattern(
                        None,
                        None,
                        IrRowPatternTerm.Pattern("A", Some(IrRowPatternQuantifier.Plus(false))),
                        Nil,
                        NonEmptyList(IrRowPatternDefineItem("A", b(true)), Nil)
                    ),
                    Nil
                )),
                None, None, None, Nil, None, None
            )),
            lines(
                "SELECT",
                """    "A"."v" AS "mv"""",
                "FROM",
                "    (",
                "        SELECT",
                """            1 AS "v"""",
                """    ) AS "t1"""",
                "    MATCH_RECOGNIZE(",
                "        MEASURES",
                """            "A"."v" AS "m"""",
                """        PATTERN ("A"+)""",
                "        DEFINE",
                """            "A" AS TRUE""",
                """    ) AS "t2""""
            )
        )

    test("match_recognize permute and anchor patterns"):
        def mr(pattern: IrRowPatternTerm, rowsMode: Option[SqlRecognizePatternRowsMode], skip: Option[SqlRowPatternSkipMode], define: NonEmptyList[IrRowPatternDefineItem]): String =
            pg(IrQuery.Select(
                None,
                NonEmptyList(IrSelectItem(col(IrTable.PatternVariable("A"), "v"), "mv"), Nil),
                Some(IrTable.MatchRecognize(
                    user,
                    Nil,
                    Nil,
                    NonEmptyList(IrSelectItem(col(IrTable.PatternVariable("A"), "v"), "m"), Nil),
                    rowsMode,
                    IrRowPattern(skip, None, pattern, Nil, define),
                    Nil
                )),
                None, None, None, Nil, None, None
            ))
        assertEquals(
            mr(
                IrRowPatternTerm.Permute(NonEmptyList(IrRowPatternTerm.Pattern("A", None), List(IrRowPatternTerm.Pattern("B", None))), Some(IrRowPatternQuantifier.Question(false))),
                Some(SqlRecognizePatternRowsMode.OneRow),
                None,
                NonEmptyList(IrRowPatternDefineItem("A", b(true)), List(IrRowPatternDefineItem("B", b(false))))
            ),
            lines(
                "SELECT",
                """    "A"."v" AS "mv"""",
                "FROM",
                """    "user" AS "t1"""",
                "    MATCH_RECOGNIZE(",
                "        MEASURES",
                """            "A"."v" AS "m"""",
                "        ONE ROW PER MATCH",
                """        PATTERN (PERMUTE("A", "B")?)""",
                "        DEFINE",
                """            "A" AS TRUE,""",
                """            "B" AS FALSE""",
                """    ) AS "t2""""
            )
        )
        assertEquals(
            mr(IrRowPatternTerm.Dollar(None), None, Some(SqlRowPatternSkipMode.PastLastRow), NonEmptyList(IrRowPatternDefineItem("A", b(true)), Nil)),
            lines(
                "SELECT",
                """    "A"."v" AS "mv"""",
                "FROM",
                """    "user" AS "t1"""",
                "    MATCH_RECOGNIZE(",
                "        MEASURES",
                """            "A"."v" AS "m"""",
                "        AFTER MATCH SKIP PAST LAST ROW",
                "        PATTERN ($)",
                "        DEFINE",
                """            "A" AS TRUE""",
                """    ) AS "t2""""
            )
        )

    test("pattern variable tables lower to plain identifiers"):
        assertEquals(
            pg(sel(List(IrSelectItem(n(1), "v")), Some(IrTable.PatternVariable("P")))),
            lines(
                "SELECT",
                """    1 AS "v"""",
                "FROM",
                """    "P""""
            )
        )
