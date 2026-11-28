package sqala.test

import sqala.ast.expr.*
import sqala.ast.window.*
import sqala.test.Coverage.assertCoverage

class L1_Printer_Window extends munit.FunSuite:
    import QueryFixture.*

    val bounds: Map[String, (SqlWindowFrameBound, String)] = Map(
        "CurrentRow" -> (SqlWindowFrameBound.CurrentRow, "CURRENT ROW"),
        "UnboundedPreceding" -> (SqlWindowFrameBound.UnboundedPreceding, "UNBOUNDED PRECEDING"),
        "UnboundedFollowing" -> (SqlWindowFrameBound.UnboundedFollowing, "UNBOUNDED FOLLOWING"),
        "Preceding" -> (SqlWindowFrameBound.Preceding(num(3)), "3 PRECEDING"),
        "Following" -> (SqlWindowFrameBound.Following(num(3)), "3 FOLLOWING")
    )

    val units: Map[String, (SqlWindowFrameUnit, String)] = Map(
        "Rows" -> (SqlWindowFrameUnit.Rows, "ROWS"),
        "Range" -> (SqlWindowFrameUnit.Range, "RANGE"),
        "Groups" -> (SqlWindowFrameUnit.Groups, "GROUPS")
    )

    val excludeModes: Map[String, (SqlWindowFrameExcludeMode, String)] = Map(
        "CurrentRow" -> (SqlWindowFrameExcludeMode.CurrentRow, "CURRENT ROW"),
        "Group" -> (SqlWindowFrameExcludeMode.Group, "GROUP"),
        "Ties" -> (SqlWindowFrameExcludeMode.Ties, "TIES"),
        "NoOthers" -> (SqlWindowFrameExcludeMode.NoOthers, "NO OTHERS")
    )

    def inlined(
        name: Option[String] = None,
        partitionBy: List[SqlExpr] = Nil,
        orderBy: List[sqala.ast.order.SqlOrderingItem] = Nil,
        frame: Option[SqlWindowFrame] = None
    ): SqlWindow.Inlined = SqlWindow.Inlined(name, partitionBy, orderBy, frame)

    test("window frame bound tokens"):
        assertCoverage[SqlWindowFrameBound](
            "CurrentRow", "UnboundedPreceding", "Preceding", "UnboundedFollowing", "Following"
        )
        assertEquals(bounds.keySet, Coverage.casesOf[SqlWindowFrameBound])
        for (name, (b, expected)) <- bounds.toList.sortBy(_._1) do
            assertEquals(createSql(_.printWindowFrameBound(b)), expected, name)

    test("window frame unit tokens"):
        assertCoverage[SqlWindowFrameUnit]("Rows", "Range", "Groups")
        assertEquals(units.keySet, Coverage.casesOf[SqlWindowFrameUnit])
        for (name, (u, expected)) <- units.toList.sortBy(_._1) do
            assertEquals(createSql(_.printWindowFrameUnit(u)), expected, name)

    test("window frame exclude mode tokens"):
        assertCoverage[SqlWindowFrameExcludeMode]("CurrentRow", "Group", "Ties", "NoOthers")
        assertEquals(excludeModes.keySet, Coverage.casesOf[SqlWindowFrameExcludeMode])
        for (name, (m, expected)) <- excludeModes.toList.sortBy(_._1) do
            assertEquals(createSql(_.printWindowFrameExcludeMode(m)), expected, name)

    test("window frame start and between"):
        assertCoverage[SqlWindowFrame]("Start", "Between")

        val start = SqlWindowFrame.Start(Nil, SqlWindowFrameUnit.Rows, SqlWindowFrameBound.UnboundedPreceding, None, None)
        assertEquals(createSql(_.printWindowFrame(start)), "ROWS UNBOUNDED PRECEDING")

        val between = SqlWindowFrame.Between(
            Nil,
            SqlWindowFrameUnit.Range,
            SqlWindowFrameBound.Preceding(num(1)),
            SqlWindowFrameBound.Following(num(2)),
            Some(SqlWindowFrameExcludeMode.Ties),
            None
        )
        assertEquals(
            createSql(_.printWindowFrame(between)),
            "RANGE BETWEEN 1 PRECEDING AND 2 FOLLOWING EXCLUDE TIES"
        )

    test("named window"):
        assertCoverage[SqlWindow]("Named", "Inlined")
        assertEquals(createSql(_.printWindow(SqlWindow.Named("w"))), """"w"""")

    test("inlined window clause combinations"):
        assertEquals(createSql(_.printWindow(inlined())), "()")
        assertEquals(createSql(_.printWindow(inlined(name = Some("w")))), """("w")""")
        assertEquals(
            createSql(_.printWindow(inlined(partitionBy = List(col("a"), col("b"))))),
            """(PARTITION BY "a", "b")"""
        )
        assertEquals(
            createSql(_.printWindow(inlined(orderBy = List(ordDesc(col("a")))))),
            """(ORDER BY "a" DESC)"""
        )
        assertEquals(
            createSql(_.printWindow(inlined(
                frame = Some(SqlWindowFrame.Start(Nil, SqlWindowFrameUnit.Rows, SqlWindowFrameBound.CurrentRow, None, None))
            ))),
            "(ROWS CURRENT ROW)"
        )
        assertEquals(
            createSql(_.printWindow(inlined(
                name = Some("w"),
                partitionBy = List(col("a")),
                orderBy = List(ord(col("b"))),
                frame = Some(SqlWindowFrame.Start(Nil, SqlWindowFrameUnit.Groups, SqlWindowFrameBound.CurrentRow, None, None))
            ))),
            """("w" PARTITION BY "a" ORDER BY "b" ASC GROUPS CURRENT ROW)"""
        )

    test("window item"):
        assertEquals(
            createSql(_.printWindowItem(SqlWindowItem("w", inlined(partitionBy = List(col("a")))))),
            """"w" AS (PARTITION BY "a")"""
        )

    test("window expression"):
        assertEquals(
            createSql(_.printExpr(SqlExpr.Window(
                SqlExpr.GeneralFunc(None, "SUM", List(col("a")), Nil, Nil, None),
                SqlWindow.Named("w")
            ))),
            """SUM("a") OVER "w""""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.Window(
                SqlExpr.GeneralFunc(None, "ROW_NUMBER", Nil, Nil, Nil, None),
                inlined(orderBy = List(ord(col("a"))))
            ))),
            """ROW_NUMBER() OVER (ORDER BY "a" ASC)"""
        )