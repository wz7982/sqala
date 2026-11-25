package sqala.test

import sqala.ast.expr.*
import sqala.ast.order.{SqlOrdering, SqlOrderingItem}
import sqala.ast.quantifier.SqlQuantifier
import sqala.util.NonEmptyList

object FuncFixture:
    def col(n: String): SqlExpr = SqlExpr.Column(None, n)
    def num(n: Int): SqlExpr = SqlExpr.NumberLiteral(n)
    def str(s: String): SqlExpr = SqlExpr.StringLiteral(s)
    def nel[A](h: A, t: A*): NonEmptyList[A] = NonEmptyList(h, t.toList)
    def ord(e: SqlExpr): SqlOrderingItem = SqlOrderingItem(e, None, None)
    def ordAsc(e: SqlExpr): SqlOrderingItem = SqlOrderingItem(e, Some(SqlOrdering.Asc), None)
    def gt(e: SqlExpr, n: Int): SqlExpr = SqlExpr.Binary(e, SqlBinaryOperator.GreaterThan, num(n))

class L1_Printer_Func extends munit.FunSuite:
    import FuncFixture.*

    test("grouping"):
        assertEquals(createSql(_.printExpr(SqlExpr.Grouping(nel(col("a"))))), """GROUPING("a")""")
        assertEquals(
            createSql(_.printExpr(SqlExpr.Grouping(nel(col("a"), col("b"))))),
            """GROUPING("a", "b")"""
        )

    test("ident func is emitted without quoting"):
        assertEquals(createSql(_.printExpr(SqlExpr.IdentFunc("CURRENT_DATE"))), "CURRENT_DATE")

    test("substring"):
        assertEquals(
            createSql(_.printExpr(SqlExpr.SubstringFunc(col("a"), num(1), None))),
            """SUBSTRING("a" FROM 1)"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.SubstringFunc(col("a"), num(1), Some(num(3))))),
            """SUBSTRING("a" FROM 1 FOR 3)"""
        )

    test("trim combinations"):
        assertEquals(createSql(_.printExpr(SqlExpr.TrimFunc(None, col("a")))), """TRIM("a")""")
        assertEquals(
            createSql(_.printExpr(SqlExpr.TrimFunc(Some(SqlTrim(Some(SqlTrimMode.Leading), None)), col("a")))),
            """TRIM(LEADING FROM "a")"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.TrimFunc(Some(SqlTrim(None, Some(str("x")))), col("a")))),
            """TRIM('x' FROM "a")"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.TrimFunc(Some(SqlTrim(Some(SqlTrimMode.Both), Some(str("x")))), col("a")))),
            """TRIM(BOTH 'x' FROM "a")"""
        )

    test("overlay"):
        assertEquals(
            createSql(_.printExpr(SqlExpr.OverlayFunc(col("a"), str("x"), num(2), None))),
            """OVERLAY("a" PLACING 'x' FROM 2)"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.OverlayFunc(col("a"), str("x"), num(2), Some(num(3))))),
            """OVERLAY("a" PLACING 'x' FROM 2 FOR 3)"""
        )

    test("position and extract"):
        assertEquals(
            createSql(_.printExpr(SqlExpr.PositionFunc(str("x"), col("a")))),
            """POSITION('x' IN "a")"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.ExtractFunc(SqlTimeUnit.Year, col("a")))),
            """EXTRACT(YEAR FROM "a")"""
        )

    test("count asterisk"):
        assertEquals(createSql(_.printExpr(SqlExpr.CountAsteriskFunc(None, None))), "COUNT(*)")
        assertEquals(createSql(_.printExpr(SqlExpr.CountAsteriskFunc(Some("t"), None))), """COUNT("t".*)""")
        assertEquals(
            createSql(_.printExpr(SqlExpr.CountAsteriskFunc(None, Some(gt(col("a"), 0))))),
            """COUNT(*) FILTER (WHERE "a" > 0)"""
        )

    test("listagg combinations"):
        val base: SqlExpr.ListAggFunc = SqlExpr.ListAggFunc(None, col("a"), str(","), None, Nil, None)
        assertEquals(createSql(_.printExpr(base)), """LISTAGG("a", ',')""")

        assertEquals(
            createSql(_.printExpr(base.copy(quantifier = Some(SqlQuantifier.Distinct)))),
            """LISTAGG(DISTINCT "a", ',')"""
        )
        assertEquals(
            createSql(_.printExpr(base.copy(onOverflow = Some(SqlListAggOnOverflow.Error)))),
            """LISTAGG("a", ',' ON OVERFLOW ERROR)"""
        )
        assertEquals(
            createSql(_.printExpr(base.copy(
                onOverflow = Some(SqlListAggOnOverflow.Truncate(str("..."), SqlListAggCountMode.With))
            ))),
            """LISTAGG("a", ',' ON OVERFLOW TRUNCATE '...' WITH COUNT)"""
        )
        assertEquals(
            createSql(_.printExpr(base.copy(withinGroup = List(ordAsc(col("b")))))),
            """LISTAGG("a", ',') WITHIN GROUP (ORDER BY "b" ASC)"""
        )
        assertEquals(
            createSql(_.printExpr(base.copy(filter = Some(gt(col("b"), 0))))),
            """LISTAGG("a", ',') FILTER (WHERE "b" > 0)"""
        )
        assertEquals(
            createSql(_.printExpr(base.copy(
                quantifier = Some(SqlQuantifier.All),
                onOverflow = Some(SqlListAggOnOverflow.Truncate(str("..."), SqlListAggCountMode.Without)),
                withinGroup = List(ordAsc(col("b"))),
                filter = Some(gt(col("b"), 0))
            ))),
            """LISTAGG(ALL "a", ',' ON OVERFLOW TRUNCATE '...' WITHOUT COUNT) WITHIN GROUP (ORDER BY "b" ASC) FILTER (WHERE "b" > 0)"""
        )

    test("nulls treatment function"):
        assertEquals(
            createSql(_.printExpr(SqlExpr.NullsTreatmentFunc("LAG", List(col("a")), None))),
            """LAG("a")"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.NullsTreatmentFunc("LEAD", List(col("a"), num(1), num(0)), Some(SqlWindowNullsMode.Ignore)))),
            """LEAD("a", 1, 0) IGNORE NULLS"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.NullsTreatmentFunc("FIRST_VALUE", List(col("a")), Some(SqlWindowNullsMode.Respect)))),
            """FIRST_VALUE("a") RESPECT NULLS"""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.NullsTreatmentFunc("LAST_VALUE", Nil, None))),
            "LAST_VALUE()"
        )

    test("nth_value combinations"):
        val base: SqlExpr.NthValueFunc = SqlExpr.NthValueFunc(col("a"), num(2), None, None)
        assertEquals(createSql(_.printExpr(base)), """NTH_VALUE("a", 2)""")
        assertEquals(
            createSql(_.printExpr(base.copy(fromMode = Some(SqlNthValueFromMode.First)))),
            """NTH_VALUE("a", 2) FROM FIRST"""
        )
        assertEquals(
            createSql(_.printExpr(base.copy(
                fromMode = Some(SqlNthValueFromMode.Last),
                nullsMode = Some(SqlWindowNullsMode.Ignore)
            ))),
            """NTH_VALUE("a", 2) FROM LAST IGNORE NULLS"""
        )

    test("general function combinations"):
        val base: SqlExpr.GeneralFunc = SqlExpr.GeneralFunc(None, "SUM", List(col("a")), Nil, Nil, None)
        assertEquals(createSql(_.printExpr(base)), """SUM("a")""")
        assertEquals(createSql(_.printExpr(base.copy(args = Nil))), "SUM()")
        assertEquals(
            createSql(_.printExpr(base.copy(quantifier = Some(SqlQuantifier.Distinct)))),
            """SUM(DISTINCT "a")"""
        )
        assertEquals(
            createSql(_.printExpr(base.copy(orderBy = List(ordAsc(col("b")))))),
            """SUM("a" ORDER BY "b" ASC)"""
        )
        assertEquals(
            createSql(_.printExpr(base.copy(withinGroup = List(ordAsc(col("b")))))),
            """SUM("a") WITHIN GROUP (ORDER BY "b" ASC)"""
        )
        assertEquals(
            createSql(_.printExpr(base.copy(filter = Some(gt(col("b"), 0))))),
            """SUM("a") FILTER (WHERE "b" > 0)"""
        )
        assertEquals(
            createSql(_.printExpr(base.copy(
                quantifier = Some(SqlQuantifier.All),
                args = List(col("a"), num(1)),
                orderBy = List(ordAsc(col("b"))),
                withinGroup = List(ord(col("c"))),
                filter = Some(gt(col("b"), 0))
            ))),
            """SUM(ALL "a", 1 ORDER BY "b" ASC) WITHIN GROUP (ORDER BY "c" ASC) FILTER (WHERE "b" > 0)"""
        )

    test("match phase"):
        assertEquals(
            createSql(_.printExpr(SqlExpr.MatchPhase(SqlMatchPhase.Final, col("a")))),
            """FINAL "a""""
        )
        assertEquals(
            createSql(_.printExpr(SqlExpr.MatchPhase(SqlMatchPhase.Running, col("a")))),
            """RUNNING "a""""
        )