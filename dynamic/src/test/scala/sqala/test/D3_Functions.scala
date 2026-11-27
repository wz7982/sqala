package sqala.test

import sqala.dynamic.dsl.*
import sqala.test.DynamicFixture.*

/**
 * The aggregate, string, numeric, and current-time function library.
 */
class D3_Functions extends munit.FunSuite:
    test("count family and basic aggregates render their forms"):
        assertEquals(
            pg(select(count().as("c"), count(column("id")).as("c2"), countDistinct(column("id")).as("c3"), sum(column("n")).as("s"), avg(column("n")).as("a"), max(column("n")).as("mx"), min(column("n")).as("mn"), anyValue(column("n")).as("av")).from(table("t"))),
            lines(
                "SELECT",
                """    COUNT(*) AS "c",""",
                """    COUNT("id") AS "c2",""",
                """    COUNT(DISTINCT "id") AS "c3",""",
                """    SUM("n") AS "s",""",
                """    AVG("n") AS "a",""",
                """    MAX("n") AS "mx",""",
                """    MIN("n") AS "mn",""",
                """    ANY_VALUE("n") AS "av"""",
                "FROM",
                """    "t""""
            )
        )

    test("statistical aggregates render the standard function names"):
        assertEquals(
            pg(select(stddevPop(column("n")).as("a"), stddevSamp(column("n")).as("b"), varPop(column("n")).as("c"), varSamp(column("n")).as("d"), covarPop(column("x"), column("y")).as("e"), covarSamp(column("x"), column("y")).as("f"), corr(column("x"), column("y")).as("g")).from(table("t"))),
            lines(
                "SELECT",
                """    STDDEV_POP("n") AS "a",""",
                """    STDDEV_SAMP("n") AS "b",""",
                """    VAR_POP("n") AS "c",""",
                """    VAR_SAMP("n") AS "d",""",
                """    COVAR_POP("x", "y") AS "e",""",
                """    COVAR_SAMP("x", "y") AS "f",""",
                """    CORR("x", "y") AS "g"""",
                "FROM",
                """    "t""""
            )
        )

    test("regression aggregates render the regr prefix family"):
        assertEquals(
            pg(select(regrSlope(column("x"), column("y")).as("a"), regrIntercept(column("x"), column("y")).as("b"), regrR2(column("x"), column("y")).as("c"), regrCount(column("x"), column("y")).as("d"), regrAvgx(column("x"), column("y")).as("e"), regrAvgy(column("x"), column("y")).as("f"), regrSxx(column("x"), column("y")).as("g"), regrSyy(column("x"), column("y")).as("h"), regrSxy(column("x"), column("y")).as("i")).from(table("t"))),
            lines(
                "SELECT",
                """    REGR_SLOPE("x", "y") AS "a",""",
                """    REGR_INTERCEPT("x", "y") AS "b",""",
                """    REGR_R2("x", "y") AS "c",""",
                """    REGR_COUNT("x", "y") AS "d",""",
                """    REGR_AVGX("x", "y") AS "e",""",
                """    REGR_AVGY("x", "y") AS "f",""",
                """    REGR_SXX("x", "y") AS "g",""",
                """    REGR_SYY("x", "y") AS "h",""",
                """    REGR_SXY("x", "y") AS "i"""",
                "FROM",
                """    "t""""
            )
        )

    test("percentile and ordered string aggregation render within group"):
        assertEquals(
            pg(select(percentileCont(value(0.5), column("n").asc).as("p1"), percentileDisc(value(0.5), column("n").asc).as("p2")).from(table("t"))),
            lines(
                "SELECT",
                """    PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "n" ASC) AS "p1",""",
                """    PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY "n" ASC) AS "p2"""",
                "FROM",
                """    "t""""
            )
        )
        assertEquals(
            pg(select(listAgg(column("s"), value(","), column("s").asc).as("a")).from(table("t"))),
            lines(
                "SELECT",
                """    STRING_AGG("s", ',' ORDER BY "s" ASC) AS "a"""",
                "FROM",
                """    "t""""
            )
        )

    test("stringAgg and groupConcat are aliases of listAgg"):
        assertEquals(
            pg(select(stringAgg(column("s"), value(","), column("s").asc).as("x")).from(table("t"))),
            pg(select(listAgg(column("s"), value(","), column("s").asc).as("x")).from(table("t")))
        )
        assertEquals(
            pg(select(groupConcat(column("s"), value(","), column("s").asc).as("x")).from(table("t"))),
            pg(select(listAgg(column("s"), value(","), column("s").asc).as("x")).from(table("t")))
        )

    test("substring renders the from form and the optional length part"):
        assertEquals(
            pg(select(substring(column("s"), value(2)).as("a"), substring(column("s"), value(2), value(3)).as("b")).from(table("t"))),
            lines(
                "SELECT",
                """    SUBSTRING("s" FROM 2) AS "a",""",
                """    SUBSTRING("s" FROM 2 FOR 3) AS "b"""",
                "FROM",
                """    "t""""
            )
        )

    test("padding and trimming functions render with and without fill arguments"):
        assertEquals(
            pg(select(lpad(column("s"), value(5)).as("a"), lpad(column("s"), value(5), value("0")).as("b"), btrim(column("s")).as("c"), btrim(column("s"), value("x")).as("d"), ltrim(column("s"), value("x")).as("e"), rtrim(column("s")).as("f")).from(table("t"))),
            lines(
                "SELECT",
                """    LPAD("s", 5) AS "a",""",
                """    LPAD("s", 5, '0') AS "b",""",
                """    BTRIM("s") AS "c",""",
                """    BTRIM("s", 'x') AS "d",""",
                """    LTRIM("s", 'x') AS "e",""",
                """    RTRIM("s") AS "f"""",
                "FROM",
                """    "t""""
            )
        )

    test("overlay and position render their keyword forms"):
        assertEquals(
            pg(select(overlay(column("s"), value("y"), value(2)).as("a"), overlay(column("s"), value("y"), value(2), value(3)).as("b"), position(column("a"), column("s")).as("c")).from(table("t"))),
            lines(
                "SELECT",
                """    OVERLAY("s" PLACING 'y' FROM 2) AS "a",""",
                """    OVERLAY("s" PLACING 'y' FROM 2 FOR 3) AS "b",""",
                """    POSITION("a" IN "s") AS "c"""",
                "FROM",
                """    "t""""
            )
        )

    test("pattern matching and length functions render as calls"):
        assertEquals(
            pg(select(regexpLike(column("s"), value("^a")).as("a"), charLength(column("s")).as("b"), octetLength(column("s")).as("c"), upper(column("s")).as("d"), lower(column("s")).as("e")).from(table("t"))),
            lines(
                "SELECT",
                """    REGEXP_LIKE("s", '^a') AS "a",""",
                """    CHAR_LENGTH("s") AS "b",""",
                """    OCTET_LENGTH("s") AS "c",""",
                """    UPPER("s") AS "d",""",
                """    LOWER("s") AS "e"""",
                "FROM",
                """    "t""""
            )
        )

    test("trigonometric and exponential functions render as calls"):
        assertEquals(
            pg(select(sin(column("n")).as("a"), cos(column("n")).as("b"), tan(column("n")).as("c"), asin(column("n")).as("d"), sinh(column("n")).as("e"), exp(column("n")).as("f"), ln(column("n")).as("g"), log10(column("n")).as("h")).from(table("t"))),
            lines(
                "SELECT",
                """    SIN("n") AS "a",""",
                """    COS("n") AS "b",""",
                """    TAN("n") AS "c",""",
                """    ASIN("n") AS "d",""",
                """    SINH("n") AS "e",""",
                """    EXP("n") AS "f",""",
                """    LN("n") AS "g",""",
                """    LOG10("n") AS "h"""",
                "FROM",
                """    "t""""
            )
        )

    test("two-argument numeric functions render both operands"):
        assertEquals(
            pg(select(mod(column("n"), value(2)).as("a"), log(column("n"), value(10)).as("b"), power(column("n"), value(2)).as("c"), round(column("n"), value(2)).as("d"), widthBucket(column("n"), value(0), value(100), value(4)).as("e")).from(table("t"))),
            lines(
                "SELECT",
                """    MOD("n", 2) AS "a",""",
                """    LOG("n", 10) AS "b",""",
                """    POWER("n", 2) AS "c",""",
                """    ROUND("n", 2) AS "d",""",
                """    WIDTH_BUCKET("n", 0, 100, 4) AS "e"""",
                "FROM",
                """    "t""""
            )
        )

    test("single-argument numeric functions render as calls"):
        assertEquals(
            pg(select(abs(column("n")).as("a"), sqrt(column("n")).as("b"), floor(column("n")).as("c"), ceil(column("n")).as("d")).from(table("t"))),
            lines(
                "SELECT",
                """    ABS("n") AS "a",""",
                """    SQRT("n") AS "b",""",
                """    FLOOR("n") AS "c",""",
                """    CEIL("n") AS "d"""",
                "FROM",
                """    "t""""
            )
        )

    test("the current date and time constants render without arguments"):
        assertEquals(
            pg(select(currentDate.as("d"), currentTime.as("t"), currentTimestamp.as("ts"), localTime.as("lt"), localTimestamp.as("lts"))),
            lines(
                "SELECT",
                """    CURRENT_DATE AS "d",""",
                """    CURRENT_TIME AS "t",""",
                """    CURRENT_TIMESTAMP AS "ts",""",
                """    LOCALTIME AS "lt",""",
                """    LOCALTIMESTAMP AS "lts""""
            )
        )
