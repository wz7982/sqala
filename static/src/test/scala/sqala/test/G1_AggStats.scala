package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.*

/**
 * Statistical aggregates: sample standard deviation, population
 * variance, sample covariance, the regression family, and the
 * `GROUP_CONCAT` alias of `listAgg`.
 */
class G1_AggStats extends munit.FunSuite:
    test("variance and covariance aggregates print the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (a = stddevSamp(w.id), b = varPop(w.id), c = covarSamp(w.id, w.score)))),
            lines(
                "SELECT",
                """    STDDEV_SAMP("t1"."id") AS "c1",""",
                """    VAR_POP("t1"."id") AS "c2",""",
                """    COVAR_SAMP("t1"."id", "t1"."score") AS "c3"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("the regression family prints the expected sql"):
        assertEquals(
            sql(from(Wide).map(w => (
                a = regrIntercept(w.id, w.score),
                b = regrCount(w.id, w.score),
                c = regrR2(w.id, w.score),
                d = regrAvgx(w.id, w.score),
                e = regrAvgy(w.id, w.score),
                f = regrSxx(w.id, w.score),
                g = regrSyy(w.id, w.score),
                h = regrSxy(w.id, w.score)
            ))),
            lines(
                "SELECT",
                """    REGR_INTERCEPT("t1"."id", "t1"."score") AS "c1",""",
                """    REGR_COUNT("t1"."id", "t1"."score") AS "c2",""",
                """    REGR_R2("t1"."id", "t1"."score") AS "c3",""",
                """    REGR_AVGX("t1"."id", "t1"."score") AS "c4",""",
                """    REGR_AVGY("t1"."id", "t1"."score") AS "c5",""",
                """    REGR_SXX("t1"."id", "t1"."score") AS "c6",""",
                """    REGR_SYY("t1"."id", "t1"."score") AS "c7",""",
                """    REGR_SXY("t1"."id", "t1"."score") AS "c8"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )

    test("groupConcat is an alias of listAgg"):
        assertEquals(
            sql(from(Wide).map(w => groupConcat(w.name, ",", w.id.asc))),
            lines(
                "SELECT",
                """    STRING_AGG("t1"."name", ',' ORDER BY "t1"."id" ASC) AS "c1"""",
                "FROM",
                """    "wide" AS "t1""""
            )
        )
        assertEquals(
            sql(from(Wide).map(w => groupConcat(w.name, ",", w.id.asc))),
            sql(from(Wide).map(w => listAgg(w.name, ",", w.id.asc)))
        )

    test("statistical aggregates yield an optional decimal"):
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[BigDecimal]](stddevSamp(w.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[BigDecimal]](varPop(w.id)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[BigDecimal]](covarSamp(w.id, w.score)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[BigDecimal]](regrIntercept(w.id, w.score)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[BigDecimal]](regrR2(w.id, w.score)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[BigDecimal]](regrAvgx(w.id, w.score)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[BigDecimal]](regrAvgy(w.id, w.score)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[BigDecimal]](regrSxx(w.id, w.score)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[BigDecimal]](regrSyy(w.id, w.score)))"), "")
        assertEquals(compileErrors("from(Wide).map(w => expect[Option[BigDecimal]](regrSxy(w.id, w.score)))"), "")

    test("regrCount yields a non-optional long"):
        assertEquals(compileErrors("from(Wide).map(w => expect[Long](regrCount(w.id, w.score)))"), "")

    test("groupConcat yields an optional string"):
        assertEquals(compileErrors("""from(Wide).map(w => expect[Option[String]](groupConcat(w.name, ",", w.id.asc)))"""), "")

    test("statistical aggregates accept numeric operand forms"):
        assertEquals(compileErrors("from(Wide).map(w => stddevSamp(1))"), "")
        assertEquals(compileErrors("from(Wide).map(w => stddevSamp(w.id))"), "")
        assertEquals(compileErrors("from(Wide).map(w => stddevSamp(w.score))"), "")
        assertEquals(compileErrors("from(Wide).map(w => stddevSamp(w.id + 1))"), "")
        assertEquals(compileErrors("from(Wide).map(w => covarSamp(w.id, w.id + 1))"), "")
        assertEquals(compileErrors("from(Wide).map(w => regrSlope(from(Wide).map(x => max(x.id)), w.score))"), "")

    test("statistical aggregates reject a non-numeric operand"):
        assertEquals(
            messages(compileErrors("from(Wide).map(w => stddevSamp(w.name))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => varPop(w.birth))")),
            List("The arguments type should be numeric, but got java.time.LocalDate.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => covarSamp(w.name, w.id))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => covarSamp(w.id, w.name))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )

    test("the regression family checks both argument positions"):
        assertEquals(
            messages(compileErrors("from(Wide).map(w => regrIntercept(w.name, w.id))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => regrCount(w.id, w.name))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => regrR2(w.flag, w.id))")),
            List("The arguments type should be numeric, but got scala.Boolean.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => regrAvgx(w.id, w.flag))")),
            List("The arguments type should be numeric, but got scala.Boolean.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => regrAvgy(w.name, w.id))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => regrSxx(w.id, w.name))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => regrSyy(w.name, w.id))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )
        assertEquals(
            messages(compileErrors("from(Wide).map(w => regrSxy(w.id, w.name))")),
            List("The arguments type should be numeric, but got java.lang.String.")
        )

    test("groupConcat rejects non-string arguments"):
        assertEquals(
            messages(compileErrors("""from(Wide).map(w => groupConcat(w.id, ",", w.id.asc))""")),
            List("The arguments type should be string, but got scala.Int.")
        )
        assert(compileErrors("""from(Wide).map(w => groupConcat(w.name, 1, w.id.asc))""").nonEmpty)

    test("statistical aggregates work as window functions and in having"):
        assertEquals(compileErrors("from(Wide).map(w => stddevSamp(w.id).over(partitionBy(w.id)))"), "")
        assertEquals(compileErrors("from(Wide).groupBy(w => w.id).having((g, w) => varPop(w.id) > 1).map((g, w) => count())"), "")
        assertEquals(compileErrors("from(Wide).map(w => regrCount(w.id, w.score).filter(w.id > 0))"), "")