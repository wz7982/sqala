package sqala.test

import sqala.parser.SqlParser

/**
 * A standard SQL corpus. Each entry is parsed, printed, and parsed again;
 * the two ASTs must be identical.
 */
object SqlCorpus:
    val queries: List[(String, String)] = List(
        "select-minimal" -> """SELECT "a" FROM "t"""",
        "select-distinct-alias" -> """SELECT DISTINCT "a", "b" AS "x" FROM "t"""",
        "select-asterisk" -> """SELECT * FROM "t"""",
        "select-qualified-asterisk" -> """SELECT "t".* FROM "t"""",
        "select-no-from" -> """SELECT 1""",
        "where-and-or" -> """SELECT "a" FROM "t" WHERE "a" > 1 AND "b" = 'x' OR "c" < 2""",
        "where-comparison-ops" -> """SELECT "a" FROM "t" WHERE "a" = 1 AND "b" <> 2 AND "c" >= 3 AND "d" <= 4""",
        "between" -> """SELECT "a" FROM "t" WHERE "a" BETWEEN 1 AND 10""",
        "not-between" -> """SELECT "a" FROM "t" WHERE "a" NOT BETWEEN 1 AND 10""",
        "in-values" -> """SELECT "a" FROM "t" WHERE "a" IN (1, 2, 3)""",
        "not-in-subquery" -> """SELECT "a" FROM "t" WHERE "a" NOT IN (SELECT "b" FROM "u")""",
        "like" -> """SELECT "a" FROM "t" WHERE "a" LIKE '%x%'""",
        "not-like-escape" -> """SELECT "a" FROM "t" WHERE "a" NOT LIKE '%x%' ESCAPE '#'""",
        "similar-to" -> """SELECT "a" FROM "t" WHERE "a" SIMILAR TO '%x%'""",
        "exists" -> """SELECT "a" FROM "t" WHERE EXISTS (SELECT 1 FROM "u")""",
        "quantified-all" -> """SELECT "a" FROM "t" WHERE "a" > ALL (SELECT "b" FROM "u")""",
        "quantified-any" -> """SELECT "a" FROM "t" WHERE "a" = ANY (SELECT "b" FROM "u")""",
        "is-null" -> """SELECT "a" FROM "t" WHERE "a" IS NULL""",
        "is-not-null" -> """SELECT "a" FROM "t" WHERE "a" IS NOT NULL""",
        "is-distinct-from" -> """SELECT "a" FROM "t" WHERE "a" IS DISTINCT FROM "b"""",
        "is-not-distinct-from" -> """SELECT "a" FROM "t" WHERE "a" IS NOT DISTINCT FROM "b"""",
        "is-true" -> """SELECT "a" FROM "t" WHERE "a" IS TRUE""",
        "not" -> """SELECT "a" FROM "t" WHERE NOT ("a" = 1)""",
        "searched-case" -> """SELECT CASE WHEN "a" = 1 THEN 'x' ELSE 'y' END FROM "t"""",
        "simple-case" -> """SELECT CASE "a" WHEN 1 THEN 'x' END FROM "t"""",
        "coalesce-nullif" -> """SELECT COALESCE("a", "b", 0), NULLIF("a", 0) FROM "t"""",
        "cast" -> """SELECT CAST("a" AS INTEGER), CAST("b" AS VARCHAR(10)), CAST("c" AS DECIMAL(10, 2)) FROM "t"""",
        "cast-temporal" -> """SELECT CAST("a" AS DATE), CAST("b" AS TIMESTAMP WITH TIME ZONE), CAST("c" AS BOOLEAN) FROM "t"""",
        "aggregates" -> """SELECT COUNT(*), COUNT(DISTINCT "a"), SUM("a"), AVG("a"), MIN("a"), MAX("a") FROM "t"""",
        "group-having" -> """SELECT "a" FROM "t" GROUP BY "a", "b" HAVING COUNT(*) > 1""",
        "grouping-sets" -> """SELECT "a" FROM "t" GROUP BY CUBE("a", "b"), ROLLUP("a"), GROUPING SETS(("a"), ())""",
        "grouping-func" -> """SELECT GROUPING("a", "b") FROM "t" GROUP BY ROLLUP("a", "b")""",
        "order-by" -> """SELECT "a" FROM "t" ORDER BY "a" ASC, "b" DESC NULLS LAST""",
        "offset-fetch" -> """SELECT "a" FROM "t" OFFSET 5 ROWS FETCH NEXT 10 ROWS ONLY""",
        "fetch-percent-ties" -> """SELECT "a" FROM "t" FETCH NEXT 10 PERCENT ROWS WITH TIES""",
        "for-update" -> """SELECT "a" FROM "t" FOR UPDATE""",
        "for-share-nowait" -> """SELECT "a" FROM "t" FOR SHARE NOWAIT""",
        "inner-join" -> """SELECT "a" FROM "t" INNER JOIN "u" ON "t"."a" = "u"."a"""",
        "left-join-using" -> """SELECT "a" FROM "t" LEFT OUTER JOIN "u" USING ("a")""",
        "cross-join" -> """SELECT "a" FROM "t" CROSS JOIN "u"""",
        "subquery-table" -> """SELECT "a" FROM (SELECT "b" FROM "u") AS "x"""",
        "table-alias-columns" -> """SELECT "a" FROM "t" AS "x"("p", "q")""",
        "union" -> """SELECT "a" FROM "t" UNION SELECT "b" FROM "u"""",
        "union-all" -> """SELECT "a" FROM "t" UNION ALL SELECT "b" FROM "u"""",
        "intersect" -> """SELECT "a" FROM "t" INTERSECT SELECT "b" FROM "u"""",
        "except-all" -> """SELECT "a" FROM "t" EXCEPT ALL SELECT "b" FROM "u"""",
        "set-order-limit" -> """SELECT "a" FROM "t" UNION SELECT "b" FROM "u" ORDER BY "a" ASC OFFSET 1 ROWS""",
        "with" -> """WITH "c" AS (SELECT "a" FROM "t") SELECT "a" FROM "c"""",
        "with-recursive" -> """WITH RECURSIVE "c"("a") AS (SELECT "a" FROM "t") SELECT "a" FROM "c"""",
        "values-query" -> """VALUES (1, 2), (3, 4)""",
        "string-funcs" -> """SELECT SUBSTRING("a" FROM 1 FOR 3), TRIM(BOTH 'x' FROM "a"), OVERLAY("a" PLACING 'x' FROM 2), POSITION('x' IN "a") FROM "t"""",
        "trim-variants" -> """SELECT TRIM("a"), TRIM(LEADING FROM "a"), TRIM('x' FROM "a") FROM "t"""",
        "extract" -> """SELECT EXTRACT(YEAR FROM "a"), EXTRACT(SECOND FROM "b") FROM "t"""",
        "temporal-literals" -> """SELECT DATE '2020-01-01', TIMESTAMP '2020-01-01 00:00:00', TIME '12:00:00', INTERVAL '1' DAY""",
        "interval-range" -> """SELECT INTERVAL '1-2' YEAR TO MONTH""",
        "window-frame" -> """SELECT SUM("a") OVER (PARTITION BY "b" ORDER BY "c" ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) FROM "t"""",
        "window-named" -> """SELECT ROW_NUMBER() OVER "w" FROM "t" WINDOW "w" AS (ORDER BY "a" ASC)""",
        "window-exclude" -> """SELECT SUM("a") OVER (ORDER BY "b" ASC RANGE BETWEEN 1 PRECEDING AND 2 FOLLOWING EXCLUDE TIES) FROM "t"""",
        "listagg" -> """SELECT LISTAGG("a", ',') WITHIN GROUP (ORDER BY "a" ASC) FROM "t"""",
        "listagg-overflow" -> """SELECT LISTAGG(DISTINCT "a", ',' ON OVERFLOW TRUNCATE '...' WITH COUNT) WITHIN GROUP (ORDER BY "a" ASC) FROM "t"""",
        "json-paths" -> """SELECT JSON_VALUE("a", '$.b'), JSON_QUERY("a", '$.b'), JSON_EXISTS("a", '$.b') FROM "t"""",
        "json-constructors" -> """SELECT JSON_OBJECT('k' VALUE "a"), JSON_ARRAY("a", "b") FROM "t"""",
        "json-behaviors" -> """SELECT JSON_VALUE("a", '$.b' RETURNING VARCHAR(10) DEFAULT 0 ON EMPTY NULL ON ERROR) FROM "t"""",
        "json-passing" -> """SELECT JSON_QUERY("a", '$.b' PASSING "c" AS "v" WITH CONDITIONAL ARRAY WRAPPER OMIT QUOTES) FROM "t"""",
        "json-test" -> """SELECT "a" FROM "t" WHERE "a" IS JSON OBJECT WITH UNIQUE KEYS""",
        "json-serialize-parse" -> """SELECT JSON_SERIALIZE("a"), JSON('{}' WITH UNIQUE KEYS) FROM "t"""",
        "json-aggregates" -> """SELECT JSON_OBJECTAGG('k' VALUE "a"), JSON_ARRAYAGG("a") FROM "t"""",
        "arithmetic-precedence" -> """SELECT "a" + "b" * "c", ("a" + "b") * "c", "a" - ("b" - "c") FROM "t"""",
        "unary" -> """SELECT -"a", +"a" FROM "t"""",
        "concat" -> """SELECT "a" || "b" FROM "t"""",
        "array-expr" -> """SELECT ARRAY[1, 2]""",
        "numeric-literals" -> """SELECT 1, -2, 3.5, 12345678901234567890""",
        "string-escape" -> """SELECT 'a''b'""",
        "null-literal" -> """SELECT NULL""",
        "boolean-literals" -> """SELECT TRUE, FALSE""",
        "nested-subquery-expr" -> """SELECT (SELECT MAX("b") FROM "u") FROM "t"""",
        "json-table" -> """SELECT "x" FROM JSON_TABLE("a", '$.b' COLUMNS("n" FOR ORDINALITY, "c" INTEGER PATH '$.c')) AS "j"""",
        "func-table" -> """SELECT "a" FROM generate_series(1, 3) WITH ORDINALITY AS "x"("a")""",
        "tablesample" -> """SELECT "a" FROM "t" TABLESAMPLE BERNOULLI (10) REPEATABLE (1)""",
        "match-recognize" -> """SELECT "m" FROM "t" MATCH_RECOGNIZE(MEASURES "a" AS "m" ONE ROW PER MATCH PATTERN ("A"+) DEFINE "A" AS "a" > 0) AS "x"""",
        "count-asterisk-filter" -> """SELECT COUNT(*) FILTER (WHERE "a" > 0) FROM "t"""",
        "count-qualified-asterisk-filter" -> """SELECT COUNT("t".*) FILTER (WHERE "a" > 0) FROM "t"""",
        "count-column-filter" -> """SELECT COUNT("a") FILTER (WHERE "a" > 0) FROM "t"""",
        "count-distinct-filter" -> """SELECT COUNT(DISTINCT "a") FILTER (WHERE "a" > 0) FROM "t"""",
        "sum-filter" -> """SELECT SUM("a") FILTER (WHERE "a" > 0) FROM "t"""",
        "listagg-within-group-filter" -> """SELECT LISTAGG("a", ',') WITHIN GROUP (ORDER BY "a" ASC) FILTER (WHERE "a" > 0) FROM "t"""",
        "lag-ignore-nulls" -> """SELECT LAG("a") IGNORE NULLS OVER (ORDER BY "b" ASC) FROM "t"""",
        "lag-respect-nulls" -> """SELECT LAG("a") RESPECT NULLS OVER (ORDER BY "b" ASC) FROM "t"""",
        "lead-ignore-nulls" -> """SELECT LEAD("a", 1, 0) IGNORE NULLS OVER (ORDER BY "b" ASC) FROM "t"""",
        "first-value-ignore-nulls" -> """SELECT FIRST_VALUE("a") IGNORE NULLS OVER (ORDER BY "b" ASC) FROM "t"""",
        "last-value-respect-nulls" -> """SELECT LAST_VALUE("a") RESPECT NULLS OVER (ORDER BY "b" ASC) FROM "t"""",
        "nth-value-from-first-ignore-nulls" -> """SELECT NTH_VALUE("a", 2) FROM FIRST IGNORE NULLS OVER (ORDER BY "b" ASC) FROM "t""""
    )

class J1_Parser_Corpus extends munit.FunSuite:
    import SqlCorpus.*

    test("corpus parses"):
        val failures = List.newBuilder[String]
        for (name, sql) <- queries do
            try SqlParser.parseQuery(sql)
            catch case ex: Exception => failures += s"$name: ${ex.getMessage}\n    $sql"
        val fs = failures.result()
        assert(fs.isEmpty, s"${fs.size} of ${queries.size} corpus entries failed to parse:\n${fs.mkString("\n")}")

    test("corpus round-trips through the printer"):
        val failures = List.newBuilder[String]
        var checked = 0
        for (name, sql) <- queries do
            try
                val ast = SqlParser.parseQuery(sql)
                val printed = createSql(_.printQuery(ast))
                val back = SqlParser.parseQuery(printed)
                checked += 1
                if back != ast then
                    failures += s"$name\n    source:  $sql\n    printed: $printed\n    first:   $ast\n    reparsed: $back"
            catch case ex: Exception => failures += s"$name: ${ex.getMessage}\n    $sql"
        val fs = failures.result()
        assert(
            fs.isEmpty,
            s"${fs.size} of $checked round-trips diverged:\n${fs.mkString("\n")}"
        )

    test("omitted ordering direction round-trips to ASC"):
        val ast = SqlParser.parseQuery("""SELECT "a" FROM "t" ORDER BY "a"""")
        val printed = createSql(_.printQuery(ast))
        assert(printed.contains("""    "a" ASC"""), printed)
