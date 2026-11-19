package bench

/**
 * Small smoke test for the benchmark harness. Run with:
 * `sbt "benchmark/runMain bench.SanityCheck"`.
 */
object SanityCheck:

    def main(args: Array[String]): Unit =
        println(s"Database: ${Db.JdbcUrl}")
        Db.ensureSeeded()

        val plainJdbc = new PlainJdbcOps(Db.dataSource)
        val myBatis = new MyBatisOps(Db.dataSource)
        val hibernate = new HibernateOps(Db.dataSource)
        val jooq = new JooqOps(Db.dataSource)
        val slick = new SlickOps(Db.dataSource)
        val sqala = new SqalaOps(Db.dataSource)

        val id = 1L
        val start = 1L
        val end = start + Db.RangeSize

        val results = List(
            "plainJdbc" -> plainJdbc.point(id).getName,
            "myBatis" -> myBatis.point(id).getName,
            "hibernate" -> hibernate.point(id).getName,
            "jooq" -> jooq.point(id).getName,
            "slick" -> slick.point(id).name,
            "sqala" -> sqala.point(id).get.name
        )
        results.foreach((name, value) => println(s"$name point: $value"))

        val rangeResults = List(
            "plainJdbc" -> plainJdbc.range(start, end).size(),
            "myBatis" -> myBatis.range(start, end).size(),
            "hibernate" -> hibernate.range(start, end).size(),
            "jooq" -> jooq.range(start, end).size(),
            "slick" -> slick.range(start, end).size,
            "sqala" -> sqala.range(start, end).size
        )
        rangeResults.foreach((name, value) => println(s"$name range: $value"))

        val parserSql =
            """SELECT u.city, COUNT(*) AS user_count, AVG(u.balance) AS avg_balance
              |FROM bench_user u
              |WHERE u.age BETWEEN 18 AND 65
              |  AND u.active = TRUE
              |  AND u.created_at >= TIMESTAMP '2024-01-01 00:00:00'
              |GROUP BY u.city
              |HAVING COUNT(*) > 10
              |ORDER BY avg_balance DESC
              |LIMIT 20 OFFSET 5""".stripMargin

        println("druid: " + com.alibaba.druid.sql.SQLUtils.parseSingleStatement(parserSql, com.alibaba.druid.DbType.postgresql).getClass.getName)
        println("calcite: " + org.apache.calcite.sql.parser.SqlParser.create(parserSql).parseQuery().getClass.getName)
        println("jsqlparser: " + net.sf.jsqlparser.parser.CCJSqlParserUtil.parse(parserSql).getClass.getName)
        println("sqala: " + _root_.sqala.parser.SqlParser.parseQuery(parserSql).getClass.getName)

        println("OK")
