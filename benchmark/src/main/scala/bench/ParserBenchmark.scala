package bench

import com.alibaba.druid.DbType
import com.alibaba.druid.sql.SQLUtils
import net.sf.jsqlparser.parser.CCJSqlParserUtil
import org.apache.calcite.sql.parser.{SqlParser as CalciteSqlParser}
import org.apache.calcite.sql.validate.SqlConformanceEnum
import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/**
 * SQL parser throughput. The same analytical statement is parsed from
 * scratch on every invocation by each parser.
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = Array("-Xms2g", "-Xmx4g"))
@Threads(8)
@State(Scope.Benchmark)
class ParserBenchmark:

    private val sql: String =
        """SELECT u.city, COUNT(*) AS user_count, AVG(u.balance) AS avg_balance
          |FROM bench_user u
          |WHERE u.age BETWEEN 18 AND 65
          |  AND u.active = TRUE
          |  AND u.created_at >= TIMESTAMP '2024-01-01 00:00:00'
          |GROUP BY u.city
          |HAVING COUNT(*) > 10
          |ORDER BY avg_balance DESC
          |LIMIT 20 OFFSET 5""".stripMargin

    private val calciteConfig =
        CalciteSqlParser.config().withConformance(SqlConformanceEnum.LENIENT)

    @Benchmark
    def druid(blackhole: Blackhole): Unit =
        blackhole.consume(SQLUtils.parseSingleStatement(sql, DbType.postgresql))

    @Benchmark
    def calcite(blackhole: Blackhole): Unit =
        blackhole.consume(CalciteSqlParser.create(sql, calciteConfig).parseQuery())

    @Benchmark
    def jsqlparser(blackhole: Blackhole): Unit =
        blackhole.consume(CCJSqlParserUtil.parse(sql))

    @Benchmark
    def sqala(blackhole: Blackhole): Unit =
        blackhole.consume(_root_.sqala.parser.SqlParser.parseQuery(sql))
