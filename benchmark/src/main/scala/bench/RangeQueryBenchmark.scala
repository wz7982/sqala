package bench

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/**
 * Range query returning [[Db.RangeSize]] rows, including decoding of
 * all columns.
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = Array("-Xms2g", "-Xmx4g"))
@Threads(8)
@State(Scope.Benchmark)
class RangeQueryBenchmark:

    private var plainJdbcOps: PlainJdbcOps = null

    private var myBatisOps: MyBatisOps = null

    private var hibernateOps: HibernateOps = null

    private var jooqOps: JooqOps = null

    private var slickOps: SlickOps = null

    private var sqalaOps: SqalaOps = null

    @Setup(Level.Trial)
    def setup(): Unit =
        Db.ensureSeeded()
        plainJdbcOps = new PlainJdbcOps(Db.dataSource)
        myBatisOps = new MyBatisOps(Db.dataSource)
        hibernateOps = new HibernateOps(Db.dataSource)
        jooqOps = new JooqOps(Db.dataSource)
        slickOps = new SlickOps(Db.dataSource)
        sqalaOps = new SqalaOps(Db.dataSource)

    @Benchmark
    def plainJdbc(blackhole: Blackhole): Unit =
        val start = Db.nextRangeStart()
        blackhole.consume(plainJdbcOps.range(start, start + Db.RangeSize))

    @Benchmark
    def myBatis(blackhole: Blackhole): Unit =
        val start = Db.nextRangeStart()
        blackhole.consume(myBatisOps.range(start, start + Db.RangeSize))

    @Benchmark
    def hibernate(blackhole: Blackhole): Unit =
        val start = Db.nextRangeStart()
        blackhole.consume(hibernateOps.range(start, start + Db.RangeSize))

    @Benchmark
    def jooq(blackhole: Blackhole): Unit =
        val start = Db.nextRangeStart()
        blackhole.consume(jooqOps.range(start, start + Db.RangeSize))

    @Benchmark
    def slick(blackhole: Blackhole): Unit =
        val start = Db.nextRangeStart()
        blackhole.consume(slickOps.range(start, start + Db.RangeSize))

    @Benchmark
    def sqala(blackhole: Blackhole): Unit =
        val start = Db.nextRangeStart()
        blackhole.consume(sqalaOps.range(start, start + Db.RangeSize))
