package bench

import org.openjdk.jmh.annotations.*
import org.openjdk.jmh.infra.Blackhole

import java.util.concurrent.TimeUnit

/**
 * Point query by random existing primary key, including decoding of
 * all columns.
 */
@BenchmarkMode(Array(Mode.AverageTime))
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Warmup(iterations = 3, time = 1)
@Measurement(iterations = 5, time = 1)
@Fork(value = 1, jvmArgsAppend = Array("-Xms2g", "-Xmx4g"))
@Threads(8)
@State(Scope.Benchmark)
class PointQueryBenchmark:

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
        blackhole.consume(plainJdbcOps.point(Db.nextPointId()))

    @Benchmark
    def myBatis(blackhole: Blackhole): Unit =
        blackhole.consume(myBatisOps.point(Db.nextPointId()))

    @Benchmark
    def hibernate(blackhole: Blackhole): Unit =
        blackhole.consume(hibernateOps.point(Db.nextPointId()))

    @Benchmark
    def jooq(blackhole: Blackhole): Unit =
        blackhole.consume(jooqOps.point(Db.nextPointId()))

    @Benchmark
    def slick(blackhole: Blackhole): Unit =
        blackhole.consume(slickOps.point(Db.nextPointId()))

    @Benchmark
    def sqala(blackhole: Blackhole): Unit =
        blackhole.consume(sqalaOps.point(Db.nextPointId()))
