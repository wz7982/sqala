package sqala.test

import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.jdbc.core.JdbcTemplate
import org.springframework.jdbc.datasource.DriverManagerDataSource
import org.springframework.core.env.MapPropertySource
import org.springframework.transaction.support.TransactionTemplate
import org.springframework.transaction.PlatformTransactionManager
import scala.jdk.CollectionConverters.*
import sqala.jdbc.*
import sqala.jdbc.dialect.{MysqlJdbcDialect, PostgresqlJdbcDialect}
import sqala.spring.boot.{SqalaExecutionContext, SqalaJdbcContextAutoConfiguration, SqalaLogging, SqalaLoggingAutoConfiguration, SqalaTransactionAutoConfiguration}

import java.sql.DriverManager
import javax.sql.DataSource

class SPR2_Wiring extends munit.FunSuite:
    private var dbIndex = 0

    private def newDb(): DriverManagerDataSource =
        dbIndex += 1
        val url = s"jdbc:h2:mem:spr_wiring_$dbIndex;DB_CLOSE_DELAY=-1"
        Class.forName("org.h2.Driver")
        val conn = DriverManager.getConnection(url, "sa", "")
        try conn.createStatement().execute("create table spr_t(id int primary key, name varchar(20))")
        finally conn.close()
        val ds = new DriverManagerDataSource(url, "sa", "")
        ds

    private def wiringContext(ds: DataSource, props: (String, String)*): AnnotationConfigApplicationContext =
        val ctx = new AnnotationConfigApplicationContext
        if props.nonEmpty then
            val map = new java.util.HashMap[String, Object]()
            props.foreach((k, v) => map.put(k, v))
            ctx.getEnvironment.getPropertySources.addFirst(new MapPropertySource("sqala-test", map))
        ctx.registerBean(classOf[DataSource], () => ds)
        ctx.register(
            classOf[SqalaJdbcContextAutoConfiguration],
            classOf[SqalaLoggingAutoConfiguration],
            classOf[SqalaTransactionAutoConfiguration]
        )
        ctx.refresh()
        ctx

    test("auto-configuration wires a JdbcContext from properties"):
        val ds = newDb()
        val ctx = wiringContext(ds, "sqala.jdbc.dialect" -> "mysql", "sqala.jdbc.standard-escape-strings" -> "false")
        try
            val jdbc = ctx.getBean(classOf[JdbcContext])
            assert(jdbc.dialect eq MysqlJdbcDialect)
            assertEquals(jdbc.standardEscapeStrings, false)
            assert(jdbc.dataSource eq ds)
        finally ctx.close()

    test("defaults resolve to postgresql with escape strings on"):
        val ds = newDb()
        val ctx = wiringContext(ds)
        try
            val jdbc = ctx.getBean(classOf[JdbcContext])
            assert(jdbc.dialect eq PostgresqlJdbcDialect)
            assertEquals(jdbc.standardEscapeStrings, true)
        finally ctx.close()

    test("a user-provided JdbcContext wins over auto-configuration"):
        val ds = newDb()
        val mine = new JdbcContext(ds, PostgresqlJdbcDialect, true)
        val ctx = new AnnotationConfigApplicationContext
        ctx.registerBean(classOf[DataSource], () => ds)
        ctx.registerBean(classOf[JdbcContext], () => mine)
        ctx.register(classOf[SqalaJdbcContextAutoConfiguration], classOf[SqalaTransactionAutoConfiguration])
        ctx.refresh()
        try
            assert(ctx.getBean(classOf[JdbcContext]) eq mine)
        finally ctx.close()

    test("ambiguous data sources disable the single-candidate beans"):
        val ds = newDb()
        val ctx = new AnnotationConfigApplicationContext
        ctx.registerBean("one", classOf[DataSource], () => ds)
        ctx.registerBean("two", classOf[DataSource], () => newDb())
        ctx.register(classOf[SqalaJdbcContextAutoConfiguration], classOf[SqalaTransactionAutoConfiguration])
        ctx.refresh()
        try
            assertEquals(ctx.getBeanNamesForType(classOf[JdbcContext]).length, 0)
            assertEquals(ctx.getBeanNamesForType(classOf[PlatformTransactionManager]).length, 0)
        finally ctx.close()

    test("logging auto-configuration backs SqalaLogging with the slf4j adapter"):
        val ds = newDb()
        val ctx = wiringContext(ds)
        try
            val logging = ctx.getBean(classOf[SqalaLogging])
            assertEquals(logging.logger.logLevel, sqala.jdbc.LogLevel.Debug)
            assert(!(logging.logger eq Logger.noop))
        finally ctx.close()

    test("disabled logging resolves to the noop logger"):
        val ds = newDb()
        val ctx = wiringContext(ds, "sqala.logging.enabled" -> "false")
        try
            assert(ctx.getBean(classOf[SqalaLogging]).logger eq Logger.noop)
        finally ctx.close()

    test("withTransaction outside a spring transaction fails fast"):
        val ds = newDb()
        val ctx = wiringContext(ds)
        try
            val jdbc = ctx.getBean(classOf[JdbcContext])
            val ec = ctx.getBean(classOf[SqalaExecutionContext])
            val caught = intercept[IllegalStateException](ec.withTransaction(jdbc, None)(()))
            assert(caught.getMessage.contains("@Transactional"), caught.getMessage)
        finally ctx.close()

    test("native sql runs on the spring-managed connection and rolls back with the transaction"):
        val ds = newDb()
        val ctx = wiringContext(ds)
        try
            given JdbcExecutionContext = ctx.getBean(classOf[SqalaExecutionContext])
            val jdbc = ctx.getBean(classOf[JdbcContext])
            val template = new TransactionTemplate(ctx.getBean(classOf[PlatformTransactionManager]))

            val one = "one"
            val two = "two"
            assertEquals(jdbc.execute(sql"insert into spr_t values (${1}, $one)"), 1)

            val thrown = intercept[RuntimeException]:
                template.execute { _ =>
                    jdbc.execute(sql"insert into spr_t values (${2}, $two)"): Unit
                    sys.error("rollback trigger")
                }
            assertEquals(thrown.getMessage, "rollback trigger")

            val names = new JdbcTemplate(ds).queryForList("select name from spr_t order by id", classOf[String])
            assertEquals(names.asScala.toList, List("one"))
        finally ctx.close()

    test("a sql error surfaces as a translated DataAccessException"):
        val ds = newDb()
        val ctx = wiringContext(ds)
        try
            val jdbc = ctx.getBean(classOf[JdbcContext])
            given JdbcExecutionContext = ctx.getBean(classOf[SqalaExecutionContext])
            intercept[org.springframework.dao.DataAccessException](
                jdbc.execute(sql"select * from no_such_table")
            )
        finally ctx.close()

    test("withTransaction runs its block on the active spring transaction"):
        val ds = newDb()
        val ctx = wiringContext(ds)
        try
            val jdbc = ctx.getBean(classOf[JdbcContext])
            val ec = ctx.getBean(classOf[SqalaExecutionContext])
            val template = new TransactionTemplate(ctx.getBean(classOf[PlatformTransactionManager]))
            val blk = "txblock"
            template.execute { _ =>
                val affected = ec.withTransaction(jdbc, None) {
                    jdbc.execute(sql"insert into spr_t values (${3}, $blk)")
                }
                assertEquals(affected, 1)
                val conn = org.springframework.jdbc.datasource.DataSourceUtils.getConnection(ds)
                val rs = conn.createStatement().executeQuery("select name from spr_t where id = 3")
                assert(rs.next())
                assertEquals(rs.getString(1), blk)
                rs.close(): Unit
            }: Unit
            val names = new JdbcTemplate(ds).queryForList("select name from spr_t where id = 3", classOf[String])
            assertEquals(names.asScala.toList, List("txblock"))
        finally ctx.close()

    test("a timeout transaction executes through the proxied connection"):
        val ds = newDb()
        val ctx = wiringContext(ds)
        try
            val jdbc = ctx.getBean(classOf[JdbcContext])
            val ec = ctx.getBean(classOf[SqalaExecutionContext])
            val template = new TransactionTemplate(ctx.getBean(classOf[PlatformTransactionManager]))
            template.setTimeout(5)
            val tmo = "timeout"
            template.execute { _ =>
                ec.withTransaction(jdbc, None) {
                    jdbc.execute(sql"insert into spr_t values (${4}, $tmo)")
                }
            }: Unit
            val names = new JdbcTemplate(ds).queryForList("select name from spr_t where id = 4", classOf[String])
            assertEquals(names.asScala.toList, List("timeout"))
        finally ctx.close()

    test("a sql error inside a timeout transaction unwraps to a DataAccessException"):
        val ds = newDb()
        val ctx = wiringContext(ds)
        try
            val jdbc = ctx.getBean(classOf[JdbcContext])
            val ec = ctx.getBean(classOf[SqalaExecutionContext])
            val template = new TransactionTemplate(ctx.getBean(classOf[PlatformTransactionManager]))
            template.setTimeout(5)
            intercept[org.springframework.dao.DataAccessException] {
                template.execute { _ =>
                    ec.withTransaction(jdbc, None) {
                        jdbc.execute(sql"select * from no_such_table_tmo")
                    }
                }: Unit
            }
            assertEquals(new JdbcTemplate(ds).queryForObject("select count(*) from spr_t", classOf[java.lang.Integer]).intValue, 0)
        finally ctx.close()

    test("executeTransactionWithIsolation joins the spring transaction and ignores the isolation argument"):
        val ds = newDb()
        val ctx = wiringContext(ds)
        try
            val jdbc = ctx.getBean(classOf[JdbcContext])
            given JdbcExecutionContext = ctx.getBean(classOf[SqalaExecutionContext])
            val template = new TransactionTemplate(ctx.getBean(classOf[PlatformTransactionManager]))
            val iso = "isoinherit"
            template.execute { _ =>
                val affected = jdbc.executeTransactionWithIsolation(TransactionIsolation.Serializable) {
                    jdbc.execute(sql"insert into spr_t values (${5}, $iso)")
                }
                assertEquals(affected, 1)
            }: Unit
            val names = new JdbcTemplate(ds).queryForList("select name from spr_t where id = 5", classOf[String])
            assertEquals(names.asScala.toList, List("isoinherit"))
        finally ctx.close()
