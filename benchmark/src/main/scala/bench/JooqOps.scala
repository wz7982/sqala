package bench

import bench.jvm.BenchUser
import org.jooq.impl.DSL
import org.jooq.impl.SQLDataType
import org.jooq.{DSLContext, Field, SQLDialect, Table}

import javax.sql.DataSource

/**
 * jOOQ using the shared HikariCP pool and a codegen-free table
 * definition. Records are mapped into POJOs by jOOQ's record mapper.
 */
final class JooqOps(dataSource: DataSource):

    private val dsl: DSLContext = DSL.using(dataSource, SQLDialect.POSTGRES)

    private val benchTable: Table[?] = DSL.table("bench_user")

    private val id: Field[java.lang.Long] = DSL.field(DSL.name("id"), SQLDataType.BIGINT)

    private val name: Field[String] = DSL.field(DSL.name("name"), SQLDataType.VARCHAR)

    private val email: Field[String] = DSL.field(DSL.name("email"), SQLDataType.VARCHAR)

    private val age: Field[java.lang.Integer] = DSL.field(DSL.name("age"), SQLDataType.INTEGER)

    private val active: Field[java.lang.Boolean] = DSL.field(DSL.name("active"), SQLDataType.BOOLEAN)

    private val score: Field[java.math.BigDecimal] =
        DSL.field(DSL.name("score"), SQLDataType.DECIMAL)

    private val createdAt: Field[java.time.LocalDateTime] =
        DSL.field(DSL.name("created_at"), SQLDataType.LOCALDATETIME)

    private val bio: Field[String] = DSL.field(DSL.name("bio"), SQLDataType.VARCHAR)

    private val city: Field[String] = DSL.field(DSL.name("city"), SQLDataType.VARCHAR)

    private val balance: Field[java.lang.Double] =
        DSL.field(DSL.name("balance"), SQLDataType.DOUBLE)

    def point(idValue: Long): BenchUser =
        dsl
            .select(id, name, email, age, active, score, createdAt, bio, city, balance)
            .from(benchTable)
            .where(id.eq(java.lang.Long.valueOf(idValue)))
            .fetchOneInto(classOf[BenchUser])

    def range(start: Long, end: Long): java.util.List[BenchUser] =
        dsl
            .select(id, name, email, age, active, score, createdAt, bio, city, balance)
            .from(benchTable)
            .where(
                id
                    .ge(java.lang.Long.valueOf(start))
                    .and(id.lt(java.lang.Long.valueOf(end)))
            )
            .fetchInto(classOf[BenchUser])
