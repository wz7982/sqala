package sqala.spring.boot

import org.h2.jdbcx.JdbcDataSource
import org.springframework.context.annotation.AnnotationConfigApplicationContext
import org.springframework.dao.DataAccessException
import org.springframework.jdbc.datasource.DataSourceTransactionManager
import org.springframework.transaction.PlatformTransactionManager
import org.springframework.transaction.support.TransactionTemplate
import sqala.dynamic.native.sql
import sqala.jdbc.JdbcContext
import sqala.jdbc.dialect.PostgresqlJdbcDialect

import javax.sql.DataSource

class TestSqalaExecutionContext extends munit.FunSuite:
    private def createDataSource(name: String): JdbcDataSource =
        val dataSource = new JdbcDataSource()
        dataSource.setURL(s"jdbc:h2:mem:$name;DB_CLOSE_DELAY=-1")
        dataSource.setUser("sa")
        dataSource.setPassword("")
        dataSource

    private def createContext(name: String): JdbcContext =
        JdbcContext(createDataSource(name), PostgresqlJdbcDialect, true)

    test("uses the DataSource from each JdbcContext"):
        val db1 = createContext("ctx_one")
        val db2 = createContext("ctx_two")
        val execution = SqalaExecutionContext()

        execution.withConnection(db1) { connection =>
            assert(connection.getMetaData.getURL.contains("ctx_one"))
        }
        execution.withConnection(db2) { connection =>
            assert(connection.getMetaData.getURL.contains("ctx_two"))
        }

    test("translates SQLException to DataAccessException"):
        val db = createContext("ctx_error")
        val execution = SqalaExecutionContext()

        intercept[DataAccessException] {
            execution.withConnection(db) { connection =>
                val statement = connection.prepareStatement("SELECT * FROM missing_table")
                try statement.executeQuery()
                finally statement.close()
            }
        }

    test("applies Spring transaction timeout to created statements"):
        val dataSource = createDataSource("ctx_timeout")
        val db = JdbcContext(dataSource, PostgresqlJdbcDialect, true)
        val execution = SqalaExecutionContext()
        val template = new TransactionTemplate(new DataSourceTransactionManager(dataSource))
        template.setTimeout(2)

        template.execute { _ =>
            execution.withConnection(db) { connection =>
                val statement = connection.prepareStatement("SELECT 1")
                try assertEquals(statement.getQueryTimeout, 2)
                finally statement.close()
            }
        }

    test("SQLException inside a Spring transaction rolls back"):
        val db = createContext("ctx_rollback")
        val execution = SqalaExecutionContext()
        import execution.given

        db.execute(sql"CREATE TABLE ctx_tx (id INT PRIMARY KEY)")

        val template = new TransactionTemplate(new DataSourceTransactionManager(db.dataSource))
        intercept[DataAccessException] {
            template.execute { _ =>
                db.execute(sql"INSERT INTO ctx_tx VALUES (1)")
                db.execute(sql"INSERT INTO missing_table VALUES (2)")
            }
        }

        assertEquals(db.fetchTo[Long](sql"SELECT COUNT(*) FROM ctx_tx").head, 0L)

    test("auto configuration does not fail with multiple DataSources without @Primary"):
        val context = new AnnotationConfigApplicationContext()
        context.registerBean("one", classOf[DataSource], () => createDataSource("auto_one"))
        context.registerBean("two", classOf[DataSource], () => createDataSource("auto_two"))
        context.register(classOf[SqalaTransactionAutoConfiguration])
        context.refresh()

        try
            assert(context.getBean(classOf[SqalaExecutionContext]) != null)
            assertEquals(context.getBeanNamesForType(classOf[PlatformTransactionManager]).length, 0)
        finally
            context.close()

    test("auto configuration uses the @Primary DataSource"):
        val context = new AnnotationConfigApplicationContext()
        context.registerBean("one", classOf[DataSource], () => createDataSource("auto_primary_one"))
        context.registerBean("two", classOf[DataSource], () => createDataSource("auto_primary_two"))
        context.getBeanFactory.getBeanDefinition("one").setPrimary(true)
        context.register(classOf[SqalaTransactionAutoConfiguration])
        context.refresh()

        try
            assert(context.getBean(classOf[PlatformTransactionManager]) != null)
        finally
            context.close()
