package sqala.test

import sqala.jdbc.LogLevel
import sqala.jdbc.dialect.{MysqlJdbcDialect, OracleJdbcDialect, PostgresqlJdbcDialect}
import sqala.spring.boot.{SqalaJdbcProperties, SqalaLoggingProperties}

class SPR1_Properties extends munit.FunSuite:
    test("jdbc properties default to postgresql with standard escape strings"):
        val p = new SqalaJdbcProperties
        assertEquals(p.getDialect, "postgresql")
        assertEquals(p.isStandardEscapeStrings, true)
        assert(p.jdbcDialect eq PostgresqlJdbcDialect)

    test("jdbc dialect names map case- and whitespace-insensitively"):
        for name <- List("postgresql", "PostgreSQL", "  postgres  ", "POSTGRES") do
            val p = new SqalaJdbcProperties
            p.setDialect(name)
            assert(p.jdbcDialect eq PostgresqlJdbcDialect, name)

        for name <- List("mysql", " MySQL ") do
            val p = new SqalaJdbcProperties
            p.setDialect(name)
            assert(p.jdbcDialect eq MysqlJdbcDialect, name)

        for name <- List("oracle", "ORACLE") do
            val p = new SqalaJdbcProperties
            p.setDialect(name)
            assert(p.jdbcDialect eq OracleJdbcDialect, name)

    test("an unknown dialect name is rejected with the supported list"):
        val p = new SqalaJdbcProperties
        p.setDialect(" db2 ")
        val caught = intercept[IllegalArgumentException](p.jdbcDialect)
        assert(caught.getMessage.contains("'db2'"), caught.getMessage)
        assert(caught.getMessage.contains("postgresql, mysql, oracle"), caught.getMessage)

    test("standard escape strings can be switched off"):
        val p = new SqalaJdbcProperties
        p.setStandardEscapeStrings(false)
        assertEquals(p.isStandardEscapeStrings, false)

    test("logging properties expose documented defaults"):
        val p = new SqalaLoggingProperties
        assertEquals(p.isEnabled, true)
        assertEquals(p.getLevel, "debug")
        assertEquals(p.isLogArgs, true)
        assertEquals(p.getMaxArgumentLength, 200)
        assertEquals(p.getName, "sqala")
        assertEquals(p.logLevel, LogLevel.Debug)

    test("log level names map case- and whitespace-insensitively"):
        val cases = List(
            "trace" -> LogLevel.Trace,
            " DEBUG " -> LogLevel.Debug,
            "Info" -> LogLevel.Info,
            "warn" -> LogLevel.Warn,
            "ERROR" -> LogLevel.Error
        )
        for (name, expected) <- cases do
            val p = new SqalaLoggingProperties
            p.setLevel(name)
            assertEquals(p.logLevel, expected, name)

    test("an unknown log level falls back to debug"):
        for name <- List("notice", "", "  ") do
            val p = new SqalaLoggingProperties
            p.setLevel(name)
            assertEquals(p.logLevel, LogLevel.Debug, s"[$name]")

    test("logging setters are reflected"):
        val p = new SqalaLoggingProperties
        p.setEnabled(false)
        p.setLogArgs(false)
        p.setMaxArgumentLength(10)
        p.setName("my-app")
        assertEquals(p.isEnabled, false)
        assertEquals(p.isLogArgs, false)
        assertEquals(p.getMaxArgumentLength, 10)
        assertEquals(p.getName, "my-app")
