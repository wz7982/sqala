package sqala.spring.boot

import org.springframework.boot.context.properties.ConfigurationProperties
import sqala.jdbc.dialect.{
    JdbcDialect,
    MysqlJdbcDialect,
    OracleJdbcDialect,
    PostgresqlJdbcDialect
}

/**
 * Configuration for the sqala JDBC context.
 */
@ConfigurationProperties(prefix = "sqala.jdbc")
class SqalaJdbcProperties:
    private var dialect: String = "postgresql"

    private var standardEscapeStrings: Boolean = true

    def getDialect: String = dialect

    def setDialect(value: String): Unit = dialect = value

    def isStandardEscapeStrings: Boolean = standardEscapeStrings

    def setStandardEscapeStrings(value: Boolean): Unit =
        standardEscapeStrings = value

    def jdbcDialect: JdbcDialect =
        dialect.trim.toLowerCase match
            case "postgresql" | "postgres" => PostgresqlJdbcDialect
            case "mysql" => MysqlJdbcDialect
            case "oracle" => OracleJdbcDialect
            case other =>
                throw new IllegalArgumentException(
                    s"Unsupported sqala.jdbc.dialect '$other'. " +
                    "Supported values: postgresql, mysql, oracle."
                )