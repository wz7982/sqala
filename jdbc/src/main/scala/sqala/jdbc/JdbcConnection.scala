package sqala.jdbc

import javax.sql.DataSource

trait JdbcConnection[T <: DataSource]:
    def init(url: String, username: String, password: String, driverClassName: String): T