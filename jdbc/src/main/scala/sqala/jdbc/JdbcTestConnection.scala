package sqala.jdbc

import sqala.printer.Dialect

case class JdbcTestConnection(
    url: String,
    user: String,
    password: String,
    driverClassName: String,
    dialect: Dialect
)