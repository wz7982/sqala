package sqala.jdbc

import sqala.printer.Dialect

case class JdbcTestConnection(
    url: String,
    user: String,
    password: String,
    driverClassName: String,
    dialect: Dialect
)

inline def createTestConnection(
    inline url: String, 
    inline user: String, 
    inline password: String, 
    inline driverClassName: String,
    inline dialct: Dialect
): JdbcTestConnection = JdbcTestConnection(url, user, password, driverClassName, dialct)