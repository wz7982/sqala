package sqala.jdbc

import sqala.dsl.TableMetaData
import sqala.printer.*

import scala.quoted.{Expr, FromExpr, Quotes, Type}

given FromExpr[JdbcTestConnection] with
    override def unapply(x: Expr[JdbcTestConnection])(using Quotes): Option[JdbcTestConnection] = x match
        case '{ JdbcTestConnection(${Expr(url)}, ${Expr(name)}, ${Expr(password)}, ${Expr(driverClassName)}, ${Expr(dialect)}) } =>
            Some(JdbcTestConnection(url, name, password, driverClassName, dialect))
        case _ => None

given [T](using Type[T]): FromExpr[SetDefaultValue[T]] with
    override def unapply(x: Expr[SetDefaultValue[T]])(using Quotes): Option[SetDefaultValue[T]] = x match
        case '{ SetDefaultValue.setInt } => Some(SetDefaultValue.setInt.asInstanceOf[SetDefaultValue[T]])
        case '{ SetDefaultValue.setLong } => Some(SetDefaultValue.setLong.asInstanceOf[SetDefaultValue[T]])
        case '{ SetDefaultValue.setFloat } => Some(SetDefaultValue.setFloat.asInstanceOf[SetDefaultValue[T]])
        case '{ SetDefaultValue.setDouble } => Some(SetDefaultValue.setDouble.asInstanceOf[SetDefaultValue[T]])
        case '{ SetDefaultValue.setDecimal } => Some(SetDefaultValue.setDecimal.asInstanceOf[SetDefaultValue[T]])
        case '{ SetDefaultValue.setBoolean } => Some(SetDefaultValue.setBoolean.asInstanceOf[SetDefaultValue[T]])
        case '{ SetDefaultValue.setString } => Some(SetDefaultValue.setString.asInstanceOf[SetDefaultValue[T]])
        case '{ SetDefaultValue.setLocalDate } => Some(SetDefaultValue.setLocalDate.asInstanceOf[SetDefaultValue[T]])
        case '{ SetDefaultValue.setLocalDateTime } => Some(SetDefaultValue.setLocalDateTime.asInstanceOf[SetDefaultValue[T]])
        case _ => None

given FromExpr[TableMetaData] with
    override def unapply(x: Expr[TableMetaData])(using Quotes): Option[TableMetaData] = x match
        case '{ TableMetaData(${Expr(tableName)}, $_, $_, ${Expr(columns)}, $_) } =>
            Some(TableMetaData(tableName, Nil, None, columns, Nil))
        case _ => None

given FromExpr[Dialect] with
    override def unapply(x: Expr[Dialect])(using Quotes): Option[Dialect] = x match
        case '{ MysqlDialect } => Some(MysqlDialect)
        case '{ PostgresqlDialect } => Some(PostgresqlDialect)
        case '{ SqliteDialect } => Some(SqliteDialect)
        case '{ MssqlDialect } => Some(MssqlDialect)
        case '{ OracleDialect } => Some(OracleDialect)
        case '{ DB2Dialect } => Some(DB2Dialect)
        case _ => None