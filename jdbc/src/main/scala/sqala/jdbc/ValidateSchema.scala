package sqala.jdbc

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.dsl.TableMetaData
import sqala.dsl.macros.TableMacro
import sqala.printer.*
import sqala.util.statementToString

import scala.compiletime.summonInline
import scala.language.unsafeNulls
import scala.quoted.{Expr, FromExpr, Quotes, Type}

trait ValidateSchema[T]

object ValidateSchema:
    private given [T](using Type[T]): FromExpr[SetDefaultValue[T]] with
        override def unapply(x: Expr[SetDefaultValue[T]])(using Quotes): Option[SetDefaultValue[T]] = x match
            case '{ SetDefaultValue.setInt } => Some(SetDefaultValue.setInt.asInstanceOf[SetDefaultValue[T]])
            case '{ SetDefaultValue.setLong } => Some(SetDefaultValue.setLong.asInstanceOf[SetDefaultValue[T]])
            case '{ SetDefaultValue.setFloat } => Some(SetDefaultValue.setFloat.asInstanceOf[SetDefaultValue[T]])
            case '{ SetDefaultValue.setDouble } => Some(SetDefaultValue.setDouble.asInstanceOf[SetDefaultValue[T]])
            case '{ SetDefaultValue.setDecimal } => Some(SetDefaultValue.setDecimal.asInstanceOf[SetDefaultValue[T]])
            case '{ SetDefaultValue.setBoolean } => Some(SetDefaultValue.setBoolean.asInstanceOf[SetDefaultValue[T]])
            case '{ SetDefaultValue.setString } => Some(SetDefaultValue.setString.asInstanceOf[SetDefaultValue[T]])
            case '{ SetDefaultValue.setDate } => Some(SetDefaultValue.setDate.asInstanceOf[SetDefaultValue[T]])
            case '{ SetDefaultValue.setLocalDate } => Some(SetDefaultValue.setLocalDate.asInstanceOf[SetDefaultValue[T]])
            case '{ SetDefaultValue.setLocalDateTime } => Some(SetDefaultValue.setLocalDateTime.asInstanceOf[SetDefaultValue[T]])
            case _ => None

    private given FromExpr[TableMetaData] with
        def unapply(x: Expr[TableMetaData])(using Quotes): Option[TableMetaData] = x match
            case '{ TableMetaData(${Expr(tableName)}, $_, $_, ${Expr(columns)}, $_) } =>
                Some(TableMetaData(tableName, Nil, None, columns, Nil))
            case _ => None

    private given FromExpr[Dialect] with
        def unapply(x: Expr[Dialect])(using Quotes): Option[Dialect] = x match
            case '{ MysqlDialect } => Some(MysqlDialect)
            case '{ PostgresqlDialect } => Some(PostgresqlDialect)
            case '{ SqliteDialect } => Some(SqliteDialect)
            case '{ MssqlDialect } => Some(MssqlDialect)
            case '{ OracleDialect } => Some(OracleDialect)
            case '{ DB2Dialect } => Some(DB2Dialect)
            case _ => None

    private given FromExpr[JdbcTestConnection] with
        def unapply(x: Expr[JdbcTestConnection])(using Quotes): Option[JdbcTestConnection] = x match
            case '{ JdbcTestConnection(${Expr(url)}, ${Expr(name)}, ${Expr(password)}, ${Expr(driverClassName)}, ${Expr(dialect)}) } =>
                Some(JdbcTestConnection(url, name, password, driverClassName, dialect))
            case _ => None

    inline given derived[T](using inline c: JdbcTestConnection): ValidateSchema[T] =
        ${ checkColumnMacro[T]('c) }

    def checkColumnMacro[T: Type](c: Expr[JdbcTestConnection])(using q: Quotes): Expr[ValidateSchema[T]] =
        import q.reflect.*

        val sym = TypeTree.of[T].symbol
        val fields = sym.declaredFields

        val conn = c.value.get
        val metaData = TableMacro.tableMetaDataMacro[T].value.get
        val table = SqlTable.IdentTable(metaData.tableName, None)
        val columns = metaData.columnNames.map: n => 
            SqlExpr.Column(None, n)
        val query = SqlStatement.Insert(table, columns, columns.map(_ => SqlExpr.NumberLiteral(0)) :: Nil, None)
        val sql = statementToString(query, conn.dialect, true)._1

        Class.forName(conn.driverClassName)

        var connection: java.sql.Connection = null
        var stmt: java.sql.PreparedStatement = null

        val errorMessage = try
            connection = java.sql.DriverManager.getConnection(conn.url, conn.user, conn.password)
            connection.setAutoCommit(false)
            stmt = connection.prepareStatement(sql)

            val types = fields.map: f =>
                f.tree match
                    case ValDef(_, t, _) => t.tpe.asType

            var index = 1
            for typ <- types do
                typ match
                    case '[Option[t]] =>
                        val expr = Expr.summon[SetDefaultValue[t]]
                        expr match
                            case Some(s) =>
                                val set = s.value.get
                                stmt.setNull(index, set.jdbcType)
                            case None =>
                    case '[t] => 
                        val expr = Expr.summon[SetDefaultValue[t]]
                        expr match
                            case Some(s) =>
                                val set = s.value.get
                                set.setValue(stmt, index)
                            case None =>
                index += 1

            stmt.execute()

            None
        catch
            case e: Exception => 
                Some(e.getMessage())
        finally
            if stmt != null then stmt.close()
            if connection != null then
                connection.rollback()
                connection.setAutoCommit(true)
                connection.close()

        errorMessage match
            case Some(e) => report.error(e)
            case _ =>

        '{
            new ValidateSchema[T] {}
        }