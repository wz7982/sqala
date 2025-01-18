package sqala.jdbc

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlStatement
import sqala.ast.table.SqlTable
import sqala.macros.TableMacro
import sqala.util.statementToString

import scala.compiletime.summonInline
import scala.language.unsafeNulls
import scala.quoted.{Expr, Quotes, Type}

trait ValidateSchema[T]

object ValidateSchema:
    inline given derived[T](using inline c: JdbcTestConnection): ValidateSchema[T] =
        ${ checkColumnMacro[T]('c) }

    def checkColumnMacro[T: Type](c: Expr[JdbcTestConnection])(using q: Quotes): Expr[ValidateSchema[T]] =
        import q.reflect.*

        val sym = TypeTree.of[T].symbol
        val fields = sym.declaredFields

        val conn = c.value.get
        val metaData = TableMacro.tableMetaDataMacro[T].value.get
        val table = SqlTable.Range(metaData.tableName, None)
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