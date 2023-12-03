package sqala.jdbc

import sqala.compiletime.{Decoder, QueryType}
import sqala.compiletime.statement.dml.*
import sqala.compiletime.statement.query.*
import DBOperator.monadId

import java.sql.Connection
import javax.sql.DataSource
import scala.annotation.targetName
import scala.deriving.Mirror

class JdbcContext(val dataSource: DataSource, val dialect: Dialect)(using Logger) extends DatabaseOperator[Id]:
    private[sqala] def getConnection(): Connection = dataSource.getConnection().nn

    private[sqala] def exec[T](handler: Connection => T): T =
        val conn = getConnection()
        val result = handler(conn)
        conn.close()
        result

    private[sqala] override def runSql(sql: String, args: Array[Any]): Id[Int] =
        Id(exec(c => jdbcExec(c, sql, args)))

    private[sqala] override def runSqlAndReturnKey(sql: String, args: Array[Any]): Id[List[Long]] =
        Id(exec(c => jdbcExecReturnKey(c, sql, args)))

    private[sqala] override def querySql[T: Decoder](sql: String, args: Array[Any]): Id[List[T]] =
        Id(exec(c => jdbcQuery(c, sql, args)))

    private[sqala] override def querySqlCount(sql: String, args: Array[Any]): Id[Long] =
        Id(exec(c => jdbcQuery[Long](c, sql, args).head))

    def run(query: Dml): Int = monadicRun(query).get

    def runAndReturnKey(query: Insert[?, ?]): List[Long] = monadicRunAndReturnKey(query).get

    def select[T <: Tuple : Decoder](query: Query[T, ?]): List[T] = monadicSelect(query).get

    @targetName("selectSingleton")
    def select[T: Decoder](query: Query[Tuple1[T], ?]): List[T] = monadicSelectSingleton(query).get

    def selectTo[P <: Product](query: Query[?, ?])(using m: Mirror.ProductOf[P])(using QueryType[query.type] =:= m.MirroredElemTypes, Decoder[QueryType[query.type]]): List[P] =
        monadicSelectTo(query).get

    def find[T <: Tuple : Decoder](query: Query[T, ?]): Option[T] = monadicFind(query).get

    @targetName("findSingleton")
    def find[T: Decoder](query: Query[Tuple1[T], ?]): Option[T] = monadicFindSingleton(query).get

    def findTo[P <: Product](query: Query[?, ?])(using m: Mirror.ProductOf[P])(using QueryType[query.type] =:= m.MirroredElemTypes, Decoder[QueryType[query.type]]): Option[P] =
        monadicFindTo(query).get

    def page[T <: Tuple : Decoder](query: Query[T, ?], pageSize: Int, pageNo: Int, returnCount: Boolean): Page[T] =
        monadicPage(query, pageSize, pageNo, returnCount).get

    @targetName("pageSingleton")
    def page[T : Decoder](query: Query[Tuple1[T], ?], pageSize: Int, pageNo: Int, returnCount: Boolean): Page[T] =
        monadicPageSingleton(query, pageSize, pageNo, returnCount).get

    def pageTo[P <: Product](query: Query[?, ?], pageSize: Int, pageNo: Int, returnCount: Boolean)(using m: Mirror.ProductOf[P])(using QueryType[query.type] =:= m.MirroredElemTypes, Decoder[QueryType[query.type]]): Page[P] =
        monadicPageTo(query, pageSize, pageNo, returnCount).get

    def selectCount(query: Query[?, ?]): Long = monadicSelectCount(query).get

    def transactionWithIsolation[T](isolation: Int)(query: JdbcTransactionContext ?=> T): T =
        val conn = getConnection()
        conn.setAutoCommit(false)
        conn.setTransactionIsolation(isolation)
        try
            given t: JdbcTransactionContext = new JdbcTransactionContext(conn, dialect)
            val result = query
            conn.commit()
            result
        catch case e: Exception =>
            conn.rollback()
            throw e
        finally
            conn.setAutoCommit(true)
            conn.close()
   
    def transaction[T](query: JdbcTransactionContext ?=> T): T =
        val conn = getConnection()
        conn.setAutoCommit(false)
        try
            given t: JdbcTransactionContext = new JdbcTransactionContext(conn, dialect)
            val result = query
            conn.commit()
            result
        catch case e: Exception =>
            conn.rollback()
            throw e
        finally
            conn.setAutoCommit(true)
            conn.close()