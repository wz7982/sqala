package sqala.jdbc

import sqala.compiletime.{Decoder, QueryType}
import sqala.compiletime.statement.dml.*
import sqala.compiletime.statement.query.*
import DBOperator.monadId

import java.sql.Connection
import scala.annotation.targetName
import scala.deriving.Mirror

class JdbcTransactionContext(val connection: Connection, val dialect: Dialect)(using Logger) extends DatabaseOperator[Id]:
    private[sqala] def getConnection(): Connection = connection

    private[sqala] override def runSql(sql: String, args: Array[Any]): Id[Int] =
        Id(jdbcExec(getConnection(), sql, args))

    private[sqala] override def runSqlAndReturnKey(sql: String, args: Array[Any]): Id[List[Long]] =
        Id(jdbcExecReturnKey(getConnection(), sql, args))

    private[sqala] override def querySql[T: Decoder](sql: String, args: Array[Any]): Id[List[T]] =
        Id(jdbcQuery(getConnection(), sql, args))

    private[sqala] override def querySqlCount(sql: String, args: Array[Any]): Id[Long] =
        Id(jdbcQuery[Long](getConnection(), sql, args).head)

def run(query: Dml)(using logger: Logger, t: JdbcTransactionContext): Int = t.monadicRun(query).get

def runAndReturnKey(query: Insert[?, ?])(using logger: Logger, t: JdbcTransactionContext): List[Long] = 
    t.monadicRunAndReturnKey(query).get

def select[T <: Tuple : Decoder](query: Query[T, ?])(using logger: Logger, t: JdbcTransactionContext): List[T] = 
    t.monadicSelect(query).get

@targetName("selectSingleton")
def select[T: Decoder](query: Query[Tuple1[T], ?])(using logger: Logger, t: JdbcTransactionContext): List[T] = 
    t.monadicSelectSingleton(query).get

def selectTo[P <: Product](query: Query[?, ?])(using m: Mirror.ProductOf[P], logger: Logger, t: JdbcTransactionContext)(using QueryType[query.type] =:= m.MirroredElemTypes, Decoder[QueryType[query.type]]): List[P] =
    t.monadicSelectTo(query).get

def find[T <: Tuple : Decoder](query: Query[T, ?])(using logger: Logger, t: JdbcTransactionContext): Option[T] = 
    t.monadicFind(query).get

@targetName("findSingleton")
def find[T: Decoder](query: Query[Tuple1[T], ?])(using logger: Logger, t: JdbcTransactionContext): Option[T] = 
    t.monadicFindSingleton(query).get

def findTo[P <: Product](query: Query[?, ?])(using m: Mirror.ProductOf[P], logger: Logger, t: JdbcTransactionContext)(using QueryType[query.type] =:= m.MirroredElemTypes, Decoder[QueryType[query.type]]): Option[P] =
    t.monadicFindTo(query).get

def page[T <: Tuple : Decoder](query: Query[T, ?], pageSize: Int, pageNo: Int, returnCount: Boolean)(using logger: Logger, t: JdbcTransactionContext): Page[T] =
    t.monadicPage(query, pageSize, pageNo, returnCount).get

@targetName("pageSingleton")
def page[T : Decoder](query: Query[Tuple1[T], ?], pageSize: Int, pageNo: Int, returnCount: Boolean)(using logger: Logger, t: JdbcTransactionContext): Page[T] =
    t.monadicPageSingleton(query, pageSize, pageNo, returnCount).get

def pageTo[P <: Product](query: Query[?, ?], pageSize: Int, pageNo: Int, returnCount: Boolean)(using m: Mirror.ProductOf[P], logger: Logger, t: JdbcTransactionContext)(using QueryType[query.type] =:= m.MirroredElemTypes, Decoder[QueryType[query.type]]): Page[P] =
    t.monadicPageTo(query, pageSize, pageNo, returnCount).get

def selectCount(query: Query[?, ?])(using logger: Logger, t: JdbcTransactionContext): Long = t.monadicSelectCount(query).get