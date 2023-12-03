package sqala.jdbc

import sqala.compiletime.*
import sqala.compiletime.statement.dml.*
import sqala.compiletime.statement.query.*
import sqala.compiletime.statement.ToSql
import sqala.util.Monad

import scala.deriving.Mirror

trait DatabaseOperator[F[_]](using logger: Logger, monad: Monad[F]):
    def dialect: Dialect

    def showSql(query: ToSql): String = query.sql(dialect)._1

    private[sqala] def runSql(sql: String, args: Array[Any]): F[Int]

    private[sqala] def runSqlAndReturnKey(sql: String, args: Array[Any]): F[List[Long]]

    private[sqala] def querySql[T: Decoder](sql: String, args: Array[Any]): F[List[T]]

    private[sqala] def querySqlCount(sql: String, args: Array[Any]): F[Long]

    private[sqala] def monadicRun(query: Dml): F[Int] =
        val sql = query.sql(dialect)
        logger.apply(s"execute sql: \n${sql._1}")
        runSql(sql._1, sql._2)

    private[sqala] def monadicRunAndReturnKey(query: Insert[?, ?]): F[List[Long]] =
        val sql = query.sql(dialect)
        logger.apply(s"execute sql: \n${sql._1}")
        runSqlAndReturnKey(sql._1, sql._2)

    private[sqala] def monadicSelect[T <: Tuple : Decoder](query: Query[T, ?]): F[List[T]] =
        val sql = query.sql(dialect)
        logger.apply(s"execute sql: \n${sql._1}")
        querySql(sql._1, sql._2)

    private[sqala] def monadicSelectSingleton[T: Decoder](query: Query[Tuple1[T], ?]): F[List[T]] =
        monadicSelect(query).map(x => x.map(row => row._1))

    private[sqala] def monadicSelectTo[P <: Product](query: Query[?, ?])(using m: Mirror.ProductOf[P])(using QueryType[query.type] =:= m.MirroredElemTypes, Decoder[QueryType[query.type]]): F[List[P]] =
        monadicSelect[QueryType[query.type]](query).map(x => x.map(row => m.fromProduct(row)))

    private[sqala] def monadicFind[T <: Tuple : Decoder](query: Query[T, ?]): F[Option[T]] =
        for result <- monadicSelect(query) yield result.headOption

    private[sqala] def monadicFindSingleton[T: Decoder](query: Query[Tuple1[T], ?]): F[Option[T]] =
        monadicFind(query).map(x => x.map(row => row._1))

    private[sqala] def monadicFindTo[P <: Product](query: Query[?, ?])(using m: Mirror.ProductOf[P])(using QueryType[query.type] =:= m.MirroredElemTypes, Decoder[QueryType[query.type]]): F[Option[P]] =
        monadicFind[QueryType[query.type]](query).map(x => x.map(row => m.fromProduct(row)))

    private[sqala] def monadicPage[T <: Tuple : Decoder](query: Query[T, ?], pageSize: Int, pageNo: Int, returnCount: Boolean): F[Page[T]] =
        val data = 
            if pageSize == 0 then summon[Monad[F]].pure(Nil)
            else
                val offset = if pageNo <= 1 then 0 else pageSize * (pageNo - 1)
                val outerQuery = query match
                    case s: Select[_, _, _, _] => s
                    case q => sqala.compiletime.query(q.unsafeAs("__q__"))
                monadicSelect(outerQuery.drop(offset).take(pageSize))
        val count = if returnCount then monadicSelectCount(query) else summon[Monad[F]].pure(0L)
        val total = 
            for c <- count 
            yield 
                if c == 0 || pageSize == 0 then 0 
                else if c % pageSize == 0 then c / pageSize
                else c / pageSize + 1
        for
            t <- total
            c <- count
            d <- data
        yield Page(t, c, pageNo, pageSize, d)

    private[sqala] def monadicPageSingleton[T : Decoder](query: Query[Tuple1[T], ?], pageSize: Int, pageNo: Int, returnCount: Boolean): F[Page[T]] =
        monadicPage(query, pageSize, pageNo, returnCount).map(x => x.map(row => row._1))

    private[sqala] def monadicPageTo[P <: Product](query: Query[?, ?], pageSize: Int, pageNo: Int, returnCount: Boolean)(using m: Mirror.ProductOf[P])(using QueryType[query.type] =:= m.MirroredElemTypes, Decoder[QueryType[query.type]]): F[Page[P]] =
        monadicPage[QueryType[query.type]](query, pageSize, pageNo, returnCount).map(x => x.map(row => m.fromProduct(row)))

    private[sqala] def monadicSelectCount(query: Query[?, ?]): F[Long] =
        val outerQuery = query match
            case s: Select[_, _, _, _] => s.count
            case q => sqala.compiletime.query(q.unsafeAs("__q__")).count
        val sql = outerQuery.sql(dialect)
        logger.apply(s"execute sql: \n${sql._1}")
        for data <- querySql[Long](sql._1, sql._2) yield data.head

object DBOperator:
    import scala.concurrent.{ExecutionContext, Future}

    given monadId: Monad[Id] with
        def pure[A](x: A): Id[A] = Id(x)

        extension [A, B] (x: Id[A])
            def map(f: A => B): Id[B] =
                Id(f(x.get))

            def flatMap(f: A => Id[B]): Id[B] =
                f(x.get)

    given monadFuture(using ExecutionContext): Monad[Future] with
        def pure[A](x: A): Future[A] = Future(x)

        extension [A, B] (x: Future[A])
            def map(f: A => B): Future[B] =
                x.map(f)

            def flatMap(f: A => Future[B]): Future[B] =
                x.flatMap(f)

opaque type Id[A] = A

object Id:
    def apply[A](x: A): Id[A] =
        x

    extension [A] (id: Id[A])
        def get: A =
            id

type Logger = String => Unit