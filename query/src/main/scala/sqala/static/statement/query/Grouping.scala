package sqala.static.statement.query

import sqala.ast.expr.SqlExpr
import sqala.ast.statement.SqlQuery
import sqala.common.TableMetaData
import sqala.static.dsl.*

import scala.NamedTuple.*
import scala.compiletime.constValue

class Grouping[T](
    private[sqala] val queryParam: T,
    val ast: SqlQuery.Select
)(using 
    private[sqala] val context: QueryContext
):
    def having(f: QueryContext ?=> T => Expr[Boolean]): Grouping[T] =
        val cond = f(queryParam)
        Grouping(queryParam, ast.addHaving(cond.asSqlExpr))

    def sortBy[S](f: QueryContext ?=> T => S)(using s: AsSort[S]): Grouping[T] =
        val sort = f(queryParam)
        val sqlOrderBy = s.asSort(sort)
        Grouping(queryParam, ast.copy(orderBy = ast.orderBy ++ sqlOrderBy))

    def orderBy[S](f: QueryContext ?=> T => S)(using s: AsSort[S]): Grouping[T] =
        sortBy(f)

    def map[M](f: QueryContext ?=> T => M)(using s: AsSelect[M]): Query[s.R] =
        val mapped = f(queryParam)
        val sqlSelect = s.selectItems(mapped, 1)
        Query(s.transform(mapped), ast.copy(select = sqlSelect))

    def select[M](f: QueryContext ?=> T => M)(using s: AsSelect[M]): Query[s.R] =
        map(f)

    def pivot[N <: Tuple, V <: Tuple : AsExpr as a](f: T => NamedTuple[N, V]): PivotQuery[T, N, V] =
        val functions = a.asExprs(f(queryParam).toTuple)
            .map(e => e.asSqlExpr.asInstanceOf[SqlExpr.Func])
        PivotQuery[T, N, V](queryParam, functions, ast)

class GroupingTable[T](
    private[sqala] val __tableName__ : String,
    private[sqala] val __aliasName__ : String,
    private[sqala] val __metaData__ : TableMetaData
) extends Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    inline def selectDynamic(name: String): Expr[?] =
        val index = 
            constValue[Index[Names[From[Unwrap[T, Option]]], name.type, 0]]
        Expr.Column(__aliasName__, __metaData__.columnNames(index))

class GroupingSubQuery[N <: Tuple, V <: Tuple](
    private[sqala] val __alias__ : String,
    private[sqala] val __items__ : V
)(using 
    private[sqala] val __context__ : QueryContext
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

trait ToGrouping[T]:
    type R

    def toGrouping(x: T): R

object ToGrouping:
    type Aux[T, O] = ToGrouping[T]:
        type R = O

    given table[T]: Aux[Table[T], GroupingTable[T]] =
        new ToGrouping[Table[T]]:
            type R = GroupingTable[T]

            def toGrouping(x: Table[T]): R =
                GroupingTable(x.__tableName__, x.__aliasName__, x.__metaData__)

    given subQuery[N <: Tuple, V <: Tuple]: Aux[SubQuery[N, V], GroupingSubQuery[N, V]] =
        new ToGrouping[SubQuery[N, V]]:
            type R = GroupingSubQuery[N, V]

            def toGrouping(x: SubQuery[N, V]): R =
                GroupingSubQuery(x.__alias__, x.__items__)(using x.__context__)

    given tuple[H, T <: Tuple](using 
        h: ToGrouping[H], 
        t: ToGrouping[T],
        tt: ToTuple[t.R]
    ): Aux[H *: T, h.R *: tt.R] =
        new ToGrouping[H *: T]:
            type R = h.R *: tt.R

            def toGrouping(x: H *: T): R =
                h.toGrouping(x.head) *: tt.toTuple(t.toGrouping(x.tail))

    given emptyTuple: Aux[EmptyTuple, EmptyTuple] =
        new ToGrouping[EmptyTuple]:
            type R = EmptyTuple

            def toGrouping(x: EmptyTuple): R = x