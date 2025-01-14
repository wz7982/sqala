package sqala.static.statement.query

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.static.dsl.*

import scala.NamedTuple.*
import scala.compiletime.ops.any.ToString
import scala.compiletime.ops.int.S
import scala.compiletime.ops.string.+

private def fetchItem(leftExpr: SqlExpr, rightExpr: SqlExpr): SqlExpr =
    rightExpr match
        case SqlExpr.Null => SqlExpr.NullTest(leftExpr, false)
        case _ => SqlExpr.Binary(leftExpr, SqlBinaryOperator.Equal, rightExpr)

private def combine(lists: List[(SqlExpr, List[SqlExpr])]): List[SqlExpr] =
    lists match
        case Nil => Nil
        case x :: Nil =>
            x._2.map: e =>
                fetchItem(x._1, e)
        case x :: xs =>
            val item =
                x._2.map: e =>
                    fetchItem(x._1, e)
            item.flatMap: e =>
                combine(xs).map(i => SqlExpr.Binary(e, SqlBinaryOperator.And, i))

class PivotQuery[T, N <: Tuple, V <: Tuple](
    private[sqala] val queryParam: T,
    private[sqala] val aggs: List[SqlExpr.Func],
    private[sqala] val ast: SqlQuery.Select
)(using val queryContext: QueryContext):
    inline def `for`[I](f: T => I)(using
        r: PivotResult[I, N, V]
    ): Query[r.R] =
        val forList = r.collect(f(queryParam))
        val conditions = combine(forList)

        val projectionList =
            for
                a <- aggs
                c <- conditions
            yield
                val firstArg = a.args.headOption.getOrElse(SqlExpr.NumberLiteral(1))
                val replacedArg = SqlExpr.Case(SqlCase(c, firstArg) :: Nil, SqlExpr.Null)
                val args = 
                    if a.args.isEmpty then replacedArg :: Nil 
                    else replacedArg :: a.args.tail
                a.copy(args = args)

        val selectItems = projectionList.zipWithIndex.map: (p, i) =>
                SqlSelectItem.Item(p, Some(s"c$i"))

        Query(
            Tuple.fromArray(projectionList.toArray.map(_ => Expr.Column("", ""))).asInstanceOf[r.R], 
            ast.copy(select = selectItems)
        )

case class PivotPair[T, N <: Tuple, V <: Tuple : AsExpr](expr: Expr[T], pivot: NamedTuple[N, V])

trait PivotResult[I, N <: Tuple, V <: Tuple]:
    type R <: AnyNamedTuple

    def collect(x: I): List[(SqlExpr, List[SqlExpr])]

object PivotResult:
    type Aux[I, N <: Tuple, V <: Tuple, O <: AnyNamedTuple] = PivotResult[I, N, V]:
        type R = O

    given [I, N <: Tuple, V <: Tuple](using
        c: CollectPivotNames[I]
    ): Aux[I, N, V, NamedTuple[FlatPivotNames[N, c.R], FlatPivotTypes[V, c.R]]] =
        new PivotResult[I, N, V]:
            type R = NamedTuple[FlatPivotNames[N, c.R], FlatPivotTypes[V, c.R]]

            def collect(x: I): List[(SqlExpr, List[SqlExpr])] =
                c.collect(x)

type CombineNames[Names <: Tuple] <: Tuple =
    Names match
        case EmptyTuple => EmptyTuple
        case (n *: ns) *: EmptyTuple => (n *: ns)
        case (n *: ns) *: xs =>
            Tuple.FlatMap[
                n *: ns,
                [e] =>> Tuple.Map[CombineNames[xs], [x] =>> ToString[e] + "_" + ToString[x]]
            ]

type FlatPivotNames[AggNames <: Tuple, ForNames <: Tuple] =
    CombineNames[AggNames *: ForNames]

type RepeatToTuple[T, L <: Int] <: Tuple =
    L match
        case 0 => EmptyTuple
        case S[n] => T *: RepeatToTuple[T, n]

type FlatPivotTypes[AggTypes <: Tuple, ForNames <: Tuple] <: Tuple =
    AggTypes match
        case t *: ts => Tuple.Concat[RepeatToTuple[t, Tuple.Size[CombineNames[ForNames]]], FlatPivotTypes[ts, ForNames]]
        case EmptyTuple => EmptyTuple

trait CollectPivotNames[T]:
    type R <: Tuple

    def collect(x: T): List[(SqlExpr, List[SqlExpr])]

object CollectPivotNames:
    type Aux[T, O <: Tuple] = CollectPivotNames[T]:
        type R = O

    given tuple[H, T <: Tuple](using 
        h: CollectPivotNames[H], 
        t: CollectPivotNames[T]
    ): Aux[H *: T, Tuple.Concat[h.R, t.R]] =
        new CollectPivotNames[H *: T]:
            type R = Tuple.Concat[h.R, t.R]

            def collect(x: H *: T): List[(SqlExpr, List[SqlExpr])] =
                h.collect(x.head) ++ t.collect(x.tail)

    given emptyTuple: Aux[EmptyTuple, EmptyTuple] =
        new CollectPivotNames[EmptyTuple]:
            type R = EmptyTuple

            def collect(x: EmptyTuple): List[(SqlExpr, List[SqlExpr])] =
                Nil

    given item[T, N <: Tuple, V <: Tuple : AsExpr]: Aux[PivotPair[T, N, V], N *: EmptyTuple] =
        new CollectPivotNames[PivotPair[T, N, V]]:
            type R = N *: EmptyTuple

            def collect(x: PivotPair[T, N, V]): List[(SqlExpr, List[SqlExpr])] =
                (x.expr.asSqlExpr, summon[AsExpr[V]].asExprs(x.pivot.toTuple).map(_.asSqlExpr)) :: 
                Nil