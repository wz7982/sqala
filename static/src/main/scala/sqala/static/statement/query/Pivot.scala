package sqala.static.statement.query

import sqala.ast.expr.*
import sqala.ast.statement.*
import sqala.static.common.*
import sqala.static.macros.ClauseMacro

import scala.NamedTuple.*
import scala.compiletime.constValueTuple
import scala.compiletime.ops.any.ToString
import scala.compiletime.ops.int.S
import scala.compiletime.ops.string.+

class PivotQuery[T, N <: Tuple, V <: Tuple](
    private[sqala] val tables: T,
    private[sqala] val tableNames: List[String],
    private[sqala] val aggs: List[SqlExpr.Func],
    private[sqala] val ast: SqlQuery.Select
)(using val queryContext: QueryContext):
    inline def `for`[I](
        inline f: T => I
    )(using
        r: PivotResult[I, N, V]
    ): Query[r.R, OneRow] =
        def fetchItem(leftExpr: SqlExpr, rightExpr: SqlExpr): SqlExpr =
            rightExpr match
                case SqlExpr.Null => SqlExpr.NullTest(leftExpr, false)
                case _ => SqlExpr.Binary(leftExpr, SqlBinaryOperator.Equal, rightExpr)

        def combine(lists: List[(SqlExpr, List[SqlExpr])]): List[SqlExpr] =
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

        val forList = ClauseMacro.fetchPivotFor(f, tableNames, queryContext)
        val conditions = combine(forList)

        val aliasList = constValueTuple[Names[r.R]].toList.map(_.asInstanceOf[String])

        val projectionList =
            for
                a <- aggs
                c <- conditions
            yield
                val firstArg = a.args.headOption.getOrElse(SqlExpr.NumberLiteral(1))
                val replacedArg = SqlExpr.Case(SqlCase(c, firstArg) :: Nil, SqlExpr.Null)
                val args = if a.args.isEmpty then replacedArg :: Nil else replacedArg :: a.args.tail
                a.copy(args = args)

        val selectItems = projectionList.zip(aliasList).map: (p, a) =>
            SqlSelectItem.Item(p, Some(a))

        Query(ast.copy(select = selectItems))

trait PivotResult[I, N <: Tuple, V <: Tuple]:
    type R <: AnyNamedTuple

object PivotResult:
    type Aux[I, N <: Tuple, V <: Tuple, O <: AnyNamedTuple] = PivotResult[I, N, V]:
        type R = O

    given [I, N <: Tuple, V <: Tuple](using
        c: CollectPivotNames[I]
    ): Aux[I, N, V, NamedTuple[FlatPivotNames[N, c.R], FlatPivotTypes[V, c.R]]] =
        new PivotResult[I, N, V]:
            type R = NamedTuple[FlatPivotNames[N, c.R], FlatPivotTypes[V, c.R]]

class GroupedPivotQuery

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

object CollectPivotNames:
    type Aux[T, O <: Tuple] = CollectPivotNames[T]:
        type R = O

    given tuple[H, T <: Tuple](using h: CollectPivotNames[H], t: CollectPivotNames[T]): Aux[H *: T, Tuple.Concat[h.R, t.R]] =
        new CollectPivotNames[H *: T]:
            type R = Tuple.Concat[h.R, t.R]

    given emptyTuple: Aux[EmptyTuple, EmptyTuple] =
        new CollectPivotNames[EmptyTuple]:
            type R = EmptyTuple

    given item[T, N <: Tuple, V <: Tuple]: Aux[PivotPair[T, N, V], N *: EmptyTuple] =
        new CollectPivotNames[PivotPair[T, N, V]]:
            type R = N *: EmptyTuple