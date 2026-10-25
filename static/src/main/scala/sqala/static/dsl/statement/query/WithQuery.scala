package sqala.static.dsl.statement.query

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.group.{SqlGroup, SqlGroupingItem}
import sqala.ast.limit.{SqlFetch, SqlFetchMode, SqlFetchUnit, SqlLimit}
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.*
import sqala.ast.table.{SqlJoinCondition, SqlJoinType, SqlTable, SqlTableAlias}
import sqala.metadata.{Dialect, SqlBoolean}
import sqala.static.dsl.*
import sqala.static.dsl.table.{Table, TransformTableKind}
import sqala.util.queryToString
import sqala.util.NonEmptyList

import scala.compiletime.ops.int.-
import sqala.ir.IrQuery
import sqala.ir.ToAst
import sqala.ir.IrLimit
import sqala.ir.IrFetch
import sqala.ir.IrLock
import sqala.ir.IrLockWaitMode
import sqala.ir.IrGroup
import sqala.ir.IrGroupingItem
import sqala.ir.IrExpr


/**
 * A hierarchical recursive query created by `connectBy`. Converts to a
 * standard `WITH RECURSIVE` CTE at SQL generation time.
 */
// final case class ConnectBy[T, OKS <: Tuple, L <: Int](
//     private[sqala] val table: Table[T, Column, L],
//     private[sqala] val connectByTree: IrQuery.Select,
//     private[sqala] val startWithTree: IrQuery.Select,
//     private[sqala] val mapTree: IrQuery.Select =
//         SqlQuery.Select(
//             None,
//             Nil,
//             SqlTable.Ident(tableCte, None, None, None, None) :: Nil,
//             None,
//             None,
//             None,
//             Nil,
//             None,
//             Nil,
//             None,
//             None
//         )
// )(using private[sqala] val qc: QueryContext[L]):
//     given ConnectByContext = ConnectByContext()

//     /**
//      * Specifies the root-level condition for starting recursion.
//      * Maps to the base `WHERE` clause of the CTE.
//      *
//      * {{{
//      * from(Comment).connectBy((c, prior) => c.parentId === prior.id)
//      *     .startWith(c => c.parentId.isNull)
//      * }}}
//      */
//     def startWith[F](f: QueryContext[L] ?=> ConnectByContext ?=> Table[T, Column, L] => F)(using
//         a: AsExpr[F, L],
//         kt: KindToTuple[a.K],
//         i: CanInFilter[a.R, kt.R],
//         e: ExcludeCurrentLevelColumn[kt.R, L],
//         c: CombineKindTuple[OKS, e.R]
//     ): ConnectBy[T, c.R, L] =
//         val cond = a.asExpr(f(table))
//         copy(
//             startWithTree = startWithTree.addWhere(cond.asIrExpr)
//         )

//     /**
//      * Limits the recursion depth. Maps to a filter on the pseudo
//      * `LEVEL` column.
//      *
//      * {{{
//      * from(Comment).connectBy(...).startWith(...).maxDepth(5)
//      * }}}
//      */
//     def maxDepth(n: Int): ConnectBy[T, OKS, L] =
//         val cond = SqlExpr.Binary(
//             SqlExpr.Column(Some(tableCte), columnPseudoLevel),
//             SqlBinaryOperator.LessThan,
//             SqlExpr.NumberLiteral(n)
//         )
//         copy(
//             connectByTree = connectByTree.addWhere(cond)
//         )

//     /**
//      * Sorts siblings within each level of the recursion.
//      *
//      * {{{
//      * from(Comment).connectBy(...).startWith(...).sortSiblingsBy(c => c.content)
//      * }}}
//      */
//     def sortSiblingsBy[S](f: QueryContext[L] ?=> ConnectByContext ?=> Table[T, Column, L] => S)(using
//         a: AsSort[S, ManyRows, L],
//         e: ExcludeCurrentLevelColumn[a.KS, L],
//         c: CombineKindTuple[OKS, e.R],
//         ci: CanInSimpleClause[c.R]
//     ): ConnectBy[T, c.R, L] =
//         val sort = f(table)
//         val sqlOrderBy = a.asSorts(sort).map(_.asIrOrderingItem)
//         copy(
//             connectByTree = connectByTree.copy(orderBy = connectByTree.orderBy ++ sqlOrderBy)
//         )

//     /**
//      * Projects the final result from the recursive CTE. Maps to
//      * `SELECT` on the CTE table, generating the complete `WITH RECURSIVE`
//      * query.
//      *
//      * {{{
//      * from(Comment).connectBy((c, prior) => c.parentId === prior.id)
//      *     .startWith(c => c.parentId.isNull)
//      *     .map(c => (id = c.id, content = c.content, level = level()))
//      * }}}
//      */
//     def map[M](f: ConnectByContext ?=> Table[T, Column, L] => M)(using
//         a: AsMap[M, L],
//         i: CanInMap[a.KS],
//         e: ExcludeCurrentLevelColumn[a.KS, L],
//         c: CombineKindTuple[OKS, e.R],
//         an: AggNotNested[a.KS]
//     ): Query[a.R, c.R, L, ManyRows] =
//         val mapped = f(
//             Table[T, Column, L](
//                 tableCte,
//                 table.__metaData__
//             )
//         )
//         val sqlSelect = a.asSelectItems(mapped, 1)
//         val metaData = table.__metaData__
//         val unionQuery = SqlQuery.Set(
//             startWithTree,
//             SqlSetOperator.Union(Some(SqlQuantifier.All)),
//             None,
//             connectByTree,
//             Nil,
//             None
//         )
//         val withItem = SqlWithItem(
//             tableCte, metaData.columns :+ columnPseudoLevel, unionQuery
//         )
//         val cteTree: SqlQuery.With = SqlQuery.With(
//             true,
//             NonEmptyList(withItem, Nil),
//             mapTree.copy(select = sqlSelect)
//         )
//         Query(a.transform(mapped), cteTree)