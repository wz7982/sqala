package sqala.dsl

import sqala.ast.expr.*
import sqala.ast.group.SqlGroupItem
import sqala.ast.limit.SqlLimit
import sqala.ast.order.SqlOrderBy
import sqala.ast.statement.{SqlQuery, SqlSelectItem, SqlSelectParam, SqlUnionType}
import sqala.ast.table.{SqlJoinType, SqlTableAlias, SqlTable}
import sqala.macros.*
import sqala.printer.Dialect
import sqala.util.queryToString

import scala.NamedTuple.NamedTuple
import scala.Tuple.Append
import scala.compiletime.summonInline
import scala.deriving.Mirror
import scala.util.{NotGiven, TupledFunction}

enum ResultSize:
    case OneRow
    case ManyRows

type OneRow = ResultSize.OneRow.type

type ManyRows = ResultSize.ManyRows.type

trait QuerySize[T]:
    type R <: ResultSize

object QuerySize:
    type Aux[T, O <: ResultSize] = QuerySize[T]:
        type R = O

    given one: Aux[1, OneRow] = new QuerySize[1]:
        type R = OneRow

    given many[T <: Int](using NotGiven[T =:= 1]): Aux[T, ManyRows] = new QuerySize[T]:
        type R = ManyRows

class Query[T, S <: ResultSize](
    val ast: SqlQuery
)(using val qc: QueryContext):
    def sql(dialect: Dialect, prepare: Boolean = true, indent: Int = 4): (String, Array[Any]) =
        queryToString(ast, dialect, prepare, indent)

    def drop(n: Int): Query[T, S] =
        val limit = ast match
            case s: SqlQuery.Select => s.limit
            case u: SqlQuery.Union => u.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(l.limit, SqlExpr.NumberLiteral(n)))
            .orElse(Some(SqlLimit(SqlExpr.NumberLiteral(1), SqlExpr.NumberLiteral(n))))
        val newAst = ast match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case u: SqlQuery.Union => u.copy(limit = sqlLimit)
            case _ => ast
        Query(newAst)

    def take(n: Int)(using s: QuerySize[n.type]): Query[T, s.R] =
        val limit = ast match
            case s: SqlQuery.Select => s.limit
            case u: SqlQuery.Union => u.limit
            case _ => None
        val sqlLimit = limit
            .map(l => SqlLimit(SqlExpr.NumberLiteral(n), l.offset))
            .orElse(Some(SqlLimit(SqlExpr.NumberLiteral(n), SqlExpr.NumberLiteral(0))))
        val newAst = ast match
            case s: SqlQuery.Select => s.copy(limit = sqlLimit)
            case u: SqlQuery.Union => u.copy(limit = sqlLimit)
            case _ => ast
        Query(newAst)

    def size: Query[Long, OneRow] =
        ast match
            case s@SqlQuery.Select(_, _, _, _, Nil, _, _, _) =>
                Query(s.copy(select = SqlSelectItem.Item(SqlExpr.Func("COUNT", Nil), None) :: Nil, limit = None))
            case _ =>
                val outerQuery: SqlQuery.Select = SqlQuery.Select(
                    select = SqlSelectItem.Item(SqlExpr.Func("COUNT", Nil), None) :: Nil,
                    from = SqlTable.SubQueryTable(ast, false, SqlTableAlias("t")) :: Nil
                )
                Query(outerQuery)

    def exists: Query[Boolean, OneRow] =
        val outerQuery: SqlQuery.Select = SqlQuery.Select(
            select = SqlSelectItem.Item(SqlExpr.SubLink(ast, SqlSubLinkType.Exists), None) :: Nil,
            from = Nil
        )
        Query(outerQuery)

class TableQuery[T](
    private[sqala] val containers: List[Container],
    override val ast: SqlQuery.Select
)(using override val qc: QueryContext) extends Query[T, ManyRows](ast):
    inline def filter[F](using
        tt: ToTuple[T],
        t: TupledFunction[F, tt.R => Boolean]
    )(inline f: QueryContext ?=> F): TableQuery[T] =
        val args = ClauseMacro.fetchArgNames(f(using qc))
        val outerContainers = args.zip(containers)
        given newContext: QueryContext = QueryContext(qc.tableIndex, outerContainers ++ qc.outerContainers)
        val condition = ClauseMacro.analysisFilter(f, newContext.outerContainers)
        TableQuery(containers, ast.addWhere(condition))