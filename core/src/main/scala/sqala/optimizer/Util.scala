package sqala.optimizer

import sqala.ast.expr.*
import sqala.ast.group.SqlGroupItem
import sqala.ast.limit.SqlLimit
import sqala.ast.order.*
import sqala.ast.statement.*
import sqala.ast.table.*
import sqala.util.*

def modidyExpr(f: SqlExpr => SqlExpr)(expr: SqlExpr): SqlExpr = 
    expr match
        case SqlExpr.Vector(items) => 
            SqlExpr.Vector(items.map(f))
        case SqlExpr.Unary(expr, op) =>
            SqlExpr.Unary(f(expr), op)
        case SqlExpr.Binary(left, op, right) =>
            SqlExpr.Binary(f(left), op, f(right))
        case SqlExpr.NullTest(expr, not) =>
            SqlExpr.NullTest(f(expr), not)
        case SqlExpr.Func(name, args, distinct, orderBy, withinGroup, filter) =>
            SqlExpr.Func(
                name,
                args.map(f),
                distinct,
                orderBy.map(modifyOrderBy(f)),
                withinGroup.map(modifyOrderBy(f)),
                filter.map(f)
            )
        case SqlExpr.Between(expr, start, end, not) =>
            SqlExpr.Between(f(expr), f(start), f(end), not)
        case SqlExpr.Case(branches, default) =>
            SqlExpr.Case(
                branches.map(c => c.copy(f(c.whenExpr), f(c.thenExpr))), 
                f(default)
            )
        case SqlExpr.Cast(expr, castType) =>
            SqlExpr.Cast(f(expr), castType)
        case SqlExpr.Window(expr, partitionBy, orderBy, frame) =>
            SqlExpr.Window(
                f(expr),
                partitionBy.map(f),
                orderBy.map(modifyOrderBy(f)),
                frame
            )
        case SqlExpr.Extract(unit, expr) =>
            SqlExpr.Extract(unit, f(expr))
        case SqlExpr.Grouping(items) =>
            SqlExpr.Grouping(items.map(f))
        case e => e

def modifyOrderBy(f: SqlExpr => SqlExpr)(order: SqlOrderBy): SqlOrderBy =
    order.copy(expr = f(order.expr))

def modifyGroupBy(f: SqlExpr => SqlExpr)(group: SqlGroupItem): SqlGroupItem =
    group match
        case SqlGroupItem.Singleton(item) =>
            SqlGroupItem.Singleton(f(item))
        case SqlGroupItem.Cube(items) =>
            SqlGroupItem.Cube(items.map(f))
        case SqlGroupItem.Rollup(items) =>
            SqlGroupItem.Rollup(items.map(f))
        case SqlGroupItem.GroupingSets(items) =>
            SqlGroupItem.GroupingSets(items.map(f))

def modifyJoinCondition(f: SqlExpr => SqlExpr)(table: SqlTable): SqlTable =
    table match
        case SqlTable.JoinTable(left, joinType, right, condition) =>
            val cond = condition.map:
                case SqlJoinCondition.On(c) =>
                    SqlJoinCondition.On(f(c))
                case SqlJoinCondition.Using(exprs) =>
                    SqlJoinCondition.Using(exprs.map(f))
            SqlTable.JoinTable(left, joinType, right, cond)
        case t => t

def modifyTable(f: SqlTable => SqlTable)(table: SqlTable): SqlTable =
    table match
        case SqlTable.JoinTable(left, joinType, right, condition) =>
            SqlTable.JoinTable(f(left), joinType, f(right), condition)
        case t => t
    
def modifyQuery(f: SqlQuery => SqlQuery)(query: SqlQuery): SqlQuery =
    query match
        case SqlQuery.Union(left, unionType, right, orderBy, limit) =>
            SqlQuery.Union(f(left), unionType, f(right), orderBy, limit)
        case q => q

def modifyQueryExpr(f: SqlExpr => SqlExpr)(query: SqlQuery): SqlQuery =
    query match
        case SqlQuery.Select(param, select, from, where, groupBy, having, orderBy, limit) =>
            SqlQuery.Select(
                param,
                select.map:
                    case SqlSelectItem.Item(expr, alias) =>
                        SqlSelectItem.Item(f(expr), alias)
                    case s => s
                ,
                from,
                where.map(f),
                groupBy.map(modifyGroupBy(f)),
                having.map(f),
                orderBy.map(modifyOrderBy(f)),
                limit.map(l => SqlLimit(f(l.limit), f(l.offset)))
            )
        case SqlQuery.Union(left, unionType, right, orderBy, limit) =>
            SqlQuery.Union(
                modifyQueryExpr(f)(left), 
                unionType, 
                modifyQueryExpr(f)(right),
                orderBy.map(modifyOrderBy(f)),
                limit.map(l => SqlLimit(f(l.limit), f(l.offset)))
            )
        case q => q
    
def analysisExpr[A](default: A)(f: SqlExpr => A)(merge: (A, A) => A)(expr: SqlExpr): A =
    expr match
        case SqlExpr.Vector(items) => 
            items.map(f).fold(default)(merge)
        case SqlExpr.Unary(expr, op) =>
            f(expr)
        case SqlExpr.Binary(left, op, right) =>
            merge(f(left), f(right))
        case SqlExpr.NullTest(expr, not) =>
            f(expr)
        case SqlExpr.Func(name, args, distinct, orderBy, withinGroup, filter) =>
            val analysisArg = args.map(f).fold(default)(merge)
            val analysisOrder = orderBy.map(o => f(o.expr)).fold(default)(merge)
            val analysisWithinGroup = withinGroup.map(o => f(o.expr)).fold(default)(merge)
            val analysisFilter = filter.map(f).getOrElse(default)
            analysisArg |> 
            ((x: A) => merge(x, analysisOrder)) |>
            ((x: A) => merge(x, analysisWithinGroup)) |>
            ((x: A) => merge(x, analysisFilter))
        case SqlExpr.Between(expr, start, end, not) =>
            val analysisExpr = f(expr)
            val analysisStart = f(start)
            val analysisEnd = f(end)
            merge(merge(analysisExpr, analysisStart), analysisEnd)
        case SqlExpr.Case(branches, defaultExpr) =>
            val analysisBranches = branches.map(c => merge(f(c.whenExpr), f(c.thenExpr))).fold(default)(merge)
            val analysisDefault = f(defaultExpr)
            merge(analysisBranches, analysisDefault)
        case SqlExpr.Cast(expr, castType) =>
            f(expr)
        case SqlExpr.Window(expr, partitionBy, orderBy, frame) =>
            val analysisExpr = f(expr)
            val analysisPartition = merge(_, partitionBy.map(f).fold(default)(merge))
            val analysisOrder = merge(_, orderBy.map(o => f(o.expr)).fold(default)(merge))
            analysisExpr |> analysisPartition |> analysisOrder
        case SqlExpr.Extract(unit, expr) =>
            f(expr)
        case SqlExpr.Grouping(items) =>
            items.map(f).fold(default)(merge)
        case e => default

def analysisJoinCondition[A](default: A)(f: SqlExpr => A)(merge: (A, A) => A)(table: SqlTable): A =
    table match
        case SqlTable.JoinTable(_, _, _, condition) =>
            condition.map:
                case SqlJoinCondition.On(c) =>
                    f(c)
                case SqlJoinCondition.Using(exprs) =>
                    exprs.map(f).fold(default)(merge)
            .getOrElse(default)
        case t => default

def analysisTable[A](default: A)(f: SqlTable => A)(merge: (A, A) => A)(table: SqlTable): A =
    table match
        case SqlTable.JoinTable(left, _, right, _) =>
            merge(f(left), f(right))
        case _ => default

def analysisQuery[A](default: A)(f: SqlQuery => A)(merge: (A, A) => A)(query: SqlQuery): A =
    query match
        case SqlQuery.Union(left, _, right, _, _) =>
            merge(f(left), f(right))
        case _ => default

def analysisQueryExpr[A](default: A)(f: SqlExpr => A)(merge: (A, A) => A)(query: SqlQuery): A =
    val mergeFunc = (x: A) => (y: A) => merge(x, y)
    query match
        case SqlQuery.Select(_, select, from, where, groupBy, having, orderBy, limit) =>
            val analysisSelect = select.map:
                case SqlSelectItem.Item(expr, _) => f(expr)
                case SqlSelectItem.Wildcard(_) => default
            .fold(default)(merge)
            val analysisWhere = where.map(f).getOrElse(default)
            val analysisGroup = groupBy.map:
                case SqlGroupItem.Singleton(item) => f(item)
                case SqlGroupItem.Cube(items) => items.map(f).fold(default)(merge)
                case SqlGroupItem.Rollup(items) => items.map(f).fold(default)(merge)
                case SqlGroupItem.GroupingSets(items) => items.map(f).fold(default)(merge)
            .fold(default)(merge)
            val analysisHaving = having.map(f).getOrElse(default)
            val analysisOrder = orderBy.map(o => f(o.expr)).fold(default)(merge)
            val analysisLimit = limit.map(l => merge(f(l.limit), f(l.offset))).getOrElse(default)
            analysisSelect |> 
            mergeFunc(analysisWhere) |> 
            mergeFunc(analysisGroup) |>
            mergeFunc(analysisHaving) |>
            mergeFunc(analysisOrder) |>
            mergeFunc(analysisLimit)
        case SqlQuery.Union(left, unionType, right, orderBy, limit) =>
            val analysisLeft = analysisQueryExpr(default)(f)(merge)(left)
            val analysisRight = analysisQueryExpr(default)(f)(merge)(right)
            val analysisOrder = orderBy.map(o => f(o.expr)).fold(default)(merge)
            val analysisLimit = limit.map(l => merge(f(l.limit), f(l.offset))).getOrElse(default)
            analysisLeft |>
            mergeFunc(analysisRight) |> 
            mergeFunc(analysisOrder) |>
            mergeFunc(analysisLimit)
        case q => default

def hasAgg(expr: SqlExpr, metaData: List[FuncMetaData]): Boolean =
    expr match
        case SqlExpr.Func(name, _, _, _, _, _) =>
            metaData
                .find(m => m.name.toUpperCase == name.toUpperCase)
                .map(_.kind == FuncKind.Agg)
                .getOrElse(false)
        case SqlExpr.Grouping(_) => true
        case w: SqlExpr.Window => false
        case e => analysisExpr(false)(x => hasAgg(x, metaData))(_ || _)(e)

def hasWindow(expr: SqlExpr): Boolean =
    expr match
        case _: SqlExpr.Window => true
        case e => analysisExpr(false)(hasWindow)(_ || _)(e)
    
def hasSubLink(expr: SqlExpr): Boolean =
    expr match
        case _: SqlExpr.SubLink => true
        case _: SqlExpr.SubQuery => true
        case e => analysisExpr(false)(hasSubLink)(_ || _)(e)

def queryAsAst(query: Query): SqlQuery =
    def asAst(expr: Expr): SqlExpr =
        expr match
            case Expr.Null => SqlExpr.Null
            case Expr.StringConstant(v) => SqlExpr.StringLiteral(v)
            case Expr.NumberConstant(v) => SqlExpr.NumberLiteral(v)
            case Expr.BooleanConstant(v) => SqlExpr.BooleanLiteral(v)
            case Expr.VarRef(v) => SqlExpr.Column(v.tableName, v.varName)
            case Expr.Vector(items) => SqlExpr.Vector(items.map(asAst))
            case Expr.Unary(expr, op) =>
                val exprOp = op match
                    case UnaryOperator.Positive => SqlUnaryOperator.Positive
                    case UnaryOperator.Negative => SqlUnaryOperator.Negative
                    case UnaryOperator.Not => SqlUnaryOperator.Not
                SqlExpr.Unary(asAst(expr), exprOp)
            case Expr.Binary(left, op, right) =>
                val exprOp = op match
                    case BinaryOperator.Times => SqlBinaryOperator.Times
                    case BinaryOperator.Div => SqlBinaryOperator.Div
                    case BinaryOperator.Mod => SqlBinaryOperator.Mod
                    case BinaryOperator.Plus => SqlBinaryOperator.Plus
                    case BinaryOperator.Minus => SqlBinaryOperator.Minus
                    case BinaryOperator.Equal => SqlBinaryOperator.Equal
                    case BinaryOperator.NotEqual => SqlBinaryOperator.NotEqual
                    case BinaryOperator.In => SqlBinaryOperator.In
                    case BinaryOperator.NotIn => SqlBinaryOperator.NotIn
                    case BinaryOperator.GreaterThan => SqlBinaryOperator.GreaterThan
                    case BinaryOperator.GreaterThanEqual => SqlBinaryOperator.GreaterThanEqual
                    case BinaryOperator.LessThan => SqlBinaryOperator.LessThan
                    case BinaryOperator.LessThanEqual => SqlBinaryOperator.LessThanEqual
                    case BinaryOperator.Like => SqlBinaryOperator.Like
                    case BinaryOperator.NotLike => SqlBinaryOperator.NotLike
                    case BinaryOperator.And => SqlBinaryOperator.And
                    case BinaryOperator.Or => SqlBinaryOperator.Or
                SqlExpr.Binary(asAst(left), exprOp, asAst(right))
            case Expr.NullTest(expr, not) => SqlExpr.NullTest(asAst(expr), not)
            case Expr.Func(name, args) => SqlExpr.Func(name, args.map(asAst))
            case Expr.Agg(name, args, distinct, sort) => 
                SqlExpr.Func(
                    name, 
                    args.map(asAst), 
                    distinct, 
                    sort.map: s =>
                        SqlOrderBy(
                            asAst(s.expr), 
                            if s.asc then Some(SqlOrderByOption.Asc) else Some(SqlOrderByOption.Desc),
                            None
                        )
                )
            case Expr.Between(expr, start, end, not) => 
                SqlExpr.Between(asAst(expr), asAst(start), asAst(end), not)
            case Expr.Case(branches, default) => 
                SqlExpr.Case(branches.map(b => SqlCase(asAst(b._1), asAst(b._2))), asAst(default))
            case Expr.Window(expr, partition, sort) =>
                SqlExpr.Window(
                    asAst(expr), 
                    partition.map(asAst),
                    sort.map: s =>
                        SqlOrderBy(
                            asAst(s.expr), 
                            if s.asc then Some(SqlOrderByOption.Asc) else Some(SqlOrderByOption.Desc),
                            None
                        ),
                    None
                )
            case Expr.SubQuery(query) => SqlExpr.SubQuery(queryAsAst(query))
            case Expr.SubLink(query, linkType) =>
                val exprLinkType = linkType match
                    case SubLinkType.Any => SqlSubLinkType.Any
                    case SubLinkType.All => SqlSubLinkType.All
                    case SubLinkType.Exists => SqlSubLinkType.Exists
                    case SubLinkType.NotExists => SqlSubLinkType.NotExists
                SqlExpr.SubLink(queryAsAst(query), exprLinkType)

    def tableAsAst(table: TableEntry): SqlTable =
        table match
            case TableEntry.Relation(name, alias) =>
                SqlTable.IdentTable(name, alias.map(a => SqlTableAlias(a.alias, a.columnAlias)))
            case TableEntry.SubQuery(query, lateral, alias) =>
                SqlTable.SubQueryTable(queryAsAst(query), lateral, SqlTableAlias(alias.alias, alias.columnAlias))

    def fromAsAst(tables: List[TableEntry], from: FromExpr): SqlTable =
        from match
            case FromExpr.TableRef(tableIndex) => tableAsAst(tables(tableIndex))
            case FromExpr.JoinExpr(left, joinType, right, condition) =>
                val astJoinType = joinType match
                    case JoinType.Inner => SqlJoinType.InnerJoin
                    case JoinType.Left => SqlJoinType.LeftJoin
                    case JoinType.Right => SqlJoinType.RightJoin
                    case JoinType.Semi => SqlJoinType.SemiJoin
                    case JoinType.Anti => SqlJoinType.AntiJoin
                SqlTable.JoinTable(
                    fromAsAst(tables, left), 
                    astJoinType, 
                    fromAsAst(tables, right),
                    Some(SqlJoinCondition.On(asAst(condition)))
                )

    SqlQuery.Select(
        param = if query.distinct then Some(SqlSelectParam.Distinct) else None,
        select = query.target.map(t => SqlSelectItem.Item(asAst(t.expr), Some(t.alias))),
        from = fromAsAst(query.tableList, query.joinTree) :: Nil,
        where = query.filter.map(asAst),
        groupBy = query.group.map(g => SqlGroupItem.Singleton(asAst(g))),
        having = query.having.map(asAst),
        orderBy = query.sort.map(s => SqlOrderBy(asAst(s.expr), if s.asc then Some(SqlOrderByOption.Asc) else Some(SqlOrderByOption.Desc), None)),
        limit = query.limitAndOffset.map(l => SqlLimit(SqlExpr.NumberLiteral(l._1), SqlExpr.NumberLiteral(l._2)))
    )

def collectConjunct(expr: Expr): List[Expr] =
    expr match
        case Expr.Binary(left, BinaryOperator.And, right) =>
            collectConjunct(left) ++ collectConjunct(right)
        case e => e :: Nil

def modifyAnalysisExpr(f: Expr => Expr)(expr: Expr): Expr = 
    expr match
        case Expr.Vector(items) => 
            Expr.Vector(items.map(f))
        case Expr.Unary(expr, op) =>
            Expr.Unary(f(expr), op)
        case Expr.Binary(left, op, right) =>
            Expr.Binary(f(left), op, f(right))
        case Expr.NullTest(expr, not) =>
            Expr.NullTest(f(expr), not)
        case Expr.Func(name, args) =>
            Expr.Func(name, args.map(f))
        case Expr.Agg(name, args, distinct, sort) =>
            Expr.Agg(name, args.map(f), distinct, sort.map(s => Sort(f(s.expr), s.asc)))
        case Expr.Between(expr, start, end, not) =>
            Expr.Between(f(expr), f(start), f(end), not)
        case Expr.Case(branches, default) =>
            Expr.Case(
                branches.map(c => c.copy(f(c._1), f(c._2))), 
                f(default)
            )
        case Expr.Window(expr, partition, sort) =>
            Expr.Window(
                f(expr),
                partition.map(f),
                sort.map(s => Sort(f(s.expr), s.asc))
            )
        case e => e