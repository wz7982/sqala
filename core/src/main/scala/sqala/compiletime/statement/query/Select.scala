package sqala.compiletime.statement.query

import sqala.ast.expr.{SqlExpr, SqlSubQueryPredicate}
import sqala.ast.limit.SqlLimit
import sqala.ast.order.SqlOrderBy
import sqala.ast.statement.{SqlQuery, SqlSelectItem}
import sqala.ast.table.SqlTable
import sqala.compiletime.*

import scala.annotation.targetName
import scala.collection.mutable.ListBuffer

class Select[T <: Tuple, AliasNames <: Tuple, TableNames <: Tuple, Tables](
    val ast: SqlQuery.Select,
    private[sqala] val cols: List[Column[?, ?, ?]],
    private[sqala] val tables: Tables
) extends Query[T, AliasNames]:
    infix def filter(f: Tables => Expr[Boolean, ?]): Select[T, AliasNames, TableNames, Tables] =
        new Select(ast.addWhere(f(tables).toSqlExpr), cols, tables)

    infix def filterIf(test: => Boolean)(f: Tables => Expr[Boolean, ?]): Select[T, AliasNames, TableNames, Tables] =
        if test then new Select(ast.addWhere(f(tables).toSqlExpr), cols, tables) 
        else new Select(ast, cols, tables)

    infix def filterNot(test: => Boolean)(f: Tables => Expr[Boolean, ?]): Select[T, AliasNames, TableNames, Tables] =
        if !test then new Select(ast.addWhere(f(tables).toSqlExpr), cols, tables) 
        else new Select(ast, cols, tables)

    def withFilter(f: Tables => Expr[Boolean, ?]): Select[T, AliasNames, TableNames, Tables] = filter(f)

    @targetName("withFilterBoolean")
    def withFilter(f: Tables => Boolean): Select[T, AliasNames, TableNames, Tables] = this

    infix def take(n: Int): Select[T, AliasNames, TableNames, Tables] =
        val sqlLimit = ast.limit.map(l => SqlLimit(n, l.offset)).orElse(Some(SqlLimit(n, 0)))
        new Select(ast.copy(limit = sqlLimit), cols, tables)

    infix def drop(n: Int): Select[T, AliasNames, TableNames, Tables] =
        val sqlLimit = ast.limit.map(l => SqlLimit(l.limit, n)).orElse(Some(SqlLimit(1, n)))
        new Select(ast.copy(limit = sqlLimit), cols, tables)

    def distinct: Select[T, AliasNames, TableNames, Tables] =
        new Select(ast.copy(param = Some("DISTINCT")), cols, tables)

    infix def map[I <: Tuple](f: Tables => I): Select[SelectTupleType[T, TableNames, I], TupleAliasNames[I], TableNames, Tables] =
        val selectItems = ListBuffer[SqlSelectItem]()
        f(tables).toArray.foreach:
            case expr: Expr[_, _] => selectItems.addOne(SqlSelectItem(expr.toSqlExpr, None))
            case selectItem: SelectItem[_, _, _] => selectItems.addOne(SqlSelectItem(selectItem.expr.toSqlExpr, Some(selectItem.alias)))
            case table: Table[_, _] => selectItems.addAll(table.__cols.map(c => SqlSelectItem(c.toSqlExpr, None)))
        val cols = ListBuffer[Column[?, ?, ?]]()
        f(tables).toArray.foreach:
            case Column(_, columnName, identName) => cols.addOne(Column("", columnName, identName))
            case PrimaryKey(_, columnName, identName) => cols.addOne(Column("", columnName, identName))
            case SelectItem(expr, alias) => cols.addOne(Column("", alias, alias))
            case table: Table[_, _] => cols.addAll(table.__cols)
            case _ => 
        new Select(ast.copy(select = selectItems.toList), cols.toList, tables)

    @targetName("mapExpr")
    infix def map[E <: Expr[?, ?]](f: Tables => E): Select[SelectTupleType[T, TableNames, Tuple1[E]], TupleAliasNames[Tuple1[E]], TableNames, Tables] =
        val expr = f(tables)
        val selectItems = SqlSelectItem(expr.toSqlExpr, None) :: Nil
        val cols = expr match
            case Column(_, columnName, identName) => Column("", columnName, identName) :: Nil
            case PrimaryKey(_, columnName, identName) => Column("", columnName, identName) :: Nil
            case _ => Nil
        new Select(ast.copy(select = selectItems.toList), cols, tables)

    @targetName("mapSelectItem")
    infix def map[A <: String, I <: SelectItem[?, ?, A]](f: Tables => I): Select[SelectTupleType[T, TableNames, Tuple1[I]], Tuple1[A], TableNames, Tables] =
        val item = f(tables)
        val selectItems = SqlSelectItem(item.expr.toSqlExpr, Some(item.alias)) :: Nil
        val cols = Column("", item.alias, item.alias) :: Nil  
        new Select(ast.copy(select = selectItems.toList), cols.toList, tables)

    @targetName("mapTable")
    infix def map[MT <: Table[?, ?]](f: Tables => MT): Select[SelectTupleType[T, TableNames, Tuple1[MT]], EmptyTuple, TableNames, Tables] =
        val table = f(tables)
        val selectItems = table.__cols.map(item => SqlSelectItem(item.toSqlExpr, None))
        val cols = table.__cols.map(_.copy(tableName = ""))
        new Select(ast.copy(select = selectItems.toList), cols, tables)

    infix def sortBy(f: Tables => OrderBy): Select[T, AliasNames, TableNames, Tables] =
        val sqlOrderBy = f(tables) match
            case OrderBy(expr, order) => SqlOrderBy(expr.toSqlExpr, Some(order))
        new Select(ast.copy(orderBy = ast.orderBy :+ sqlOrderBy), cols, tables)

    infix def groupBy[G <: Tuple](f: Tables => G): Select[GroupTupleType[T, TableNames, G], AliasNames, TableNames, (G, Tables)] =
        val exprs = f(tables)
        val sqlGroupBy = exprs.toArray.map:
            case expr: Expr[_, _] => expr.toSqlExpr
        new Select(ast.copy(groupBy = sqlGroupBy.toList), cols, (exprs, tables))

    @targetName("groupByExpr")
    infix def groupBy[G <: Expr[?, ?]](f: Tables => G): Select[GroupTupleType[T, TableNames, Tuple1[G]], AliasNames, TableNames, (G, Tables)] =
        val expr = f(tables)
        val sqlGroupBy = expr.toSqlExpr :: Nil
        new Select(ast.copy(groupBy = sqlGroupBy), cols, (expr, tables))

    infix def having(f: Tables => Expr[Boolean, ?]): Select[T, AliasNames, TableNames, Tables] =
        val expr = f(tables)
        new Select(ast.addHaving(expr.toSqlExpr), cols, tables)

    def count: Select[Tuple1[Long], EmptyTuple, TableNames, Tables] = ast match
        case SqlQuery.Select(_, _, _, _, Nil, _, _, _, _) =>
            new Select(ast.copy(
                select = SqlSelectItem(SqlExpr.Agg("COUNT", Nil, false, Map(), Nil), None) :: Nil,
                limit = None,
                orderBy = Nil
            ), cols, tables)
        case _ =>
            val outerQuery: SqlQuery.Select = SqlQuery.Select(
                select = SqlSelectItem(SqlExpr.Agg("COUNT", Nil, false, Map(), Nil), None) :: Nil,
                from = SqlTable.SubQueryTable(ast, false, "__q__") :: Nil
            )
            new Select(outerQuery, cols, tables)

    def exists: Select[Tuple1[Boolean], EmptyTuple, TableNames, Tables] =
        val outerQuery: SqlQuery.Select = SqlQuery.Select(
            select = SqlSelectItem(SqlExpr.SubQueryPredicate(ast, SqlSubQueryPredicate.Exists), None) :: Nil,
            from = Nil
        )
        new Select(outerQuery, cols, tables)

object Select:
    def apply[T <: Product, TableName <: String](table: Table[T, TableName]): Select[Tuple1[T], EmptyTuple, Tuple1[TableName], table.type] =
        new Select(SqlQuery.Select(select = table.__cols.map(c => SqlSelectItem(c.toSqlExpr, None)), from = table.toSqlTable :: Nil), table.__cols, table)

    def apply[T <: Tuple, TableNames <: Tuple, Tables <: Tuple](table: JoinTable[T, TableNames, Tables]): Select[T, EmptyTuple, TableNames, Tables] =
        new Select(SqlQuery.Select(select = table.__cols.map(c => SqlSelectItem(c.toSqlExpr, None)), from = table.toSqlTable :: Nil), table.__cols, table.__tables)

    def apply[T <: Tuple, TableName <: String, ColumnNames <: Tuple](table: SubQueryTable[T, TableName, ColumnNames]): Select[T, EmptyTuple, Tuple1[TableName], table.type] =
        new Select(SqlQuery.Select(select = table.__cols.map(c => SqlSelectItem(c.toSqlExpr, None)), from = table.toSqlTable :: Nil), table.__cols, table)