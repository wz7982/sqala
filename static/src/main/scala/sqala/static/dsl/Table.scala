package sqala.static.dsl

import sqala.ast.expr.SqlExpr
import sqala.static.metadata.{TableMacro, TableMetaData}

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}
import sqala.ast.table.SqlTable
import sqala.ast.table.SqlTableAlias
import sqala.static.metadata.SqlBoolean
import sqala.ast.table.SqlJoinCondition
import scala.compiletime.constValue
import scala.compiletime.constValueTuple
import sqala.ast.expr.SqlJsonTableColumn
import sqala.ast.expr.SqlType
import sqala.static.dsl.statement.query.Query
import sqala.ast.table.SqlRowPattern
import sqala.ast.table.SqlRowPatternTerm
import sqala.ast.table.SqlRowPatternQuantifier
import sqala.static.metadata.SqlNumber
import scala.language.dynamics
import sqala.ast.table.SqlRowPatternDefineItem
import sqala.ast.table.SqlRowPatternSkipMode
import sqala.ast.table.SqlPatternRowsPerMatchMode
import sqala.static.dsl.statement.query.AsMap
import sqala.ast.table.SqlMeasureItem

// TODO insert表别名为None update也要加表别名
case class Table[T](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __metaData__ : TableMetaData,
    private[sqala] val __sqlTable__ : SqlTable.Standard
) extends Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    def selectDynamic(name: String): Expr[?] =
        val index = __metaData__.fieldNames.indexWhere(f => f == name)
        Expr(SqlExpr.Column(__aliasName__, __metaData__.columnNames(index)))

case class JoinTable[T](
    private[sqala] val params: T,
    private[sqala] val sqlTable: SqlTable.Join
)

case class JoinPart[T](
    private[sqala] val params: T,
    private[sqala] val sqlTable: SqlTable.Join
):
    infix def on[F: AsExpr as a](f: T => F)(using SqlBoolean[a.R]): JoinTable[T] =
        val cond = a.asExpr(f(params))
        JoinTable(
            params,
            sqlTable.copy(condition = Some(SqlJoinCondition.On(cond.asSqlExpr)))
        )

case class FuncTable[T](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __fieldNames__ : List[String],
    private[sqala] val __columnNames__ : List[String],
    private[sqala] val __sqlTable__ : SqlTable.Func
) extends Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    def selectDynamic(name: String): Expr[?] =
        val index = __fieldNames__.indexWhere(f => f == name)
        Expr(SqlExpr.Column(__aliasName__, __columnNames__(index)))

case class JsonTable[N <: Tuple, V <: Tuple](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.Json
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

object JsonTable:
    def apply[N <: Tuple, V <: Tuple](
        expr: SqlExpr,
        path: SqlExpr,
        alias: Option[String], 
        columns: JsonTableColumns[N, V]
    )(using
        p: AsTableParam[JsonTableColumnFlatten[V]]
    ): JsonTable[JsonTableColumnNameFlatten[N, V], JsonTableColumnFlatten[V]] =
        var index = 0

        def toSqlColumns(columns: List[JsonTableColumn]): List[SqlJsonTableColumn] =
            columns.map:
                case p: JsonTablePathColumn[?] =>
                    index += 1
                    SqlJsonTableColumn.Column(s"c$index", p.`type`, None, Some(p.path), None, None, None, None)
                case o: JsonTableOrdinalColumn =>
                    index += 1
                    SqlJsonTableColumn.Ordinality(s"c$index")
                case e: JsonTableExistsColumn =>
                    index += 1
                    SqlJsonTableColumn.Exists(s"c$index", SqlType.Boolean, Some(e.path), None)
                case n: JsonTableNestedColumns[?, ?] =>
                    SqlJsonTableColumn.Nested(n.path, None, toSqlColumns(n.columns))
                
        val sqlColumns = toSqlColumns(columns.columns)
        new JsonTable(
            alias,
            p.asTableParam(alias, 1),
            SqlTable.Json(
                expr,
                path,
                None,
                Nil,
                sqlColumns,
                None,
                false,
                alias.map(SqlTableAlias(_, Nil)),
                None
            )
        )

case class JsonTableColumns[N <: Tuple, V <: Tuple](private[sqala] columns: List[JsonTableColumn])

sealed trait JsonTableColumn
case class JsonTableNestedColumns[N <: Tuple, V <: Tuple](
    private[sqala] path: SqlExpr, 
    private[sqala] columns: List[JsonTableColumn]
) extends JsonTableColumn
class JsonTableOrdinalColumn extends JsonTableColumn
case class JsonTablePathColumn[T](
    private[sqala] path: SqlExpr, 
    private[sqala] `type`: SqlType
) extends JsonTableColumn
case class JsonTableExistsColumn(private[sqala] path: SqlExpr) extends JsonTableColumn

case class SubQueryTable[N <: Tuple, V <: Tuple](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable.SubQuery
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)

object SubQuery:
    def apply[N <: Tuple, V <: Tuple](query: Query[?], lateral: Boolean, alias: Option[String])(using 
        p: AsTableParam[V]
    ): SubQueryTable[N, V] =
        new SubQueryTable(
            alias, 
            p.asTableParam(alias, 1),
            SqlTable.SubQuery(
                query.tree,
                lateral,
                alias.map(SqlTableAlias(_, Nil)),
                None
            )
        )

class MatchRecognizeContext

case class RecognizePredefine[T](
    private[sqala] val __table__ : T
)(using r: AsRecognizeTable[T]):
    def sortBy[S: AsSort as a](sortValue: S)(using
        QueryContext, 
        MatchRecognizeContext
    ): RecognizePredefine[T] =
        val sort = a.asSort(sortValue).map(_.asSqlOrderBy)
        RecognizePredefine(r.setOrderBy(__table__, sort))

    def orderBy[S: AsSort as a](sortValue: S)(using
        QueryContext, 
        MatchRecognizeContext
    ): RecognizePredefine[T] =
        sortBy(sortValue)

    def oneRowPerMatch(using
        QueryContext, 
        MatchRecognizeContext
    ): RecognizePredefine[T] =
        RecognizePredefine(r.setPerMatch(__table__, SqlPatternRowsPerMatchMode.OneRow))

    def allRowsPerMatch(using
        QueryContext, 
        MatchRecognizeContext
    ): RecognizePredefine[T] =
        RecognizePredefine(r.setPerMatch(__table__, SqlPatternRowsPerMatchMode.AllRows(None)))

    def predefine[N <: Tuple](using
        QueryContext, 
        MatchRecognizeContext
    ): Recognize[N, T] =
        Recognize(__table__)

case class Recognize[N <: Tuple, T](
    private[sqala] val __table__ : T
)(using r: AsRecognizeTable[T]) extends Dynamic:
    inline def define[V <: Tuple](f: RecognizeDefine[N, T] => NamedTuple[N, V])(using
        a: AsExpr[V], 
        i: CanIn[Boolean, V],
        c: QueryContext, 
        mc: MatchRecognizeContext
    ): Recognize[N, T] =
        val items = f(RecognizeDefine[N, T](__table__))
        val names = constValueTuple[N].toList.map(_.toString)
        val exprs = a.exprs(items.toTuple)
        val defines = names.zip(exprs).map: (n, e) =>
            SqlRowPatternDefineItem(n, e.asSqlExpr)
        val recognize = 
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(rowPattern = recognize.rowPattern.copy(define = defines))
        Recognize(r.setRecognize(__table__, newRecognize))

    def pattern(f: RecognizePattern[N, T] => Pattern)(using
        QueryContext, 
        MatchRecognizeContext
    ): Recognize[N, T] =
        val p = f(RecognizePattern[N, T](__table__))
        val recognize = 
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(rowPattern = recognize.rowPattern.copy(pattern = p.pattern))
        Recognize(r.setRecognize(__table__, newRecognize))

    def afterMatchSkipToNextRow(using QueryContext, MatchRecognizeContext): Recognize[N, T] =
        val recognize = 
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(
                rowPattern = recognize.rowPattern.copy(
                    afterMatch = Some(SqlRowPatternSkipMode.SkipToNextRow)
                )
            )
        Recognize(r.setRecognize(__table__, newRecognize))

    def afterMatchSkipPastLastRow(using QueryContext, MatchRecognizeContext): Recognize[N, T] =
        val recognize = 
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(
                rowPattern = recognize.rowPattern.copy(
                    afterMatch = Some(SqlRowPatternSkipMode.SkipPastLastRow)
                )
            )
        Recognize(r.setRecognize(__table__, newRecognize))

    def afterMatchSkipToFirst(f: RecognizePatternName[N, T] => String)(using 
        QueryContext, 
        MatchRecognizeContext
    ): Recognize[N, T] =
        val recognize = 
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(
                rowPattern = recognize.rowPattern.copy(
                    afterMatch = Some(
                        SqlRowPatternSkipMode.SkipToFirst(f(RecognizePatternName[N, T](__table__)))
                    )
                )
            )
        Recognize(r.setRecognize(__table__, newRecognize))

    def afterMatchSkipToLast(f: RecognizePatternName[N, T] => String)(using 
        QueryContext, 
        MatchRecognizeContext
    ): Recognize[N, T] =
        val recognize = 
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(
                rowPattern = recognize.rowPattern.copy(
                    afterMatch = Some(
                        SqlRowPatternSkipMode.SkipToLast(f(RecognizePatternName[N, T](__table__)))
                    )
                )
            )
        Recognize(r.setRecognize(__table__, newRecognize))

    def afterMatchSkipTo(f: RecognizePatternName[N, T] => String)(using 
        QueryContext, 
        MatchRecognizeContext
    ): Recognize[N, T] =
        val recognize = 
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(
                rowPattern = recognize.rowPattern.copy(
                    afterMatch = Some(
                        SqlRowPatternSkipMode.SkipTo(f(RecognizePatternName[N, T](__table__)))
                    )
                )
            )
        Recognize(r.setRecognize(__table__, newRecognize))

    def measures[MN <: Tuple, MV <: Tuple](f: RecognizeDefine[N, T] => NamedTuple[MN, MV])(using
        m: AsMap[MV],
        t: ToTuple[m.R],
        p: AsTableParam[m.R],
        c: QueryContext,
        mc: MatchRecognizeContext
    ): RecognizeTable[MN, t.R] =
        c.tableIndex += 1
        val aliasName = s"t${c.tableIndex}"
        val items = m.selectItems(f(RecognizeDefine[N, T](__table__)), 1)
        val measureItems = items.map: i =>
            SqlMeasureItem(i.expr, i.alias.get)
        val recognize = 
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(
                measures = measureItems,
                alias = Some(SqlTableAlias(aliasName, Nil))
            )
        RecognizeTable[MN, t.R](
            Some(aliasName),
            t.toTuple(p.asTableParam(Some(aliasName), 1)),
            r.fetchSqlTable(r.setRecognize(__table__, newRecognize))
        )

case class RecognizeDefine[N <: Tuple, T](
    private[sqala] val __table__ : T
)(using r: AsRecognizeTable[T]) extends Dynamic:
    def selectDynamic(name: String): T =
        r.alias(__table__, name)

case class RecognizePattern[N <: Tuple, T](
    private[sqala] val __table__ : T
)(using AsRecognizeTable[T]) extends Dynamic:
    def selectDynamic(name: String): Pattern =
        new Pattern(SqlRowPatternTerm.Pattern(name, None))

case class RecognizePatternName[N <: Tuple, T](
    private[sqala] val __table__ : T
)(using AsRecognizeTable[T]) extends Dynamic:
    def selectDynamic(name: String): String = name

class Pattern(val pattern: SqlRowPatternTerm):
    private def setQuantifier(quantifier: SqlRowPatternQuantifier): SqlRowPatternTerm =
        pattern match
            case p: SqlRowPatternTerm.Pattern =>
                p.copy(quantifier = Some(quantifier))
            case c: SqlRowPatternTerm.Circumflex =>
                c.copy(quantifier = Some(quantifier))
            case d: SqlRowPatternTerm.Dollar =>
                d.copy(quantifier = Some(quantifier))
            case e: SqlRowPatternTerm.Exclusion =>
                e.copy(quantifier = Some(quantifier))
            case p: SqlRowPatternTerm.Permute =>
                p.copy(quantifier = Some(quantifier))
            case t: SqlRowPatternTerm.Then =>
                t.copy(quantifier = Some(quantifier))
            case o: SqlRowPatternTerm.Or =>
                o.copy(quantifier = Some(quantifier))

    def +(using QueryContext, MatchRecognizeContext): Pattern =
        new Pattern(setQuantifier(SqlRowPatternQuantifier.Plus(false)))

    def *(using QueryContext, MatchRecognizeContext): Pattern =
        new Pattern(setQuantifier(SqlRowPatternQuantifier.Asterisk(false)))

    def ?(using QueryContext, MatchRecognizeContext): Pattern =
        new Pattern(setQuantifier(SqlRowPatternQuantifier.Question(false)))

    def between[S: AsExpr as s, E: AsExpr as e](start: S, end: E)(using
        SqlNumber[s.R],
        SqlNumber[e.R],
        QueryContext,
        MatchRecognizeContext
    ): Pattern =
        new Pattern(
            setQuantifier(
                SqlRowPatternQuantifier.Between(
                    Some(s.asExpr(start).asSqlExpr),
                    Some(e.asExpr(end).asSqlExpr),
                    false
                )
            )
        )

    def min[T: AsExpr as a](x: T)(using QueryContext, MatchRecognizeContext): Pattern =
        new Pattern(
            setQuantifier(
                SqlRowPatternQuantifier.Between(
                    Some(a.asExpr(x).asSqlExpr),
                    None,
                    false
                )
            )
        )

    def max[T: AsExpr as a](x: T)(using QueryContext, MatchRecognizeContext): Pattern =
        new Pattern(
            setQuantifier(
                SqlRowPatternQuantifier.Between(
                    None,
                    Some(a.asExpr(x).asSqlExpr),
                    false
                )
            )
        )

    def apply[T: AsExpr as a](x: T)(using QueryContext, MatchRecognizeContext): Pattern =
        new Pattern(
            setQuantifier(
                SqlRowPatternQuantifier.Quantity(
                    a.asExpr(x).asSqlExpr
                )
            )
        )

    def ~(that: Pattern)(using QueryContext, MatchRecognizeContext): Pattern =
        new Pattern(
            SqlRowPatternTerm.Then(
                this.pattern,
                that.pattern,
                None
            )
        )

    def |(that: Pattern)(using QueryContext, MatchRecognizeContext): Pattern =
        new Pattern(
            SqlRowPatternTerm.Or(
                this.pattern,
                that.pattern,
                None
            )
        )

case class RecognizeTable[N <: Tuple, V <: Tuple](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable
) extends Selectable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)