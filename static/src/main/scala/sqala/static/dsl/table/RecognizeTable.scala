package sqala.static.dsl.table

import sqala.ast.table.*
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.AsMap
import sqala.static.metadata.SqlNumber

import scala.NamedTuple.NamedTuple
import scala.compiletime.{constValue, constValueTuple}
import scala.language.dynamics

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
        RecognizePredefine(r.setPerMatch(__table__, SqlRecognizePatternRowsPerMatchMode.OneRow))

    def allRowsPerMatch(using
        QueryContext, 
        MatchRecognizeContext
    ): RecognizePredefine[T] =
        RecognizePredefine(r.setPerMatch(__table__, SqlRecognizePatternRowsPerMatchMode.AllRows(None)))

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
        val alias = c.fetchAlias
        val items = m.selectItems(f(RecognizeDefine[N, T](__table__)), 1)
        val measureItems = items.map: i =>
            SqlRecognizeMeasureItem(i.expr, i.alias.get)
        val recognize = 
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(
                measures = measureItems,
                alias = Some(SqlTableAlias(alias, Nil))
            )
        RecognizeTable[MN, t.R](
            Some(alias),
            t.toTuple(p.asTableParam(Some(alias), 1)),
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