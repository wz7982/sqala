package sqala.static.dsl.table

import sqala.ast.table.*
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.AsMap
import sqala.static.metadata.SqlNumber

import scala.NamedTuple.NamedTuple
import scala.compiletime.{constValue, constValueTuple}
import scala.language.dynamics

final case class RecognizePredefine[T, L <: Int](
    private[sqala] val __table__ : T
)(using
    private[sqala] val qc: QueryContext[L],
    private[sqala] val mc: MatchRecognizeContext,
    private[sqala] val r: AsRecognize[T]
):
    def sortBy[S](sortValue: S)(using a: AsColumnSort[S, L]): RecognizePredefine[T, L] =
        val sort = a.asSorts(sortValue).map(_.asSqlOrderBy)
        RecognizePredefine(r.setOrderBy(__table__, sort))

    def orderBy[S](sortValue: S)(using a: AsColumnSort[S, L]): RecognizePredefine[T, L] =
        sortBy(sortValue)

    def oneRowPerMatch: RecognizePredefine[T, L] =
        RecognizePredefine(r.setPerMatch(__table__, SqlRecognizePatternRowsPerMatchMode.OneRow))

    def allRowsPerMatch: RecognizePredefine[T, L] =
        RecognizePredefine(r.setPerMatch(__table__, SqlRecognizePatternRowsPerMatchMode.AllRows(None)))

    def predefine[N <: Tuple](using
        t: TransformTableKind[T, [l <: Int] =>> Grouped[Column[l] *: EmptyTuple]],
        rr: AsRecognize[t.R],
    ): Recognize[N, t.R, L] =
        Recognize(t.transform(__table__))

final case class Recognize[N <: Tuple, T, L <: Int](
    private[sqala] val __table__ : T
)(using
    private[sqala] val qc: QueryContext[L],
    private[sqala] val mc: MatchRecognizeContext,
    private[sqala] val r: AsRecognize[T]
):
    inline def define[V <: Tuple](f: RecognizeDefine[N, T, L] => NamedTuple[N, V])(using
        a: AsExpr[V, L],
        kt: KindToTuple[a.K],
        i: CanInRecognizeDefine[kt.R, L]
    ): Recognize[N, T, L] =
        val items = f(RecognizeDefine[N, T, L](__table__))
        val names = constValueTuple[N].toList.map(_.toString)
        val exprs = a.asExprs(items.toTuple)
        val defines = names.zip(exprs).map: (n, e) =>
            SqlRowPatternDefineItem(n, e.asSqlExpr)
        val recognize =
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(rowPattern = recognize.rowPattern.copy(define = defines))
        Recognize(r.setRecognize(__table__, newRecognize))

    def pattern(f: RecognizePattern[N, T, L] => RecognizePatternTerm[L]): Recognize[N, T, L] =
        val p = f(RecognizePattern[N, T, L](__table__))
        val recognize =
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(rowPattern = recognize.rowPattern.copy(pattern = p.pattern))
        Recognize(r.setRecognize(__table__, newRecognize))

    def afterMatchSkipToNextRow: Recognize[N, T, L] =
        val recognize =
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(
                rowPattern = recognize.rowPattern.copy(
                    afterMatch = Some(SqlRowPatternSkipMode.ToNextRow)
                )
            )
        Recognize(r.setRecognize(__table__, newRecognize))

    def afterMatchSkipPastLastRow: Recognize[N, T, L] =
        val recognize =
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(
                rowPattern = recognize.rowPattern.copy(
                    afterMatch = Some(SqlRowPatternSkipMode.PastLastRow)
                )
            )
        Recognize(r.setRecognize(__table__, newRecognize))

    def afterMatchSkipToFirst(f: RecognizePatternName[N, T, L] => String): Recognize[N, T, L] =
        val recognize =
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(
                rowPattern = recognize.rowPattern.copy(
                    afterMatch = Some(
                        SqlRowPatternSkipMode.ToFirst(f(RecognizePatternName[N, T, L](__table__)))
                    )
                )
            )
        Recognize(r.setRecognize(__table__, newRecognize))

    def afterMatchSkipToLast(f: RecognizePatternName[N, T, L] => String): Recognize[N, T, L] =
        val recognize =
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(
                rowPattern = recognize.rowPattern.copy(
                    afterMatch = Some(
                        SqlRowPatternSkipMode.ToLast(f(RecognizePatternName[N, T, L](__table__)))
                    )
                )
            )
        Recognize(r.setRecognize(__table__, newRecognize))

    def afterMatchSkipTo(f: RecognizePatternName[N, T, L] => String): Recognize[N, T, L] =
        val recognize =
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(
                rowPattern = recognize.rowPattern.copy(
                    afterMatch = Some(
                        SqlRowPatternSkipMode.To(f(RecognizePatternName[N, T, L](__table__)))
                    )
                )
            )
        Recognize(r.setRecognize(__table__, newRecognize))

    def measures[MN <: Tuple, MV <: Tuple](f: RecognizeDefine[N, T, L] => NamedTuple[MN, MV])(using
        m: AsMap[MV, L],
        p: AsTableParam[m.R, L],
        t: ToTuple[p.R],
        i: CanInGroupedMap[m.KS],
        e: ExcludeCurrentLevelColumn[m.KS, L],
        refl: e.R =:= EmptyTuple
    ): RecognizeMeasures[MN, t.R, L] =
        val alias = qc.fetchAlias
        val items = m.asSelectItems(f(RecognizeDefine[N, T, L](__table__)), 1)
        val measureItems = items.map: i =>
            SqlRecognizeMeasureItem(i.expr, i.alias.get)
        val recognize =
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(
                measures = measureItems,
                alias = Some(SqlTableAlias(alias, Nil))
            )
        RecognizeMeasures[MN, t.R, L](
            Some(alias),
            t.toTuple(p.asTableParam(Some(alias), 1)),
            r.fetchSqlTable(r.setRecognize(__table__, newRecognize))
        )

final case class RecognizeDefine[N <: Tuple, T, L <: Int](
    private[sqala] val __table__ : T
)(using
    private[sqala] val r: AsRecognize[T]
) extends Dynamic:
    def selectDynamic(name: String): T =
        r.alias(__table__, name)

final class RecognizePattern[N <: Tuple, T, L <: Int](
    private[sqala] val __table__ : T
)(using
    private[sqala] val qc: QueryContext[L],
    private[sqala] val mc: MatchRecognizeContext,
    private[sqala] val r: AsRecognize[T]
) extends Dynamic:
    def selectDynamic(name: String): RecognizePatternTerm[L] =
        RecognizePatternTerm(SqlRowPatternTerm.Pattern(name, None))

final case class RecognizePatternName[N <: Tuple, T, L <: Int](
    private[sqala] val __table__ : T
)(using
    private[sqala] val r: AsRecognize[T]
) extends Dynamic:
    def selectDynamic(name: String): String = name

final case class RecognizePatternTerm[L <: Int](private[sqala] val pattern: SqlRowPatternTerm)(using
    private[sqala] val qc: QueryContext[L],
    private[sqala] val mc: MatchRecognizeContext
):
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

    def + : RecognizePatternTerm[L] =
        RecognizePatternTerm(setQuantifier(SqlRowPatternQuantifier.Plus(false)))

    def * : RecognizePatternTerm[L] =
        RecognizePatternTerm(setQuantifier(SqlRowPatternQuantifier.Asterisk(false)))

    def ? : RecognizePatternTerm[L] =
        RecognizePatternTerm(setQuantifier(SqlRowPatternQuantifier.Question(false)))

    def between[S, E](start: S, end: E)(using
        as: AsExpr[S, L],
        ae: AsExpr[E, L],
        ns: SqlNumber[as.R],
        ne: SqlNumber[ae.R],
        kts: KindToTuple[as.K],
        kte: KindToTuple[ae.K],
        is: CanInSimpleClause[kts.R],
        ie: CanInSimpleClause[kte.R],
        es: ExcludeCurrentLevelColumn[kts.R, L],
        ee: ExcludeCurrentLevelColumn[kte.R, L],
        rs: es.R =:= EmptyTuple,
        re: ee.R =:= EmptyTuple
    ): RecognizePatternTerm[L] =
        RecognizePatternTerm(
            setQuantifier(
                SqlRowPatternQuantifier.Between(
                    Some(as.asExpr(start).asSqlExpr),
                    Some(ae.asExpr(end).asSqlExpr),
                    false
                )
            )
        )

    def least[T](x: T)(using
        a: AsExpr[T, L],
        n: SqlNumber[a.R],
        kt: KindToTuple[a.K],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        refl: e.R =:= EmptyTuple
    ): RecognizePatternTerm[L] =
        RecognizePatternTerm(
            setQuantifier(
                SqlRowPatternQuantifier.Between(
                    Some(a.asExpr(x).asSqlExpr),
                    None,
                    false
                )
            )
        )

    def most[T](x: T)(using
        a: AsExpr[T, L],
        n: SqlNumber[a.R],
        kt: KindToTuple[a.K],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        refl: e.R =:= EmptyTuple
    ): RecognizePatternTerm[L] =
        RecognizePatternTerm(
            setQuantifier(
                SqlRowPatternQuantifier.Between(
                    None,
                    Some(a.asExpr(x).asSqlExpr),
                    false
                )
            )
        )

    def at[T](x: T)(using
        a: AsExpr[T, L],
        n: SqlNumber[a.R],
        kt: KindToTuple[a.K],
        e: ExcludeCurrentLevelColumn[kt.R, L],
        refl: e.R =:= EmptyTuple
    ): RecognizePatternTerm[L] =
        RecognizePatternTerm(
            setQuantifier(
                SqlRowPatternQuantifier.Quantity(
                    a.asExpr(x).asSqlExpr
                )
            )
        )

    def ~(that: RecognizePatternTerm[L]): RecognizePatternTerm[L] =
        RecognizePatternTerm(
            SqlRowPatternTerm.Then(
                this.pattern,
                that.pattern,
                None
            )
        )

    def |(that: RecognizePatternTerm[L]): RecognizePatternTerm[L] =
        RecognizePatternTerm(
            SqlRowPatternTerm.Or(
                this.pattern,
                that.pattern,
                None
            )
        )

final case class RecognizeMeasures[N <: Tuple, V <: Tuple, CL <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable
) extends AnyTable

final case class FromRecognize[N <: Tuple, V <: Tuple, OKS <: Tuple, CL <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable
) extends AnyTable

final case class RecognizeTable[N <: Tuple, V <: Tuple, L <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable
) extends Selectable with AnyTable:
    type Fields = NamedTuple[N, V]

    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)