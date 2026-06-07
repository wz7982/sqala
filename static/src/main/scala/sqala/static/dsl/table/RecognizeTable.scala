package sqala.static.dsl.table

import sqala.ast.table.*
import sqala.metadata.SqlNumber
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.AsMap

import scala.NamedTuple.NamedTuple
import scala.compiletime.{constValue, constValueTuple}
import scala.language.dynamics

/**
 * Intermediate builder after `partitionBy` and `sortBy` in
 * `matchRecognize`.
 */
final case class RecognizePredefine[T, L <: Int](
    private[sqala] val __table__ : T
)(using
    private[sqala] val qc: QueryContext[L],
    private[sqala] val mc: MatchRecognizeContext,
    private[sqala] val r: AsRecognize[T]
):
    /**
     * Specifies `ORDER BY` for `matchRecognize`. The sort
     * value must be a column expression.
     *
     * {{{
     * s.sortBy(s.tradeTime)
     * }}}
     */
    def sortBy[S](sortValue: S)(using a: AsColumnSort[S, L]): RecognizePredefine[T, L] =
        val sort = a.asSorts(sortValue).map(_.asSqlOrderingItem)
        RecognizePredefine(r.setOrderBy(__table__, sort))

    /**
     * Alias of `sortBy`, provided for users familiar with `ORDER BY`.
     *
     * {{{
     * s.orderBy(s.tradeTime)
     * }}}
     */
    def orderBy[S](sortValue: S)(using a: AsColumnSort[S, L]): RecognizePredefine[T, L] =
        sortBy(sortValue)

    /**
     * Returns one row per match. Maps to `ONE ROW PER MATCH`.
     *
     * {{{
     * s.oneRowPerMatch
     * }}}
     */
    def oneRowPerMatch: RecognizePredefine[T, L] =
        RecognizePredefine(r.setPerMatch(__table__, SqlRecognizePatternRowsPerMatchMode.OneRow))

    /**
     * Returns all rows per match. Maps to `ALL ROWS PER MATCH`.
     *
     * {{{
     * s.allRowsPerMatch
     * }}}
     */
    def allRowsPerMatch: RecognizePredefine[T, L] =
        RecognizePredefine(r.setPerMatch(__table__, SqlRecognizePatternRowsPerMatchMode.AllRows(None)))

    /**
     * Pre-defines pattern variable labels as a literal type tuple.
     * After this call, pattern variables become available as typed
     * fields on the `define`, `pattern`, and `measures` context objects.
     *
     * {{{
     * .predefine[("start", "down", "up")]
     * }}}
     */
    def predefine[N <: Tuple](using
        t: TransformTableKind[T, [l <: Int] =>> Grouped[Column[l] *: EmptyTuple]],
        rr: AsRecognize[t.R],
    ): Recognize[N, t.R, L] =
        Recognize(t.transform(__table__))

/**
 * Main builder for `matchRecognize` pattern definition. After
 * `predefine`.
 */
final case class Recognize[N <: Tuple, T, L <: Int](
    private[sqala] val __table__ : T
)(using
    private[sqala] val qc: QueryContext[L],
    private[sqala] val mc: MatchRecognizeContext,
    private[sqala] val r: AsRecognize[T]
):
    /**
     * Defines the conditions for each pattern variable.
     *
     * {{{
     * .define(d => (
     *     start = true,
     *     down = d.down.price < prev(d.down.price),
     *     up = d.up.price > prev(d.up.price)
     * ))
     * }}}
     */
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

    /**
     * Defines the row pattern expression using the pattern variable
     * names from `predefine`.
     *
     * {{{
     * .pattern(d => d.start ~ d.down.+ ~ d.up.+)
     * }}}
     */
    def pattern(f: RecognizePattern[N, T, L] => RecognizePatternTerm[L]): Recognize[N, T, L] =
        val p = f(RecognizePattern[N, T, L](__table__))
        val recognize =
            r.fetchRecognize(__table__)
        val newRecognize = recognize
            .copy(rowPattern = recognize.rowPattern.copy(pattern = p.pattern))
        Recognize(r.setRecognize(__table__, newRecognize))

    /**
     * Resumes pattern matching at the next row after a match.
     * Maps to `AFTER MATCH SKIP TO NEXT ROW`.
     */
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

    /**
     * Resumes pattern matching after the last row of the match.
     * Maps to `AFTER MATCH SKIP PAST LAST ROW`.
     */
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

    /**
     * Resumes pattern matching at the first occurrence of a
     * pattern variable. Maps to `AFTER MATCH SKIP TO FIRST p`.
     *
     * {{{
     * .afterMatchSkipToFirst(d => d.up)
     * }}}
     */
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

    /**
     * Resumes pattern matching at the last occurrence of a
     * pattern variable. Maps to `AFTER MATCH SKIP TO LAST p`.
     *
     * {{{
     * .afterMatchSkipToLast(d => d.up)
     * }}}
     */
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

    /**
     * Resumes pattern matching at a pattern variable.
     * Maps to `AFTER MATCH SKIP TO p`.
     *
     * {{{
     * .afterMatchSkipTo(d => d.up)
     * }}}
     */
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

    /**
     * Defines the output measures (columns) of the
     * `matchRecognize` table.
     *
     * {{{
     * .measures(d => (
     *     startTime = d.start.tradeTime,
     *     bottomTime = d.bottom.tradeTime,
     *     endTime = finalized(last(d.up.tradeTime)),
     *     matchNum = matchNumber(),
     *     matchVar = classifier()
     * ))
     * }}}
     */
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

/**
 * Dynamic context for `define` and `measures` clauses.
 */
final case class RecognizeDefine[N <: Tuple, T, L <: Int](
    private[sqala] val __table__ : T
)(using
    private[sqala] val r: AsRecognize[T]
) extends Dynamic:
    def selectDynamic(name: String): T =
        r.alias(__table__, name)

/**
 * Dynamic context for `pattern` clauses.
 */
final class RecognizePattern[N <: Tuple, T, L <: Int](
    private[sqala] val __table__ : T
)(using
    private[sqala] val qc: QueryContext[L],
    private[sqala] val mc: MatchRecognizeContext,
    private[sqala] val r: AsRecognize[T]
) extends Dynamic:
    def selectDynamic(name: String): RecognizePatternTerm[L] =
        RecognizePatternTerm(SqlRowPatternTerm.Pattern(name, None))

/**
 * Dynamic context for `afterMatchSkipTo` clauses.
 */
final case class RecognizePatternName[N <: Tuple, T, L <: Int](
    private[sqala] val __table__ : T
)(using
    private[sqala] val r: AsRecognize[T]
) extends Dynamic:
    def selectDynamic(name: String): String =
        name

/**
 * A term in a `matchRecognize` row pattern.
 */
final case class RecognizePatternTerm[L <: Int](private[sqala] val pattern: SqlRowPatternTerm)(using
    private[sqala] val qc: QueryContext[L],
    private[sqala] val mc: MatchRecognizeContext
):
     /**
     * Applies a quantifier to the underlying pattern term, returning a new
     * term with the updated quantifier.
     */
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

    /**
     * One or more repetitions. Maps to `+`.
     *
     * {{{
     * d.down.+
     * }}}
     */
    def + : RecognizePatternTerm[L] =
        RecognizePatternTerm(setQuantifier(SqlRowPatternQuantifier.Plus(false)))

    /**
     * Zero or more repetitions. Maps to `*`.
     *
     * {{{
     * d.down.*
     * }}}
     */
    def * : RecognizePatternTerm[L] =
        RecognizePatternTerm(setQuantifier(SqlRowPatternQuantifier.Asterisk(false)))

    /**
     * Zero or one repetition. Maps to `?`.
     *
     * {{{
     * d.down.?
     * }}}
     */
    def ? : RecognizePatternTerm[L] =
        RecognizePatternTerm(setQuantifier(SqlRowPatternQuantifier.Question(false)))

    /**
     * Between `m` and `n` repetitions. Maps to `{m, n}`.
     *
     * {{{
     * d.down.between(1, 10)
     * }}}
     */
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

    /**
     * At least `n` repetitions. Maps to `{m,}`.
     *
     * {{{
     * d.down.least(3)
     * }}}
     */
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

    /**
     * At most `n` repetitions. Maps to `{,n}`.
     *
     * {{{
     * d.down.most(10)
     * }}}
     */
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

    /**
     * Exactly `n` repetitions. Maps to `{n}`.
     *
     * {{{
     * d.down.at(3)
     * }}}
     */
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

    /**
     * Concatenates two pattern terms. Maps to SQL row pattern
     * sequence (space-separated).
     *
     * {{{
     * d.start ~ d.down.+
     * }}}
     */
    def ~(that: RecognizePatternTerm[L]): RecognizePatternTerm[L] =
        RecognizePatternTerm(
            SqlRowPatternTerm.Then(
                this.pattern,
                that.pattern,
                None
            )
        )

    /**
     * Alternation between two pattern terms. Maps to `|`.
     *
     * {{{
     * d.a | d.b
     * }}}
     */
    def |(that: RecognizePatternTerm[L]): RecognizePatternTerm[L] =
        RecognizePatternTerm(
            SqlRowPatternTerm.Or(
                this.pattern,
                that.pattern,
                None
            )
        )

/**
 * The final `matchRecognize` result, produced by `measures`.
 * Ready to be passed to `from` as a table source.
 */
final case class RecognizeMeasures[N <: Tuple, V <: Tuple, CL <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable
) extends AnyTable

/**
 * A `matchRecognize` table source produced by `measures`.
 */
final case class FromRecognize[N <: Tuple, V <: Tuple, OKS <: Tuple, CL <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable
) extends AnyTable

/**
 * A table reference produced by `from` when a `FromRecognize`
 * is passed, enabling typed column access via `selectDynamic`.
 */
final case class RecognizeTable[N <: Tuple, V <: Tuple, L <: Int](
    private[sqala] val __aliasName__ : Option[String],
    private[sqala] val __items__ : V,
    private[sqala] val __sqlTable__ : SqlTable
) extends Selectable with AnyTable:
    /**
     * The structural type declaring available columns as a named tuple.
     * Required by `Selectable`.
     */
    type Fields = NamedTuple[N, V]

    /**
     * Runtime column accessor. Required by `Selectable`.
     */
    inline def selectDynamic(name: String): Any =
        val index = constValue[Index[N, name.type, 0]]
        __items__.toList(index)
