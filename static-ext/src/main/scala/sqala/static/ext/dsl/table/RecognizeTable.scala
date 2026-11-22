package sqala.static.ext.dsl.table

import sqala.ast.expr.SqlExpr
import sqala.ast.table.{SqlRecognizePatternRowsMode, SqlRowPatternSkipMode, SqlRowPatternStrategy, SqlRowPatternSubsetItem}
import sqala.ir.*
import sqala.static.dsl.*
import sqala.static.dsl.statement.query.AsMap
import sqala.static.dsl.table.{AnyTable, AsTable, AsTableParam, MappedTable}
import sqala.static.ext.dsl.{CanInRecognizeDefine, MatchRecognizeContext}
import sqala.util.NonEmptyList

import scala.NamedTuple.NamedTuple
import scala.compiletime.constValueTuple
import scala.quoted.{Expr, Quotes, Type}

/**
 * `MATCH_RECOGNIZE` specification before the `DEFINE` and `PATTERN`
 * clauses are set.
 */
case class RecognizeInitSpec(
    partitionBy: List[IrExpr],
    orderBy: List[IrOrderingItem],
    rowsMode: Option[SqlRecognizePatternRowsMode],
    afterMatchMode: Option[SqlRowPatternSkipMode],
    strategy: Option[SqlRowPatternStrategy],
    subset: List[SqlRowPatternSubsetItem]
)

/**
 * `MATCH_RECOGNIZE` specification after the `DEFINE` clause is set.
 */
case class RecognizeDefinedSpec(
    partitionBy: List[IrExpr],
    orderBy: List[IrOrderingItem],
    rowsMode: Option[SqlRecognizePatternRowsMode],
    afterMatchMode: Option[SqlRowPatternSkipMode],
    strategy: Option[SqlRowPatternStrategy],
    subset: List[SqlRowPatternSubsetItem],
    define: NonEmptyList[IrRowPatternDefineItem]
)

/**
 * Complete `MATCH_RECOGNIZE` specification after both `DEFINE` and
 * `PATTERN` clauses are set.
 */
case class RecognizePatternedSpec(
    partitionBy: List[IrExpr],
    orderBy: List[IrOrderingItem],
    rowsMode: Option[SqlRecognizePatternRowsMode],
    afterMatchMode: Option[SqlRowPatternSkipMode],
    strategy: Option[SqlRowPatternStrategy],
    subset: List[SqlRowPatternSubsetItem],
    define: NonEmptyList[IrRowPatternDefineItem],
    pattern: IrRowPatternTerm
)

final class RecognizeSource[T, L <: Int](
    private[sqala] val table: T,
    private[sqala] val source: IrTable.MatchRecognizable
)(using
    private[sqala] val qc: QueryContext[L],
    private[sqala] val mc: MatchRecognizeContext
):
    /**
     * Specifies `PARTITION BY` for `matchRecognize`.
     *
     * {{{
     * s.partitionBy(t => t.stockSymbol)
     * }}}
     */
    def partitionBy[P](f: T => P)(using
        a: AsRecognizePartition[P, L]
    ): RecognizePredefine[T, L] =
        val partition = f(table)
        RecognizePredefine(
            table,
            source,
            RecognizeInitSpec(
                a.asExprs(partition).map(_.expr),
                Nil,
                None,
                None,
                None,
                Nil
            )
        )

    /**
     * Specifies `ORDER BY` for `matchRecognize`.
     *
     * {{{
     * s.sortBy(t => t.tradeTime)
     * }}}
     */
    def sortBy[S](f: T => S)(using
        a: AsRecognizeSort[S, L]
    ): RecognizePredefine[T, L] =
        val sort = f(table)
        new RecognizePredefine(
            table,
            source,
            RecognizeInitSpec(
                Nil,
                a.asSorts(sort).map(_.ordering),
                None,
                None,
                None,
                Nil
            )
        )

    /**
     * Returns one row per match. Maps to `ONE ROW PER MATCH`.
     *
     * {{{
     * s.oneRowPerMatch
     * }}}
     */
    def oneRowPerMatch: RecognizePredefine[T, L] =
        new RecognizePredefine(
            table,
            source,
            RecognizeInitSpec(
                Nil,
                Nil,
                Some(SqlRecognizePatternRowsMode.OneRow),
                None,
                None,
                Nil
            )
        )

    /**
     * Returns all rows per match. Maps to `ALL ROWS PER MATCH`.
     *
     * {{{
     * s.allRowsPerMatch
     * }}}
     */
    def allRowsPerMatch: RecognizePredefine[T, L] =
        new RecognizePredefine(
            table,
            source,
            RecognizeInitSpec(
                Nil,
                Nil,
                Some(SqlRecognizePatternRowsMode.AllRows(None)),
                None,
                None,
                Nil
            )
        )

/**
 * Intermediate builder after `partitionBy` and `sortBy` in
 * `matchRecognize`.
 */
final class RecognizePredefine[T, L <: Int](
    private[sqala] val table: T,
    private[sqala] val source: IrTable.MatchRecognizable,
    private[sqala] val spec: RecognizeInitSpec
)(using
    private[sqala] val qc: QueryContext[L],
    private[sqala] val mc: MatchRecognizeContext
):
    /**
     * Specifies `ORDER BY` for `matchRecognize`. The sort
     * value must be a column expression.
     *
     * {{{
     * s.sortBy(t => t.tradeTime)
     * }}}
     */
    def sortBy[S](f: T => S)(using a: AsRecognizeSort[S, L]): RecognizePredefine[T, L] =
        val sort = f(table)
        RecognizePredefine(table, source, spec.copy(orderBy = spec.orderBy ++ a.asSorts(sort).map(_.ordering)))

    /**
     * Returns one row per match. Maps to `ONE ROW PER MATCH`.
     *
     * {{{
     * s.oneRowPerMatch
     * }}}
     */
    def oneRowPerMatch: RecognizePredefine[T, L] =
        RecognizePredefine(table, source, spec.copy(rowsMode = Some(SqlRecognizePatternRowsMode.OneRow)))

    /**
     * Returns all rows per match. Maps to `ALL ROWS PER MATCH`.
     *
     * {{{
     * s.allRowsPerMatch
     * }}}
     */
    def allRowsPerMatch: RecognizePredefine[T, L] =
        RecognizePredefine(table, source, spec.copy(rowsMode = Some(SqlRecognizePatternRowsMode.AllRows(None))))

    /**
     * Pre-defines the row pattern variable names. The arguments must be
     * string literals: they are turned into a literal type tuple at compile
     * time, so each name becomes a typed field on the context objects of
     * `pattern`, `define` and `measures`.
     *
     * Names must be unique and there must be at least one.
     *
     * {{{
     * .predefine("start", "down", "up")
     * }}}
     */
    transparent inline def predefine(inline names: String*): Recognize[?, T, L] =
        ${ RecognizePredefineMacro.predefine[T, L]('this, 'names) }

object RecognizePredefineMacro:
    def predefine[T: Type, L <: Int: Type](
        recognize: Expr[RecognizePredefine[T, L]],
        names: Expr[Seq[String]]
    )(using
        q: Quotes
    ): Expr[Recognize[?, T, L]] =
        import q.reflect.*

        val nameList =
            names.asTerm match
                case Inlined(_, _, Typed(Repeated(elems, _), _)) =>
                    elems.map:
                        case Literal(StringConstant(name)) =>
                            name
                        case other =>
                            report.errorAndAbort(
                                "Predefine names must be string literals.",
                                other.pos
                            )
                case _ =>
                    report.errorAndAbort("Predefine names must be string literals.")

        if nameList.isEmpty then
            report.errorAndAbort("Predefine requires at least one name.")

        def tupleType(names: List[String]): TypeRepr =
            names match
                case Nil =>
                    TypeRepr.of[EmptyTuple]
                case head :: tail =>
                    val cons = Symbol.requiredClass("scala.*:")
                    AppliedType(
                        cons.typeRef,
                        List(
                            ConstantType(StringConstant(head)),
                            tupleType(tail)
                        )
                    )

        val namesType = tupleType(nameList)

        namesType.asType match
            case '[type nt <: Tuple; nt] =>
                '{
                    Recognize[nt, T, L](
                        $recognize.table,
                        $recognize.source,
                        $recognize.spec
                    )(using
                        $recognize.qc,
                        $recognize.mc
                    )
                }

/**
 * Main builder for `matchRecognize` pattern definition. After
 * `predefine`.
 */
final class Recognize[N <: Tuple, T, L <: Int](
    private[sqala] val table: T,
    private[sqala] val source: IrTable.MatchRecognizable,
    private[sqala] val spec: RecognizeInitSpec
)(using
    private[sqala] val qc: QueryContext[L],
    private[sqala] val mc: MatchRecognizeContext
)

object Recognize:
    extension [N <: Tuple, T, L <: Int](recognize: Recognize[N, T, L])(using tr: TransformKind[T, [l <: Int] =>> Grouped[Column[l] *: EmptyTuple], L])
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
        inline def define[V <: Tuple](f: RecognizeDefine[N, T, tr.R, L] => NamedTuple[N, V])(using
            ar: AliasRecognize[T],
            a: AsExpr[V, L],
            kt: KindToTuple[a.K],
            i: CanInRecognizeDefine[kt.R, L],
            an: AggNotNested[kt.R]
        ): DefinedRecognize[N, T, L] =
            val items = f(RecognizeDefine[N, T, tr.R, L](recognize.table))
            val names = constValueTuple[N].toList.map(_.toString)
            val exprs = a.asExprs(items.toTuple).toList
            val defines = names.zip(exprs).map: (name, expr) =>
                IrRowPatternDefineItem(name, expr.expr)
            val definedSpec =
                RecognizeDefinedSpec(
                    recognize.spec.partitionBy,
                    recognize.spec.orderBy,
                    recognize.spec.rowsMode,
                    recognize.spec.afterMatchMode,
                    recognize.spec.strategy,
                    recognize.spec.subset,
                    NonEmptyList(defines.head, defines.tail)
                )
            DefinedRecognize[N, T, L](
                recognize.table,
                recognize.source,
                definedSpec
            )(using
                recognize.qc,
                recognize.mc
            )

/**
 * Intermediate `matchRecognize` builder after the `define` clause and
 * before the `pattern` clause.
 */
final class DefinedRecognize[N <: Tuple, T, L <: Int](
    private[sqala] val table: T,
    private[sqala] val source: IrTable.MatchRecognizable,
    private[sqala] val spec: RecognizeDefinedSpec
)(using
    private[sqala] val qc: QueryContext[L],
    private[sqala] val mc: MatchRecognizeContext
):
    /**
     * Defines the row pattern expression using the pattern variable
     * names from `predefine`.
     *
     * {{{
     * .pattern(d => d.start ~ d.down.+ ~ d.up.+)
     * }}}
     */
    def pattern(f: RecognizePattern[N, T, L] => RecognizePatternTerm[L]): PatternedRecognize[N, T, L] =
        val p = f(RecognizePattern[N, T, L](table))
        PatternedRecognize(
            table,
            source,
            RecognizePatternedSpec(
                spec.partitionBy,
                spec.orderBy,
                spec.rowsMode,
                spec.afterMatchMode,
                spec.strategy,
                spec.subset,
                spec.define,
                p.pattern
            )
        )

    /**
     * Resumes pattern matching at the next row after a match.
     * Maps to `AFTER MATCH SKIP TO NEXT ROW`.
     */
    def afterMatchSkipToNextRow: DefinedRecognize[N, T, L] =
        DefinedRecognize(
            table,
            source,
            spec.copy(afterMatchMode = Some(SqlRowPatternSkipMode.ToNextRow))
        )

    /**
     * Resumes pattern matching after the last row of the match.
     * Maps to `AFTER MATCH SKIP PAST LAST ROW`.
     */
    def afterMatchSkipPastLastRow: DefinedRecognize[N, T, L] =
        DefinedRecognize(
            table,
            source,
            spec.copy(afterMatchMode = Some(SqlRowPatternSkipMode.PastLastRow))
        )

    /**
     * Resumes pattern matching at the first occurrence of a
     * pattern variable. Maps to `AFTER MATCH SKIP TO FIRST p`.
     *
     * {{{
     * .afterMatchSkipToFirst(d => d.up)
     * }}}
     */
    def afterMatchSkipToFirst(f: RecognizePatternName[N, T, L] => String): DefinedRecognize[N, T, L] =
        DefinedRecognize(
            table,
            source,
            spec.copy(
                afterMatchMode = Some(
                    SqlRowPatternSkipMode.ToFirst(f(RecognizePatternName[N, T, L]()))
                )
            )
        )

    /**
     * Resumes pattern matching at the last occurrence of a
     * pattern variable. Maps to `AFTER MATCH SKIP TO LAST p`.
     *
     * {{{
     * .afterMatchSkipToLast(d => d.up)
     * }}}
     */
    def afterMatchSkipToLast(f: RecognizePatternName[N, T, L] => String): DefinedRecognize[N, T, L] =
        DefinedRecognize(
            table,
            source,
            spec.copy(
                afterMatchMode = Some(
                    SqlRowPatternSkipMode.ToLast(f(RecognizePatternName[N, T, L]()))
                )
            )
        )

    /**
     * Resumes pattern matching at a pattern variable.
     * Maps to `AFTER MATCH SKIP TO p`.
     *
     * {{{
     * .afterMatchSkipTo(d => d.up)
     * }}}
     */
    def afterMatchSkipTo(f: RecognizePatternName[N, T, L] => String): DefinedRecognize[N, T, L] =
        DefinedRecognize(
            table,
            source,
            spec.copy(
                afterMatchMode = Some(
                    SqlRowPatternSkipMode.To(f(RecognizePatternName[N, T, L]()))
                )
            )
        )

/**
 * Intermediate `matchRecognize` builder after the `pattern` clause and
 * before the `measures` clause.
 */
final class PatternedRecognize[N <: Tuple, T, L <: Int](
    private[sqala] val table: T,
    private[sqala] val source: IrTable.MatchRecognizable,
    private[sqala] val spec: RecognizePatternedSpec
)(using
    private[sqala] val qc: QueryContext[L],
    private[sqala] val mc: MatchRecognizeContext
)

object PatternedRecognize:
    extension [N <: Tuple, T, L <: Int](recognize: PatternedRecognize[N, T, L])(using tr: TransformKind[T, [l <: Int] =>> Grouped[Column[l] *: EmptyTuple], L])
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
        def measures[MN <: Tuple, MV <: Tuple](
            f: RecognizeDefine[N, T, tr.R, L] => NamedTuple[MN, MV]
        )(using
            ar: AliasRecognize[T],
            m: AsMap[MV, L],
            p: AsTableParam[m.R, L],
            t: ToTuple[p.R],
            i: CanInGroupedMap[m.KS],
            e: ExcludeCurrentLevelColumn[m.KS, L],
            refl: e.R =:= EmptyTuple,
            an: AggNotNested[m.KS]
        ): FinalRecognize[N, t.R, L] =
            val items = m.asSelectItems(f(RecognizeDefine[N, T, tr.R, L](recognize.table)), 1)
            val node: IrTable.MatchRecognize =
                IrTable.MatchRecognize(
                    recognize.source,
                    recognize.spec.partitionBy,
                    recognize.spec.orderBy,
                    items,
                    recognize.spec.rowsMode,
                    IrRowPattern(
                        recognize.spec.afterMatchMode,
                        recognize.spec.strategy,
                        recognize.spec.pattern,
                        recognize.spec.subset,
                        recognize.spec.define
                    ),
                    Nil
                )
            FinalRecognize[N, t.R, L](
                node,
                t.toTuple(p.asTableParam(node, 1))
            )(using
                recognize.qc,
                recognize.mc
            )

/**
 * Final `matchRecognize` table source produced after `measures`, holding
 * the match-recognize IR table and its output items.
 */
final class FinalRecognize[N <: Tuple, V <: Tuple, L <: Int](
    private[sqala] val table: IrTable.MatchRecognize,
    private[sqala] val items: V
)(using
    private[sqala] val qc: QueryContext[L],
    private[sqala] val mc: MatchRecognizeContext
)

/**
 * Dynamic context for `define` and `measures` clauses.
 */
final class RecognizeDefine[N <: Tuple, T, GT, L <: Int](
    private[sqala] val __table__ : T,
)(using
    private[sqala] val __ar__ : AliasRecognize[T],
    private[sqala] val __tr__ : TransformKind.Aux[T, [l <: Int] =>> Grouped[Column[l] *: EmptyTuple], L, GT]
) extends Selectable:
    /**
     * The predefined pattern variable names, each mapping to the
     * transformed expression type `GT`.
     */
    type Fields = NamedTuple[N, Tuple.Map[N, [n] =>> GT]]

    /**
     * Dynamic member access used in `define` and `measures` clauses,
     * turning `d.<name>` into a grouped column expression aliased to
     * `<name>`.
     */
    def selectDynamic(name: String): GT =
        __tr__.transform(__ar__.alias(__table__, name))

/**
 * Dynamic context for `pattern` clauses.
 */
final class RecognizePattern[N <: Tuple, T, L <: Int](
    private[sqala] val __table__ : T
)(using
    private[sqala] val __qc__ : QueryContext[L],
    private[sqala] val __mc__ : MatchRecognizeContext
) extends Selectable:
    /**
     * The predefined pattern variable names, each mapping to a row
     * pattern term.
     */
    type Fields = NamedTuple[N, Tuple.Map[N, [n] =>> RecognizePatternTerm[L]]]

    /**
     * Dynamic member access used in `pattern` clauses, turning `d.<name>`
     * into a row-pattern term referring to the pattern variable `<name>`.
     */
    def selectDynamic(name: String): RecognizePatternTerm[L] =
        RecognizePatternTerm(IrRowPatternTerm.Pattern(name, None))

/**
 * Dynamic context for `afterMatchSkipTo` clauses.
 */
final class RecognizePatternName[N <: Tuple, T, L <: Int] extends Selectable:
    /**
     * The predefined pattern variable names, each mapping to its name
     * string.
     */
    type Fields = NamedTuple[N, Tuple.Map[N, [n] =>> String]]

    /**
     * Dynamic member access used in `afterMatchSkipTo`, returning the
     * referenced pattern variable name.
     */
    def selectDynamic(name: String): String =
        name

/**
 * A term in a `matchRecognize` row pattern.
 */
final class RecognizePatternTerm[L <: Int](private[sqala] val pattern: IrRowPatternTerm)(using
    private[sqala] val qc: QueryContext[L],
    private[sqala] val mc: MatchRecognizeContext
):
     /**
     * Applies a quantifier to the underlying pattern term, returning a new
     * term with the updated quantifier.
     */
    private def setQuantifier(quantifier: IrRowPatternQuantifier): IrRowPatternTerm =
        pattern match
            case p: IrRowPatternTerm.Pattern =>
                p.copy(quantifier = Some(quantifier))
            case c: IrRowPatternTerm.Circumflex =>
                c.copy(quantifier = Some(quantifier))
            case d: IrRowPatternTerm.Dollar =>
                d.copy(quantifier = Some(quantifier))
            case e: IrRowPatternTerm.Exclusion =>
                e.copy(quantifier = Some(quantifier))
            case p: IrRowPatternTerm.Permute =>
                p.copy(quantifier = Some(quantifier))
            case t: IrRowPatternTerm.Then =>
                t.copy(quantifier = Some(quantifier))
            case o: IrRowPatternTerm.Or =>
                o.copy(quantifier = Some(quantifier))

    /**
     * One or more repetitions. Maps to `+`.
     *
     * {{{
     * d.down.+
     * }}}
     */
    def + : RecognizePatternTerm[L] =
        RecognizePatternTerm(setQuantifier(IrRowPatternQuantifier.Plus(false)))

    /**
     * Zero or more repetitions. Maps to `*`.
     *
     * {{{
     * d.down.*
     * }}}
     */
    def * : RecognizePatternTerm[L] =
        RecognizePatternTerm(setQuantifier(IrRowPatternQuantifier.Asterisk(false)))

    /**
     * Zero or one repetition. Maps to `?`.
     *
     * {{{
     * d.down.?
     * }}}
     */
    def ? : RecognizePatternTerm[L] =
        RecognizePatternTerm(setQuantifier(IrRowPatternQuantifier.Question(false)))

    /**
     * Between `m` and `n` repetitions. Maps to `{m, n}`.
     *
     * {{{
     * d.down.between(1, 10)
     * }}}
     */
    def between(start: Int, end: Int): RecognizePatternTerm[L] =
        RecognizePatternTerm(
            setQuantifier(
                IrRowPatternQuantifier.Between(
                    Some(IrExpr.Native(SqlExpr.InlinedLiteral(SqlExpr.NumberLiteral(BigDecimal(start))))),
                    Some(IrExpr.Native(SqlExpr.InlinedLiteral(SqlExpr.NumberLiteral(BigDecimal(end))))),
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
    def least(x: Int): RecognizePatternTerm[L] =
        RecognizePatternTerm(
            setQuantifier(
                IrRowPatternQuantifier.Between(
                    Some(IrExpr.Native(SqlExpr.InlinedLiteral(SqlExpr.NumberLiteral(BigDecimal(x))))),
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
    def most(x: Int): RecognizePatternTerm[L] =
        RecognizePatternTerm(
            setQuantifier(
                IrRowPatternQuantifier.Between(
                    None,
                    Some(IrExpr.Native(SqlExpr.InlinedLiteral(SqlExpr.NumberLiteral(BigDecimal(x))))),
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
    def at(x: Int): RecognizePatternTerm[L] =
        RecognizePatternTerm(
            setQuantifier(
                IrRowPatternQuantifier.Quantity(
                    IrExpr.Native(SqlExpr.InlinedLiteral(SqlExpr.NumberLiteral(BigDecimal(x))))
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
            IrRowPatternTerm.Then(
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
            IrRowPatternTerm.Or(
                this.pattern,
                that.pattern,
                None
            )
        )

/**
 * A `matchRecognize` table source produced by `measures`.
 */
final class FromRecognize[N <: Tuple, V <: Tuple, OKS <: Tuple, CL <: Int](
    private[sqala] val table: IrTable.MatchRecognize,
    private[sqala] val items: V
) extends AnyTable

object FromRecognize:
    given asTable[N <: Tuple, V <: Tuple, TOKS <: Tuple, CL <: Int]: AsTable.Aux[FromRecognize[N, V, TOKS, CL], CL, MappedTable[N, V, false, CL], TOKS] =
        new AsTable[FromRecognize[N, V, TOKS, CL], CL]:
            type R = MappedTable[N, V, false, CL]

            type OKS = TOKS

            def asTable(x: FromRecognize[N, V, TOKS, CL]): (R, IrTable) =
                (new MappedTable[N, V, false, CL](x.items), x.table)