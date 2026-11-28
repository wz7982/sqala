package sqala.static.ext.dsl

import sqala.ir.{IrExpr, IrJsonObjectItem}
import sqala.metadata.{AsSqlExpr, Json, SqlJson, SqlString}
import sqala.static.dsl.*

import scala.util.NotGiven

/**
 * Serializes a JSON expression as a string expression. Maps to `JSON_SERIALIZE(expr)`.
 *
 * {{{
 * from(Log).map(l => jsonSerialize(json(l.data)))
 * }}}
 */
def jsonSerialize[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    s: ValidateArgType[SqlJson, a.R, "json"],
    kt: KindToTuple[a.K]
): Expr[Option[String], Composite[kt.R]] =
    Expr(
        IrExpr.JsonSerializeFunc(
            a.asExpr(x).expr
        )
    )

/**
 * Parses a string expression as a JSON expression. Maps to `JSON(expr)`.
 *
 * {{{
 * from(Log).map(l => json(l.data))
 * }}}
 */
def json[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    s: ValidateArgType[SqlString, a.R, "string"],
    kt: KindToTuple[a.K]
): Expr[Option[Json], Composite[kt.R]] =
    Expr(
        IrExpr.JsonParseFunc(
            a.asExpr(x).expr
        )
    )

/**
 * Extracts a scalar JSON value at a path. Maps to
 * `JSON_VALUE(expr, path)`.
 *
 * {{{
 * from(Log).map(l => jsonValue(json(l.data), "$.name"))
 * }}}
 */
def jsonValue[A, CL <: Int](x: A, path: String)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    j: ValidateArgType[SqlJson, a.R, "json"],
    kt: KindToTuple[a.K]
): Expr[Option[String], Composite[kt.R]] =
    Expr(
        IrExpr.JsonValueFunc(
            a.asExpr(x).expr,
            path.asInlinedExpr.expr
        )
    )

/**
 * Extracts a JSON object or array at a path. Maps to
 * `JSON_QUERY(expr, path)`.
 *
 * {{{
 * from(Log).map(l => jsonQuery(json(l.data), "$.items"))
 * }}}
 */
def jsonQuery[A, CL <: Int](x: A, path: String)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    j: ValidateArgType[SqlJson, a.R, "json"],
    kt: KindToTuple[a.K]
): Expr[Option[Json], Composite[kt.R]] =
    Expr(
        IrExpr.JsonQueryFunc(
            a.asExpr(x).expr,
            path.asInlinedExpr.expr
        )
    )

/**
 * Tests whether a JSON path exists. Maps to `JSON_EXISTS(expr, path)`.
 *
 * {{{
 * from(Log).filter(l => jsonExists(json(l.data), "$.name"))
 * }}}
 */
def jsonExists[A, CL <: Int](x: A, path: String)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    j: ValidateArgType[SqlJson, a.R, "json"],
    kt: KindToTuple[a.K]
): Expr[Option[Boolean], Composite[kt.R]] =
    Expr(
        IrExpr.JsonExistsFunc(
            a.asExpr(x).expr,
            path.asInlinedExpr.expr
        )
    )

/**
 * A key and value pair for `jsonObject`.
 */
final class JsonObjectPair[KS <: Tuple](private[sqala] val key: IrExpr, private[sqala] val value: IrExpr)

/**
 * Converts key and value pairs into the argument list for
 * `jsonObject`.
 */
trait AsJsonObject[T]:
    /**
     * The combined kind tuple of the key and value pairs.
     */
    type KS <: Tuple

    /**
     * Converts the value to a list of `JsonObjectPair`.
     */
    def asJsonObjects(x: T): List[JsonObjectPair[?]]

object AsJsonObject:
    type Aux[T, OKS <: Tuple] = AsJsonObject[T]:
        type KS = OKS

    given pair[K, V, CL <: Int](using
        qc: QueryContext[CL],
        knt: NotGiven[K <:< Tuple],
        vnt: NotGiven[V <:< Tuple],
        ak: AsExpr[K, CL],
        av: AsExpr[V, CL],
        ask: ValidateArgType[AsSqlExpr, ak.R, "scalar"],
        asv: ValidateArgType[AsSqlExpr, av.R, "scalar"],
        ktk: KindToTuple[ak.K],
        ktv: KindToTuple[av.K],
        c: CombineKindTuple[ktk.R, ktv.R]
    ): Aux[(K, V), c.R] =
        new AsJsonObject[(K, V)]:
            type KS = c.R

            def asJsonObjects(x: (K, V)): List[JsonObjectPair[?]] =
                JsonObjectPair(ak.asExpr(x._1).expr, av.asExpr(x._2).expr) :: Nil

    given tuple[HK, HV, T <: Tuple](using
        h: AsJsonObject[(HK, HV)],
        t: AsJsonObject[T],
        c: CombineKindTuple[h.KS, t.KS]
    ): Aux[(HK, HV) *: T, c.R] =
        new AsJsonObject[(HK, HV) *: T]:
            type KS = c.R

            def asJsonObjects(x: (HK, HV) *: T): List[JsonObjectPair[?]] =
                h.asJsonObjects(x.head) ++ t.asJsonObjects(x.tail)

    given tuple1[HK, HV](using
        h: AsJsonObject[(HK, HV)]
    ): Aux[(HK, HV) *: EmptyTuple, h.KS] =
        new AsJsonObject[(HK, HV) *: EmptyTuple]:
            type KS = h.KS

            def asJsonObjects(x: (HK, HV) *: EmptyTuple): List[JsonObjectPair[?]] =
                h.asJsonObjects(x.head)

/**
 * Constructs a JSON object from key and value pairs. Maps to
 * `JSON_OBJECT(key VALUE value, ...)`.
 *
 * {{{
 * from(Post).map(p => jsonObject((p.id, p.title)))
 * }}}
 */
def jsonObject[T, CL <: Int](items: T)(using
    qc: QueryContext[CL],
    j: AsJsonObject[T]
): Expr[Option[Json], Composite[j.KS]] =
    Expr(
        IrExpr.JsonObjectFunc(
            j.asJsonObjects(items).map(i => IrJsonObjectItem(i.key, i.value))
        )
    )

/**
 * Constructs a JSON array from elements. Maps to
 * `JSON_ARRAY(expr, ...)`.
 *
 * {{{
 * from(Post).map(p => jsonArray(p.id, p.title))
 * }}}
 */
def jsonArray[A, CL <: Int](items: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    kt: KindToTuple[a.K]
): Expr[Option[Json], Composite[kt.R]] =
    Expr(
        IrExpr.JsonArrayFunc(
            a.asExprs(items).map(i => i.expr).toList
        )
    )

/**
 * Aggregates key and value pairs into a JSON object. Maps to
 * `JSON_OBJECTAGG(key VALUE value)`. Aggregate functions, window
 * functions, and other expressions not allowed in aggregate calls
 * are rejected at compile time.
 *
 * {{{
 * from(Post).groupBy(p => p.authorId).map((g, p) => jsonObjectAgg(p.id -> p.title))
 * }}}
 */
def jsonObjectAgg[K, V, CL <: Int](pair: (K, V))(using
    qc: QueryContext[CL],
    knt: NotGiven[K <:< Tuple],
    vnt: NotGiven[V <:< Tuple],
    ak: AsExpr[K, CL],
    av: AsExpr[V, CL],
    ask: ValidateArgType[AsSqlExpr, ak.R, "scalar"],
    asv: ValidateArgType[AsSqlExpr, av.R, "scalar"],
    ktk: KindToTuple[ak.K],
    ktv: KindToTuple[av.K],
    c: CombineKindTuple[ktk.R, ktv.R],
    i: CanInAgg[c.R, CL]
): AggExpr[Option[Json], c.R] =
    val key = ak.asExpr(pair._1).expr
    val value = av.asExpr(pair._2).expr
    AggExpr(
        IrExpr.JsonObjectAggFunc(
            IrJsonObjectItem(key, value),
            None
        )
    )

/**
 * Aggregates values into a JSON array. Maps to `JSON_ARRAYAGG(expr)`.
 * Aggregate functions, window functions, and other expressions not
 * allowed in aggregate calls are rejected at compile time.
 *
 * {{{
 * from(Post).groupBy(p => p.authorId).map((g, p) => jsonArrayAgg(p.title))
 * }}}
 */
def jsonArrayAgg[A, CL <: Int](x: A)(using
    qc: QueryContext[CL],
    a: AsExpr[A, CL],
    as: ValidateArgType[AsSqlExpr, a.R, "scalar"],
    kt: KindToTuple[a.K],
    i: CanInAgg[kt.R, CL]
): AggExpr[Option[Json], kt.R] =
    AggExpr(
        IrExpr.JsonArrayAggFunc(
            a.asExpr(x).expr,
            Nil,
            None

        )
    )