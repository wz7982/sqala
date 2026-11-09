package sqala.static.ext.dsl

import sqala.static.dsl.QueryContext
import sqala.static.dsl.AsExpr
import sqala.static.dsl.ValidateArgType
import sqala.metadata.SqlString
import sqala.static.dsl.KindToTuple
import sqala.static.dsl.Expr
import sqala.metadata.Json
import sqala.static.dsl.Composite
import sqala.ir.IrExpr
import sqala.metadata.SqlJson
import sqala.static.dsl.CombineKind
import sqala.static.dsl.CombineKindTuple
import sqala.metadata.AsSqlExpr
import sqala.ir.IrJsonObjectItem
import sqala.static.dsl.CanInAgg
import sqala.static.dsl.AggExpr

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
def jsonValue[A, P, CL <: Int](x: A, path: P)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ap: AsExpr[P, CL],
    j: ValidateArgType[SqlJson, aa.R, "json"],
    s: ValidateArgType[SqlString, ap.R, "string"],
    c: CombineKind[aa.K, ap.K]
): Expr[Option[String], c.R] =
    Expr(
        IrExpr.JsonValueFunc(
            aa.asExpr(x).expr,
            ap.asExpr(path).expr
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
def jsonQuery[A, P, CL <: Int](x: A, path: P)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ap: AsExpr[P, CL],
    j: ValidateArgType[SqlJson, aa.R, "json"],
    s: ValidateArgType[SqlString, ap.R, "string"],
    c: CombineKind[aa.K, ap.K]
): Expr[Option[Json], c.R] =
    Expr(
        IrExpr.JsonQueryFunc(
            aa.asExpr(x).expr,
            ap.asExpr(path).expr
        )
    )

/**
 * Tests whether a JSON path exists. Maps to `JSON_EXISTS(expr, path)`.
 *
 * {{{
 * from(Log).filter(l => jsonExists(json(l.data), "$.name"))
 * }}}
 */
def jsonExists[A, P, CL <: Int](x: A, path: P)(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ap: AsExpr[P, CL],
    j: ValidateArgType[SqlJson, aa.R, "json"],
    s: ValidateArgType[SqlString, ap.R, "string"],
    c: CombineKind[aa.K, ap.K]
): Expr[Option[Boolean], c.R] =
    Expr(
        IrExpr.JsonExistsFunc(
            aa.asExpr(x).expr,
            ap.asExpr(path).expr
        )
    )

/**
 * A key and value pair for `jsonObject`.
 */
final case class JsonObjectPair[KS <: Tuple](key: IrExpr, value: IrExpr)

extension [K, CL <: Int](key: K)(using 
    qc: QueryContext[CL], 
    ak: AsExpr[K, CL], 
    ask: ValidateArgType[AsSqlExpr, ak.R, "scalar"], 
    ktk: KindToTuple[ak.K]
)
    /**
     * Pairs a key with a value for `jsonObject`. Maps to
     * `key VALUE value`.
     *
     * {{{
     * jsonObject(p.id value p.title)
     * }}}
     */
    infix def value[V](value: V)(using
        av: AsExpr[V, CL],
        asv: ValidateArgType[AsSqlExpr, av.R, "scalar"],
        ktv: KindToTuple[av.K],
        c: CombineKindTuple[ktk.R, ktv.R]
    ): JsonObjectPair[c.R] =
        JsonObjectPair(ak.asExpr(key).expr, av.asExpr(value).expr)

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

    given pair[EKS <: Tuple]: Aux[JsonObjectPair[EKS], EKS] =
        new AsJsonObject[JsonObjectPair[EKS]]:
            type KS = EKS

            def asJsonObjects(x: JsonObjectPair[EKS]): List[JsonObjectPair[?]] =
                x :: Nil

    given tuple[EKS <: Tuple, T <: Tuple](using
        j: AsJsonObject[T],
        c: CombineKindTuple[EKS, j.KS]
    ): Aux[JsonObjectPair[EKS] *: T, c.R] =
        new AsJsonObject[JsonObjectPair[EKS] *: T]:
            type KS = c.R

            def asJsonObjects(x: JsonObjectPair[EKS] *: T): List[JsonObjectPair[?]] =
                x.head :: j.asJsonObjects(x.tail)

    given tuple1[EKS <: Tuple](using
        c: CombineKindTuple[EKS, EmptyTuple]
    ): Aux[JsonObjectPair[EKS] *: EmptyTuple, c.R] =
        new AsJsonObject[JsonObjectPair[EKS] *: EmptyTuple]:
            type KS = c.R

            def asJsonObjects(x: JsonObjectPair[EKS] *: EmptyTuple): List[JsonObjectPair[?]] =
                x.head :: Nil

/**
 * Constructs a JSON object from key and value pairs. Maps to
 * `JSON_OBJECT(key VALUE value, ...)`.
 *
 * {{{
 * from(Post).map(p => jsonObject(p.id.value(p.title)))
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
    as: AsSqlExpr[a.R],
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
 * from(Post).groupBy(p => p.authorId).map((g, p) => jsonObjectAgg(p.id value p.title))
 * }}}
 */
def jsonObjectAgg[KS <: Tuple, CL <: Int](pair: JsonObjectPair[KS])(using
    qc: QueryContext[CL],
    i: CanInAgg[KS]
): AggExpr[Option[Json], KS] =
    AggExpr(
        IrExpr.JsonObjectAggFunc(
            IrJsonObjectItem(pair.key, pair.value),
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
    i: CanInAgg[kt.R]
): AggExpr[Option[Json], kt.R] =
    AggExpr(
        IrExpr.JsonArrayAggFunc(
            a.asExpr(x).expr,
            Nil,
            None
            
        )
    )