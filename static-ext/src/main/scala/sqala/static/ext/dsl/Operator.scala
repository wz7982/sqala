package sqala.static.ext.dsl

import sqala.static.dsl.QueryContext
import sqala.static.dsl.AsExpr
import sqala.static.dsl.Relation
import sqala.static.dsl.KindToTuple
import sqala.static.dsl.Expr
import sqala.static.dsl.Composite
import sqala.metadata.SqlString
import sqala.static.dsl.ValidateArgType
import sqala.ir.IrExpr
import sqala.ast.expr.SqlJsonNodeType

extension [A, CL <: Int](self: A)(using qc: QueryContext[CL], aa: AsExpr[A, CL])
    /**
     * `IS JSON` predicate. Typically used in `filter`, `on`, and
     * `having` clause. Tests whether the string expression is valid JSON.
     *
     * {{{
     * from(Log).filter(l => l.log.isJson)
     * }}}
     */
    def isJson(using
        s: ValidateArgType[SqlString, aa.R, "string"],
        r: Relation[aa.R, String],
        kt: KindToTuple[aa.K]
    ): Expr[r.R, Composite[kt.R]] =
        Expr(
            IrExpr.JsonTest(aa.asExpr(self).expr, None, false)
        )

    /**
     * `IS JSON OBJECT` predicate. Typically used in `filter`, `on`, and
     * `having` clause. Tests whether the string expression is a valid JSON
     * object.
     *
     * {{{
     * from(Log).filter(l => l.log.isJsonObject)
     * }}}
     */
    def isJsonObject(using
        s: ValidateArgType[SqlString, aa.R, "string"],
        r: Relation[aa.R, String],
        kt: KindToTuple[aa.K]
    ): Expr[r.R, Composite[kt.R]] =
        Expr(
            IrExpr.JsonTest(aa.asExpr(self).expr, Some(SqlJsonNodeType.Object), false)
        )

    /**
     * `IS JSON ARRAY` predicate. Typically used in `filter`, `on`, and
     * `having` clause. Tests whether the string expression is a valid JSON
     * array.
     *
     * {{{
     * from(Log).filter(l => l.log.isJsonArray)
     * }}}
     */
    def isJsonArray(using
        s: ValidateArgType[SqlString, aa.R, "string"],
        r: Relation[aa.R, String],
        kt: KindToTuple[aa.K]
    ): Expr[r.R, Composite[kt.R]] =
        Expr(
            IrExpr.JsonTest(aa.asExpr(self).expr, Some(SqlJsonNodeType.Array), false)
        )

    /**
     * `IS JSON SCALAR` predicate. Typically used in `filter`, `on`, and
     * `having` clause. Tests whether the string expression is a valid JSON
     * scalar value (string, number, boolean, or null).
     *
     * {{{
     * from(Log).filter(l => l.log.isJsonScalar)
     * }}}
     */
    def isJsonScalar(using
        s: ValidateArgType[SqlString, aa.R, "string"],
        r: Relation[aa.R, String],
        kt: KindToTuple[aa.K]
    ): Expr[r.R, Composite[kt.R]] =
        Expr(
            IrExpr.JsonTest(aa.asExpr(self).expr, Some(SqlJsonNodeType.Scalar), false)
        )