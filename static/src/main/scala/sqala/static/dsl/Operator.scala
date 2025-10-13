package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.order.{SqlNullsOrdering, SqlOrdering}
import sqala.static.metadata.*

import scala.annotation.targetName
import scala.compiletime.ops.boolean.||

extension [T: AsExpr as at](self: T)
    @targetName("equal")
    def ===[R](that: R)(using
        ar: AsRightOperand[R],
        r: Relation[Unwrap[at.R, Option], Unwrap[ar.R, Option], IsOption[at.R] || IsOption[ar.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.Equal, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("notEqual")
    def <>[R](that: R)(using
        ar: AsRightOperand[R],
        r: Relation[Unwrap[at.R, Option], Unwrap[ar.R, Option], IsOption[at.R] || IsOption[ar.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.NotEqual, 
                ar.asExpr(that).asSqlExpr
            )
        )

    def isNull(using QueryContext): Expr[Boolean] =
        Expr(
            SqlExpr.NullTest(at.asExpr(self).asSqlExpr, false)
        )

    @targetName("eqIgnoreNulls")
    def <=>[R](that: R)(using
        ar: AsRightOperand[R],
        c: Compare[Unwrap[at.R, Option], Unwrap[ar.R, Option]],
        qc: QueryContext
    ): Expr[Boolean] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.IsNotDistinctFrom, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("gt")
    def >[R](that: R)(using
        ar: AsRightOperand[R],
        r: Relation[Unwrap[at.R, Option], Unwrap[ar.R, Option], IsOption[at.R] || IsOption[ar.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.GreaterThan, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("ge")
    def >=[R](that: R)(using
        ar: AsRightOperand[R],
        r: Relation[Unwrap[at.R, Option], Unwrap[ar.R, Option], IsOption[at.R] || IsOption[ar.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.GreaterThanEqual, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("lt")
    def <[R](that: R)(using
        ar: AsRightOperand[R],
        r: Relation[Unwrap[at.R, Option], Unwrap[ar.R, Option], IsOption[at.R] || IsOption[ar.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.LessThan, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("le")
    def <=[R](that: R)(using
        ar: AsRightOperand[R],
        r: Relation[Unwrap[at.R, Option], Unwrap[ar.R, Option], IsOption[at.R] || IsOption[ar.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.LessThanEqual, 
                ar.asExpr(that).asSqlExpr
            )
        )

    def in[R](exprs: R)(using
        c: CanIn[at.R, R],
        qc: QueryContext
    ): Expr[c.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.In, 
                c.asExpr(exprs).asSqlExpr
            )
        )

    def between[S, E](start: S, end: E)(using
        as: AsExpr[S],
        rs: Relation[Unwrap[at.R, Option], Unwrap[as.R, Option], IsOption[at.R] || IsOption[as.R]],
        ae: AsExpr[E],
        re: Relation[Unwrap[at.R, Option], Unwrap[ae.R, Option], IsOption[at.R] || IsOption[ae.R]],
        r: Relation[Unwrap[rs.R, Option], Unwrap[re.R, Option], IsOption[rs.R] || IsOption[re.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Between(
                at.asExpr(self).asSqlExpr, 
                as.asExpr(start).asSqlExpr,
                ae.asExpr(end).asSqlExpr,
                false
            )
        )

    @targetName("plus")
    def +[R](that: R)(using
        ar: AsExpr[R],
        r: Plus[Unwrap[at.R, Option], Unwrap[ar.R, Option], IsOption[at.R] || IsOption[ar.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(r.plus(at.asExpr(self).asSqlExpr, ar.asExpr(that).asSqlExpr))

    @targetName("minus")
    def -[R](that: R)(using
        ar: AsExpr[R],
        r: Minus[Unwrap[at.R, Option], Unwrap[ar.R, Option], IsOption[at.R] || IsOption[ar.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.Minus, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("times")
    def *[R](that: R)(using
        nt: SqlNumber[at.R],
        ar: AsExpr[R],
        nr: SqlNumber[ar.R],
        r: Return[Unwrap[at.R, Option], Unwrap[ar.R, Option], IsOption[at.R] || IsOption[ar.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.Times, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("div")
    def /[R](that: R)(using
        nt: SqlNumber[at.R],
        ar: AsExpr[R],
        nr: SqlNumber[ar.R],
        qc: QueryContext
    ): Expr[Option[BigDecimal]] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.Div, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("mod")
    def %[R](that: R)(using
        nt: SqlNumber[at.R],
        ar: AsExpr[R],
        nr: SqlNumber[ar.R],
        qc: QueryContext
    ): Expr[Option[BigDecimal]] =
        Expr(
            SqlExpr.StandardFunc(
                None,
                "MOD",
                at.asExpr(self).asSqlExpr :: ar.asExpr(that).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )

    @targetName("positive")
    def unary_+(using SqlNumber[at.R], QueryContext): Expr[at.R] =
        Expr(SqlExpr.Unary(SqlUnaryOperator.Positive, at.asExpr(self).asSqlExpr))

    @targetName("negative")
    def unary_-(using SqlNumber[at.R], QueryContext): Expr[at.R] =
        Expr(SqlExpr.Unary(SqlUnaryOperator.Negative, at.asExpr(self).asSqlExpr))

    @targetName("and")
    def &&[R](that: R)(using
        rt: SqlBoolean[at.R],
        ar: AsExpr[R],
        rr: SqlBoolean[ar.R],
        r: Relation[Unwrap[at.R, Option], Unwrap[ar.R, Option], IsOption[at.R] || IsOption[ar.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.And, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("or")
    def ||[R](that: R)(using
        rt: SqlBoolean[at.R],
        ar: AsExpr[R],
        rr: SqlBoolean[ar.R],
        r: Relation[Unwrap[at.R, Option], Unwrap[ar.R, Option], IsOption[at.R] || IsOption[ar.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.Or, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("not")
    def unary_!(using SqlBoolean[at.R], QueryContext): Expr[at.R] =
        Expr(SqlExpr.Unary(SqlUnaryOperator.Not, at.asExpr(self).asSqlExpr))

    def like[R](that: R)(using
        rt: SqlString[at.R],
        ar: AsExpr[R],
        rr: SqlString[ar.R],
        r: Relation[Unwrap[at.R, Option], Unwrap[ar.R, Option], IsOption[at.R] || IsOption[ar.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.Like, 
                ar.asExpr(that).asSqlExpr
            )
        )

    def contains(value: String)(using 
        rt: SqlString[at.R],
        r: Relation[Unwrap[at.R, Option], String, IsOption[at.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.Like, 
                s"%$value%".asExpr.asSqlExpr
            )
        )

    def startsWith(value: String)(using 
        rt: SqlString[at.R],
        r: Relation[Unwrap[at.R, Option], String, IsOption[at.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.Like, 
                s"$value%".asExpr.asSqlExpr
            )
        )

    def endsWith(value: String)(using 
        rt: SqlString[at.R],
        r: Relation[Unwrap[at.R, Option], String, IsOption[at.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.Like, 
                s"%$value".asExpr.asSqlExpr
            )
        )

    def isJson(using
        rt: SqlString[at.R],
        r: Relation[Unwrap[at.R, Option], String, IsOption[at.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.JsonTest(at.asExpr(self).asSqlExpr, false, None, None)
        )

    def isJsonObject(using
        rt: SqlString[at.R],
        r: Relation[Unwrap[at.R, Option], String, IsOption[at.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.JsonTest(at.asExpr(self).asSqlExpr, false, Some(SqlJsonNodeType.Object), None)
        )

    def isJsonArray(using
        rt: SqlString[at.R],
        r: Relation[Unwrap[at.R, Option], String, IsOption[at.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.JsonTest(at.asExpr(self).asSqlExpr, false, Some(SqlJsonNodeType.Array), None)
        )

    def isJsonScalar(using
        rt: SqlString[at.R],
        r: Relation[Unwrap[at.R, Option], String, IsOption[at.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.JsonTest(at.asExpr(self).asSqlExpr, false, Some(SqlJsonNodeType.Scalar), None)
        )

    def asc(using AsSqlExpr[at.R], QueryContext): Sort = 
        Sort(at.asExpr(self), SqlOrdering.Asc, None)

    def ascNullsFirst(using AsSqlExpr[at.R], QueryContext): Sort =
        Sort(at.asExpr(self), SqlOrdering.Asc, Some(SqlNullsOrdering.First))

    def ascNullsLast(using AsSqlExpr[at.R], QueryContext): Sort =
        Sort(at.asExpr(self), SqlOrdering.Asc, Some(SqlNullsOrdering.Last))

    def desc(using AsSqlExpr[at.R], QueryContext): Sort =
        Sort(at.asExpr(self), SqlOrdering.Desc, None)

    def descNullsFirst(using AsSqlExpr[at.R], QueryContext): Sort =
        Sort(at.asExpr(self), SqlOrdering.Desc, Some(SqlNullsOrdering.First))

    def descNullsLast(using AsSqlExpr[at.R], QueryContext): Sort =
        Sort(at.asExpr(self), SqlOrdering.Desc, Some(SqlNullsOrdering.Last))

extension [A: AsExpr as aa, B: AsExpr as ab](self: (A, B))
    def overlaps[C: AsExpr as ac, D: AsExpr as ad](that: (C, D))(using
        da: SqlDateTime[aa.R],
        db: SqlDateTime[ab.R],
        dc: SqlDateTime[ac.R],
        dd: SqlDateTime[ad.R],
        r1: Relation[Unwrap[aa.R, Option], Unwrap[ab.R, Option], IsOption[aa.R] || IsOption[ab.R]],
        r2: Relation[Unwrap[ac.R, Option], Unwrap[ad.R, Option], IsOption[ac.R] || IsOption[ad.R]],
        r: Relation[Unwrap[r1.R, Option], Unwrap[r2.R, Option], IsOption[r1.R] || IsOption[r2.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                SqlExpr.Tuple(
                    aa.asExpr(self._1).asSqlExpr :: 
                    ab.asExpr(self._2).asSqlExpr ::
                    Nil
                ),
                SqlBinaryOperator.Overlaps, 
                SqlExpr.Tuple(
                    ac.asExpr(that._1).asSqlExpr :: 
                    ad.asExpr(that._2).asSqlExpr ::
                    Nil
                )
            )
        )