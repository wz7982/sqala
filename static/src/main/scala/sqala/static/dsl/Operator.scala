package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.order.{SqlNullsOrdering, SqlOrdering}
import sqala.static.metadata.*

import scala.annotation.targetName
import scala.compiletime.ops.boolean.||

extension [A, CL <: Int](self: A)(using qc: QueryContext[CL], aa: AsExpr[A, CL])
    @targetName("equal")
    def ===[B](that: B)(using
        ab: AsRightOperand[B, CL],
        r: Relation[aa.R, ab.R, IsOption[aa.R] || IsOption[ab.R]],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.Equal,
                ab.asExpr(that).asSqlExpr
            )
        )

    @targetName("notEqual")
    def <>[B](that: B)(using
        ab: AsRightOperand[B, CL],
        r: Relation[aa.R, ab.R, IsOption[aa.R] || IsOption[ab.R]],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.NotEqual,
                ab.asExpr(that).asSqlExpr
            )
        )

    def isNull(using t: KindToTuple[aa.K]): Expr[Boolean, Composite[t.R]] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.Is,
                SqlExpr.NullLiteral
            )
        )

    @targetName("eqIgnoreNulls")
    def <=>[B](that: B)(using
        ab: AsRightOperand[B, CL],
        r: Relation[aa.R, ab.R, IsOption[aa.R] || IsOption[ab.R]],
        c: CombineKind[aa.K, ab.K]
    ): Expr[Boolean, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.IsNotDistinctFrom,
                ab.asExpr(that).asSqlExpr
            )
        )

    @targetName("gt")
    def >[B](that: B)(using
        ab: AsRightOperand[B, CL],
        r: Relation[aa.R, ab.R, IsOption[aa.R] || IsOption[ab.R]],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.GreaterThan,
                ab.asExpr(that).asSqlExpr
            )
        )

    @targetName("ge")
    def >=[B](that: B)(using
        ab: AsRightOperand[B, CL],
        r: Relation[aa.R, ab.R, IsOption[aa.R] || IsOption[ab.R]],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.GreaterThanEqual,
                ab.asExpr(that).asSqlExpr
            )
        )

    @targetName("lt")
    def <[B](that: B)(using
        ab: AsRightOperand[B, CL],
        r: Relation[aa.R, ab.R, IsOption[aa.R] || IsOption[ab.R]],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.LessThan,
                ab.asExpr(that).asSqlExpr
            )
        )

    @targetName("le")
    def <=[B](that: B)(using
        ab: AsRightOperand[B, CL],
        r: Relation[aa.R, ab.R, IsOption[aa.R] || IsOption[ab.R]],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.LessThanEqual,
                ab.asExpr(that).asSqlExpr
            )
        )

    def in[B](exprs: B)(using
        i: CanIn[A, B, CL],
        kt: KindToTuple[aa.K],
        c: CombineKindTuple[kt.R, i.KS]
    ): Expr[i.R, Composite[c.R]] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.In,
                i.asExpr(exprs).asSqlExpr
            )
        )

    def between[S, E](start: S, end: E)(using
        as: AsExpr[S, CL],
        ae: AsExpr[E, CL],
        rs: Relation[aa.R, as.R, IsOption[aa.R] || IsOption[as.R]],
        r: Relation[rs.R, ae.R, IsOption[rs.R] || IsOption[ae.R]],
        cs: CombineKind[aa.K, as.K],
        c: CombineKind[cs.R, ae.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Between(
                aa.asExpr(self).asSqlExpr,
                as.asExpr(start).asSqlExpr,
                ae.asExpr(end).asSqlExpr,
                false
            )
        )

    @targetName("plus")
    def +[B](that: B)(using
        ab: AsExpr[B, CL],
        r: Plus[aa.R, ab.R, IsOption[aa.R] || IsOption[ab.R]],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(r.plus(aa.asExpr(self).asSqlExpr, ab.asExpr(that).asSqlExpr))

    @targetName("minus")
    def -[B](that: B)(using
        ab: AsExpr[B, CL],
        r: Minus[aa.R, ab.R, IsOption[aa.R] || IsOption[ab.R]],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.Minus,
                ab.asExpr(that).asSqlExpr
            )
        )

    @targetName("times")
    def *[B](that: B)(using
        ab: AsExpr[B, CL],
        na: SqlNumber[aa.R],
        nb: SqlNumber[ab.R],
        r: Return[aa.R, ab.R, IsOption[aa.R] || IsOption[ab.R]],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.Times,
                ab.asExpr(that).asSqlExpr
            )
        )

    @targetName("div")
    def /[B](that: B)(using
        ab: AsExpr[B, CL],
        na: SqlNumber[aa.R],
        nb: SqlNumber[ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[Option[BigDecimal], c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.Div,
                ab.asExpr(that).asSqlExpr
            )
        )

    @targetName("mod")
    def %[B](that: B)(using
        ab: AsExpr[B, CL],
        na: SqlNumber[aa.R],
        nb: SqlNumber[ab.R],
        c: CombineKind[aa.K, ab.K]
    ): Expr[Option[BigDecimal], c.R] =
        Expr(
            SqlExpr.GeneralFunc(
                None,
                "MOD",
                aa.asExpr(self).asSqlExpr :: ab.asExpr(that).asSqlExpr :: Nil,
                Nil,
                Nil,
                None
            )
        )

    @targetName("positive")
    def unary_+(using
        n: SqlNumber[aa.R],
        kt: KindToTuple[aa.K]
    ): Expr[aa.R, Composite[kt.R]] =
        Expr(SqlExpr.Unary(SqlUnaryOperator.Positive, aa.asExpr(self).asSqlExpr))

    @targetName("negative")
    def unary_-(using
        n: SqlNumber[aa.R],
        kt: KindToTuple[aa.K]
    ): Expr[aa.R, Composite[kt.R]] =
        Expr(SqlExpr.Unary(SqlUnaryOperator.Negative, aa.asExpr(self).asSqlExpr))

    @targetName("and")
    def &&[B](that: B)(using
        ab: AsExpr[B, CL],
        ba: SqlBoolean[aa.R],
        bb: SqlBoolean[ab.R],
        r: Relation[aa.R, ab.R, IsOption[aa.R] || IsOption[ab.R]],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.And,
                ab.asExpr(that).asSqlExpr
            )
        )

    @targetName("or")
    def ||[B](that: B)(using
        ab: AsExpr[B, CL],
        ba: SqlBoolean[aa.R],
        bb: SqlBoolean[ab.R],
        r: Relation[aa.R, ab.R, IsOption[aa.R] || IsOption[ab.R]],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Binary(
                aa.asExpr(self).asSqlExpr,
                SqlBinaryOperator.Or,
                ab.asExpr(that).asSqlExpr
            )
        )

    @targetName("not")
    def unary_!(using
        b: SqlBoolean[aa.R],
        kt: KindToTuple[aa.K]
    ): Expr[aa.R, Composite[kt.R]] =
        Expr(SqlExpr.Unary(SqlUnaryOperator.Not, aa.asExpr(self).asSqlExpr))

    def like[B](that: B)(using
        ab: AsExpr[B, CL],
        sa: SqlString[aa.R],
        sb: SqlString[ab.R],
        r: Relation[aa.R, ab.R, IsOption[aa.R] || IsOption[ab.R]],
        c: CombineKind[aa.K, ab.K]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Like(
                aa.asExpr(self).asSqlExpr,
                ab.asExpr(that).asSqlExpr,
                None,
                false
            )
        )

    def contains(value: String)(using
        s: SqlString[aa.R],
        r: Relation[Unwrap[aa.R, Option], String, IsOption[aa.R]],
        c: CombineKind[aa.K, Value]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Like(
                aa.asExpr(self).asSqlExpr,
                s"%$value%".asExpr.asSqlExpr,
                None,
                false
            )
        )

    def startsWith(value: String)(using
        s: SqlString[aa.R],
        r: Relation[Unwrap[aa.R, Option], String, IsOption[aa.R]],
        c: CombineKind[aa.K, Value]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Like(
                aa.asExpr(self).asSqlExpr,
                s"$value%".asExpr.asSqlExpr,
                None,
                false
            )
        )

    def endsWith(value: String)(using
        s: SqlString[aa.R],
        r: Relation[Unwrap[aa.R, Option], String, IsOption[aa.R]],
        c: CombineKind[aa.K, Value]
    ): Expr[r.R, c.R] =
        Expr(
            SqlExpr.Like(
                aa.asExpr(self).asSqlExpr,
                s"%$value".asExpr.asSqlExpr,
                None,
                false
            )
        )

    def isJson(using
        s: SqlString[aa.R],
        r: Relation[Unwrap[aa.R, Option], String, IsOption[aa.R]],
        kt: KindToTuple[aa.K]
    ): Expr[r.R, Composite[kt.R]] =
        Expr(
            SqlExpr.JsonTest(aa.asExpr(self).asSqlExpr, None, None, false)
        )

    def isJsonObject(using
        s: SqlString[aa.R],
        r: Relation[Unwrap[aa.R, Option], String, IsOption[aa.R]],
        kt: KindToTuple[aa.K]
    ): Expr[r.R, Composite[kt.R]] =
        Expr(
            SqlExpr.JsonTest(aa.asExpr(self).asSqlExpr, Some(SqlJsonNodeType.Object), None, false)
        )

    def isJsonArray(using
        s: SqlString[aa.R],
        r: Relation[Unwrap[aa.R, Option], String, IsOption[aa.R]],
        kt: KindToTuple[aa.K]
    ): Expr[r.R, Composite[kt.R]] =
        Expr(
            SqlExpr.JsonTest(aa.asExpr(self).asSqlExpr, Some(SqlJsonNodeType.Array), None, false)
        )

    def isJsonScalar(using
        s: SqlString[aa.R],
        r: Relation[Unwrap[aa.R, Option], String, IsOption[aa.R]],
        kt: KindToTuple[aa.K]
    ): Expr[r.R, Composite[kt.R]] =
        Expr(
            SqlExpr.JsonTest(aa.asExpr(self).asSqlExpr, Some(SqlJsonNodeType.Scalar), None, false)
        )

    def asc(using AsSqlExpr[aa.R]): Sort[aa.R, aa.K] =
        Sort(aa.asExpr(self), SqlOrdering.Asc, None)

    def ascNullsFirst(using AsSqlExpr[aa.R]): Sort[aa.R, aa.K] =
        Sort(aa.asExpr(self), SqlOrdering.Asc, Some(SqlNullsOrdering.First))

    def ascNullsLast(using AsSqlExpr[aa.R]): Sort[aa.R, aa.K] =
        Sort(aa.asExpr(self), SqlOrdering.Asc, Some(SqlNullsOrdering.Last))

    def desc(using AsSqlExpr[aa.R]): Sort[aa.R, aa.K] =
        Sort(aa.asExpr(self), SqlOrdering.Desc, None)

    def descNullsFirst(using AsSqlExpr[aa.R]): Sort[aa.R, aa.K] =
        Sort(aa.asExpr(self), SqlOrdering.Desc, Some(SqlNullsOrdering.First))

    def descNullsLast(using AsSqlExpr[aa.R]): Sort[aa.R, aa.K] =
        Sort(aa.asExpr(self), SqlOrdering.Desc, Some(SqlNullsOrdering.Last))

extension [A, B, CL <: Int](self: (A, B))(using
    qc: QueryContext[CL],
    aa: AsExpr[A, CL],
    ab: AsExpr[B, CL]
)
    def overlaps[C, D](that: (C, D))(using
        ac: AsExpr[C, CL],
        ad: AsExpr[D, CL],
        da: SqlDateTime[aa.R],
        db: SqlDateTime[ab.R],
        dc: SqlDateTime[ac.R],
        dd: SqlDateTime[ad.R],
        r1: Relation[Unwrap[aa.R, Option], Unwrap[ab.R, Option], IsOption[aa.R] || IsOption[ab.R]],
        r2: Relation[Unwrap[ac.R, Option], Unwrap[ad.R, Option], IsOption[ac.R] || IsOption[ad.R]],
        r: Relation[Unwrap[r1.R, Option], Unwrap[r2.R, Option], IsOption[r1.R] || IsOption[r2.R]],
        c1: CombineKind[aa.K, ab.K],
        c2: CombineKind[ac.K, ad.K],
        c: CombineKind[c1.R, c2.R]
    ): Expr[r.R, c.R] =
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