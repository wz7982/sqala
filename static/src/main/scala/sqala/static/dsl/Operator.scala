package sqala.static.dsl

import sqala.ast.expr.*
import sqala.ast.order.{SqlNullsOrdering, SqlOrdering}
import sqala.metadata.{AsSqlExpr, DateTime, Json, Number, Vector}

import java.time.LocalDateTime
import scala.annotation.targetName
import scala.compiletime.ops.boolean.||

extension [T: AsExpr as at](self: T)
    @targetName("equal")
    def ===[R](that: R)(using
        ar: AsRightOperand[R],
        c: Compare[Unwrap[at.R, Option], Unwrap[ar.R, Option]],
        qc: QueryContext
    ): Expr[Option[Boolean]] =
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
        c: Compare[Unwrap[at.R, Option], Unwrap[ar.R, Option]],
        qc: QueryContext
    ): Expr[Option[Boolean]] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.NotEqual, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("gt")
    def >[R](that: R)(using
        ar: AsRightOperand[R],
        c: Compare[Unwrap[at.R, Option], Unwrap[ar.R, Option]],
        qc: QueryContext
    ): Expr[Option[Boolean]] =
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
        c: Compare[Unwrap[at.R, Option], Unwrap[ar.R, Option]],
        qc: QueryContext
    ): Expr[Option[Boolean]] =
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
        c: Compare[Unwrap[at.R, Option], Unwrap[ar.R, Option]],
        qc: QueryContext
    ): Expr[Option[Boolean]] =
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
        c: Compare[Unwrap[at.R, Option], Unwrap[ar.R, Option]],
        qc: QueryContext
    ): Expr[Option[Boolean]] =
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
    ): Expr[Option[Boolean]] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.In, 
                c.asExpr(exprs).asSqlExpr
            )
        )

    def between[S, E](start: S, end: E)(using
        as: AsExpr[S],
        cs: Compare[Unwrap[at.R, Option], Unwrap[as.R, Option]],
        ae: AsExpr[E],
        ce: Compare[Unwrap[at.R, Option], Unwrap[ae.R, Option]],
        qc: QueryContext
    ): Expr[Option[Boolean]] =
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
        nt: Number[at.R],
        ar: AsExpr[R],
        nr: Number[ar.R],
        r: Return[Unwrap[at.R, Option], Unwrap[ar.R, Option], IsOption[at.R] || IsOption[ar.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.Plus, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("plus")
    def +(interval: TimeInterval)(using
        d: DateTime[at.R],
        r: Return[Unwrap[at.R, Option], LocalDateTime, IsOption[at.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.Plus, 
                SqlExpr.IntervalLiteral(interval.value, SqlIntervalField.Single(interval.unit))
            )
        )

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

    @targetName("minus")
    def -(interval: TimeInterval)(using
        d: DateTime[at.R],
        r: Return[Unwrap[at.R, Option], LocalDateTime, IsOption[at.R]],
        qc: QueryContext
    ): Expr[r.R] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.Minus, 
                SqlExpr.IntervalLiteral(interval.value, SqlIntervalField.Single(interval.unit))
            )
        )

    @targetName("times")
    def *[R](that: R)(using
        nt: Number[at.R],
        ar: AsExpr[R],
        nr: Number[ar.R],
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
        nt: Number[at.R],
        ar: AsExpr[R],
        nr: Number[ar.R],
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
        nt: Number[at.R],
        ar: AsExpr[R],
        nr: Number[ar.R],
        qc: QueryContext
    ): Expr[Option[BigDecimal]] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.Mod, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("positive")
    def unary_+(using Number[at.R], QueryContext): Expr[at.R] =
        Expr(SqlExpr.Unary(at.asExpr(self).asSqlExpr, SqlUnaryOperator.Positive))

    @targetName("negative")
    def unary_-(using Number[at.R], QueryContext): Expr[at.R] =
        Expr(SqlExpr.Unary(at.asExpr(self).asSqlExpr, SqlUnaryOperator.Negative))

    @targetName("and")
    def &&[R](that: R)(using
        rt: at.R <:< (Boolean | Option[Boolean]),
        ar: AsExpr[R],
        rr: ar.R <:< (Boolean | Option[Boolean]),
        qc: QueryContext
    ): Expr[Option[Boolean]] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.And, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("or")
    def ||[R](that: R)(using
        rt: at.R <:< (Boolean | Option[Boolean]),
        ar: AsExpr[R],
        rr: ar.R <:< (Boolean | Option[Boolean]),
        qc: QueryContext
    ): Expr[Option[Boolean]] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.Or, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("not")
    def unary_!(using at.R <:< (Boolean | Option[Boolean]), QueryContext): Expr[Option[Boolean]] =
        Expr(SqlExpr.Unary(at.asExpr(self).asSqlExpr, SqlUnaryOperator.Not))

    def like[R](that: R)(using
        rt: at.R <:< (String | Option[String]),
        ar: AsExpr[R],
        rr: ar.R <:< (String | Option[String]),
        qc: QueryContext
    ): Expr[Option[Boolean]] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.Like, 
                ar.asExpr(that).asSqlExpr
            )
        )

    def contains(value: String)(using 
        at.R <:< (String | Option[String]),
        QueryContext
    ): Expr[Option[Boolean]] =
        like(s"%$value%")

    def startsWith(value: String)(using 
        at.R <:< (String | Option[String]),
        QueryContext
    ): Expr[Option[Boolean]] =
        like(s"$value%")

    def endsWith(value: String)(using 
        at.R <:< (String | Option[String]),
        QueryContext
    ): Expr[Option[Boolean]] =
        like(s"%$value")

    @targetName("concatString")
    def ++[R](that: R)(using
        rt: at.R <:< (String | Option[String]),
        ar: AsExpr[R],
        rr: ar.R <:< (String | Option[String]),
        qc: QueryContext
    ): Expr[Option[String]] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.Concat, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("json")
    def ->(path: Int | String)(using 
        at.R <:< (Json | Option[Json]),
        QueryContext
    ): Expr[Option[Json]] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.Json, 
                path match
                    case p: Int => SqlExpr.NumberLiteral(p)
                    case p: String => SqlExpr.StringLiteral(p)
            )
        )

    @targetName("jsonText")
    def ->>(path: Int | String)(using 
        at.R <:< (Json | Option[Json]),
        QueryContext
    ): Expr[Option[String]] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.JsonText, 
                path match
                    case p: Int => SqlExpr.NumberLiteral(p)
                    case p: String => SqlExpr.StringLiteral(p)
            )
        )

    @targetName("euclideanDistance")
    def <->[R](that: R)(using
        rt: at.R <:< (Vector | Option[Vector]),
        ar: AsExpr[R],
        rr: ar.R <:< (Vector | Option[Vector]),
        qc: QueryContext
    ): Expr[Option[Float]] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.EuclideanDistance, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("euclideanDistance")
    def <->(that: String)(using
        rt: at.R <:< (Vector | Option[Vector]),
        qc: QueryContext
    ): Expr[Option[Float]] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.EuclideanDistance, 
                summon[AsSqlExpr[Vector]].asSqlExpr(Vector(that))
            )
        )

    @targetName("cosineDistance")
    def <=>[R](that: R)(using
        rt: at.R <:< (Vector | Option[Vector]),
        ar: AsExpr[R],
        rr: ar.R <:< (Vector | Option[Vector]),
        qc: QueryContext
    ): Expr[Option[Float]] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.CosineDistance, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("cosineDistance")
    def <=>(that: String)(using
        rt: at.R <:< (Vector | Option[Vector]),
        qc: QueryContext
    ): Expr[Option[Float]] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.CosineDistance, 
                summon[AsSqlExpr[Vector]].asSqlExpr(Vector(that))
            )
        )

    @targetName("dotDistance")
    def <#>[R](that: R)(using
        rt: at.R <:< (Vector | Option[Vector]),
        ar: AsExpr[R],
        rr: ar.R <:< (Vector | Option[Vector]),
        qc: QueryContext
    ): Expr[Option[Float]] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.DotDistance, 
                ar.asExpr(that).asSqlExpr
            )
        )

    @targetName("dotDistance")
    def <#>(that: String)(using
        rt: at.R <:< (Vector | Option[Vector]),
        qc: QueryContext
    ): Expr[Option[Float]] =
        Expr(
            SqlExpr.Binary(
                at.asExpr(self).asSqlExpr, 
                SqlBinaryOperator.DotDistance, 
                summon[AsSqlExpr[Vector]].asSqlExpr(Vector(that))
            )
        )

    def asc(using AsSqlExpr[at.R], QueryContext): Sort[at.R] = 
        Sort(at.asExpr(self), SqlOrdering.Asc, None)

    def ascNullsFirst(using AsSqlExpr[at.R], QueryContext): Sort[at.R] =
        Sort(at.asExpr(self), SqlOrdering.Asc, Some(SqlNullsOrdering.First))

    def ascNullsLast(using AsSqlExpr[at.R], QueryContext): Sort[at.R] =
        Sort(at.asExpr(self), SqlOrdering.Asc, Some(SqlNullsOrdering.Last))

    def desc(using AsSqlExpr[at.R], QueryContext): Sort[at.R] =
        Sort(at.asExpr(self), SqlOrdering.Desc, None)

    def descNullsFirst(using AsSqlExpr[at.R], QueryContext): Sort[at.R] =
        Sort(at.asExpr(self), SqlOrdering.Desc, Some(SqlNullsOrdering.First))

    def descNullsLast(using AsSqlExpr[at.R], QueryContext): Sort[at.R] =
        Sort(at.asExpr(self), SqlOrdering.Desc, Some(SqlNullsOrdering.Last))