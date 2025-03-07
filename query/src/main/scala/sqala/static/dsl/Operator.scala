package sqala.static.dsl

import sqala.ast.expr.SqlBinaryOperator
import sqala.ast.expr.SqlBinaryOperator.*
import sqala.ast.expr.SqlUnaryOperator.*
import sqala.ast.order.SqlOrderOption.*
import sqala.ast.order.SqlOrderNullsOption.*
import sqala.common.*

import java.time.LocalDateTime
import scala.annotation.targetName
import scala.compiletime.ops.boolean.||

extension [T: AsExpr as at](self: T)
    @targetName("equal")
    def ===[R](that: R)(using
        ar: AsExpr[R],
        c: CompareOperation[Unwrap[at.R, Option], Unwrap[ar.R, Option]]
    ): Expr[Boolean] =
        Expr.Binary(at.asExpr(self), Equal, ar.asExpr(that))

    @targetName("equal")
    def ===[R](item: SubLinkItem[R])(using
        CompareOperation[Unwrap[at.R, Option], Unwrap[R, Option]]
    ): Expr[Boolean] =
        Expr.Binary(at.asExpr(self), Equal, Expr.SubLink(item.query, item.linkType))

    @targetName("notEqual")
    def <>[R](that: R)(using
        ar: AsExpr[R],
        c: CompareOperation[Unwrap[at.R, Option], Unwrap[ar.R, Option]]
    ): Expr[Boolean] =
        Expr.Binary(at.asExpr(self), NotEqual, ar.asExpr(that))

    @targetName("notEqual")
    def <>[R](item: SubLinkItem[R])(using
        CompareOperation[Unwrap[at.R, Option], Unwrap[R, Option]]
    ): Expr[Boolean] =
        Expr.Binary(at.asExpr(self), NotEqual, Expr.SubLink(item.query, item.linkType))

    @targetName("gt")
    def >[R](that: R)(using
        ar: AsExpr[R],
        c: CompareOperation[Unwrap[at.R, Option], Unwrap[ar.R, Option]]
    ): Expr[Boolean] =
        Expr.Binary(at.asExpr(self), GreaterThan, ar.asExpr(that))

    @targetName("gt")
    def >[R](item: SubLinkItem[R])(using
        CompareOperation[Unwrap[at.R, Option], Unwrap[R, Option]]
    ): Expr[Boolean] =
        Expr.Binary(at.asExpr(self), GreaterThan, Expr.SubLink(item.query, item.linkType))

    @targetName("ge")
    def >=[R](that: R)(using
        ar: AsExpr[R],
        c: CompareOperation[Unwrap[at.R, Option], Unwrap[ar.R, Option]]
    ): Expr[Boolean] =
        Expr.Binary(at.asExpr(self), GreaterThanEqual, ar.asExpr(that))

    @targetName("ge")
    def >=[R](item: SubLinkItem[R])(using
        CompareOperation[Unwrap[at.R, Option], Unwrap[R, Option]]
    ): Expr[Boolean] =
        Expr.Binary(at.asExpr(self), GreaterThanEqual, Expr.SubLink(item.query, item.linkType))

    @targetName("lt")
    def <[R](that: R)(using
        ar: AsExpr[R],
        c: CompareOperation[Unwrap[at.R, Option], Unwrap[ar.R, Option]]
    ): Expr[Boolean] =
        Expr.Binary(at.asExpr(self), LessThan, ar.asExpr(that))

    @targetName("lt")
    def <[R](item: SubLinkItem[R])(using
        CompareOperation[Unwrap[at.R, Option], Unwrap[R, Option]]
    ): Expr[Boolean] =
        Expr.Binary(at.asExpr(self), LessThan, Expr.SubLink(item.query, item.linkType))

    @targetName("le")
    def <=[R](that: R)(using
        ar: AsExpr[R],
        c: CompareOperation[Unwrap[at.R, Option], Unwrap[ar.R, Option]]
    ): Expr[Boolean] =
        Expr.Binary(at.asExpr(self), LessThanEqual, ar.asExpr(that))

    @targetName("le")
    def <=[R](item: SubLinkItem[R])(using
        CompareOperation[Unwrap[at.R, Option], Unwrap[R, Option]]
    ): Expr[Boolean] =
        Expr.Binary(at.asExpr(self), LessThanEqual, Expr.SubLink(item.query, item.linkType))

    def in[R](list: Seq[R])(using
        ar: AsExpr[R],
        o: CompareOperation[Unwrap[at.R, Option], Unwrap[ar.R, Option]]
    ): Expr[Boolean] =
        Expr.In(at.asExpr(self), Expr.Tuple(list.toList.map(ar.asExpr(_))), false)

    def in[R](exprs: R)(using
        c: CanIn[at.R, R]
    ): Expr[Boolean] =
        Expr.In(at.asExpr(self), c.asExpr(exprs), false)

    def between[S, E](start: S, end: E)(using
        as: AsExpr[S],
        cs: CompareOperation[Unwrap[at.R, Option], Unwrap[as.R, Option]],
        ae: AsExpr[E],
        ce: CompareOperation[Unwrap[at.R, Option], Unwrap[ae.R, Option]]
    ): Expr[Boolean] =
        Expr.Between(at.asExpr(self), as.asExpr(start), ae.asExpr(end), false)

    @targetName("plus")
    def +[R](that: R)(using
        nt: Number[at.R],
        ar: AsExpr[R],
        nr: Number[ar.R],
        r: ResultOperation[Unwrap[at.R, Option], Unwrap[ar.R, Option], IsOption[at.R] || IsOption[ar.R]]
    ): Expr[r.R] =
        Expr.Binary(at.asExpr(self), Plus, ar.asExpr(that))

    @targetName("plus")
    def +(interval: TimeInterval)(using
        d: DateTime[at.R],
        r: ResultOperation[Unwrap[at.R, Option], LocalDateTime, IsOption[at.R]]
    ): Expr[r.R] =
        Expr.Binary(at.asExpr(self), Plus, Expr.Interval(interval.value, interval.unit))

    @targetName("minus")
    def -[R](that: R)(using
        ar: AsExpr[R],
        r: MinusOperation[Unwrap[at.R, Option], Unwrap[ar.R, Option], IsOption[at.R] || IsOption[ar.R]]
    ): Expr[r.R] =
        Expr.Binary(at.asExpr(self), Minus, ar.asExpr(that))

    @targetName("minus")
    def -(interval: TimeInterval)(using
        d: DateTime[at.R],
        r: ResultOperation[Unwrap[at.R, Option], LocalDateTime, IsOption[at.R]]
    ): Expr[r.R] =
        Expr.Binary(at.asExpr(self), Minus, Expr.Interval(interval.value, interval.unit))

    @targetName("times")
    def *[R](that: R)(using
        nt: Number[at.R],
        ar: AsExpr[R],
        nr: Number[ar.R],
        r: ResultOperation[Unwrap[at.R, Option], Unwrap[ar.R, Option], IsOption[at.R] || IsOption[ar.R]]
    ): Expr[r.R] =
        Expr.Binary(at.asExpr(self), Times, ar.asExpr(that))

    @targetName("div")
    def /[R](that: R)(using
        nt: Number[at.R],
        ar: AsExpr[R],
        nr: Number[ar.R]
    ): Expr[Option[BigDecimal]] =
        Expr.Binary(at.asExpr(self), Div, ar.asExpr(that))

    @targetName("mod")
    def %[R](that: R)(using
        nt: Number[at.R],
        ar: AsExpr[R],
        nr: Number[ar.R]
    ): Expr[Option[BigDecimal]] =
        Expr.Binary(at.asExpr(self), Mod, ar.asExpr(that))

    @targetName("positive")
    def unary_+(using Number[at.R]): Expr[at.R] =
        Expr.Unary(at.asExpr(self), Positive)

    @targetName("negative")
    def unary_-(using Number[at.R]): Expr[at.R] =
        Expr.Unary(at.asExpr(self), Negative)

    @targetName("and")
    def &&[R](that: R)(using
        rt: at.R =:= Boolean,
        ar: AsExpr[R],
        rr: ar.R =:= Boolean
    ): Expr[Boolean] =
        Expr.Binary(at.asExpr(self), And, ar.asExpr(that))

    @targetName("or")
    def ||[R](that: R)(using
        rt: at.R =:= Boolean,
        ar: AsExpr[R],
        rr: ar.R =:= Boolean
    ): Expr[Boolean] =
        Expr.Binary(at.asExpr(self), Or, ar.asExpr(that))

    @targetName("not")
    def unary_!(using at.R =:= Boolean): Expr[Boolean] =
        Expr.Unary(at.asExpr(self), Not)

    def like[R](that: R)(using
        rt: at.R <:< (String | Option[String]),
        ar: AsExpr[R],
        rr: ar.R <:< (String | Option[String])
    ): Expr[Boolean] =
        Expr.Binary(at.asExpr(self), Like, ar.asExpr(that))

    def contains(value: String)(using at.R <:< (String | Option[String])): Expr[Boolean] =
        like("%" + value + "%")

    def startWith(value: String)(using at.R <:< (String | Option[String])): Expr[Boolean] =
        like(value + "%")

    def endWith(value: String)(using at.R <:< (String | Option[String])): Expr[Boolean] =
        like("%" + value)

    @targetName("json")
    def ->(n: Int)(using at.R <:< (Json | Option[Json])): Expr[Option[Json]] =
        Expr.Binary(at.asExpr(self), SqlBinaryOperator.Json, Expr.Literal(n, summon[AsSqlExpr[Int]]))

    @targetName("json")
    def ->(n: String)(using at.R <:< (Json | Option[Json])): Expr[Option[Json]] =
        Expr.Binary(at.asExpr(self), SqlBinaryOperator.Json, Expr.Literal(n, summon[AsSqlExpr[String]]))

    @targetName("jsonText")
    def ->>(n: Int)(using at.R <:< (Json | Option[Json])): Expr[Option[String]] =
        Expr.Binary(at.asExpr(self), JsonText, Expr.Literal(n, summon[AsSqlExpr[Int]]))

    @targetName("jsonText")
    def ->>(n: String)(using at.R <:< (Json | Option[Json])): Expr[Option[String]] =
        Expr.Binary(at.asExpr(self), JsonText, Expr.Literal(n, summon[AsSqlExpr[String]]))

    def asc: Sort[at.R] = Sort(at.asExpr(self), Asc, None)

    def desc: Sort[at.R] = Sort(at.asExpr(self), Desc, None)

    def ascNullsFirst: Sort[at.R] = Sort(at.asExpr(self), Asc, Some(First))

    def ascNullsLast: Sort[at.R] = Sort(at.asExpr(self), Asc, Some(Last))

    def descNullsFirst: Sort[at.R] = Sort(at.asExpr(self), Desc, Some(First))

    def descNullsLast: Sort[at.R] = Sort(at.asExpr(self), Desc, Some(Last))