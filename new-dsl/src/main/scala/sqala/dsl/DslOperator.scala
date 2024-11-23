package sqala.dsl

import java.util.Date
import java.time.{LocalDate, LocalDateTime}
import scala.annotation.targetName

// TODO query、subLink
given valueAndTuple1Equal[L, R](using CanEqual[L, R], QueryContext): CanEqual[L, Tuple1[R]] =
    CanEqual.derived

given tuple1AndValueEqual[L, R](using CanEqual[L, R], QueryContext): CanEqual[Tuple1[L], R] =
    CanEqual.derived

given valueAndOptionEqual[L, R](using CanEqual[L, R], QueryContext): CanEqual[L, Option[R]] =
    CanEqual.derived

given optionAndValueEqual[L, R](using CanEqual[L, R], QueryContext): CanEqual[Option[L], R] =
    CanEqual.derived

given numericEqual[L, R](using Numeric[L], Numeric[R], QueryContext): CanEqual[L, R] =
    CanEqual.derived

given timeEqual[L, R](using DateTime[L], DateTime[R], QueryContext): CanEqual[L, R] =
    CanEqual.derived

given timeStringEqual[L, R <: String | Option[String]](using DateTime[L], QueryContext): CanEqual[L, R] =
    CanEqual.derived

given stringTimeEqual[L <: String | Option[String], R](using DateTime[R], QueryContext): CanEqual[L, R] =
    CanEqual.derived

given nothingEqual[L](using QueryContext): CanEqual[L, Nothing] =
    CanEqual.derived

extension [X](x: X)
    @targetName("gt")
    def >[Y](y: Y)(using QueryContext, CanEqual[X, Y]): Boolean =
        compileTimeOnly

    @targetName("ge")
    def >=[Y](y: Y)(using QueryContext, CanEqual[X, Y]): Boolean =
        compileTimeOnly

    @targetName("lt")
    def <[Y](y: Y)(using QueryContext, CanEqual[X, Y]): Boolean =
        compileTimeOnly

    @targetName("le")
    def <=[Y](y: Y)(using QueryContext, CanEqual[X, Y]): Boolean =
        compileTimeOnly

    def between[S, E](s: S, e: E)(using QueryContext, CanEqual[X, S], CanEqual[X, E]): Boolean =
        compileTimeOnly

    def in[I <: Tuple](items: I)(using QueryContext, CanIn[X, I]): Boolean =
        compileTimeOnly

extension (x: Json | Option[Json])
    def ->(n: Int)(using QueryContext): Option[Json] = compileTimeOnly

    def ->(k: String)(using QueryContext): Option[Json] = compileTimeOnly

    def ->>(n: Int)(using QueryContext): Option[String] = compileTimeOnly

    def ->>(k: String)(using QueryContext): Option[String] = compileTimeOnly

extension [X: Numeric](x: X)
    @targetName("plus")
    def +[Y: Numeric](y: Y)(using QueryContext): NumericOperationResult[X, Y] = compileTimeOnly

    @targetName("plus")
    def +(y: String | Option[String])(using QueryContext): Option[String] = compileTimeOnly

    @targetName("minus")
    def -[Y: Numeric](y: Y)(using QueryContext): NumericOperationResult[X, Y] = compileTimeOnly

    @targetName("times")
    def *[Y: Numeric](y: Y)(using QueryContext): NumericOperationResult[X, Y] = compileTimeOnly

    @targetName("div")
    def /[Y: Numeric](y: Y)(using QueryContext): NumericOperationResult[X, Y] = compileTimeOnly

    @targetName("mod")
    def %[Y: Numeric](y: Y)(using QueryContext): NumericOperationResult[X, Y] = compileTimeOnly

extension (x: String)
    def like(y: String | Option[String])(using QueryContext): Boolean = compileTimeOnly

    def startsWith(y: Option[String])(using QueryContext): Boolean = compileTimeOnly

    def endsWith(y: Option[String])(using QueryContext): Boolean = compileTimeOnly

    def contains(y: Option[String])(using QueryContext): Boolean = compileTimeOnly

extension (x: Option[String])
    @targetName("plus")
    def +[Y: AsSqlExpr](y: Y)(using QueryContext): Option[String] = compileTimeOnly

    def like(y: String | Option[String])(using QueryContext): Boolean = compileTimeOnly

    def startsWith(y: String | Option[String])(using QueryContext): Boolean = compileTimeOnly

    def endsWith(y: String | Option[String])(using QueryContext): Boolean = compileTimeOnly

    def contains(y: String | Option[String])(using QueryContext): Boolean = compileTimeOnly

// TODO 与exists查询的&& || exists的!

extension [X: DateTime](x: X)
    @targetName("plus")
    def +(interval: Interval)(using QueryContext): X = compileTimeOnly

    @targetName("minus")
    def -(interval: Interval)(using QueryContext): X = compileTimeOnly

    @targetName("minus")
    def -(y: Date)(using QueryContext): X = compileTimeOnly

    @targetName("minusOptionDate")
    def -(y: Option[Date])(using QueryContext): X = compileTimeOnly

    @targetName("minus")
    def -(y: LocalDate)(using QueryContext): X = compileTimeOnly

    @targetName("minusOptionLocalDate")
    def -(y: Option[LocalDate])(using QueryContext): X = compileTimeOnly

    @targetName("minus")
    def -(y: LocalDateTime)(using QueryContext): X = compileTimeOnly

    @targetName("minusOptionLocalDateTime")
    def -(y: Option[LocalDateTime])(using QueryContext): X = compileTimeOnly