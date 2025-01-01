package sqala.static.dsl

import sqala.static.common.*
import sqala.static.statement.query.*

import java.time.{LocalDate, LocalDateTime}
import scala.annotation.targetName

given valueAndOptionEqual[L, R](using CanEqual[L, R], QueryContext): CanEqual[L, Option[R]] =
    CanEqual.derived

given optionAndValueEqual[L, R](using CanEqual[L, R], QueryContext): CanEqual[Option[L], R] =
    CanEqual.derived

given valueAndNoneEqual[T](using QueryContext): CanEqual[T, None.type] =
    CanEqual.derived

given numberEqual[L, R](using Number[L], Number[R], QueryContext): CanEqual[L, R] =
    CanEqual.derived

given timeEqual[L, R](using DateTime[L], DateTime[R], QueryContext): CanEqual[L, R] =
    CanEqual.derived

given timeStringEqual[T](using DateTime[T], QueryContext): CanEqual[T, String] =
    CanEqual.derived

given stringTimeEqual[T](using DateTime[T], QueryContext): CanEqual[String, T] =
    CanEqual.derived

given valueAndQueryEqual[L, R, S <: ResultSize](using CanEqual[L, R], QueryContext): CanEqual[L, Query[R, S]] =
    CanEqual.derived

given valueAndSubLinkEqual[L, R](using CanEqual[L, R], QueryContext): CanEqual[L, SubLink[R]] =
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

    def in[Y](items: Seq[Y])(using QueryContext, CanEqual[X, Y]): Boolean =
        compileTimeOnly

    def in[Y, S <: ResultSize](query: Query[Y, S])(using QueryContext, CanEqual[X, Y]): Boolean =
        compileTimeOnly

extension (x: Json | Option[Json])
    def ->(n: Int)(using QueryContext): Option[Json] = compileTimeOnly

    def ->(k: String)(using QueryContext): Option[Json] = compileTimeOnly

    def ->>(n: Int)(using QueryContext): Option[String] = compileTimeOnly

    def ->>(k: String)(using QueryContext): Option[String] = compileTimeOnly

extension [X: Number](x: X)
    @targetName("plus")
    def +[Y: Number](y: Y)(using QueryContext): NumericResult[X, Y, HasOption[X, Y]] = compileTimeOnly

    @targetName("plus")
    def +(y: String | Option[String])(using QueryContext): Option[String] = compileTimeOnly

    @targetName("minus")
    def -[Y: Number](y: Y)(using QueryContext): NumericResult[X, Y, HasOption[X, Y]] = compileTimeOnly

    @targetName("times")
    def *[Y: Number](y: Y)(using QueryContext): NumericResult[X, Y, HasOption[X, Y]] = compileTimeOnly

    @targetName("div")
    def /[Y: Number](y: Y)(using QueryContext): NumericResult[X, Y, HasOption[X, Y]] = compileTimeOnly

    @targetName("mod")
    def %[Y: Number](y: Y)(using QueryContext): NumericResult[X, Y, HasOption[X, Y]] = compileTimeOnly

    @targetName("positive")
    def unary_+(using QueryContext): X = compileTimeOnly

    @targetName("negative")
    def unary_-(using QueryContext): X = compileTimeOnly

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

extension [X: DateTime](x: X)
    @targetName("plus")
    def +(interval: TimeInterval)(using QueryContext): X = compileTimeOnly

    @targetName("minus")
    def -(interval: TimeInterval)(using QueryContext): X = compileTimeOnly

    @targetName("minus")
    def -(y: LocalDate | Option[LocalDate] | LocalDateTime | Option[LocalDateTime])(using
        QueryContext
    ): Option[Interval] = compileTimeOnly