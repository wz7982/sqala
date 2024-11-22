package sqala.dsl

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

extension [X](x: X)(using AsSqlExpr[X], QueryContext)
    def asc: SortOption[X] = compileTimeOnly

    def ascNullsFirst: SortOption[X] = compileTimeOnly

    def ascNullsLast: SortOption[X] = compileTimeOnly

    def desc: SortOption[X] = compileTimeOnly

    def descNullsFirst: SortOption[X] = compileTimeOnly

    def descNullsLast: SortOption[X] = compileTimeOnly

    def as[Y](using Cast[X, Y]): Option[Y] = compileTimeOnly

extension [X](x: X)(using QueryContext)
    @targetName("gt")
    def >[Y](y: Y)(using CanEqual[X, Y]): Boolean =
        compileTimeOnly

    @targetName("ge")
    def >=[Y](y: Y)(using CanEqual[X, Y]): Boolean =
        compileTimeOnly

    @targetName("lt")
    def <[Y](y: Y)(using CanEqual[X, Y]): Boolean =
        compileTimeOnly

    @targetName("le")
    def <=[Y](y: Y)(using CanEqual[X, Y]): Boolean =
        compileTimeOnly

    def between[S, E](s: S, e: E)(using CanEqual[X, S], CanEqual[X, E]): Boolean =
        compileTimeOnly

    def in[I <: Tuple](items: I)(using CanIn[X, I]): Boolean =
        compileTimeOnly

extension (x: Json | Option[Json])(using QueryContext)
    def ->(n: Int): Option[Json] = compileTimeOnly

    def ->(k: String): Option[Json] = compileTimeOnly

    def ->>(n: Int): Option[String] = compileTimeOnly

    def ->>(k: String): Option[String] = compileTimeOnly

extension [X: Numeric](x: X)(using QueryContext)
    @targetName("plus")
    def +[Y: Numeric](y: Y): NumericOperationResult[X, Y] = compileTimeOnly

    @targetName("plus")
    def +(y: String | Option[String]): Option[String] = compileTimeOnly

    @targetName("minus")
    def -[Y: Numeric](y: Y): NumericOperationResult[X, Y] = compileTimeOnly

    @targetName("times")
    def *[Y: Numeric](y: Y): NumericOperationResult[X, Y] = compileTimeOnly

    @targetName("div")
    def /[Y: Numeric](y: Y): NumericOperationResult[X, Y] = compileTimeOnly

    @targetName("mod")
    def %[Y: Numeric](y: Y): NumericOperationResult[X, Y] = compileTimeOnly

    @targetName("positive")
    def unary_+ : X = compileTimeOnly

    @targetName("negative")
    def unary_- : X = compileTimeOnly

extension (x: String)(using QueryContext)
    @targetName("plus")
    def +(y: Option[String]): Option[String] = compileTimeOnly

    @targetName("plus")
    def +[Y: Numeric](y: Y): Option[String] = compileTimeOnly

    def like(y: String | Option[String]): Boolean = compileTimeOnly

    def startsWith(y: Option[String]): Boolean = compileTimeOnly

    def endsWith(y: Option[String]): Boolean = compileTimeOnly

    def contains(y: Option[String]): Boolean = compileTimeOnly

extension (x: Option[String])(using QueryContext)
    @targetName("plus")
    def +(y: String | Option[String]): Option[String] = compileTimeOnly

    @targetName("plus")
    def +[Y: Numeric](y: Y): Option[String] = compileTimeOnly

    def like(y: String | Option[String]): Boolean = compileTimeOnly

    def startsWith(y: String | Option[String]): Boolean = compileTimeOnly

    def endsWith(y: String | Option[String]): Boolean = compileTimeOnly

    def contains(y: String | Option[String]): Boolean = compileTimeOnly

// TODO 与exists查询的&& || exists的!