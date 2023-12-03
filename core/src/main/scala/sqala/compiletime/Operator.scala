package sqala.compiletime

import sqala.ast.expr.SqlBinaryOperator.*
import sqala.ast.expr.SqlUnaryOperator.*
import sqala.ast.order.SqlOrderByOption.{Asc, Desc}
import sqala.compiletime.statement.query.Query

import scala.annotation.targetName

trait Operator[T: AsSqlExpr]:
    extension [N <: Tuple] (expr: Expr[T, N])
        @targetName("eq")
        def ===(value: T): Expr[Boolean, N] = Binary(expr, Equal, Literal(value))

        @targetName("eq")
        def ===[R <: WrapToOption[T] | UnwrapFromOption[T], RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Boolean, Combine[N, RN]] =
            Binary(expr, Equal, rightExpr)

        @targetName("eq")
        def ===(query: Query[Tuple1[T], ?]): Expr[Boolean, N] = Binary(expr, Equal, SubQuery(query))

        @targetName("ne")
        def <>(value: T): Expr[Boolean, N] = Binary(expr, NotEqual, Literal(value))

        @targetName("ne")
        def <>[R <: WrapToOption[T] | UnwrapFromOption[T], RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Boolean, Combine[N, RN]] =
            Binary(expr, NotEqual, rightExpr)

        @targetName("ne")
        def <>(query: Query[Tuple1[T], ?]): Expr[Boolean, N] = Binary(expr, NotEqual, SubQuery(query))

        @targetName("gt")
        def >(value: T): Expr[Boolean, N] = Binary(expr, GreaterThan, Literal(value))

        @targetName("gt")
        def >[R <: WrapToOption[T] | UnwrapFromOption[T], RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Boolean, Combine[N, RN]] =
            Binary(expr, GreaterThan, rightExpr)

        @targetName("gt")
        def >(query: Query[Tuple1[T], ?]): Expr[Boolean, N] = Binary(expr, GreaterThan, SubQuery(query))

        @targetName("ge")
        def >=(value: T): Expr[Boolean, N] = Binary(expr, GreaterThanEqual, Literal(value))

        @targetName("ge")
        def >=[R <: WrapToOption[T] | UnwrapFromOption[T], RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Boolean, N] =
            Binary(expr, GreaterThanEqual, rightExpr)

        @targetName("ge")
        def >=(query: Query[Tuple1[T], ?]): Expr[Boolean, N] = Binary(expr, GreaterThanEqual, SubQuery(query))

        @targetName("lt")
        def <(value: T): Expr[Boolean, N] = Binary(expr, LessThan, Literal(value))

        @targetName("lt")
        def <[R <: WrapToOption[T] | UnwrapFromOption[T], RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Boolean, N] =
            Binary(expr, LessThan, rightExpr)

        @targetName("lt")
        def <(query: Query[Tuple1[T], ?]): Expr[Boolean, N] = Binary(expr, LessThan, SubQuery(query))

        @targetName("le")
        def <=(value: T): Expr[Boolean, N] = Binary(expr, LessThanEqual, Literal(value))

        @targetName("le")
        def <=[R <: WrapToOption[T] | UnwrapFromOption[T], RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Boolean, Combine[N, RN]] =
            Binary(expr, LessThanEqual, rightExpr)

        @targetName("le")
        def <=(query: Query[Tuple1[T], ?]): Expr[Boolean, N] = Binary(expr, LessThanEqual, SubQuery(query))

        def in(list: List[T]): Expr[Boolean, N] = In(expr, Vector(list.map(Literal(_))), false)

        def in(query: Query[Tuple1[T], ?]): Expr[Boolean, N] = In(expr, SubQuery(query), false)

        def notIn(list: List[T]): Expr[Boolean, N] = In(expr, Vector(list.map(Literal(_))), true)

        def notIn(query: Query[Tuple1[T], ?]): Expr[Boolean, N] = In(expr, SubQuery(query), true)

        def between(start: T, end: T): Expr[Boolean, N] = Between(expr, Literal(start), Literal(end), false)

        def between[S <: WrapToOption[T] | UnwrapFromOption[T], SN <: Tuple, E <: WrapToOption[T] | UnwrapFromOption[T], EN <: Tuple](start: Expr[S, SN], end: Expr[E, EN]): Expr[Boolean, Combine[Combine[N, SN], EN]] =
            Between(expr, start, end, false)

        def notBetween(start: T, end: T): Expr[Boolean, N] = Between(expr, Literal(start), Literal(end), true)

        def notBetween[S <: WrapToOption[T] | UnwrapFromOption[T], SN <: Tuple, E <: WrapToOption[T] | UnwrapFromOption[T], EN <: Tuple](start: Expr[S, SN], end: Expr[E, EN]): Expr[Boolean, Combine[Combine[N, SN], EN]] =
            Between(expr, start, end, true)

        def asc: OrderBy = OrderBy(expr, Asc)

        def desc: OrderBy = OrderBy(expr, Desc)

        infix def as(name: String)(using NonEmpty[name.type] =:= true): SelectItem[T, N, name.type] = 
            SelectItem(expr, name)

    extension (value: T)
        infix def as(name: String)(using NonEmpty[name.type] =:= true): SelectItem[T, EmptyTuple, name.type] =
            SelectItem(Literal(value), name)

trait NumberOperator[T: ExprNumber : AsSqlExpr]:
    extension [N <: Tuple] (expr: Expr[T, N])
        @targetName("plus")
        def +[R: ExprNumber : AsSqlExpr](value: R): Expr[Option[BigDecimal], N] = Binary(expr, Plus, Literal(value))

        @targetName("plus")
        def +[R: ExprNumber, RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Option[BigDecimal], Combine[N, RN]] =
            Binary(expr, Plus, rightExpr)

        @targetName("minus")
        def -[R: ExprNumber : AsSqlExpr](value: R): Expr[Option[BigDecimal], N] = Binary(expr, Minus, Literal(value))

        @targetName("minus")
        def -[R: ExprNumber, RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Option[BigDecimal], Combine[N, RN]] =
            Binary(expr, Minus, rightExpr)

        @targetName("times")
        def *[R: ExprNumber : AsSqlExpr](value: R): Expr[Option[BigDecimal], N] = Binary(expr, Times, Literal(value))

        @targetName("times")
        def *[R: ExprNumber, RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Option[BigDecimal], Combine[N, RN]] =
            Binary(expr, Times, rightExpr)

        @targetName("div")
        def /[R: ExprNumber : AsSqlExpr](value: R): Expr[Option[BigDecimal], N] = Binary(expr, Div, Literal(value))

        @targetName("div")
        def /[R: ExprNumber, RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Option[BigDecimal], Combine[N, RN]] =
            Binary(expr, Div, rightExpr)

        @targetName("mod")
        def %[R: ExprNumber : AsSqlExpr](value: R): Expr[Option[BigDecimal], N] = Binary(expr, Mod, Literal(value))

        @targetName("mod")
        def %[R: ExprNumber, RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Option[BigDecimal], Combine[N, RN]] =
            Binary(expr, Mod, rightExpr)

        @targetName("positive")
        def unary_+ : Expr[Option[BigDecimal], N] = Unary(expr, Positive)

        @targetName("negative")
        def unary_- : Expr[Option[BigDecimal], N] = Unary(expr, Negative)

        @targetName("eq")
        def ===[R: ExprNumber : AsSqlExpr](value: R): Expr[Boolean, N] = Binary(expr, Equal, Literal(value))

        @targetName("eq")
        def ===[R: ExprNumber, RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Boolean, Combine[N, RN]] =
            Binary(expr, Equal, rightExpr)

        @targetName("eq")
        def ===[R: ExprNumber](query: Query[Tuple1[R], ?]): Expr[Boolean, N] = Binary(expr, Equal, SubQuery(query))

        @targetName("ne")
        def <>[R: ExprNumber : AsSqlExpr](value: R): Expr[Boolean, N] = Binary(expr, NotEqual, Literal(value))

        @targetName("ne")
        def <>[R: ExprNumber, RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Boolean, Combine[N, RN]] =
            Binary(expr, NotEqual, rightExpr)

        @targetName("ne")
        def <>[R: ExprNumber](query: Query[Tuple1[R], ?]): Expr[Boolean, N] = Binary(expr, NotEqual, SubQuery(query))

        @targetName("gt")
        def >[R: ExprNumber : AsSqlExpr](value: R): Expr[Boolean, N] = Binary(expr, GreaterThan, Literal(value))

        @targetName("gt")
        def >[R: ExprNumber, RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Boolean, Combine[N, RN]] =
            Binary(expr, GreaterThan, rightExpr)

        @targetName("gt")
        def >[R: ExprNumber](query: Query[Tuple1[R], ?]): Expr[Boolean, N] = Binary(expr, GreaterThan, SubQuery(query))

        @targetName("ge")
        def >=[R: ExprNumber : AsSqlExpr](value: R): Expr[Boolean, N] = Binary(expr, GreaterThanEqual, Literal(value))

        @targetName("ge")
        def >=[R: ExprNumber, RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Boolean, Combine[N, RN]] =
            Binary(expr, GreaterThanEqual, rightExpr)

        @targetName("ge")
        def >=[R: ExprNumber](query: Query[Tuple1[R], ?]): Expr[Boolean, N] = Binary(expr, GreaterThanEqual, SubQuery(query))

        @targetName("lt")
        def <[R: ExprNumber : AsSqlExpr](value: R): Expr[Boolean, N] = Binary(expr, LessThan, Literal(value))

        @targetName("lt")
        def <[R: ExprNumber, RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Boolean, Combine[N, RN]] =
            Binary(expr, LessThan, rightExpr)

        @targetName("lt")
        def <[R: ExprNumber](query: Query[Tuple1[R], ?]): Expr[Boolean, N] = Binary(expr, LessThan, SubQuery(query))

        @targetName("le")
        def <=[R: ExprNumber : AsSqlExpr](value: R): Expr[Boolean, N] = Binary(expr, LessThanEqual, Literal(value))

        @targetName("le")
        def <=[R: ExprNumber, RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Boolean, Combine[N, RN]] =
            Binary(expr, LessThanEqual, rightExpr)

        @targetName("le")
        def <=[R: ExprNumber](query: Query[Tuple1[R], ?]): Expr[Boolean, N] = Binary(expr, LessThanEqual, SubQuery(query))

        def in(list: List[T])(using AsSqlExpr[T]): Expr[Boolean, N] =
            In(expr, Vector(list.map(Literal(_))), false)

        def in[R: ExprNumber](query: Query[Tuple1[R], ?]): Expr[Boolean, N] =
            In(expr, SubQuery(query), false)

        def notIn(list: List[T])(using AsSqlExpr[T]): Expr[Boolean, N] =
            In(expr, Vector(list.map(Literal(_))), true)

        def notIn[R: ExprNumber](query: Query[Tuple1[R], ?]): Expr[Boolean, N] =
            In(expr, SubQuery(query), true)

        def between(start: T, end: T)(using AsSqlExpr[T]): Expr[Boolean, N] =
            Between(expr, Literal(start), Literal(end), false)

        def between[S: ExprNumber, SN <: Tuple, E: ExprNumber, EN <: Tuple](start: Expr[S, SN], end: Expr[E, EN]): Expr[Boolean, Combine[Combine[N, SN], EN]] =
            Between(expr, start, end, false)

        def notBetween(start: T, end: T)(using AsSqlExpr[T]): Expr[Boolean, N] =
            Between(expr, Literal(start), Literal(end), true)

        def notBetween[S: ExprNumber, SN <: Tuple, E: ExprNumber, EN <: Tuple](start: Expr[S, SN], end: Expr[E, EN]): Expr[Boolean, Combine[Combine[N, SN], EN]] =
            Between(expr, start, end, true)

        def asc: OrderBy = OrderBy(expr, Asc)

        def desc: OrderBy = OrderBy(expr, Desc)

        infix def as(name: String)(using NonEmpty[name.type] =:= true): SelectItem[Option[T], N, name.type] =
            SelectItem(expr, name)

    extension (value: T)
        infix def as(name: String)(using NonEmpty[name.type] =:= true): SelectItem[T, EmptyTuple, name.type] =
            SelectItem(Literal(value), name)

trait StringOperator[T: AsSqlExpr] extends Operator[T]:
    extension [N <: Tuple] (expr: Expr[T, N])
        def like(value: String): Expr[Boolean, N] = Binary(expr, Like, Literal(value))

        def like[R <: String | Option[String], RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Boolean, Combine[N, RN]] =
            Binary(expr, Like, rightExpr)

        def notLike(value: String): Expr[Boolean, N] = Binary(expr, NotLike, Literal(value))

        def notLike[R <: String | Option[String], RN <: Tuple](rightExpr: Expr[R, RN]): Expr[Boolean, Combine[N, RN]] =
            Binary(expr, NotLike, rightExpr)

        @targetName("json")
        def ->(n: Int): Expr[Option[String], N] = Binary(expr, Json, Literal(n))

        @targetName("json")
        def ->(key: String): Expr[Option[String], N] = Binary(expr, Json, Literal(key))

        @targetName("jsonText")
        def ->>(n: Int): Expr[Option[String], N] = Binary(expr, JsonText, Literal(n))

        @targetName("jsonText")
        def ->>(key: String): Expr[Option[String], N] = Binary(expr, JsonText, Literal(key))

trait BooleanOperator extends Operator[Boolean]:
    extension [N <: Tuple] (expr: Expr[Boolean, N])
        @targetName("and")
        def &&(value: Boolean): Expr[Boolean, N] = Binary(expr, And, Literal(value))

        @targetName("and")
        def &&[RN <: Tuple](rightExpr: Expr[Boolean, RN]): Expr[Boolean, Combine[N, RN]] = Binary(expr, And, rightExpr)

        @targetName("or")
        def ||(value: Boolean): Expr[Boolean, N] = Binary(expr, Or, Literal(value))

        @targetName("or")
        def ||[RN <: Tuple](rightExpr: Expr[Boolean, RN]): Expr[Boolean, Combine[N, RN]] = Binary(expr, Or, rightExpr)

        @targetName("xor")
        def ^(value: Boolean): Expr[Boolean, N] = Binary(expr, Xor, Literal(value))

        @targetName("xor")
        def ^[RN <: Tuple](rightExpr: Expr[Boolean, RN]): Expr[Boolean, Combine[N, RN]] = Binary(expr, Xor, rightExpr)

        @targetName("not")
        def unary_! : Expr[Boolean, EmptyTuple] = Func("NOT", expr :: Nil)

trait DateOperator[T: AsSqlExpr] extends Operator[T]:
    extension [N <: Tuple] (expr: Expr[T, N])
        @targetName("eq")
        def ===(s: String): Expr[Boolean, N] = Binary(expr, Equal, Literal(s))

        @targetName("ne")
        def <>(s: String): Expr[Boolean, N] = Binary(expr, NotEqual, Literal(s))

        @targetName("gt")
        def >(s: String): Expr[Boolean, N] = Binary(expr, GreaterThan, Literal(s))

        @targetName("ge")
        def >=(s: String): Expr[Boolean, N] = Binary(expr, GreaterThanEqual, Literal(s))

        @targetName("lt")
        def <(s: String): Expr[Boolean, N] = Binary(expr, LessThan, Literal(s))

        @targetName("le")
        def <=(s: String): Expr[Boolean, N] = Binary(expr, LessThanEqual, Literal(s))

        def between(start: String, end: String): Expr[Boolean, N] =
            Between(expr, Literal(start), Literal(end), false)

        def notBetween(start: String, end: String): Expr[Boolean, N] =
            Between(expr, Literal(start), Literal(end), true)