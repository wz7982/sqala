package sqala.static.dsl

import sqala.ast.expr.{SqlTimeZoneMode, SqlType}
import sqala.ir.{IrExpr, IrInRightOperand}
import sqala.metadata.*
import sqala.static.dsl.table.{AsTableParam, MappedTable, Table}

import java.time.{LocalDateTime, OffsetDateTime}
import scala.NamedTuple.NamedTuple
import scala.compiletime.error
import scala.compiletime.ops.boolean.||
import scala.deriving.Mirror
import scala.quoted.{Quotes, Type, Expr as ScalaExpr}
import scala.util.NotGiven

/**
 * Determines whether two types are compatible for comparison
 * operators (===, <>, >, <, etc.).
 */
trait CanCompare[A, B]

object CanCompare:
    given number[A: SqlNumber, B: SqlNumber]: CanCompare[A, B]()

    given date[A: SqlDate, B: SqlDate]: CanCompare[A, B]()

    given dateTime[A: SqlDateTime, B: SqlDateTime]: CanCompare[A, B]()

    given time[A: SqlTime, B: SqlTime]: CanCompare[A, B]()

    given string[A: SqlString, B: SqlString]: CanCompare[A, B]()

    given boolean[A: SqlBoolean, B: SqlBoolean]: CanCompare[A, B]()

    given json[A: SqlJson, B: SqlJson]: CanCompare[A, B]()

    given geometry[A: SqlGeometry, B: SqlGeometry]: CanCompare[A, B]()

    given interval[A: SqlInterval, B: SqlInterval]: CanCompare[A, B]()

    given dateAndString[A: SqlDate, B: SqlString]: CanCompare[A, B]()

    given stringAndDate[A: SqlString, B: SqlDate]: CanCompare[A, B]()

    given dateTimeAndString[A: SqlDateTime, B: SqlString]: CanCompare[A, B]()

    given stringAndDateTime[A: SqlString, B: SqlDateTime]: CanCompare[A, B]()

    given timeAndString[A: SqlTime, B: SqlString]: CanCompare[A, B]()

    given stringAndTime[A: SqlString, B: SqlTime]: CanCompare[A, B]()

    given custom[T, R](using CustomField[T, R]): CanCompare[T, T]()

    given customAndOptionCustom[T, R](using
        CustomField[T, R]
    ): CanCompare[T, Option[T]]()

    given optionCustomAndCustom[T, R](using
        CustomField[T, R]
    ): CanCompare[Option[T], T]()

    given optionCustom[T, R](using
        CustomField[T, R]
    ): CanCompare[Option[T], Option[T]]()

    given array[A, B](using
        CanCompare[A, B]
    ): CanCompare[Array[A], Array[B]]()

    given arrayAndOptionArray[A, B](using
        CanCompare[A, B]
    ): CanCompare[Array[A], Option[Array[B]]]()

    given optionArrayAndArray[A, B](using
        CanCompare[A, B]
    ): CanCompare[Option[Array[A]], Array[B]]()

    given optionArray[A, B](using
        CanCompare[A, B]
    ): CanCompare[Option[Array[A]], Option[Array[B]]]()

    given tuple[AH, AT <: Tuple, BH, BT <: Tuple](using
        CanCompare[AH, BH],
        CanCompare[AT, BT]
    ): CanCompare[AH *: AT, BH *: BT]()

    given tuple1[AH, BH](using
        CanCompare[AH, BH]
    ): CanCompare[AH *: EmptyTuple, BH *: EmptyTuple]()

    inline given fallback[A, B](using NotGiven[CanCompare[A, B]]): CanCompare[A, B] =
        ${ errorMacro[A, B] }

    def errorMacro[A: Type, B: Type](using q: Quotes): Nothing =
        import q.reflect.*

        val aType = Type.show[A]
        val bType = Type.show[B]

        report.errorAndAbort(s"Cannot compare types $aType and $bType.")

/**
 * Lifts values, tuples, sequences, and arrays into `in` operands,
 * computing the result type and kind tuple. `CL` is the
 * current query context level.
 */
trait In[A, B, CL <: Int]:
    /**
     * The result type.
     */
    type R

    /**
     * The kind tuple.
     */
    type KS <: Tuple

    /**
     * Converts the value to a list of expressions.
     */
    def asExprs(x: B): List[Expr[?, ?]]

    /**
     * Converts the value to a single expression, wrapping multiple
     * operands in a tuple. Subqueries are passed through without
     * wrapping.
     */
    def asIrExpr(expr: IrExpr, x: B): IrExpr =
        val exprList = asExprs(x)
        IrExpr.In(
            expr,
            IrInRightOperand.Values(exprList.map(_.expr)),
            false
        )

object In:
    type Aux[A, B, CL <: Int, O, OKS <: Tuple] = In[A, B, CL]:
        type R = O

        type KS = OKS

    given expr[A, B, CL <: Int](using
        aa: AsExpr[A, CL],
        ab: AsExpr[B, CL],
        r: Relation[aa.R, ab.R],
        kt: KindToTuple[ab.K]
    ): Aux[A, B, CL, r.R, kt.R] =
        new In[A, B, CL]:
            type R = r.R

            type KS = kt.R

            def asExprs(x: B): List[Expr[?, ?]] =
                ab.asExpr(x) :: Nil

    given tuple[A, H, T <: Tuple, CL <: Int](using
        aa: AsExpr[A, CL],
        ah: AsExpr[H, CL],
        t: In[A, T, CL],
        rh: Relation[aa.R, ah.R],
        r: Relation[rh.R, t.R],
        kth: KindToTuple[ah.K],
        c: CombineKindTuple[kth.R, t.KS]
    ): Aux[A, H *: T, CL, r.R, c.R] =
        new In[A, H *: T, CL]:
            type R = r.R

            type KS = c.R

            def asExprs(x: H *: T): List[Expr[?, ?]] =
                ah.asExpr(x.head) :: t.asExprs(x.tail)

    given tuple1[A, H, CL <: Int](using
        aa: AsExpr[A, CL],
        ah: AsExpr[H, CL],
        r: Relation[aa.R, ah.R],
        kth: KindToTuple[ah.K]
    ): Aux[A, H *: EmptyTuple, CL, r.R, kth.R] =
        new In[A, H *: EmptyTuple, CL]:
            type R = r.R

            type KS = kth.R

            def asExprs(x: H *: EmptyTuple): List[Expr[?, ?]] =
                ah.asExpr(x.head) :: Nil

    given seq[A, B, S <: Seq[B], CL <: Int](using
        aa: AsExpr[A, CL],
        ab: AsExpr[B, CL],
        r: Relation[aa.R, B]
    ): Aux[A, S, CL, r.R, Value *: EmptyTuple] =
        new In[A, S, CL]:
            type R = r.R

            type KS = Value *: EmptyTuple

            def asExprs(x: S): List[Expr[?, ?]] =
                x.toList.map(i => ab.asExpr(i))

    given array[A, B, CL <: Int](using
        aa: AsExpr[A, CL],
        ab: AsExpr[B, CL],
        r: Relation[aa.R, B],
    ): Aux[A, Array[B], CL, r.R, Value *: EmptyTuple] =
        new In[A, Array[B], CL]:
            type R = r.R

            type KS = Value *: EmptyTuple

            def asExprs(x: Array[B]): List[Expr[?, ?]] =
                x.toList.map(i => ab.asExpr(i))

/**
 * Computes the result type of addition (via `+`).
 */
trait Plus[A, B]:
    /**
     * The result type.
     */
    type R

object Plus:
    type Aux[A, B, O] = Plus[A, B]:
        type R = O

    given number[A: SqlNumber, B: SqlNumber]: Aux[A, B, NumericResult[A, B]] =
        new Plus[A, B]:
            type R = NumericResult[A, B]

    given dateAndInterval[A: SqlDate, B: SqlInterval]: Aux[A, B, DateTimeResult[A, B]] =
        new Plus[A, B]:
            type R = DateTimeResult[A, B]

    given dateTimeAndInterval[A: SqlDateTime, B: SqlInterval]: Aux[A, B, DateTimeResult[A, B]] =
        new Plus[A, B]:
            type R = DateTimeResult[A, B]

    given timeAndInterval[A: SqlTime, B: SqlInterval]: Aux[A, B, TimeResult[A, B]] =
        new Plus[A, B]:
            type R = TimeResult[A, B]

    inline given fallback[A, B](using NotGiven[Plus[A, B]]): Aux[A, B, Nothing] =
        ${ errorMacro[A, B] }

    def errorMacro[A: Type, B: Type](using q: Quotes): Nothing =
        import q.reflect.*

        val aType = Type.show[A]
        val bType = Type.show[B]

        report.errorAndAbort(s"Types $aType and $bType cannot be added.")

/**
 * Computes the result type of subtraction (via `-`).
 */
trait Minus[A, B]:
    /**
     * The result type.
     */
    type R

object Minus:
    type Aux[A, B, O] = Minus[A, B]:
        type R = O

    given number[A: SqlNumber, B: SqlNumber]: Aux[A, B, NumericResult[A, B]] =
        new Minus[A, B]:
            type R = NumericResult[A, B]

    given date[A: SqlDate, B: SqlDate]: Aux[A, B, WrapIf[Interval, IsOption[A] || IsOption[B], Option]] =
        new Minus[A, B]:
            type R = WrapIf[Interval, IsOption[A] || IsOption[B], Option]

    given dateTime[A: SqlDateTime, B: SqlDateTime]: Aux[A, B, WrapIf[Interval, IsOption[A] || IsOption[B], Option]] =
        new Minus[A, B]:
            type R = WrapIf[Interval, IsOption[A] || IsOption[B], Option]

    given dateAndDateTime[A: SqlDate, B: SqlDateTime]: Aux[A, B, WrapIf[Interval, IsOption[A] || IsOption[B], Option]] =
        new Minus[A, B]:
            type R = WrapIf[Interval, IsOption[A] || IsOption[B], Option]

    given dateTimeAndDate[A: SqlDateTime, B: SqlDate]: Aux[A, B, WrapIf[Interval, IsOption[A] || IsOption[B], Option]] =
        new Minus[A, B]:
            type R = WrapIf[Interval, IsOption[A] || IsOption[B], Option]

    given time[A: SqlTime, B: SqlTime]: Aux[A, B, WrapIf[Interval, IsOption[A] || IsOption[B], Option]] =
        new Minus[A, B]:
            type R = WrapIf[Interval, IsOption[A] || IsOption[B], Option]

    given dateAndInterval[A: SqlDate, B: SqlInterval]: Aux[A, B, DateTimeResult[A, B]] =
        new Minus[A, B]:
            type R = DateTimeResult[A, B]

    given dateTimeAndInterval[A: SqlDateTime, B: SqlInterval]: Aux[A, B, DateTimeResult[A, B]] =
        new Minus[A, B]:
            type R = DateTimeResult[A, B]

    given timeAndInterval[A: SqlTime, B: SqlInterval]: Aux[A, B, TimeResult[A, B]] =
        new Minus[A, B]:
            type R = TimeResult[A, B]

    inline given fallback[A, B](using NotGiven[Minus[A, B]]): Aux[A, B, Nothing] =
        ${ errorMacro[A, B] }

    def errorMacro[A: Type, B: Type](using q: Quotes): Nothing =
        import q.reflect.*

        val aType = Type.show[A]
        val bType = Type.show[B]

        report.errorAndAbort(s"Types $aType and $bType cannot be subtracted.")

/**
 * Computes the result type of a comparison.
 * Always wraps `Boolean` with `Option` if either side is optional.
 */
trait Relation[A, B]:
    /**
     * The result type.
     */
    type R

object Relation:
    type Aux[A, B, O] = Relation[A, B]:
        type R = O

    given relation[A, B](using
        CanCompare[A, B]
    ): Aux[A, B, WrapIf[Boolean, IsOption[A] || IsOption[B], Option]] =
        new Relation[A, B]:
            type R = WrapIf[Boolean, IsOption[A] || IsOption[B], Option]

/**
 * Computes the result type of a conditional expression (such as
 * `caseWhen` or `ifNull`).
 */
trait Return[A, B]:
    /**
     * The result type.
     */
    type R

object Return:
    type Aux[A, B, O] = Return[A, B]:
        type R = O

    given number[A: SqlNumber, B: SqlNumber]: Aux[A, B, NumericResult[A, B]] =
        new Return[A, B]:
            type R = NumericResult[A, B]

    given string[A: SqlString, B: SqlString]: Aux[A, B, WrapIf[String, IsOption[A] || IsOption[B], Option]] =
        new Return[A, B]:
            type R = WrapIf[String, IsOption[A] || IsOption[B], Option]

    given boolean[A: SqlBoolean, B: SqlBoolean]: Aux[A, B, WrapIf[Boolean, IsOption[A] || IsOption[B], Option]] =
        new Return[A, B]:
            type R = WrapIf[Boolean, IsOption[A] || IsOption[B], Option]

    given json[A: SqlJson, B: SqlJson]: Aux[A, B, WrapIf[Json, IsOption[A] || IsOption[B], Option]] =
        new Return[A, B]:
            type R = WrapIf[Json, IsOption[A] || IsOption[B], Option]

    given geometry[A: SqlGeometry, B: SqlGeometry]: Aux[A, B, WrapIf[Geometry, IsOption[A] || IsOption[B], Option]] =
        new Return[A, B]:
            type R = WrapIf[Geometry, IsOption[A] || IsOption[B], Option]

    given interval[A: SqlInterval, B: SqlInterval]: Aux[A, B, WrapIf[Interval, IsOption[A] || IsOption[B], Option]] =
        new Return[A, B]:
            type R = WrapIf[Interval, IsOption[A] || IsOption[B], Option]

    given date[A: SqlDate, B: SqlDate]: Aux[A, B, DateTimeResult[A, B]] =
        new Return[A, B]:
            type R = DateTimeResult[A, B]

    given dateTime[A: SqlDateTime, B: SqlDateTime]: Aux[A, B, DateTimeResult[A, B]] =
        new Return[A, B]:
            type R = DateTimeResult[A, B]

    given dateAndDateTime[A: SqlDate, B: SqlDateTime]: Aux[A, B, DateTimeResult[A, B]] =
        new Return[A, B]:
            type R = DateTimeResult[A, B]

    given dateTimeAndDate[A: SqlDateTime, B: SqlDate]: Aux[A, B, DateTimeResult[A, B]] =
        new Return[A, B]:
            type R = DateTimeResult[A, B]

    given time[A: SqlTime, B: SqlTime]: Aux[A, B, TimeResult[A, B]] =
        new Return[A, B]:
            type R = TimeResult[A, B]

    given dateAndString[A: SqlDate, B: SqlString]: Aux[A, B, DateTimeResult[A, B]] =
        new Return[A, B]:
            type R = DateTimeResult[A, B]

    given stringAndDate[A: SqlString, B: SqlDate]: Aux[A, B, DateTimeResult[A, B]] =
        new Return[A, B]:
            type R = DateTimeResult[A, B]

    given dateTimeAndString[A: SqlDateTime, B: SqlString]: Aux[A, B, DateTimeResult[A, B]] =
        new Return[A, B]:
            type R = DateTimeResult[A, B]

    given stringAndDateTime[A: SqlString, B: SqlDateTime]: Aux[A, B, DateTimeResult[A, B]] =
        new Return[A, B]:
            type R = DateTimeResult[A, B]

    given timeAndString[A: SqlTime, B: SqlString]: Aux[A, B, TimeResult[A, B]] =
        new Return[A, B]:
            type R = TimeResult[A, B]

    given stringAndTime[A: SqlString, B: SqlTime]: Aux[A, B, TimeResult[A, B]] =
        new Return[A, B]:
            type R = TimeResult[A, B]

    given custom[T, R](using
        CustomField[T, R]
    ): Aux[T, T, T] =
        new Return[T, T]:
            type R = T

    given customAndOptionCustom[T, R](using
        CustomField[T, R]
    ): Aux[T, Option[T], Option[T]] =
        new Return[T, Option[T]]:
            type R = Option[T]

    given optionCustomAndCustom[T, R](using
        CustomField[T, R]
    ): Aux[Option[T], T, Option[T]] =
        new Return[Option[T], T]:
            type R = Option[T]

    given optionCustom[T, R](using
        CustomField[T, R]
    ): Aux[Option[T], Option[T], Option[T]] =
        new Return[Option[T], Option[T]]:
            type R = Option[T]

    given array[A, B](using
        r: Return[A, B]
    ): Aux[Array[A], Array[B], Array[r.R]] =
        new Return[Array[A], Array[B]]:
            type R = Array[r.R]

    given arrayAndOptionArray[A, B](using
        r: Return[A, B]
    ): Aux[Array[A], Option[Array[B]], Option[Array[r.R]]] =
        new Return[Array[A], Option[Array[B]]]:
            type R = Option[Array[r.R]]

    given optionArrayAndArray[A, B](using
        r: Return[A, B]
    ): Aux[Option[Array[A]], Array[B], Option[Array[r.R]]] =
        new Return[Option[Array[A]], Array[B]]:
            type R = Option[Array[r.R]]

    given optionArray[A, B](using
        r: Return[A, B]
    ): Aux[Option[Array[A]], Option[Array[B]], Option[Array[r.R]]] =
        new Return[Option[Array[A]], Option[Array[B]]]:
            type R = Option[Array[r.R]]

    inline given fallback[A, B](using NotGiven[Return[A, B]]): Aux[A, B, Nothing] =
        ${ errorMacro[A, B] }

    def errorMacro[A: Type, B: Type](using q: Quotes): Nothing =
        import q.reflect.*

        val aType = Type.show[A]
        val bType = Type.show[B]

        report.errorAndAbort(s"Types $aType and $bType are incompatible.")

/**
 * Computes the result type of a coalesce expression.
 */
trait CoalesceReturn[A, B]:
    /**
     * The result type.
     */
    type R

object CoalesceReturn:
    type Aux[A, B, O] = CoalesceReturn[A, B]:
        type R = O

    given nonEmpty[A, B](using
        p: NotGiven[B <:< Option[?]],
        r: Return[A, B]
    ): Aux[A, B, Unwrap[r.R, Option]] =
        new CoalesceReturn[A, B]:
            type R = Unwrap[r.R, Option]

    given empty[A, B](using
        p: B <:< Option[?],
        r: Return[A, B]
    ): Aux[A, B, r.R] =
        new CoalesceReturn[A, B]:
            type R = r.R

/**
 * Validates that an argument type is acceptable for a given requirement,
 * producing a readable compile-time error when it is not.
 */
trait ValidateArgType[F[_], T, N <: String]

object ValidateArgType:
    given validate[F[_], T, N <: String](using F[T]): ValidateArgType[F, T, N]()

    inline given fallback[F[_], T, N <: String](using NotGiven[F[T]]): ValidateArgType[F, T, N] =
        ${ errorMacro[F, T, N] }

    def errorMacro[F[_]: Type, T: Type, N <: String : Type](using q: Quotes): ScalaExpr[ValidateArgType[F, T, N]] =
        import q.reflect.*

        val typeName = Type.show[T]

        if typeName.contains("Nothing") then
            '{
                new ValidateArgType[F, T, N] {}
            }
        else
            val needTypeName = Type.show[N].replace("\"", "")

            report.errorAndAbort(s"The arguments type should be $needTypeName, but got $typeName.")

/**
 * Determines whether a type can be used as a `rows` or `groups`
 * frame bound.
 */
trait CanInRowsOrGroupsFrame[T]

object CanInRowsOrGroupsFrame:
    given int: CanInRowsOrGroupsFrame[Int]()

    given long: CanInRowsOrGroupsFrame[Long]()

    given nothing: CanInRowsOrGroupsFrame[Nothing]()

    inline given fallback[T](using
        NotGiven[CanInRowsOrGroupsFrame[T]]
    ): CanInRowsOrGroupsFrame[T] =
        error("Window frame type is incompatible with the ordering expression.")

/**
 * Determines whether `S` and `T` can be used together in a `range`
 * frame (`S` is the sort key type, `T` is the bound type).
 */
trait CanInRangeFrame[S, T]

object CanInRangeFrame:
    given number[S: SqlNumber, N: SqlNumber]: CanInRangeFrame[S, N]()

    given date[T: SqlDate, I: SqlInterval]: CanInRangeFrame[T, I]()

    given dateTime[T: SqlDateTime, I: SqlInterval]: CanInRangeFrame[T, I]()

    given time[T: SqlTime, I: SqlInterval]: CanInRangeFrame[T, I]()

    given nothing[S]: CanInRangeFrame[S, Nothing]()

    inline given fallback[S, T](using
        NotGiven[CanInRangeFrame[S, T]]
    ): CanInRangeFrame[S, T] =
        error("Window frame type is incompatible with the ordering expression.")

/**
 * Provides the target type for a `as` method.
 */
trait Cast[T, R]:
    /**
     * The target type.
     */
    def castType: SqlType

object Cast:
    given string[T: AsSqlExpr]: Cast[T, String] with
        def castType: SqlType =
            SqlType.Varchar(None)

    given int[T: SqlNumber]: Cast[T, Int] with
        def castType: SqlType =
            SqlType.Int

    given long[T: SqlNumber]: Cast[T, Long] with
        def castType: SqlType =
            SqlType.Long

    given float[T: SqlNumber]: Cast[T, Float] with
        def castType: SqlType =
            SqlType.Float

    given double[T: SqlNumber]: Cast[T, Double] with
        def castType: SqlType =
            SqlType.Double

    given decimal[T: SqlNumber]: Cast[T, BigDecimal] with
        def castType: SqlType =
            SqlType.Decimal(Some(38, 10))

    given json[T: SqlString]: Cast[T, Json] with
        def castType: SqlType =
            SqlType.Json

    given localDateTime[T: SqlString]: Cast[T, LocalDateTime] with
        def castType: SqlType =
            SqlType.Timestamp(None)

    given offsetDateTime[T: SqlString]: Cast[T, OffsetDateTime] with
        def castType: SqlType =
            SqlType.Timestamp(Some(SqlTimeZoneMode.With))

    inline given fallback[A, B](using NotGiven[Cast[A, B]]): Cast[A, B] =
        ${ errorMacro[A, B] }

    def errorMacro[A: Type, B: Type](using q: Quotes): Nothing =
        import q.reflect.*

        val aType = Type.show[A]
        val bType = Type.show[B]

        report.errorAndAbort(s"Cannot convert type $aType to type $bType.")

/**
 * Constrains the types that can be used with `year`, `month`, `day`.
 * extract functions.
 */
trait ExtractDate[T]

object ExtractDate:
    given date[T: SqlDate]: ExtractDate[T]()

    given dateTime[T: SqlDateTime]: ExtractDate[T]()

    given interval[T: SqlInterval]: ExtractDate[T]()

    inline given fallback[T](using NotGiven[ExtractDate[T]]): ExtractDate[T] =
        ${ errorMacro[T] }

    def errorMacro[T: Type](using q: Quotes): Nothing =
        import q.reflect.*

        val typeName = Type.show[T]

        report.errorAndAbort(s"The argument type should be a date/datetime or interval type, but got $typeName.")

/**
 * Constrains the types that can be used with `hour`, `minute`, `second`.
 * extract functions.
 */
trait ExtractTime[T]

object ExtractTime:
    given dateTime[T: SqlDateTime]: ExtractTime[T]()

    given time[T: SqlTime]: ExtractTime[T]()

    given interval[T: SqlInterval]: ExtractTime[T]()

    inline given fallback[T](using NotGiven[ExtractTime[T]]): ExtractTime[T] =
        ${ errorMacro[T] }

    def errorMacro[T: Type](using q: Quotes): Nothing =
        import q.reflect.*

        val typeName = Type.show[T]

        report.errorAndAbort(s"The argument type should be a time/datetime or interval type, but got $typeName.")

/**
 * Maps union query items, producing column expressions
 * for `union`, `unionAll`, etc. `CL` is the query context level.
 */
trait Union[A, B, CL <: Int]:
    /**
     * The result type.
     */
    type R

    /**
     * The number of expressions consumed by each query item.
     */
    def offset(x: A): Int

    /**
     * Maps the query item at the given cursor position to a column
     * expression.
     */
    def unionQueryItems(x: A, cursor: Int): R

object Union:
    type Aux[A, B, CL <: Int, O] = Union[A, B, CL]:
        type R = O

    given expr[A, AK <: ExprKind, AE <: Expr[A, AK], B, BK <: ExprKind, BE <: Expr[B, BK], CL <: Int](using
        r: Return[A, B]
    ): Aux[AE, BE, CL, Expr[r.R, Column[CL]]] =
        new Union[AE, BE, CL]:
            type R = Expr[r.R, Column[CL]]

            def offset(x: AE): Int =
                1

            def unionQueryItems(x: AE, cursor: Int): R =
                Expr(IrExpr.Column(None, s"c$cursor"))

    given table[A, AL <: Int, B, BL <: Int, CL <: Int](using
        ma: Mirror.ProductOf[A],
        mb: Mirror.ProductOf[B],
        p: ma.MirroredElemTypes =:= mb.MirroredElemTypes
    ): Aux[Table[A, Column, AL], Table[B, Column, BL], CL, Table[A, Column, CL]] =
        new Union[Table[A, Column, AL], Table[B, Column, BL], CL]:
            type R = Table[A, Column, CL]

            def offset(x: Table[A, Column, AL]): Int =
                x.__metaData__.columns.size

            def unionQueryItems(x: Table[A, Column, AL], cursor: Int): R =
                val columns = x.__metaData__.columns.indices.map: i =>
                    s"c${cursor + i}"
                Table(x.__table__, x.__metaData__.copy(columns = columns), false)

    given tableAndOptionTable[A, AL <: Int, B, BL <: Int, CL <: Int](using
        ma: Mirror.ProductOf[A],
        mb: Mirror.ProductOf[B],
        p: ma.MirroredElemTypes =:= mb.MirroredElemTypes
    ): Aux[Table[A, Column, AL], Table[Option[B], Column, BL], CL, Table[Option[A], Column, CL]] =
        new Union[Table[A, Column, AL], Table[Option[B], Column, BL], CL]:
            type R = Table[Option[A], Column, CL]

            def offset(x: Table[A, Column, AL]): Int =
                x.__metaData__.columns.size

            def unionQueryItems(x: Table[A, Column, AL], cursor: Int): R =
                val columns = x.__metaData__.columns.indices.map: i =>
                    s"c${cursor + i}"
                Table(x.__table__, x.__metaData__.copy(columns = columns), false)

    given optionTableAndTable[A, AL <: Int, B, BL <: Int, CL <: Int](using
        ma: Mirror.ProductOf[A],
        mb: Mirror.ProductOf[B],
        p: ma.MirroredElemTypes =:= mb.MirroredElemTypes
    ): Aux[Table[Option[A], Column, AL], Table[B, Column, BL], CL, Table[Option[A], Column, CL]] =
        new Union[Table[Option[A], Column, AL], Table[B, Column, BL], CL]:
            type R = Table[Option[A], Column, CL]

            def offset(x: Table[Option[A], Column, AL]): Int =
                x.__metaData__.columns.size

            def unionQueryItems(x: Table[Option[A], Column, AL], cursor: Int): R =
                val columns = x.__metaData__.columns.indices.map: i =>
                    s"c${cursor + i}"
                Table(x.__table__, x.__metaData__.copy(columns = columns), false)

    given optionTableAndOptionTable[A, AL <: Int, B, BL <: Int, CL <: Int](using
        ma: Mirror.ProductOf[A],
        mb: Mirror.ProductOf[B],
        p: ma.MirroredElemTypes =:= mb.MirroredElemTypes
    ): Aux[Table[Option[A], Column, AL], Table[Option[B], Column, BL], CL, Table[Option[A], Column, CL]] =
        new Union[Table[Option[A], Column, AL], Table[Option[B], Column, BL], CL]:
            type R = Table[Option[A], Column, CL]

            def offset(x: Table[Option[A], Column, AL]): Int =
                x.__metaData__.columns.size

            def unionQueryItems(x: Table[Option[A], Column, AL], cursor: Int): R =
                val columns = x.__metaData__.columns.indices.map: i =>
                    s"c${cursor + i}"
                Table(x.__table__, x.__metaData__.copy(columns = columns), false)

    given mappedTable[AN <: Tuple, AV <: Tuple, AO <: Boolean, AL <: Int, BN <: Tuple, BV <: Tuple, BO <: Boolean, BL <: Int, CL <: Int](using
        u: Union[AV, BV, CL],
        a: AsTableParam[AV, CL]
    ): Aux[MappedTable[AN, AV, AO, AL], MappedTable[BN, BV, BO, BL], CL, MappedTable[AN, WrapToTuple[u.R], AO || BO, CL]] =
        new Union[MappedTable[AN, AV, AO, AL], MappedTable[BN, BV, BO, BL], CL]:
            type R = MappedTable[AN, WrapToTuple[u.R], AO || BO, CL]

            def offset(x: MappedTable[AN, AV, AO, AL]): Int =
                a.offset

            def unionQueryItems(x: MappedTable[AN, AV, AO, AL], cursor: Int): R =
                new MappedTable[AN, WrapToTuple[u.R], AO || BO, CL](
                    wrapToTuple(u.unionQueryItems(x.__items__, cursor))
                )

    given tuple[AH, AT <: Tuple, BH, BT <: Tuple, CL <: Int](using
        an: NotGiven[AT =:= EmptyTuple],
        bn: NotGiven[BT =:= EmptyTuple],
        h: Union[AH, BH, CL],
        t: Union[AT, BT, CL]
    ): Aux[AH *: AT, BH *: BT, CL, h.R *: WrapToTuple[t.R]] =
        new Union[AH *: AT, BH *: BT, CL]:
            type R = h.R *: WrapToTuple[t.R]

            def offset(x: AH *: AT): Int =
                h.offset(x.head) + t.offset(x.tail)

            def unionQueryItems(x: AH *: AT, cursor: Int): R =
                h.unionQueryItems(x.head, cursor) *:
                wrapToTuple(t.unionQueryItems(x.tail, cursor + h.offset(x.head)))

    given tuple1[AH, BH, CL <: Int](using
        h: Union[AH, BH, CL]
    ): Aux[AH *: EmptyTuple, BH *: EmptyTuple, CL, h.R *: EmptyTuple] =
        new Union[AH *: EmptyTuple, BH *: EmptyTuple, CL]:
            type R = h.R *: EmptyTuple

            def offset(x: AH *: EmptyTuple): Int =
                h.offset(x.head)

            def unionQueryItems(x: AH *: EmptyTuple, cursor: Int): R =
                h.unionQueryItems(x.head, cursor) *: EmptyTuple

    given namedTuple[AN <: Tuple, AV <: Tuple, BN <: Tuple, BV <: Tuple, CL <: Int](using
        u: Union[AV, BV, CL]
    ): Aux[NamedTuple[AN, AV], NamedTuple[BN, BV], CL, NamedTuple[AN, WrapToTuple[u.R]]] =
        new Union[NamedTuple[AN, AV], NamedTuple[BN, BV], CL]:
            type R = NamedTuple[AN, WrapToTuple[u.R]]

            def offset(x: NamedTuple[AN, AV]): Int =
                u.offset(x.toTuple)

            def unionQueryItems(x: NamedTuple[AN, AV], cursor: Int): R =
                NamedTuple(wrapToTuple(u.unionQueryItems(x.toTuple, cursor)))

    given namedTupleAndTuple[AN <: Tuple, AV <: Tuple, BV <: Tuple, CL <: Int](using
        u: Union[AV, BV, CL],
    ): Aux[NamedTuple[AN, AV], BV, CL, NamedTuple[AN, WrapToTuple[u.R]]] =
        new Union[NamedTuple[AN, AV], BV, CL]:
            type R = NamedTuple[AN, WrapToTuple[u.R]]

            def offset(x: NamedTuple[AN, AV]): Int =
                u.offset(x.toTuple)

            def unionQueryItems(x: NamedTuple[AN, AV], cursor: Int): R =
                NamedTuple(wrapToTuple(u.unionQueryItems(x.toTuple, cursor)))

    inline given fallback[A, B, CL <: Int](using NotGiven[Union[A, B, CL]]): Aux[A, B, CL, A] =
        ${ errorMacro[A, B] }

    def errorMacro[A: Type, B: Type](using q: Quotes): Nothing =
        import q.reflect.*

        val aType = Type.show[A]
        val bType = Type.show[B]

        report.errorAndAbort(
            s"Set operations (union/except/intersect) require both sides to be " +
            s"mapped queries with matching column shapes. " +
            s"Found $aType and $bType. Call map on each side to project into " +
            s"matching named tuples first."
        )