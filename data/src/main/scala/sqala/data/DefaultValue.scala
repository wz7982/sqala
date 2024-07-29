package sqala.data

import sqala.util.fetchTypes

import scala.compiletime.{erasedValue, summonInline}
import scala.deriving.Mirror
import scala.quoted.*
import java.time.{LocalDate, LocalDateTime}
import java.util.Date

trait DefaultValue[T]:
    def defaultValue: T

object DefaultValue:
    inline def defaultValues[T <: Tuple]: List[Any] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => summonInline[DefaultValue[t]].defaultValue :: defaultValues[ts]

    given intDefaultValue: DefaultValue[Int] with
        override def defaultValue: Int = 0

    given longDefaultValue: DefaultValue[Long] with
        override def defaultValue: Long = 0L

    given floatDefaultValue: DefaultValue[Float] with
        override def defaultValue: Float = 0F

    given doubleDefaultValue: DefaultValue[Double] with
        override def defaultValue: Double = 0D

    given decimalDefaultValue: DefaultValue[BigDecimal] with
        override def defaultValue: BigDecimal = BigDecimal("0")

    given stringDefaultValue: DefaultValue[String] with
        override def defaultValue: String = ""

    given booleanDefaultValue: DefaultValue[Boolean] with
        override def defaultValue: Boolean = false

    given dateDefaultValue: DefaultValue[Date] with
        override def defaultValue: Date = new Date

    given localDateDefaultValue: DefaultValue[LocalDate] with
        override def defaultValue: LocalDate = LocalDate.now()

    given localDateTimeDefaultValue: DefaultValue[LocalDateTime] with
        override def defaultValue: LocalDateTime = LocalDateTime.now()

    given optionDefaultValue[T: DefaultValue]: DefaultValue[Option[T]] with
        override def defaultValue: Option[T] = None

    given listDefaultValue[T: DefaultValue]: DefaultValue[List[T]] with
        override def defaultValue: List[T] = Nil

    inline given derived[T](using m: Mirror.Of[T]): DefaultValue[T] =
        ${ defaultValueMacro[T] }

    private def defaultValueMacro[T: Type](using q: Quotes): Expr[DefaultValue[T]] =
        import q.reflect.*

        val mirror: Expr[Mirror.Of[T]] = Expr.summon[Mirror.Of[T]].get

        mirror match
            case '{ $m: Mirror.ProductOf[T] { type MirroredElemTypes = EmptyTuple } } =>
                '{
                    new DefaultValue[T]:
                        override def defaultValue: T = 
                            $m.fromProduct(EmptyTuple)
                }
            case '{ $m: Mirror.ProductOf[T] { type MirroredElemTypes = elementTypes } } =>
                val elemTypes = fetchTypes[elementTypes]
                val tpr = TypeRepr.of[T]
                val types = tpr match
                    case AppliedType(_, ts) => ts
                    case _ => Nil
                val symbol = tpr.typeSymbol
                val ctor = symbol.primaryConstructor
                val exprs = elemTypes.map:
                    case '[t] =>
                        val summonExpr = Expr.summon[DefaultValue[t]].get
                        '{ $summonExpr.defaultValue }
                '{
                    new DefaultValue[T]:
                        override def defaultValue: T = 
                            ${
                                if types.isEmpty then
                                    New(Inferred(tpr)).select(ctor).appliedToArgs(exprs.map(_.asTerm)).asExprOf[T]
                                else
                                    New(Inferred(tpr)).select(ctor).appliedToTypes(types).appliedToArgs(exprs.map(_.asTerm)).asExprOf[T]
                            }
                }
            case '{ $m: Mirror.SumOf[T] { type MirroredElemTypes = elementTypes } } =>
                val typ = fetchTypes[elementTypes].head
                val expr = typ match
                    case '[t] => 
                        val summonExpr = Expr.summon[DefaultValue[t]].get
                        '{ $summonExpr.defaultValue }.asExprOf[T]
                '{
                    new DefaultValue[T]:
                        override def defaultValue: T = $expr
                }