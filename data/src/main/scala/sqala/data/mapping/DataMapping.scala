package sqala.data.mapping

import sqala.data.DefaultValue
import sqala.util.fetchTypes

import scala.compiletime.summonInline
import scala.deriving.Mirror
import scala.quoted.*
import java.time.{LocalDate, LocalDateTime}

trait DataMapping[A, B]:
    def map(x: A): B

object DataMapping:
    given intMapping: DataMapping[Int, Int] with
        override inline def map(x: Int): Int = x

    given longMapping: DataMapping[Long, Long] with
        override inline def map(x: Long): Long = x

    given floatMapping: DataMapping[Float, Float] with
        override inline def map(x: Float): Float = x

    given doubleMapping: DataMapping[Double, Double] with
        override inline def map(x: Double): Double = x

    given decimalMapping: DataMapping[BigDecimal, BigDecimal] with
        override inline def map(x: BigDecimal): BigDecimal = BigDecimal(x.toString)

    given stringMapping: DataMapping[String, String] with
        override inline def map(x: String): String = x

    given booleanMapping: DataMapping[Boolean, Boolean] with
        override inline def map(x: Boolean): Boolean = x

    given localDateMapping: DataMapping[LocalDate, LocalDate] with
        override inline def map(x: LocalDate): LocalDate = LocalDate.from(x)

    given localDateTimeMapping: DataMapping[LocalDateTime, LocalDateTime] with
        override inline def map(x: LocalDateTime): LocalDateTime = LocalDateTime.from(x)

    given optionMapping[T](using c: DataMapping[T, T]): DataMapping[Option[T], Option[T]] with
        override inline def map(x: Option[T]): Option[T] = x.map(i => c.map(i))

    given listMapping[T](using c: DataMapping[T, T]): DataMapping[List[T], List[T]] with
        override inline def map(x: List[T]): List[T] = x.map(i => c.map(i))

    inline given productMapping[A <: Product, B <: Product](using Mirror.ProductOf[A], Mirror.ProductOf[B]): DataMapping[A, B] =
        ${ productMappingMacro[A, B] }

    private def productMappingMacro[A <: Product: Type, B <: Product: Type](using q: Quotes): Expr[DataMapping[A, B]] =
        import q.reflect.*

        val tpr = TypeRepr.of[B]
        val types = tpr match
            case AppliedType(_, ts) => ts
            case _ => Nil
        val symbol = tpr.typeSymbol
        val ctor = symbol.primaryConstructor
        val comp = symbol.companionClass
        val mod = Ref(symbol.companionModule)
        val body = comp.tree.asInstanceOf[ClassDef].body

        val aMirror: Expr[Mirror.Of[A]] = Expr.summon[Mirror.Of[A]].get
        val aSymbol = TypeRepr.of[A].typeSymbol
        val aNames = aSymbol.caseFields.map(_.name)
        val aTypes = aMirror match
            case '{ $m: Mirror.ProductOf[A] { type MirroredElemTypes = elementTypes } } =>
                fetchTypes[elementTypes]
        val aEles = aNames.zip(aTypes).toMap

        val bMirror: Expr[Mirror.Of[B]] = Expr.summon[Mirror.Of[B]].get
        val bNames = symbol.caseFields.map(_.name)
        val bTypes = bMirror match
            case '{ $m: Mirror.ProductOf[B] { type MirroredElemTypes = elementTypes } } =>
                fetchTypes[elementTypes]
        val bEles = bNames.zip(bTypes)

        def mappingList(x: Expr[A]): List[Expr[Any]] =
            bEles.zipWithIndex.map: i =>
                val ((name, typ), index) = i
                if aEles.contains(name) then
                    aEles(name) -> typ match
                        case ('[a], '[b]) =>
                            val showTypeA = TypeRepr.of[a].show
                            val showTypeB = TypeRepr.of[b].show
                            val fieldExpr = Select.unique(x.asTerm, name).asExprOf[a]
                            val mappingExpr = Expr.summon[DataMapping[a, b]] match
                                case None => report.errorAndAbort(s"Field '$name' cannot be mapped. It's possible to fix this by adding: given DataMapping[$showTypeA, $showTypeB].")
                                case Some(s) => s
                            '{ $mappingExpr.map($fieldExpr) }
                else
                    val p = symbol.caseFields(index)
                    if p.flags.is(Flags.HasDefault) then
                        val defaultList =
                            for case deff @ DefDef(defName, _, _, _) <- body
                                if defName.startsWith("$lessinit$greater$default$" + (index + 1))
                            yield mod.select(deff.symbol).asExpr
                        defaultList.head
                    else
                        typ match
                            case '[t] =>
                                val showType = TypeRepr.of[t].show
                                val defaultValueExpr = Expr.summon[DefaultValue[t]] match
                                    case None => report.errorAndAbort(s"Cannot create a default value for field ($name: $showType). It's possible to fix this by adding: given DefaultValue[$showType].")
                                    case Some(s) => s
                                '{ $defaultValueExpr.defaultValue }

        '{
            new DataMapping[A, B]:
                override def map(x: A): B =
                    ${
                        if types.isEmpty then
                            New(Inferred(tpr)).select(ctor).appliedToArgs(mappingList('x).map(_.asTerm)).asExprOf[B]
                        else
                            New(Inferred(tpr)).select(ctor).appliedToTypes(types).appliedToArgs(mappingList('x).map(_.asTerm)).asExprOf[B]
                    }
        }

given shallowCopyMapping[T]: DataMapping[T, T] with
    override inline def map(x: T): T = x

extension [A](x: A)
    inline def mapTo[B](using d: DataMapping[A, B]): B = d.map(x)