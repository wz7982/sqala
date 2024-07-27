package sqala.mapping

import sqala.data.DefaultValue

import scala.compiletime.summonInline
import scala.deriving.Mirror
import scala.quoted.*

trait DataMapping[A, B]:
    def map(x: A): B

object DataMapping:
    given dataMapping[T](using c: Copy[T]): DataMapping[T, T] with
        override def map(x: T): T = c.copy(x)

    inline given productMapping[A <: Product, B <: Product](using Mirror.ProductOf[A], Mirror.ProductOf[B]): DataMapping[A, B] =
        ${ productMappingMacro[A, B] }

    private def fetchTypes[T: Type](using q: Quotes): List[Type[?]] =
        Type.of[T] match
            case '[x *: xs] => Type.of[x] :: fetchTypes[xs]
            case '[EmptyTuple] => Nil

    private def productMappingMacro[A <: Product: Type, B <: Product: Type](using q: Quotes): Expr[DataMapping[A, B]] =
        import q.reflect.*

        val tpr = TypeRepr.of[B]
        val symbol = tpr.typeSymbol
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
                            val fieldExpr = Select.unique(x.asTerm, name).asExprOf[a]
                            '{ summonInline[DataMapping[a, b]].map($fieldExpr) }
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
                            case '[t] => '{ summonInline[DefaultValue[t]].defaultValue }

        '{
            new DataMapping[A, B]:
                override def map(x: A): B =
                    val m = summonInline[Mirror.ProductOf[B]]
                    val values = ${
                        Expr.ofList(mappingList('x))
                    }.toArray
                    m.fromProduct(Tuple.fromArray(values))
        }

extension [A](x: A)
    def mapTo[B](using d: DataMapping[A, B]): B = d.map(x)