package sqala.mapping

import scala.compiletime.summonInline
import scala.deriving.Mirror
import scala.quoted.*

trait DataMapping[A, B]:
    def map(x: A): B

object DataMapping:
    given dataMapping[T](using c: Copy[T]): DataMapping[T, T] with
        override def map(x: T): T = c.copy(x)

    inline given productMapping[A, B](using Mirror.ProductOf[A], Mirror.ProductOf[B]): DataMapping[A, B] =
        ${ productMappingMacro[A, B] }

    private def productMappingMacro[A: Type, B: Type](using q: Quotes): Expr[DataMapping[A, B]] =
        import q.reflect.*

        val tpr = TypeRepr.of[B]
        val symbol = tpr.typeSymbol
        val ctor = symbol.primaryConstructor
        val comp = symbol.companionClass
        val mod = Ref(symbol.companionModule)
        val body = comp.tree.asInstanceOf[ClassDef].body

        val aSymbol = TypeRepr.of[A].typeSymbol
        val aEles = aSymbol.declaredFields.map: i => 
            i.tree match
                case ValDef(name, typeTree, _) => 
                    name -> typeTree.tpe.asType
        .toMap

        val bEles = symbol.declaredFields.map: i => 
            i.tree match
                case ValDef(name, typeTree, _) => 
                    name -> typeTree.tpe.asType

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
                    ${
                        val terms = mappingList('x).map(_.asTerm)
                        New(Inferred(tpr)).select(ctor).appliedToArgs(terms).asExprOf[B]
                    }
        }

extension [A](x: A)
    def mapTo[B](using d: DataMapping[A, B]): B = d.map(x)