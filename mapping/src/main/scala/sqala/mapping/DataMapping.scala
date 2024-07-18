package sqala.mapping

import scala.compiletime.summonInline
import scala.quoted.*

trait DataMapping[A, B]:
    def map(x: A): B

object DataMapping:
    given dataMapping[T](using c: Copy[T]): DataMapping[T, T] with
        override def map(x: T): T = c.copy(x)

    inline given productMapping[A <: Product, B <: Product]: DataMapping[A, B] =
        ${ productMappingMacro[A, B] }

    def productMappingMacro[A <: Product : Type, B <: Product : Type](using q: Quotes): Expr[DataMapping[A, B]] =
        import q.reflect.*

        val tpr = TypeRepr.of[B]
        val ctor = tpr.typeSymbol.primaryConstructor

        val aSym = TypeTree.of[A].symbol
        val aEles = aSym.declaredFields.map: i => 
            i.tree match
                case ValDef(name, typeTree, _) => 
                    name -> typeTree.tpe.asType
        .toMap

        val bSym = TypeTree.of[B].symbol
        val bEles = bSym.declaredFields.map: i => 
            i.tree match
                case ValDef(name, typeTree, _) => 
                    name -> typeTree.tpe.asType

        def mappingList(x: Expr[A]): List[Expr[Any]] = 
            bEles.map: i =>
                if aEles.contains(i._1) then
                    aEles(i._1) -> i._2 match
                        case ('[a], '[b]) =>
                            val fieldExpr = Select.unique(x.asTerm, i._1).asExprOf[a]
                            '{ summonInline[DataMapping[a, b]].map($fieldExpr) }
                else
                    i._2 match
                        case '[t] => '{ summonInline[DefaultValue[t]].defaultValue }
        
        '{
            new DataMapping[A, B] {
                override def map(x: A): B = {
                    ${
                        val terms = mappingList('x).map(_.asTerm)
                        New(Inferred(tpr)).select(ctor).appliedToArgs(terms).asExprOf[B]
                    }
                }
            }
        }

extension [A](x: A)
    def mapTo[B](using d: DataMapping[A, B]): B = d.map(x)