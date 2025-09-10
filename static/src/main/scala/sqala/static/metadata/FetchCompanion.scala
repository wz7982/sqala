package sqala.static.metadata

import scala.quoted.*

trait FetchCompanion[T]:
    type R

    def metaData: TableMetaData

object FetchCompanion:
    type Aux[T, O] = FetchCompanion[T]:
        type R = O

    transparent inline given derived[T]: Aux[T, ?] = 
        ${ derivedImpl[T] }

    def derivedImpl[T](using q: Quotes, t: Type[T]): Expr[Aux[T, ?]] =
        import q.reflect.*

        val typeSymbol = TypeTree.of[T].symbol
        val tpe = typeSymbol.companionClass.typeRef.asType

        tpe match
            case '[t] =>
                val metaDataExpr = TableMacroImpl.tableMetaData[t]
                '{
                    val comp = new FetchCompanion[T]:
                        type R = t

                        def metaData: TableMetaData =
                            $metaDataExpr

                    comp.asInstanceOf[Aux[T, t]]
                }