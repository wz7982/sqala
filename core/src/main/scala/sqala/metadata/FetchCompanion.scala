package sqala.metadata

import scala.quoted.{Expr, Quotes, Type}

/**
 * Resolves the companion class of an entity's companion object at compile
 * time and provides its `TableMetaData`.
 *
 * @tparam T the companion object type.
 */
private[sqala] trait FetchCompanion[T]:
    /** The companion class type. */
    type R

    /**
     * Returns the table metadata for the entity.
     */
    def metaData: TableMetaData

private[sqala] object FetchCompanion:
    type Aux[T, O] = FetchCompanion[T]:
        type R = O

    transparent inline given derived[T]: Aux[T, ?] =
        ${ derivedImpl[T] }

    def derivedImpl[T](using q: Quotes, t: Type[T]): Expr[Aux[T, ?]] =
        import q.reflect.*

        val typeSymbol = TypeTree.of[T].symbol
        if !typeSymbol.flags.is(Flags.Module) then report.error(s"Object ${typeSymbol.name} is not a companion object.")
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