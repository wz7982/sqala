package sqala.metadata

import scala.quoted.{Expr, Quotes, Type}

/**
 * Binds a database-generated primary key to an entity object at compile
 * time, producing a new copy of the entity with the key populated.
 */
private[sqala] object InsertMacro:
    /**
     * Replaces the `@autoInc` field of `entity` with `id` and returns a
     * new instance. The original entity is not modified.
     */
    inline def bindGeneratedPrimaryKey[A](id: Long, entity: A): A =
        ${ InsertMacroImpl.bindGeneratedPrimaryKey[A]('id, 'entity) }

private[sqala] object InsertMacroImpl:
    def bindGeneratedPrimaryKey[A: Type](
        id: Expr[Long],
        entity: Expr[A]
    )(using q: Quotes): Expr[A] =
        import q.reflect.*

        val tpr = TypeRepr.of[A]
        val fields = tpr.typeSymbol.declaredFields
        val ctor = tpr.typeSymbol.primaryConstructor

        val terms = fields.map: f =>
            val autoInc = f.annotations.find:
                case Apply(Select(New(TypeIdent("autoInc")), _), _) => true
                case _ => false
            if autoInc.isDefined then
                f.typeRef.asType match
                    case '[Long] =>
                        id.asTerm
                    case '[Int] =>
                        '{ $id.toInt }.asTerm
            else
                Select.unique(entity.asTerm, f.name)

        New(Inferred(tpr)).select(ctor).appliedToArgs(terms).asExprOf[A]