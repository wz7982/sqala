package sqala.static.metadata

import scala.quoted.{Expr, Quotes, Type}

private[sqala] object InsertMacro:
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