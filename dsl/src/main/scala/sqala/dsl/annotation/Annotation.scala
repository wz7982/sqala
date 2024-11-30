package sqala.dsl.annotation

import scala.annotation.{StaticAnnotation, MacroAnnotation}
import scala.quoted.Quotes

@scala.annotation.meta.field
case class table(tableName: String) extends StaticAnnotation

@scala.annotation.meta.field
case class column(columnName: String) extends StaticAnnotation

@scala.annotation.meta.field
class primaryKey extends MacroAnnotation:
    def transform(using 
        q: Quotes
    )(
        definition: q.reflect.Definition, 
        companion: Option[q.reflect.Definition]
    ): List[q.reflect.Definition] =
        import q.reflect.*

        val t = definition.symbol.typeRef.asType

        t match
            case '[Option[t]] =>
                report.error:
                    "Primary key must be not empty."
            case _ =>

        definition :: Nil

@scala.annotation.meta.field
class autoInc extends MacroAnnotation:
    def transform(using 
        q: Quotes
    )(
        definition: q.reflect.Definition, 
        companion: Option[q.reflect.Definition]
    ): List[q.reflect.Definition] =
        import q.reflect.*

        val t = definition.symbol.typeRef.asType

        t match
            case '[Int] =>
            case '[Long] =>
            case _ => 
                report.error:
                    "Auto-increment primary key must be of type Int or Long."

        val owner = definition.symbol.owner
        val fields = owner.declaredFields
        
        for f <- fields if f.name != definition.name do
            f.annotations.foreach:
                case Apply(Select(New(TypeIdent("autoInc")), _), _) =>
                    report.error(s"There can only one auto-increment primary key.")
                case _ =>

        definition :: Nil

@scala.annotation.meta.field
class sqlFunction extends StaticAnnotation

@scala.annotation.meta.field
class sqlAgg extends StaticAnnotation

@scala.annotation.meta.field
class sqlWindow extends StaticAnnotation