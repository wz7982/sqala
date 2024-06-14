package sqala.json

import scala.collection.mutable.ListBuffer
import scala.quoted.{Expr, Quotes, Type}

inline def jsonMetaDataMacro[T]: JsonMetaData = ${ jsonMetaDataMacroImpl[T] }

def jsonMetaDataMacroImpl[T: Type](using q: Quotes): Expr[JsonMetaData] =
    import q.reflect.*
    
    val tpe = TypeTree.of[T]
    
    val symbol = tpe.tpe.typeSymbol

    val comp = symbol.companionClass
    val mod = Ref(symbol.companionModule)
    val body = comp.tree.asInstanceOf[ClassDef].body
    val fields = symbol.declaredFields

    val fieldNames = ListBuffer[Expr[String]]()
    val aliasNames = ListBuffer[Expr[Option[String]]]()
    val ignoreList = ListBuffer[Expr[Boolean]]()
    val defaultValues = ListBuffer[Expr[Option[?]]]()

    for field <- fields do
        fieldNames.addOne(Expr(field.name))

        val annotations = field.annotations

        val alias = annotations.find:
            case Apply(Select(New(TypeIdent("jsonAlias")), _), _)  => true
            case _ => false
        match
            case Some(Apply(Select(New(TypeIdent(_)), _), Literal(v) :: Nil)) =>
                Expr(Option(v.value.toString))
            case _ => Expr(None)
        aliasNames.addOne(alias)

        val ignore = annotations.find:
            case Apply(Select(New(TypeIdent("jsonIgnore")), _), _)  => true
            case _ => false
        match
            case Some(_) => Expr(true)
            case None => Expr(false)
        ignoreList.addOne(ignore)

    for (p, i) <- symbol.caseFields.zipWithIndex do
        if p.flags.is(Flags.HasDefault) then
            for case deff @ DefDef(name, _, _, _) <- body
                if name.startsWith("$lessinit$greater$default$" + (i + 1))
            do 
                val default = mod.select(deff.symbol).asExpr
                defaultValues.addOne('{ Option($default) })
        else defaultValues.addOne(Expr(None))

    val fieldNamesExpr = Expr.ofList(fieldNames.toList)
    val aliasNamesExpr = Expr.ofList(aliasNames.toList)
    val ignoreListExpr = Expr.ofList(ignoreList.toList)
    val defaultValuesExpr = Expr.ofList(defaultValues.toList)

    '{ JsonMetaData($fieldNamesExpr, $aliasNamesExpr, $ignoreListExpr, $defaultValuesExpr) }