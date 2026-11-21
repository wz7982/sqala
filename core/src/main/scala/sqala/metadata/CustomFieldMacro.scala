package sqala.metadata

import scala.quoted.{Expr, Quotes, Type}

/**
 * Implementation of `CustomField.derived`.
 */
object CustomFieldMacro:
    private val annotationValue = "enumValue"
    
    private val annotationName = "enumName"

    def derive[T, R](using q: Quotes, t: Type[T], r: Type[R]): Expr[CustomField[T, R]] =
        import q.reflect.*

        val tType = TypeRepr.of[T]
        val rType = TypeRepr.of[R]
        val enumSymbol = tType.typeSymbol
        val enumName = enumSymbol.fullName

        val isInt = rType =:= TypeRepr.of[Int]
        val isString = rType =:= TypeRepr.of[String]

        if !isInt && !isString then
            report.errorAndAbort(
                s"CustomField.derived supports Int and String storage only, found ${rType.show}."
            )

        val hasTypeArguments = tType match
            case AppliedType(_, _) => true
            case _ => false

        if hasTypeArguments then
            report.errorAndAbort(
                s"CustomField.derived does not support generic enums: ${tType.show}."
            )

        def hasValueParameters(symbol: Symbol): Boolean =
            val constructor = symbol.primaryConstructor
            constructor != Symbol.noSymbol &&
            constructor.paramSymss.flatten.exists { parameter =>
                !parameter.flags.is(Flags.Synthetic) && !parameter.name.startsWith("$")
            }

        if hasValueParameters(enumSymbol) then
            report.errorAndAbort(
                s"CustomField.derived does not support enums with parameters: ${tType.show}. " +
                "Declare a CustomField instance manually instead."
            )

        val children = enumSymbol.children
        if children.isEmpty then
            report.errorAndAbort(
                s"CustomField.derived requires an enum or a sealed hierarchy, " +
                s"but ${tType.show} has no cases."
            )

        final case class CaseEntry(module: Symbol, name: String, value: Any)

        def typeTreeName(tpt: TypeTree): String =
            tpt match
                case TypeIdent(name) => name
                case _ => tpt.tpe.typeSymbol.name

        def annotationArguments(annotation: Term, name: String): Option[List[Term]] =
            annotation match
                case Apply(Select(New(tpt), _), args) if typeTreeName(tpt) == name =>
                    Some(args)
                case Apply(New(tpt), args) if typeTreeName(tpt) == name =>
                    Some(args)
                case Inlined(_, _, inner) =>
                    annotationArguments(inner, name)
                case Typed(inner, _) =>
                    annotationArguments(inner, name)
                case _ => None

        def annotationArgument(symbol: Symbol, name: String): Option[Term] =
            if symbol == Symbol.noSymbol then None
            else
                symbol.annotations.iterator
                    .flatMap(annotation => annotationArguments(annotation, name))
                    .flatMap(_.headOption)
                    .nextOption()

        def findAnnotation(symbols: List[Symbol], name: String): Option[Term] =
            symbols.iterator.flatMap(symbol => annotationArgument(symbol, name)).nextOption()

        def literalValue(term: Term): Option[Any] =
            term match
                case Literal(IntConstant(value)) => Some(value)
                case Literal(StringConstant(value)) => Some(value)
                case Typed(inner, _) => literalValue(inner)
                case Inlined(_, _, inner) => literalValue(inner)
                case _ => None

        def moduleOf(child: Symbol): Symbol =
            val companion = child.companionModule
            if companion != Symbol.noSymbol && companion.isTerm then companion
            else if child.isTerm then child
            else Symbol.noSymbol

        val entries = children.map { child =>
            if hasValueParameters(child) then
                report.errorAndAbort(
                    s"CustomField.derived does not support enum cases with parameters: " +
                    s"${enumName}.${child.name}."
                )

            val module = moduleOf(child)
            if module == Symbol.noSymbol then
                report.errorAndAbort(
                    s"CustomField.derived cannot read the value of enum case " +
                    s"${enumName}.${child.name}."
                )

            val name = child.name.stripSuffix("$")
            val symbols = List(module, child, child.companionClass)
            val valueTerm = findAnnotation(symbols, annotationValue)
            val nameTerm = findAnnotation(symbols, annotationName)

            val value: Any =
                if isInt then
                    if nameTerm.isDefined then
                        report.errorAndAbort(
                            s"Enum case $enumName.$name uses @enumName, " +
                            "but the storage type is Int."
                        )
                    valueTerm.flatMap(literalValue) match
                        case Some(value: Int) => value
                        case Some(other) =>
                            report.errorAndAbort(
                                s"@enumValue on $enumName.$name must be an Int literal, " +
                                s"found $other."
                            )
                        case None =>
                            report.errorAndAbort(
                                s"Enum case $enumName.$name must declare @enumValue(...) " +
                                "when the storage type is Int."
                            )
                else
                    if valueTerm.isDefined then
                        report.errorAndAbort(
                            s"Enum case $enumName.$name uses @enumValue, " +
                            "but the storage type is String."
                        )
                    nameTerm.flatMap(literalValue) match
                        case Some(value: String) => value
                        case Some(other) =>
                            report.errorAndAbort(
                                s"@enumName on $enumName.$name must be a String literal, " +
                                s"found $other."
                            )
                        case None => name

            CaseEntry(module, name, value)
        }

        val duplicated = entries
            .groupBy(_.value)
            .collect { case (value, cases) if cases.size > 1 => value }

        if duplicated.nonEmpty then
            report.errorAndAbort(
                s"Duplicate enum values in $enumName: ${duplicated.mkString(", ")}."
            )

        val asSqlExpr = Expr.summon[AsSqlExpr[R]].getOrElse(
            report.errorAndAbort(
                s"No AsSqlExpr instance found for ${rType.show}."
            )
        )

        def literalTerm(value: Any): Term =
            value match
                case value: Int => Literal(IntConstant(value))
                case value: String => Literal(StringConstant(value))
                case other => report.errorAndAbort(s"Unsupported enum value: $other.")

        def toValueBody(value: Expr[T]): Expr[R] =
            val cases = entries.map { entry =>
                CaseDef(Ref(entry.module), None, literalTerm(entry.value))
            }
            val matched = Match(value.asTerm, cases)
            Typed(matched, TypeTree.of[R]).asExprOf[R]

        def fromValueBody(value: Expr[R]): Expr[T] =
            val cases = entries.map { entry =>
                CaseDef(literalTerm(entry.value), None, Ref(entry.module))
            }
            val unknown = CaseDef(
                Wildcard(),
                None,
                '{
                    throw EnumDecodeException(
                        ${ Expr(enumName) },
                        ${ value.asTerm.asExprOf[Any] }
                    )
                }.asTerm
            )
            val matched = Match(value.asTerm, cases :+ unknown)
            Typed(matched, TypeTree.of[T]).asExprOf[T]

        '{
            new CustomField[T, R](using $asSqlExpr):
                def toValue(x: T): R =
                    ${ toValueBody('x) }

                def fromValue(x: R): T =
                    ${ fromValueBody('x) }
        }