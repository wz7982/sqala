package sqala.jdbc

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.order.{SqlOrdering, SqlOrderingItem}
import sqala.ast.quantifier.SqlQuantifier
import sqala.ast.statement.{SqlQuery, SqlSelectItem}
import sqala.ast.table.SqlTable
import sqala.printer.Dialect
import sqala.static.dsl.QueryContext

import sqala.static.dsl.statement.query.Query
import sqala.static.metadata.{AsSqlExpr, TableMacroImpl}
import scala.quoted.{Expr, Quotes, Type}

trait Repository[T, N <: String]:
    type Args

    type R

    def createQuery(
        dialect: Dialect,
        standardEscapeStrings: Boolean,
        args: Args, 
        fetch: Query[?] => List[T],
        find: Query[?] => Option[T],
        page: (Query[?], Int, Int, Boolean) => Page[T],
        count: Query[?] => Long,
        exists: Query[?] => Boolean
    )(using 
        JdbcDecoder[T],
        Logger
    ): R

object Repository:
    type Aux[T, N <: String, A, O] = Repository[T, N]:
        type Args = A

        type R = O

    enum Token:
        case Keyword(s: String)
        case Column(s: String)

    case class Context(var ident: String)

    def lex(chars: List[Char], context: Context): List[Token] =
        import Token.*

        def recordIdent: Option[Token] =
            if context.ident.nonEmpty then
                val token = 
                    Some(Column(context.ident.charAt(0).toLower + context.ident.substring(1)))
                context.ident = ""
                token
            else None

        chars match
            case 'f' :: 'e' :: 't' :: 'c' :: 'h' :: 'B' :: 'y' :: xs =>
                Keyword("fetchBy") :: lex(xs, context)
            case 'f' :: 'e' :: 't' :: 'c' :: 'h' :: 'D' :: 'i' :: 's' :: 't' :: 'i' :: 'n' :: 'c' :: 't' :: 'B' :: 'y' :: xs =>
                Keyword("fetchDistinctBy") :: lex(xs, context)
            case 'f' :: 'i' :: 'n' :: 'd' :: 'B' :: 'y' :: xs =>
                Keyword("findBy") :: lex(xs, context)
            case 'f' :: 'i' :: 'n' :: 'd' :: 'D' :: 'i' :: 's' :: 't' :: 'i' :: 'n' :: 'c' :: 't' :: 'B' :: 'y' :: xs =>
                Keyword("findDistinctBy") :: lex(xs, context)
            case 'c' :: 'o' :: 'u' :: 'n' :: 't' :: 'B' :: 'y' :: xs =>
                Keyword("countBy") :: lex(xs, context)
            case 'c' :: 'o' :: 'u' :: 'n' :: 't' :: 'D' :: 'i' :: 's' :: 't' :: 'i' :: 'n' :: 'c' :: 't' :: 'B' :: 'y' :: xs =>
                Keyword("countDistinctBy") :: lex(xs, context)
            case 'p' :: 'a' :: 'g' :: 'e' :: 'B' :: 'y' :: xs =>
                Keyword("pageBy") :: lex(xs, context)
            case 'p' :: 'a' :: 'g' :: 'e' :: 'D' :: 'i' :: 's' :: 't' :: 'i' :: 'n' :: 'c' :: 't' :: 'B' :: 'y' :: xs =>
                Keyword("pageDistinctBy") :: lex(xs, context)
            case 'e' :: 'x' :: 'i' :: 's' :: 't' :: 's' :: 'B' :: 'y' :: xs =>
                Keyword("existsBy") :: lex(xs, context)
            case 'e' :: 'x' :: 'i' :: 's' :: 't' :: 's' :: 'D' :: 'i' :: 's' :: 't' :: 'i' :: 'n' :: 'c' :: 't' :: 'B' :: 'y' :: xs =>
                Keyword("existsDistinctBy") :: lex(xs, context)
            case 'O' :: 'r' :: 'd' :: 'e' :: 'r' :: 'B' :: 'y' :: xs =>
                recordIdent.toList ++ (Keyword("orderBy") :: lex(xs, context))
            case 'A' :: 's' :: 'c' :: xs =>
                recordIdent.toList ++ (Keyword("asc") :: lex(xs, context))
            case 'D' :: 'e' :: 's' :: 'c' :: xs =>
                recordIdent.toList ++ (Keyword("desc") :: lex(xs, context))
            case 'A' :: 'n' :: 'd' :: xs =>
                recordIdent.toList ++ (Keyword("and") :: lex(xs, context))
            case 'I' :: 'n' :: xs =>
                recordIdent.toList ++ (Keyword("in") :: lex(xs, context))
            case 'N' :: 'o' :: 't' :: 'I' :: 'n' :: xs =>
                recordIdent.toList ++ (Keyword("notIn") :: lex(xs, context))
            case 'B' :: 'e' :: 't' :: 'w' :: 'e' :: 'e' :: 'n' :: xs =>
                recordIdent.toList ++ (Keyword("between") :: lex(xs, context))
            case 'N' :: 'o' :: 't' :: 'B' :: 'e' :: 't' :: 'w' :: 'e' :: 'e' :: 'n' :: xs =>
                recordIdent.toList ++ (Keyword("notBetween") :: lex(xs, context))
            case 'L' :: 'i' :: 'k' :: 'e' :: xs =>
                recordIdent.toList ++ (Keyword("like") :: lex(xs, context))
            case 'N' :: 'o' :: 't' :: 'L' :: 'i' :: 'k' :: 'e' :: xs =>
                recordIdent.toList ++ (Keyword("notLike") :: lex(xs, context))
            case 'c' :: 'o' :: 'n' :: 't' :: 'a' :: 'i' :: 'n' :: 's' :: xs =>
                recordIdent.toList ++ (Keyword("contains") :: lex(xs, context))
            case 's' :: 't' :: 'a' :: 'r' :: 't' :: 's' :: 'W' :: 'i' :: 't' :: 'h' :: xs =>
                recordIdent.toList ++ (Keyword("startsWith") :: lex(xs, context))
            case 'e' :: 'n' :: 'd' :: 's' :: 'W' :: 'i' :: 't' :: 'h' :: xs =>
                recordIdent.toList ++ (Keyword("endsWith") :: lex(xs, context))
            case 'N' :: 'o' :: 't' :: xs =>
                recordIdent.toList ++ (Keyword("not") :: lex(xs, context))
            case 'I' :: 's' :: 'N' :: 'u' :: 'l' :: 'l' :: xs =>
                recordIdent.toList ++ (Keyword("isNull") :: lex(xs, context))
            case 'I' :: 's' :: 'N' :: 'o' :: 't' :: 'N' :: 'u' :: 'l' :: 'l' :: xs =>
                recordIdent.toList ++ (Keyword("isNotNull") :: lex(xs, context))
            case 'G' :: 'r' :: 'e' :: 'a' :: 't' :: 'e' :: 'r' :: 'T' :: 'h' :: 'a' :: 'n' :: 'E' :: 'q' :: 'u' :: 'a' :: 'l' :: xs =>
                recordIdent.toList ++ (Keyword("greaterThanEqual") :: lex(xs, context))
            case 'G' :: 'r' :: 'e' :: 'a' :: 't' :: 'e' :: 'r' :: 'T' :: 'h' :: 'a' :: 'n' :: xs =>
                recordIdent.toList ++ (Keyword("greaterThan") :: lex(xs, context))
            case 'L' :: 'e' :: 's' :: 's' :: 'T' :: 'h' :: 'a' :: 'n' :: 'E' :: 'q' :: 'u' :: 'a' :: 'l' :: xs =>
                recordIdent.toList ++ (Keyword("lessThanEqual") :: lex(xs, context))
            case 'L' :: 'e' :: 's' :: 's' :: 'T' :: 'h' :: 'a' :: 'n' :: xs =>
                recordIdent.toList ++ (Keyword("lessThan") :: lex(xs, context))
            case x :: xs =>
                context.ident += x
                lex(xs, context)
            case Nil => 
                recordIdent.toList ++ Nil

    transparent inline given derived[T, N <: String]: Aux[T, N, ?, ?] =
        ${ derivedImpl[T, N] }

    def derivedImpl[T: Type, N <: String : Type](using q: Quotes): Expr[Aux[T, N, ?, ?]] =
        import Token.*
        import q.reflect.*

        val name = TypeRepr.of[N].show.drop(1).dropRight(1)
        val tokens = lex(name.toList, Context(""))
        val (mode, body) = tokens match
            case Keyword(n) :: xs =>
                (n, xs)
            case _ =>
                report.errorAndAbort("Unsupported operation.")
        if body.isEmpty then report.errorAndAbort("Unsupported operation.")
        val resultType = mode match
            case "fetchBy" | "fetchDistinctBy" => Type.of[List[T]]
            case "findBy" | "findDistinctBy" => Type.of[Option[T]]
            case "pageBy" | "pageDistinctBy" => Type.of[Page[T]]
            case "countBy" | "countDistinctBy" => Type.of[Long]
            case "existsBy" | "existsDistinctBy" => Type.of[Boolean]
        val distinct = mode.contains("Distinct")

        val sym = TypeTree.of[T].symbol
        val eles = sym.declaredFields
        val metaData = TableMacroImpl.tableMetaDataMacro[T]
        val nameMap = metaData.fieldNames.zip(metaData.columnNames).toMap

        val (condition, sorts) =
            val (before, afterWithTarget) = body.span(_ != Keyword("orderBy"))
            val after = if afterWithTarget.isEmpty then Nil else afterWithTarget.tail
            (before, after)

        def split(list: List[Token], target: Token): List[List[Token]] =
            def splitRec(remaining: List[Token], current: List[Token], acc: List[List[Token]]): List[List[Token]] =
                remaining match
                    case Nil => 
                        if (current.nonEmpty) (current :: acc).reverse else acc.reverse
                    case x :: xs if x == target =>
                        splitRec(xs, Nil, current :: acc)
                    case x :: xs =>
                        splitRec(xs, current :+ x, acc)
            splitRec(list, Nil, Nil)

        def createColumnInfo(fieldName: String): (String, Expr[AsSqlExpr[?]], TypeRef) =
            val elementType = 
                eles.find(_.name == fieldName).get.termRef.typeSymbol.typeRef.asType
            val elementTypeRef =
                eles.find(_.name == fieldName).get.termRef.typeSymbol.typeRef
            elementType match
                case '[t] =>
                    (nameMap(fieldName), Expr.summon[AsSqlExpr[t]].get, elementTypeRef)

        val conditions = split(condition, Keyword("and"))
            .map:
                case Column(n) :: Keyword(k@("in" | "notIn")) :: Nil =>
                    val info = createColumnInfo(n)
                    info._3.asType match
                        case '[t] =>
                            (info._1, k, info._2 :: Nil, TypeRepr.of[Seq[t]] :: Nil)
                case Column(n) :: Keyword(k@("between" | "notBetween")) :: Nil =>
                    val info = createColumnInfo(n)
                    (info._1, k, info._2 :: info._2 :: Nil, info._3 :: info._3 :: Nil)
                case Column(n) :: Keyword(k@("like" | "notLike" | "contains" | "startsWith" | "endsWith")) :: Nil =>
                    val info = createColumnInfo(n)
                    (info._1, k, Expr.summon[AsSqlExpr[String]].get :: Nil, TypeRepr.of[String] :: Nil)
                case Column(n) :: Keyword(k@("isNull" | "isNotNull")) :: Nil =>
                    val info = createColumnInfo(n)
                    (info._1, k, Nil, Nil)
                case Column(n) :: Keyword(k) :: Nil =>
                    val info = createColumnInfo(n)
                    (info._1, k, info._2 :: Nil, info._3 :: Nil)
                case Column(n) :: Nil =>
                    val info = createColumnInfo(n)
                    (info._1, "equal", info._2 :: Nil, info._3 :: Nil)
                case _ =>
                    report.errorAndAbort("Unsupported operation.")

        val types = 
            if !mode.startsWith("page") then conditions.flatMap(_._4)
            else conditions.flatMap(_._4) ++ List(TypeRepr.of[Int], TypeRepr.of[Int], TypeRepr.of[Boolean])

        val instances =
            conditions.flatMap(_._3)
        
        def typesToTupleType(list: List[TypeRepr]): TypeRepr =
            list match
                case x :: xs =>
                    val remaining = typesToTupleType(xs)
                    x.asType match
                        case '[h] =>
                            remaining.asType match
                                case '[type t <: Tuple; t] =>
                                    TypeRepr.of[h *: t]
                case Nil =>
                    TypeRepr.of[EmptyTuple]

        val argsType = 
            if types.size == 1 then types.head.asType
            else
                typesToTupleType(types).asType

        val sortInfo = sorts.grouped(2).toList.map:
            case Column(n) :: Keyword("asc") :: Nil =>
                (nameMap(n), "asc")
            case Column(n) :: Keyword("desc") :: Nil =>
                (nameMap(n), "desc")
            case _ =>
                report.errorAndAbort("Unsupported operation.")

        val distinctExpr = Expr(distinct)
        val instanceExpr = Expr.ofList(instances)
        val condExpr = Expr.ofList(conditions.map(c => Expr(c._1, c._2)))
        val sortExpr = Expr.ofList(sortInfo.map(s => Expr(s._1, s._2)))
        val columnExpr = Expr.ofList(metaData.columnNames.map(Expr(_)))
        val tableNameExpr = Expr(metaData.tableName)
        val modeExpr = Expr(mode)

        argsType match
            case '[args] =>
                resultType match
                    case '[r] =>
                        '{
                            val repository = new Repository[T, N]:
                                type Args = args

                                type R = r

                                def createQuery(
                                    dialect: Dialect,
                                    standardEscapeStrings: Boolean,
                                    args: Args, 
                                    fetch: Query[?] => List[T],
                                    find: Query[?] => Option[T],
                                    page: (Query[?], Int, Int, Boolean) => Page[T],
                                    count: Query[?] => Long,
                                    exists: Query[?] => Boolean
                                )(using 
                                    d: JdbcDecoder[T],
                                    l: Logger
                                ): R =
                                    val values = args match
                                        case t: Tuple => t.toArray.toList
                                        case _ => List[Any](args)
                                    val valueIterator = values.iterator
                                    val condIterator = $condExpr.iterator
                                    val instanceIterator = $instanceExpr.iterator
                                    val condBuffer = collection.mutable.ListBuffer[SqlExpr]()
                                    while condIterator.hasNext do
                                        val (columnName, op) = condIterator.next()
                                        val left = SqlExpr.Column(None, columnName)
                                        op match
                                            case "isNull" =>
                                                condBuffer.addOne(SqlExpr.NullTest(left, false))
                                            case "isNotNull" =>
                                                condBuffer.addOne(SqlExpr.NullTest(left, false))
                                            case "in" =>
                                                val value = valueIterator.next().asInstanceOf[Seq[Any]]
                                                val instance = instanceIterator.next().asInstanceOf[AsSqlExpr[Any]]
                                                val exprs = value.map(v => instance.asSqlExpr(v))
                                                if exprs.isEmpty then
                                                    condBuffer.addOne(SqlExpr.BooleanLiteral(false))
                                                else
                                                    condBuffer.addOne(SqlExpr.Binary(left, SqlBinaryOperator.In, SqlExpr.Tuple(exprs.toList)))
                                            case "notIn" =>
                                                val value = valueIterator.next().asInstanceOf[Seq[Any]]
                                                val instance = instanceIterator.next().asInstanceOf[AsSqlExpr[Any]]
                                                val exprs = value.map(v => instance.asSqlExpr(v))
                                                if exprs.isEmpty then
                                                    condBuffer.addOne(SqlExpr.BooleanLiteral(true))
                                                else
                                                    condBuffer.addOne(SqlExpr.Binary(left, SqlBinaryOperator.NotIn, SqlExpr.Tuple(exprs.toList)))
                                            case "between" =>
                                                val start = valueIterator.next()
                                                val startInstance = instanceIterator.next().asInstanceOf[AsSqlExpr[Any]]
                                                val end = valueIterator.next()
                                                val endInstance = instanceIterator.next().asInstanceOf[AsSqlExpr[Any]]
                                                condBuffer.addOne(
                                                    SqlExpr.Between(left, startInstance.asSqlExpr(start), endInstance.asSqlExpr(end), false)
                                                )
                                            case "notBetween" =>
                                                val start = valueIterator.next()
                                                val startInstance = instanceIterator.next().asInstanceOf[AsSqlExpr[Any]]
                                                val end = valueIterator.next()
                                                val endInstance = instanceIterator.next().asInstanceOf[AsSqlExpr[Any]]
                                                condBuffer.addOne(
                                                    SqlExpr.Between(left, startInstance.asSqlExpr(start), endInstance.asSqlExpr(end), true)
                                                )
                                            case "contains" =>
                                                val value = valueIterator.next().asInstanceOf[String]
                                                val instance = instanceIterator.next().asInstanceOf[AsSqlExpr[String]]
                                                condBuffer.addOne(SqlExpr.Like(left, instance.asSqlExpr("%" + value + "%"), None, false))
                                            case "startsWith" =>
                                                val value = valueIterator.next().asInstanceOf[String]
                                                val instance = instanceIterator.next().asInstanceOf[AsSqlExpr[String]]
                                                condBuffer.addOne(SqlExpr.Like(left, instance.asSqlExpr(value + "%"), None, false))
                                            case "endsWith" =>
                                                val value = valueIterator.next().asInstanceOf[String]
                                                val instance = instanceIterator.next().asInstanceOf[AsSqlExpr[String]]
                                                condBuffer.addOne(SqlExpr.Like(left, instance.asSqlExpr("%" + value), None, false))
                                            case "like" =>
                                                val value = valueIterator.next().asInstanceOf[String]
                                                val instance = instanceIterator.next().asInstanceOf[AsSqlExpr[String]]
                                                condBuffer.addOne(SqlExpr.Like(left, instance.asSqlExpr(value), None, false))
                                            case "notLike" =>
                                                val value = valueIterator.next().asInstanceOf[String]
                                                val instance = instanceIterator.next().asInstanceOf[AsSqlExpr[String]]
                                                condBuffer.addOne(SqlExpr.Like(left, instance.asSqlExpr(value), None, true))
                                            case opString =>
                                                val value = valueIterator.next()
                                                val instance = instanceIterator.next().asInstanceOf[AsSqlExpr[Any]]
                                                val op = opString match
                                                    case "equal" => SqlBinaryOperator.Equal
                                                    case "not" => SqlBinaryOperator.NotEqual
                                                    case "greaterThan" => SqlBinaryOperator.GreaterThan
                                                    case "greaterThanEqual" => SqlBinaryOperator.GreaterThanEqual
                                                    case "lessThan" => SqlBinaryOperator.LessThan
                                                    case "lessThanEqual" => SqlBinaryOperator.LessThanEqual
                                                condBuffer.addOne(SqlExpr.Binary(left, op, instance.asSqlExpr(value)))
                                    val table = SqlTable.Standard($tableNameExpr, None, None, None, None)
                                    val sort = $sortExpr.map: (n, s) =>
                                        s match
                                            case "asc" => SqlOrderingItem(SqlExpr.Column(None, n), Some(SqlOrdering.Asc), None)
                                            case "desc" => SqlOrderingItem(SqlExpr.Column(None, n), Some(SqlOrdering.Desc), None)
                                    val baseTree = 
                                        SqlQuery.Select(
                                            if $distinctExpr then Some(SqlQuantifier.Distinct) else None,
                                            $columnExpr.map(n => SqlSelectItem.Expr(SqlExpr.Column(None, n), None)),
                                            table :: Nil,
                                            Some(condBuffer.toList.reduce((x, y) => SqlExpr.Binary(x, SqlBinaryOperator.And, y))),
                                            None,
                                            None,
                                            Nil,
                                            sort,
                                            None,
                                            None
                                        )
                                    val query = Query(null, baseTree)(using QueryContext(0))

                                    val result = if $modeExpr.startsWith("find") then
                                        find(query)
                                    else if $modeExpr.startsWith("page") then
                                        page(
                                            query, 
                                            valueIterator.next().asInstanceOf[Int],
                                            valueIterator.next().asInstanceOf[Int],
                                            valueIterator.next().asInstanceOf[Boolean]
                                        )
                                    else if $modeExpr.startsWith("count") then
                                        count(query)
                                    else if $modeExpr.startsWith("exists") then
                                        exists(query)
                                    else
                                        fetch(query)

                                    result.asInstanceOf[R]

                            repository.asInstanceOf[Aux[T, N, args, r]]
                        }