package sqala.compiletime.macros

import sqala.compiletime.*
import sqala.util.*

import scala.collection.mutable.ListBuffer
import scala.quoted.{Expr, Quotes, Type}

def tableNameMacroImpl[T <: Product](using q: Quotes, t: Type[T]): Expr[String] =
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val tableName = sym.annotations.map {
        case Apply(Select(New(TypeIdent("table")), _), Literal(v) :: Nil) => v.value.toString
        case _ => ""
    }.find(_ != "") match
        case None => camelToSnake(sym.name)
        case Some(value) => value

    Expr(tableName)

def tableInfoMacroImpl[T <: Product](using q: Quotes, t: Type[T]): Expr[Any] =
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val fields = sym.declaredFields
    val tableName = tableNameMacroImpl[T]

    val typs = fields.map: field =>
        val singletonName = Singleton(Expr(field.name).asTerm)
        val annoNames = List("primaryKey", "incrementKey", "column")

        val columnInfo = field.annotations.find:
            case Apply(Select(New(TypeIdent(name)), _), _) if annoNames.contains(name) => true
            case _ => false
        match
            case Some(Apply(Select(New(TypeIdent("primaryKey")), _), _)) => "pk"
            case Some(Apply(Select(New(TypeIdent("incrementKey")), _), _)) => "pk"
            case _ => "column"

        (singletonName.tpe.asType, tableName.asTerm.tpe.asType) match
            case ('[n], '[tn]) => columnInfo match
                case "pk" => field.tree match
                    case vd: ValDef =>
                        val vdt = vd.tpt.tpe.asType
                        vdt match
                            case '[t] => (field.name, TypeRepr.of[PrimaryKey[t, tn & String, n & String]])
                case _ => field.tree match
                    case vd: ValDef =>
                        val vdt = vd.tpt.tpe.asType
                            vdt match
                                case '[t] => (field.name, TypeRepr.of[Column[t, tn & String, n & String]])

    var refinement = tableName.asTerm.tpe.asType match
        case '[tn] => Refinement(TypeRepr.of[Table[T, tn & String]], typs.head._1, typs.head._2)

    for i <- 1 until typs.size do
        refinement = Refinement(refinement, typs(i)._1, typs(i)._2)

    refinement.asType match
        case '[t] => '{
            val columns = columnsMetaDataMacro[T].map((column, ident) => Column($tableName, column, ident))
            new Table($tableName, None, columns).asInstanceOf[t]
        }

def tableAliasMacroImpl[T <: Product](name: Expr[String])(using q: Quotes, t: Type[T]): Expr[Any] =
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val fields = sym.declaredFields
    val tableName = tableNameMacroImpl[T]

    val typs = fields map: field =>
        val singletonName = Singleton(Expr(field.name).asTerm)
        val annoNames = List("primaryKey", "incrementKey", "column")

        val columnType: String = field.annotations.find:
            case Apply(Select(New(TypeIdent(name)), _), _) if annoNames.contains(name) => true
            case _ => false
        match
            case Some(Apply(Select(New(TypeIdent("primaryKey")), _), _)) => "pk"
            case Some(Apply(Select(New(TypeIdent("incrementKey")), _), _)) => "pk"
            case _ => "column"

        (singletonName.tpe.asType, name.asTerm.tpe.asType) match
            case ('[n], '[tn]) => columnType match
                case "pk" => field.tree match
                    case vd: ValDef =>
                        val vdt = vd.tpt.tpe.asType
                        vdt match
                            case '[t] => (field.name, TypeRepr.of[PrimaryKey[t, tn & String, n & String]])
                case _ => field.tree match
                    case vd: ValDef =>
                        val vdt = vd.tpt.tpe.asType
                            vdt match
                                case '[t] => (field.name, TypeRepr.of[Column[t, tn & String, n & String]])

    var refinement = name.asTerm.tpe.asType match
        case '[tn] => Refinement(TypeRepr.of[Table[T, tn & String]], typs.head._1, typs.head._2)

    for i <- 1 until typs.size do
        refinement = Refinement(refinement, typs(i)._1, typs(i)._2)

    refinement.asType match
        case '[t] => '{
            val columns = columnsMetaDataMacro[T].map((column, ident) => Column($name, column, ident))
            new Table($tableName, Some($name), columns).asInstanceOf[t]
        }

def exprMetaDataMacroImpl[T](name: Expr[String])(using q: Quotes, t: Type[T]): Expr[String] =
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val ele = sym.declaredField(name.value.get)
    var eleTag = "column"

    val annoNames = List("primaryKey", "incrementKey", "column")

    ele.annotations.find:
        case Apply(Select(New(TypeIdent(name)), _), _) if annoNames.contains(name) => true
        case _ => false
    match
        case Some(Apply(Select(New(TypeIdent(name)), _), _)) =>
            name match
                case "primaryKey" | "incrementKey" => eleTag = "pk"
                case _ =>
        case _ =>

    Expr(eleTag)

def columnsMetaDataMacroImpl[T](using q: Quotes, t: Type[T]): Expr[List[(String, String)]] =
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val eles = sym.declaredFields

    val info = eles.map: e =>
        var eleName = camelToSnake(e.name)

        val annoNames = List("primaryKey", "incrementKey", "column")

        e.annotations.find:
            case Apply(Select(New(TypeIdent(name)), _), _) if annoNames.contains(name) => true
            case _ => false
        match
            case Some(Apply(_, args)) =>
                args match
                    case Literal(v) :: _ => eleName = v.value.toString
                    case _ =>
            case _ =>

        eleName -> e.name

    Expr(info)

def tableMetaDataMacroImpl[T <: Product](using q: Quotes, t: Type[T]): Expr[TableMetaData] =
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val eles = sym.declaredFields
    val tableNameExpr = tableNameMacroImpl[T]
    val columnNameExprs = ListBuffer[Expr[String]]()
    val columnFieldExprs = ListBuffer[Expr[String]]()
    val primaryKeyFieldExprs = ListBuffer[Expr[String]]()
    val incrementKeyFieldExprs = ListBuffer[Expr[String]]()
    
    eles.foreach: e =>
        val annotations = e.annotations

        columnFieldExprs.addOne(Expr(e.name))

        val annoNames = List("primaryKey", "incrementKey", "column")

        val args = annotations.find:
            case Apply(Select(New(TypeIdent(name)), _), _) if annoNames.contains(name) => true
            case _ => false
        match
            case Some(Apply(Select(New(TypeIdent("primaryKey")), _), args)) => 
                primaryKeyFieldExprs.addOne(Expr(e.name))
                args
            case Some(Apply(Select(New(TypeIdent("incrementKey")), _), args)) =>
                primaryKeyFieldExprs.addOne(Expr(e.name))
                incrementKeyFieldExprs.addOne(Expr(e.name))
                args
            case Some(Apply(Select(New(TypeIdent("column")), _), args)) =>
                args
            case _ => Nil

        args match
            case Literal(name) :: Nil => columnNameExprs.addOne(Expr(name.value.toString))
            case _ => columnNameExprs.addOne(Expr(camelToSnake(e.name)))
    
    val columnNamesExpr = Expr.ofList(columnNameExprs.toList)
    val columnFieldsExpr = Expr.ofList(columnFieldExprs.toList)
    val primaryKeyFieldsExpr = Expr.ofList(primaryKeyFieldExprs.toList)
    val incrementKeyFieldExpr = Expr.ofList(incrementKeyFieldExprs.toList)

    '{
        TableMetaData($tableNameExpr, $primaryKeyFieldsExpr, $incrementKeyFieldExpr.headOption, $columnNamesExpr, $columnFieldsExpr)
    }