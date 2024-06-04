package sqala.dsl.macros

import sqala.dsl.TableMetaData
import sqala.util.camelToSnake

import scala.collection.mutable.ListBuffer
import scala.quoted.{Expr, Quotes, Type}

val annoNameTable = "table"
val annoNameColumn = "column"
val annoNamePrimaryKey = "primaryKey"
val annoNameAutoInc = "autoInc"

def tableNameMacroImpl[T](using q: Quotes, t: Type[T]): Expr[String] =
    import q.reflect.*

    val sym = TypeTree.of[T].symbol
    val tableName = sym.annotations.map {
        case Apply(Select(New(TypeIdent(annoNameTable)), _), Literal(v) :: Nil) => v.value.toString
        case _ => ""
    }.find(_ != "") match
        case None => camelToSnake(sym.name)
        case Some(value) => value

    Expr(tableName)

def tableMetaDataMacroImpl[T](using q: Quotes, t: Type[T]): Expr[TableMetaData] =
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

        annotations.find:
            case Apply(Select(New(TypeIdent(name)), _), _) if name == annoNameAutoInc => true
            case _ => false
        match
            case Some(_) => incrementKeyFieldExprs.addOne(Expr(e.name))
            case _ =>

        annotations.find:
            case Apply(Select(New(TypeIdent(name)), _), _) if name == annoNameAutoInc || name == annoNamePrimaryKey => true
            case _ => false
        match
            case Some(_) => primaryKeyFieldExprs.addOne(Expr(e.name))
            case _ =>

        annotations.find:
            case Apply(Select(New(TypeIdent(name)), _), _) if name == annoNameColumn => true
            case _ => false
        match
            case Some(Apply(Select(New(TypeIdent(_)), _), Literal(name) :: Nil)) =>
                columnNameExprs.addOne(Expr(name.value.toString))
            case _ => columnNameExprs.addOne(Expr(camelToSnake(e.name)))
    
    val columnNamesExpr = Expr.ofList(columnNameExprs.toList)
    val columnFieldsExpr = Expr.ofList(columnFieldExprs.toList)
    val primaryKeyFieldsExpr = Expr.ofList(primaryKeyFieldExprs.toList)
    val incrementKeyFieldExpr = Expr.ofList(incrementKeyFieldExprs.toList)

    '{
        TableMetaData($tableNameExpr, $primaryKeyFieldsExpr, $incrementKeyFieldExpr.headOption, $columnNamesExpr, $columnFieldsExpr)
    }