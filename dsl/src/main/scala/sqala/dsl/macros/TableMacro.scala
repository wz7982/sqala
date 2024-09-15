package sqala.dsl.macros

import sqala.dsl.*
import sqala.util.camelToSnake

import scala.collection.mutable.ListBuffer
import scala.quoted.{Expr, Quotes, Type}

object TableMacro:
    val annoNameTable = "table"
    val annoNameColumn = "column"
    val annoNamePrimaryKey = "primaryKey"
    val annoNameAutoInc = "autoInc"

    inline def tableName[T]: String = ${ tableNameMacro[T] }

    inline def tableMetaData[T]: TableMetaData = ${ tableMetaDataMacro[T] }

    def tableNameMacro[T](using q: Quotes, t: Type[T]): Expr[String] =
        import q.reflect.*

        val sym = TypeTree.of[T].symbol
        val tableName = sym.annotations.map {
            case Apply(Select(New(TypeIdent(annoNameTable)), _), Literal(v) :: Nil) => v.value.toString
            case _ => ""
        }.find(_ != "") match
            case None => camelToSnake(sym.name)
            case Some(value) => value

        Expr(tableName)

    def tableMetaDataMacro[T](using q: Quotes, t: Type[T]): Expr[TableMetaData] =
        import q.reflect.*

        val sym = TypeTree.of[T].symbol
        val eles = sym.declaredFields
        val tableNameExpr = tableNameMacro[T]
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

    def asTableMacro[T: Type](using q: Quotes): Expr[AsTable[T]] =
        import q.reflect.*

        val sym = TypeTree.of[T].symbol
        val fields = sym.declaredFields

        val info = fields.map: f =>
            f.tree match
                case ValDef(n, t, _) =>
                    val tpr = t.tpe.asType match
                        case '[t] => TypeRepr.of[sqala.dsl.Expr[t, ColumnKind]]
                    n -> tpr

        var refinement = Refinement(TypeRepr.of[Table[T]], info.head._1, info.head._2)
        for (name, tpr) <- info.tail do
            refinement = Refinement(refinement, name, tpr)

        refinement.asType match
            case '[r] => '{
                new AsTable[T]:
                    type R = r
            }

    def toOptionTableMacro[X: Type, T <: Table[X]: Type](using q: Quotes): Expr[ToOption[T]] =
        import q.reflect.*

        val tpe = Type.of[X] match
            case '[Option[x]] => TypeTree.of[x]
            case _ => TypeTree.of[X]
        val sym = tpe.symbol
        val fields = sym.declaredFields

        val info = fields.map: f =>
            f.tree match
                case ValDef(n, t, _) =>
                    val tpr = t.tpe.asType match
                        case '[t] => TypeRepr.of[sqala.dsl.Expr[Wrap[t, Option], ColumnKind]]
                    n -> tpr

        var refinement = Refinement(TypeRepr.of[Table[Wrap[X, Option]]], info.head._1, info.head._2)
        for (name, tpr) <- info.tail do
            refinement = Refinement(refinement, name, tpr)

        refinement.asType match
            case '[r] => '{
                new ToOption[T]:
                    type R = r

                    def toOption(x: T): R = x.asInstanceOf[R]
            }