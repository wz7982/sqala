package sqala.static.metadata

import sqala.util.camelToSnake

import scala.collection.mutable.ListBuffer
import scala.quoted.{Expr, Quotes, Type}

private[sqala] object TableMacro:
    inline def tableName[T]: String = ${ TableMacroImpl.tableName[T] }

    inline def tableMetaData[T]: TableMetaData = ${ TableMacroImpl.tableMetaData[T] }

private[sqala] object TableMacroImpl:
    val annoNameTable = "table"
    val annoNameColumn = "column"
    val annoNamePrimaryKey = "primaryKey"
    val annoNameAutoInc = "autoInc"

    def tableNameMacro[T](using q: Quotes, t: Type[T]): String =
        import q.reflect.*

        val sym = TypeTree.of[T].symbol
        val tableName = sym.annotations.map {
            case Apply(Select(New(TypeIdent(annoNameTable)), _), Literal(v) :: Nil) => 
                v.value.toString
            case _ => ""
        }.find(_ != "") match
            case None => camelToSnake(sym.name)
            case Some(value) => value

        tableName

    def tableName[T](using q: Quotes, t: Type[T]): Expr[String] =
        Expr(tableNameMacro[T])

    def tableMetaDataMacro[T](using q: Quotes, t: Type[T]): TableMetaData =
        import q.reflect.*

        val sym = TypeTree.of[T].symbol
        val eles = sym.declaredFields
        val tableName = tableNameMacro[T]
        val columnNames = ListBuffer[String]()
        val columnFields = ListBuffer[String]()
        val primaryKeyFields = ListBuffer[String]()
        val incrementKeyFields = ListBuffer[String]()
        
        eles.foreach: e =>
            val annotations = e.annotations

            columnFields.addOne(e.name)

            annotations.find:
                case Apply(Select(New(TypeIdent(name)), _), _) if name == annoNameAutoInc => true
                case _ => false
            match
                case Some(_) => incrementKeyFields.addOne(e.name)
                case _ =>

            annotations.find:
                case Apply(Select(New(TypeIdent(name)), _), _) if name == annoNameAutoInc || name == annoNamePrimaryKey => true
                case _ => false
            match
                case Some(_) => primaryKeyFields.addOne(e.name)
                case _ =>

            annotations.find:
                case Apply(Select(New(TypeIdent(name)), _), _) if name == annoNameColumn => true
                case _ => false
            match
                case Some(Apply(Select(New(TypeIdent(_)), _), Literal(name) :: Nil)) =>
                    columnNames.addOne(name.value.toString)
                case _ => columnNames.addOne(camelToSnake(e.name))
        
        TableMetaData(
            tableName,
            primaryKeyFields.toList, 
            incrementKeyFields.headOption, 
            columnNames.toList, 
            columnFields.toList
        )

    def tableMetaData[T](using q: Quotes, t: Type[T]): Expr[TableMetaData] =
        val metaData = tableMetaDataMacro[T]

        val tableNameExpr = Expr(metaData.tableName)
        val primaryKeyExpr = Expr.ofList(metaData.primaryKeyFields.map(Expr(_)))
        val incrementExpr = metaData.incrementField match
            case None => Expr(Option.empty[String])
            case Some(e) => Expr(Some(e))
        val columnsExpr = Expr.ofList(metaData.columnNames.map(Expr(_)))
        val fieldsExpr = Expr.ofList(metaData.fieldNames.map(Expr(_)))

        '{
            TableMetaData(
                $tableNameExpr,
                $primaryKeyExpr,
                $incrementExpr,
                $columnsExpr,
                $fieldsExpr
            )
        }