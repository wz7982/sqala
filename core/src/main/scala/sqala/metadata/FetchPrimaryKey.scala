package sqala.metadata

import sqala.ast.expr.{SqlBinaryOperator, SqlExpr}
import sqala.ast.statement.{SqlQuery, SqlSelectItem, SqlStatement}
import sqala.ast.table.SqlTable

import scala.quoted.{Expr, Quotes, Type}

import sqala.{metadata => SqlStatementDelete}
private[sqala] trait FetchPrimaryKey[T]:
    type Args

    def createQueryTree(x: Seq[Args]): SqlQuery

    def createDeleteTree(x: Seq[Args]): SqlStatement.Delete

private[sqala] object FetchPrimaryKey:
    type Aux[T, A] = FetchPrimaryKey[T]:
        type Args = A

    transparent inline given derived[T]: Aux[T, ?] =
        ${ derivedImpl[T] }

    def derivedImpl[T](using q: Quotes, t: Type[T]): Expr[Aux[T, ?]] =
        import q.reflect.*

        val sym = TypeTree.of[T].symbol
        val eles = sym.declaredFields
        val metaData = TableMacroImpl.tableMetaDataMacro[T]
        val pkFields = metaData.primaryKeyFields
        if pkFields.isEmpty then
            report.error("The entity does not have a primary key field.")
        val pkColumnNames =
            metaData.fieldNames
                .zip(metaData.columnNames)
                .filter((f, _) => pkFields.contains(f))
                .map((_, c) => c)
        val pkEles = eles.filter(e => pkFields.contains(e.name))
        val instances = pkEles.map: e =>
            val tpe = e.termRef.typeSymbol.typeRef.asType
            tpe match
                case '[t] =>
                    Expr.summon[AsSqlExpr[t]].get
        val types = pkEles.map: e =>
            e.termRef.typeSymbol.typeRef

        def typesToTupleType(list: List[TypeRef]): TypeRepr =
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

        val tpe =
            if types.size == 1 then types.head.asType
            else
                typesToTupleType(types).asType

        val pkColumNamesExpr = Expr.ofList(pkColumnNames.map(Expr(_)))
        val instancesExpr = Expr.ofList(instances)
        val tableNameExpr = Expr(metaData.tableName)
        val columnNamesExpr = Expr.ofList(metaData.columnNames.map(Expr(_)))

        tpe match
            case '[t] =>
                '{
                    val pk = new FetchPrimaryKey[T]:
                        type Args = t

                        def createInfo(x: Seq[Args]): (SqlExpr, SqlTable.Ident) =
                            val sqlConditions = x.map: p =>
                                val values = p match
                                    case t: Tuple => t.toArray.toList
                                    case _ => List[Any](p)
                                val instances = $instancesExpr.map(_.asInstanceOf[AsSqlExpr[Any]])
                                val sqlValues = instances.zip(values).map: (i, v) =>
                                    i.asSqlExpr(v)
                                val sqlConditions = $pkColumNamesExpr.zip(sqlValues).map: (n, v) =>
                                    SqlExpr.Binary(SqlExpr.Column(None, n), SqlBinaryOperator.Equal, v)
                                sqlConditions
                                    .reduce: (x, y) =>
                                        SqlExpr.Binary(x, SqlBinaryOperator.And, y)
                            val sqlCondition =
                                if sqlConditions.isEmpty then SqlExpr.BooleanLiteral(false)
                                else sqlConditions.reduce((x, y) => SqlExpr.Binary(x, SqlBinaryOperator.Or, y))
                            val table: SqlTable.Ident = SqlTable.Ident($tableNameExpr, None, None, None, None)
                            (sqlCondition, table)

                        def createQueryTree(x: Seq[Args]): SqlQuery =
                            val (sqlCondition, table) = createInfo(x)
                            SqlQuery.Select(
                                None,
                                $columnNamesExpr.map(n => SqlSelectItem.Expr(SqlExpr.Column(None, n), None)),
                                table :: Nil,
                                Some(sqlCondition),
                                None,
                                None,
                                Nil,
                                None,
                                None
                            )

                        def createDeleteTree(x: Seq[t]): SqlStatement.Delete =
                            val (sqlCondition, table) = createInfo(x)
                            SqlStatement.Delete(
                                table,
                                Some(sqlCondition)
                            )

                    pk.asInstanceOf[Aux[T, t]]
                }