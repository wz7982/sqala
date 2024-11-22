package sqala.dsl

import sqala.ast.expr.SqlExpr

import scala.NamedTuple.*
import scala.compiletime.constValueTuple

sealed abstract class Container:
    private[sqala] def __fieldNames__ : List[String]

    private[sqala] def __exprs__ : List[SqlExpr]

    private[sqala] def __fetchExpr__(name: String): SqlExpr =
        __fieldNames__.zip(__exprs__).find(_._1 == name).map(_._2).get

class Table[T](
    private[sqala] val __tableName__ : String,
    private[sqala] val __aliasName__ : String,
    private[sqala] val __metaData__ : TableMetaData
) extends Container with Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    private[sqala] def __fieldNames__ : List[String] =
        __metaData__.fieldNames

    private[sqala] def __exprs__ : List[SqlExpr] =
        for field <- __metaData__.columnNames yield
            SqlExpr.Column(Some(__aliasName__), field)

    def selectDynamic(name: String): Any = compileTimeOnly

object Table:
    extension [T](table: Table[T])
        def * : table.Fields = compileTimeOnly

class SubQuery[N <: Tuple, V <: Tuple](
    private[sqala] val __aliasName__ : String,
    private[sqala] val __columnNames__ : List[String]
)(using val qc: QueryContext) extends Container with Selectable:
    type Fields = NamedTuple[N, V]

    private[sqala] def __fieldNames__ : List[String] =
        __columnNames__

    private[sqala] def __exprs__ : List[SqlExpr] =
        for (_, i) <- __columnNames__.zipWithIndex yield
            SqlExpr.Column(Some(__aliasName__), s"c$i")

    def selectDynamic(name: String): Any = compileTimeOnly

    def * : Fields = compileTimeOnly

object SubQuery:
    inline def apply[N <: Tuple, V <: Tuple](alias: String)(using QueryContext): SubQuery[N, V] =
        val names = constValueTuple[N].toList.map(_.asInstanceOf[String])
        new SubQuery(alias, names)

class UngroupedTable[T](
    private[sqala] val __tableName__ : String,
    private[sqala] val __aliasName__ : String,
    private[sqala] val __metaData__ : TableMetaData
) extends Container with Selectable:
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T]]
        ]

    private[sqala] def __fieldNames__ : List[String] =
        __metaData__.fieldNames

    private[sqala] def __exprs__ : List[SqlExpr] =
        for field <- __metaData__.columnNames yield
            SqlExpr.Column(Some(__aliasName__), field)

    def selectDynamic(name: String): Any = compileTimeOnly

class UngroupedSubQuery[N <: Tuple, V <: Tuple](
    private[sqala] val __aliasName__ : String,
    private[sqala] val __columnNames__ : List[String]
)(using val qc: QueryContext) extends Container with Selectable:
    type Fields = NamedTuple[N, V]

    private[sqala] def __fieldNames__ : List[String] =
        __columnNames__

    private[sqala] def __exprs__ : List[SqlExpr] =
        for (_, i) <- __columnNames__.zipWithIndex yield
            SqlExpr.Column(Some(__aliasName__), s"c$i")

    def selectDynamic(name: String): Any = compileTimeOnly

    def * : Fields = compileTimeOnly

object UngroupedSubQuery:
    inline def apply[N <: Tuple, V <: Tuple](alias: String)(using QueryContext): UngroupedSubQuery[N, V] =
        val names = constValueTuple[N].toList.map(_.asInstanceOf[String])
        new UngroupedSubQuery(alias, names)

class Sort[N <: Tuple, V <: Tuple](
    private[sqala] val __names__ : List[String],
    private[sqala] val __values__ : List[SqlExpr]
) extends Container with Selectable:
    type Fields = NamedTuple[N, V]

    private[sqala] def __fieldNames__ : List[String] =
        __names__

    private[sqala] def __exprs__ : List[SqlExpr] =
        __values__

    def selectDynamic(name: String): Any = compileTimeOnly

object Sort:
    inline def apply[N <: Tuple, V <: Tuple](values: List[SqlExpr]): Sort[N, V] =
        val names = constValueTuple[N].toList.map(_.asInstanceOf[String])
        new Sort(names, values)

class Group[N <: Tuple, V <: Tuple](
    private[sqala] val __names__ : List[String],
    private[sqala] val __values__ : List[SqlExpr]
) extends Container with Selectable:
    type Fields = NamedTuple[N, V]

    private[sqala] def __fieldNames__ : List[String] =
        __names__

    private[sqala] def __exprs__ : List[SqlExpr] =
        __values__

    def selectDynamic(name: String): Any = compileTimeOnly

object Group:
    inline def apply[N <: Tuple, V <: Tuple](values: List[SqlExpr]): Group[N, V] =
        val names = constValueTuple[N].toList.map(_.asInstanceOf[String])
        new Group(names, values)