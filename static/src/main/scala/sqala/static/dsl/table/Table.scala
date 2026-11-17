package sqala.static.dsl.table

import sqala.ir.{IrExpr, IrTable}
import sqala.metadata.{TableMacroImpl, TableMetaData}
import sqala.static.dsl.*

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}
import scala.quoted.{Quotes, Type, Expr as ScalaExpr}

/**
 * A table reference produced by `from` when an entity companion object
 * is passed, mapping entity class fields to typed expressions via
 * `Fields`.
 */
final class Table[T, K[_ <: Int] <: ExprKind, L <: Int](
    private[sqala] val __table__ : IrTable,
    private[sqala] val __metaData__ : TableMetaData,
    private[sqala] val __withTable__ : Boolean = true
) extends Selectable with AnyTable:
    /**
     * The structural type declaring available columns as a named tuple.
     * Required by `Selectable`.
     */
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T, K, L]]
        ]

    /**
     * Runtime column accessor. Required by `Selectable`.
     */
    inline def selectDynamic(name: String): Any =
        TableSelectMacro.select[T, K[L]](__table__, __withTable__, __metaData__, name)

object Table:
    extension [T, L <: Int](table: Table[T, Column, L])
        /**
         * Exclude columns from the table.
         *
         * {{{
         * val q = from(Post).map(p => p.exclude("content"))
         * }}}
         */
        transparent inline def exclude(inline columns: String*): MappedTable[?, ?, ?, ?] =
            ${ ExcludeMacro.excludeMacro[Unwrap[T, Option], T, IsOption[T], L]('table, 'columns) }

object ExcludeMacro:
    def excludeMacro[T: Type, OT: Type, O <: Boolean : Type, L <: Int : Type](
        table: ScalaExpr[Table[?, ?, ?]],
        columns: ScalaExpr[Seq[String]],
    )(using
        q: Quotes
    ): ScalaExpr[MappedTable[?, ?, ?, ?]] =
        import q.reflect.*

        val columnsTerm = columns.asTerm
        val columnNames =
            columnsTerm match
                case Inlined(_, _, Typed(Repeated(cs, _), _)) =>
                    cs.map:
                        case Literal(StringConstant(n)) =>
                            n
                        case _ =>
                            report.errorAndAbort("Invalid column names.")
                case _ =>
                    report.errorAndAbort("Invalid column names.")

        val tpe = TypeRepr.of[T]
        val sym = tpe.typeSymbol
        val compileTimeMetaData = TableMacroImpl.tableMetaDataMacro[T]
        val compileTimeFields = compileTimeMetaData.fields.toList

        val knownFields = compileTimeFields.toSet
        val unknownFields = columnNames.filterNot(knownFields.contains)

        if unknownFields.nonEmpty then
            report.errorAndAbort(s"Unknown fields: ${unknownFields.mkString(", ")}")

        val keepFields = sym.caseFields
            .filterNot(f => columnNames.contains(f.name))
            .map(f => (f.name, compileTimeFields.indexOf(f.name), tpe.memberType(f)))

        if keepFields.isEmpty then
            report.errorAndAbort("Cannot exclude all fields. At least one field must be retained.")

        def buildTupleType(types: List[TypeRepr]): TypeRepr =
            types match
                case Nil => TypeRepr.of[EmptyTuple]
                case head :: tail =>
                    val consSym = Symbol.requiredClass("scala.*:")
                    AppliedType(consSym.typeRef, List(head, buildTupleType(tail)))

        val names = keepFields.map(_._1).map(name => ConstantType(StringConstant(name)))
        val nameType = buildTupleType(names)
        val valueTypes: List[TypeRepr] = keepFields.map: field =>
            field._3.asType match
                case '[t] =>
                    TypeRepr.of[MapField[t, OT, Column, L]].dealias
        val valueType = buildTupleType(valueTypes)

        (nameType.asType, valueType.asType) match
            case ('[type n <: Tuple; n], '[type v <: Tuple; v]) =>
                val items = keepFields.zip(valueTypes).map: pair =>
                    val (fieldName, fieldIndex, _) = pair._1
                    val elemType = pair._2

                    if fieldIndex < 0 then
                        report.errorAndAbort(s"Unknown field `$fieldName`.")

                    val columnIndex = ScalaExpr(fieldIndex)

                    elemType.asType match
                        case '[ColumnExpr[ft, fl]] =>
                            '{
                                ColumnExpr[ft, fl](
                                    IrExpr.Column(
                                        if $table.__withTable__ then Some($table.__table__) else None,
                                        $table.__metaData__.columns.toList($columnIndex)
                                    )
                                )
                            }
                        case _ =>
                            report.errorAndAbort("Excluded column must be a column expression.")
                '{
                    new MappedTable[n, v, O, L](${ScalaExpr.ofTupleFromSeq(items).asExprOf[v]})
                }
            case _ =>
                report.errorAndAbort("Cannot exclude all fields. At least one field must be retained.")

/**
  * A table reference produced by `from` when a `MappedTable` is passed,
  * mapping entity class fields to typed expressions via `Fields`.
  */
final class MappedTable[N <: Tuple, V <: Tuple, O <: Boolean, L <: Int](
    private[sqala] val __items__ : V
) extends Selectable with AnyTable:
    /**
     * The structural type declaring available columns as a named tuple.
     * Required by `Selectable`.
     */
    type Fields = NamedTuple[N, MapOptionField[V, O]]

    /**
     * Runtime column accessor. Required by `Selectable`.
     */
    inline def selectDynamic(name: String): Any =
        TableSelectMacro.selectMapped[N](__items__, name)

object MappedTable:
    inline def apply[N <: Tuple, V <: Tuple, O <: Boolean, L <: Int](table: IrTable)(using
        p: AsTableParam[V, L],
        t: ToTuple[p.R]
    ): MappedTable[N, t.R, O, L] =
        new MappedTable(t.toTuple(p.asTableParam(table, 1)))

/**
 * Compile-time implementation of `selectDynamic` for `Table` and `MappedTable`.
 *
 * A structural selection always passes a literal field name, so the column
 * name (or tuple index) can be resolved at compile time.
 */
object TableSelectMacro:
    inline def select[T, EK <: ExprKind](
        table: IrTable,
        withTable: Boolean,
        metaData: TableMetaData,
        inline name: String
    ): Any =
        ${ selectImpl[T, EK]('table, 'withTable, 'metaData, 'name) }

    def selectImpl[T: Type, EK <: ExprKind : Type](
        table: ScalaExpr[IrTable],
        withTable: ScalaExpr[Boolean],
        metaData: ScalaExpr[TableMetaData],
        name: ScalaExpr[String]
    )(using q: Quotes): ScalaExpr[Any] =
        import q.reflect.*

        val fieldName = name.valueOrAbort
        val index = TableMacroImpl.tableMetaDataMacro[T].fields.toList.indexOf(fieldName)

        if index < 0 then
            report.errorAndAbort(s"Unknown field `$fieldName`.")
        else
            val columnIndex = ScalaExpr(index)

            val column: ScalaExpr[IrExpr.Column] = '{
                IrExpr.Column(
                    if $withTable then Some($table) else None,
                    $metaData.columns.toList($columnIndex)
                )
            }

            TypeRepr.of[EK].asType match
                case '[Column[l]] =>
                    '{ ColumnExpr($column) }
                case _ =>
                    '{ Expr($column) }

    inline def selectMapped[N <: Tuple](
        items: Tuple,
        inline name: String
    ): Any =
        ${ selectMappedImpl[N]('items, 'name) }

    def selectMappedImpl[N <: Tuple: Type](
        items: ScalaExpr[Tuple],
        name: ScalaExpr[String]
    )(using q: Quotes): ScalaExpr[Any] =
        import q.reflect.*

        def constantName(repr: TypeRepr): String =
            repr.dealias match
                case ConstantType(StringConstant(n)) => n
                case _ => report.errorAndAbort("Mapped table field names must be literal strings.")

        def names(repr: TypeRepr): List[String] =
            repr.dealias match
                case AppliedType(cons, args) if cons.typeSymbol == Symbol.requiredClass("scala.*:") =>
                    constantName(args.head) :: names(args(1))
                case AppliedType(cons, args) if cons.typeSymbol.name.startsWith("Tuple") =>
                    args.map(constantName)
                case t if t =:= TypeRepr.of[EmptyTuple] => Nil
                case _ => report.errorAndAbort("Mapped table field names must be literal strings.")

        val fieldName = name.valueOrAbort
        val index = names(TypeRepr.of[N]).indexOf(fieldName)

        if index < 0 then
            report.errorAndAbort(s"Unknown field `$fieldName`.")
        else
            '{
                $items.productElement(${ScalaExpr(index)})
            }