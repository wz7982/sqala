package sqala.static.dsl.table

import sqala.ir.IrExpr
import sqala.ir.IrTable
import sqala.metadata.TableMetaData
import sqala.static.dsl.*

import scala.NamedTuple.{DropNames, From, NamedTuple, Names}
import scala.compiletime.constValueTuple
import scala.quoted.Quotes
import scala.quoted.Type
import scala.quoted.{Expr => ScalaExpr}
import scala.quoted.Varargs

/**
 * A table reference produced by `from` when an entity companion object
 * is passed, mapping entity class fields to typed expressions via
 * `Fields`.
 */
final case class Table[T, K[_ <: Int] <: ExprKind, L <: Int](
    private[sqala] val __table__ : IrTable,
    private[sqala] val __metaData__ : TableMetaData
) extends Selectable with AnyTable:
    /**
     * The structural type declaring available columns as a named tuple.
     * Required by `Selectable`.
     */
    type Fields =
        NamedTuple[
            Names[From[Unwrap[T, Option]]],
            Tuple.Map[DropNames[From[Unwrap[T, Option]]], [x] =>> MapField[x, T, L]]
        ]

    /**
     * Runtime column accessor. Required by `Selectable`.
     */
    def selectDynamic(name: String): Any =
        val index = __metaData__.fields.toList.indexOf(name)
        ColumnExpr(IrExpr.Column(__table__, __metaData__.columns.toList(index)))

    // TODO 注释
    transparent inline def exclude(inline columns: String*): MappedTable[?, ?, ?, ?, ?] =
        ${ ExcludeMacro.excludeMacro[Unwrap[T, Option], T, IsOption[T], L]('__table__, '__metaData__, 'columns) }

object ExcludeMacro:
    def excludeMacro[T: Type, OT: Type, O <: Boolean : Type, L <: Int : Type](
        table: ScalaExpr[IrTable], 
        metaData: ScalaExpr[TableMetaData],
        columns: ScalaExpr[Seq[String]],
    )(using 
        q: Quotes
    ): ScalaExpr[MappedTable[?, ?, ?, ?, ?]] =
        import q.reflect.*

        val columnsTerm = columns.asTerm
        val columnNames = 
            columnsTerm match
                case Inlined(_, _, Typed(Repeated(cs, _), _)) =>
                    cs.map:
                        case Literal(StringConstant(n)) =>
                            n
        val tpe = TypeRepr.of[T]
        val sym = tpe.typeSymbol
        val fields = sym.caseFields
        val excludedFields = fields
            .filterNot(f => columnNames.contains(f.name))
            .map(f => (f.name, tpe.memberType(f)))
            
        if excludedFields.isEmpty then
            report.error("Cannot exclude all fields. At least one field must be retained.")

        def buildTupleType(types: List[TypeRepr]): TypeRepr =
            types match
                case Nil => TypeRepr.of[EmptyTuple]
                case head :: tail =>
                    val consSym = Symbol.requiredClass("scala.*:")
                    AppliedType(consSym.typeRef, List(head, buildTupleType(tail)))
        
        val names = excludedFields.map(f => ConstantType(StringConstant(f._1)))
        val nameType = buildTupleType(names)
        val types = excludedFields.map(_._2).map: v =>
            v.asType match
                case '[t] =>
                    TypeRepr.of[MapField[t, OT, L]]
        val valueType = buildTupleType(types)
        val resultTypes = excludedFields.map(_._2).map: v =>
            v.asType match
                case '[t] =>
                    TypeRepr.of[ColumnExpr[t, L]]
        val resultType = buildTupleType(resultTypes)
        val fieldNamesExpr =
            ScalaExpr.ofList(excludedFields.map(_._1).map(n => ScalaExpr(n)))

        (nameType.asType, valueType.asType, resultType.asType) match
            case ('[type n <: Tuple; n], '[type v <: Tuple; v], '[type r <: Tuple; r]) =>
                '{
                    val columns = 
                        $metaData.fields.toList
                            .zip($metaData.columns.toList)
                            .filter((f, _) => $fieldNamesExpr.contains(f))
                            .map((_, c) => ColumnExpr(IrExpr.Column($table, c)))
                            .toArray
                    val items = Tuple.fromArray(columns).asInstanceOf[v]
                    MappedTable[n, v, r, O, L]($table, items, $fieldNamesExpr)
                }
            case _ =>
                report.errorAndAbort("Cannot exclude all fields. At least one field must be retained.")

/**
  * A table reference produced by `from` when a `MappedTable` is passed,
  * mapping entity class fields to typed expressions via `Fields`.
  */
final case class MappedTable[N <: Tuple, V <: Tuple, RV <: Tuple, O <: Boolean, L <: Int](
    private[sqala] val __table__ : IrTable,
    private[sqala] val __items__ : V,
    private[sqala] val __fields__ : List[String]
) extends Selectable with AnyTable:
    /**
     * The structural type declaring available columns as a named tuple.
     * Required by `Selectable`.
     */
    type Fields = NamedTuple[N, V]

    /**
     * Runtime column accessor. Required by `Selectable`.
     */
    def selectDynamic(name: String): Any =
        val index = __fields__.indexOf(name)
        __items__.toList(index)

object MappedTable:
    inline def apply[N <: Tuple, V <: Tuple, RV <: Tuple, O <: Boolean, L <: Int](table: IrTable, items: V): MappedTable[N, V, RV, O, L] =
        val fields = constValueTuple[N].toArray.toList.map(_.toString)
        new MappedTable(
            table,
            items,
            fields
        )

    inline def apply[N <: Tuple, V <: Tuple, RV <: Tuple, O <: Boolean, L <: Int](table: IrTable)(using
        p: AsTableParam[V, L],
        t: ToTuple[p.R]
    ): MappedTable[N, t.R, RV, O, L] =
        MappedTable(table, t.toTuple(p.asTableParam(table, 1)))