package sqala.optimizer

enum DataType:
    case Int
    case Long
    case Float
    case Double
    case Decimal
    case String
    case Boolean
    case Date

enum FuncKind:
    case Normal
    case Agg
    case Window

case class ColumnMetaData(name: String, dataType: DataType, nullable: Boolean)

case class TableMetaData(name: String, columns: List[ColumnMetaData])

case class FuncMetaData(
    name: String,
    checkArgs: List[DataType] => Boolean,
    returnType: List[DataType] => DataType,
    strict: Boolean,
    immutable: Boolean,
    kind: FuncKind,
    nullable: Boolean
)