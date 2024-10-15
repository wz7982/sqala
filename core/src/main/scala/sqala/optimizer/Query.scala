package sqala.optimizer

case class Query(
    hasAgg: Boolean,
    hasWindow: Boolean,
    hasSubLink: Boolean,
    distinct: Boolean,
    tableList: List[TableEntry],
    joinTree: FromExpr,
    filter: Option[Expr],
    target: List[TargetEntry],
    group: List[Expr],
    having: Option[Expr],
    sort: List[Sort],
    limitAndOffset: Option[(Int, Int)]
)

case class Var(
    tableIndex: Int,
    tableName: Option[String],
    varIndex: Int,
    varName: String,
    varLevelSup: Int
)

enum BinaryOperator:
    case Times
    case Div
    case Mod
    case Plus
    case Minus
    case Equal
    case NotEqual
    case In
    case NotIn
    case GreaterThan
    case GreaterThanEqual
    case LessThan
    case LessThanEqual
    case Like
    case NotLike
    case And
    case Or

enum UnaryOperator:
    case Positive
    case Negative
    case Not

enum SubLinkType:
    case Any
    case All
    case Exists
    case NotExists

enum Expr:
    case Null
    case StringConstant(string: String)
    case NumberConstant(number: Number)
    case BooleanConstant(boolean: Boolean)
    case VarRef(v: Var)
    case Vector(items: List[Expr])
    case Unary(expr: Expr, op: UnaryOperator)
    case Binary(left: Expr, op: BinaryOperator, right: Expr)
    case NullTest(expr: Expr, not: Boolean)
    case Func(name: String, args: List[Expr])
    case Agg(name: String, args: List[Expr], distinct: Boolean, sort: List[Sort])
    case Between(expr: Expr, start: Expr, end: Expr, not: Boolean)
    case Case(branches: List[(Expr, Expr)], default: Expr)
    case Window(expr: Expr, partition: List[Expr], sort: List[Sort])
    case SubQuery(query: Query)
    case SubLink(query: Query, linkType: SubLinkType)

case class TargetEntry(expr: Expr, alias: String)

case class Sort(expr: Expr, asc: Boolean)

enum JoinType:
    case Inner
    case Left
    case Right
    case Semi
    case Anti

case class TableAlias(alias: String, columnAlias: List[String])

enum TableEntry:
    case Relation(name: String, alias: Option[TableAlias])
    case SubQuery(query: Query, lateral: Boolean, alias: TableAlias)

enum FromExpr:
    case TableRef(tableIndex: Int)
    case JoinExpr(left: FromExpr, joinType: JoinType, right: FromExpr, condition: Expr)