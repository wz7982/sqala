package sqala.ast.expr

enum SqlBinaryOperator(val precedence: Int):
    case Times extends SqlBinaryOperator(60)
    case Div extends SqlBinaryOperator(60)
    case Plus extends SqlBinaryOperator(50)
    case Minus extends SqlBinaryOperator(50)
    case Concat extends SqlBinaryOperator(40)
    case Equal extends SqlBinaryOperator(30)
    case NotEqual extends SqlBinaryOperator(30)
    case IsDistinctFrom extends SqlBinaryOperator(30)
    case IsNotDistinctFrom extends SqlBinaryOperator(30)
    case In extends SqlBinaryOperator(30)
    case NotIn extends SqlBinaryOperator(30)
    case GreaterThan extends SqlBinaryOperator(30)
    case GreaterThanEqual extends SqlBinaryOperator(30)
    case LessThan extends SqlBinaryOperator(30)
    case LessThanEqual extends SqlBinaryOperator(30)
    case Overlaps extends SqlBinaryOperator(30)
    case And extends SqlBinaryOperator(20)
    case Or extends SqlBinaryOperator(10)
    case Custom(operator: String) extends SqlBinaryOperator(0)

enum SqlUnaryOperator:
    case Positive
    case Negative
    case Not
    case Custom(operator: String)