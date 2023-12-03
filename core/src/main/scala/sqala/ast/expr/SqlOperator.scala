package sqala.ast.expr

enum SqlBinaryOperator(val operator: String):
    case Equal extends SqlBinaryOperator("=")
    case NotEqual extends SqlBinaryOperator("<>")
    case GreaterThan extends SqlBinaryOperator(">")
    case GreaterThanEqual extends SqlBinaryOperator(">=")
    case LessThan extends SqlBinaryOperator("<")
    case LessThanEqual extends SqlBinaryOperator("<=")
    case Is extends SqlBinaryOperator("IS")
    case IsNot extends SqlBinaryOperator("IS NOT")
    case Like extends SqlBinaryOperator("LIKE")
    case NotLike extends SqlBinaryOperator("NOT LIKE")
    case And extends SqlBinaryOperator("AND")
    case Xor extends SqlBinaryOperator("XOR")
    case Or extends SqlBinaryOperator("OR")
    case Plus extends SqlBinaryOperator("+")
    case Minus extends SqlBinaryOperator("-")
    case Times extends SqlBinaryOperator("*")
    case Div extends SqlBinaryOperator("/")
    case Mod extends SqlBinaryOperator("%")
    case Json extends SqlBinaryOperator("->")
    case JsonText extends SqlBinaryOperator("->>")

enum SqlUnaryOperator(val operator: String):
    case Positive extends SqlUnaryOperator("+")
    case Negative extends SqlUnaryOperator("-")
    case Not extends SqlUnaryOperator("!")