package sqala.ast.expr

enum SqlType(val `type`: String):
    case Varchar(maxLength: Option[Int]) extends SqlType(s"VARCHAR${maxLength.map(l => s"($l)").getOrElse("")}")
    case Int extends SqlType("INTEGER")
    case Long extends SqlType("BIGINT")
    case Float extends SqlType("REAL")
    case Double extends SqlType("DOUBLE PRECISION")
    case Decimal(precision: Option[(Int, Int)]) extends SqlType(s"DECIMAL${precision.map((p, s) => s"($p, $s)").getOrElse("")}")
    case Date extends SqlType("DATE")
    case Timestamp extends SqlType("TIMESTAMP")
    case Time extends SqlType("TIME")
    case Json extends SqlType("JSON")
    case Boolean extends SqlType("BOOLEAN")
    case Custom(override val `type`: String) extends SqlType(`type`)