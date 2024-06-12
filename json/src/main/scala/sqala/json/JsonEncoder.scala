package sqala.json

trait JsonEncoder[T]:
    def encode(x: T): JsonNode

object JsonEncoder:
    given intEncoder: JsonEncoder[Int] with
        override def encode(x: Int): JsonNode = JsonNode.NumberLiteral(x)

    given longEncoder: JsonEncoder[Long] with
        override def encode(x: Long): JsonNode = JsonNode.NumberLiteral(x)

    given floatEncoder: JsonEncoder[Float] with
        override def encode(x: Float): JsonNode = JsonNode.NumberLiteral(x)

    given doubleEncoder: JsonEncoder[Double] with
        override def encode(x: Double): JsonNode = JsonNode.NumberLiteral(x)

    given decimalEncoder: JsonEncoder[BigDecimal] with
        override def encode(x: BigDecimal): JsonNode = JsonNode.NumberLiteral(x)

    given stringEncoder: JsonEncoder[String] with
        override def encode(x: String): JsonNode = JsonNode.StringLiteral(x)

    given booleanEncoder: JsonEncoder[Boolean] with
        override def encode(x: Boolean): JsonNode = JsonNode.BooleanLiteral(x)

    given optionEncoder[T](using e: JsonEncoder[T]): JsonEncoder[Option[T]] with
        override def encode(x: Option[T]): JsonNode = x match
            case None => JsonNode.NullLiteral
            case Some(value) => e.encode(value)

    given listEncoder[T](using e: JsonEncoder[T]): JsonEncoder[List[T]] with
        override def encode(x: List[T]): JsonNode = JsonNode.Vector(x.map(e.encode))

    // TODO 日期类型 summon 和类型