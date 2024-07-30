package sqala.json

import sqala.util.*

import scala.compiletime.{constValue, erasedValue, summonInline}
import scala.deriving.Mirror
import scala.quoted.*
import java.time.{LocalDate, LocalDateTime}
import java.time.format.DateTimeFormatter
import java.text.SimpleDateFormat
import java.util.Date

trait JsonEncoder[T]:
    def encode(x: T)(using JsonDateFormat): JsonNode

object JsonEncoder:
    inline def summonInstances[T <: Tuple]: List[JsonEncoder[?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => summonInline[JsonEncoder[t]] :: summonInstances[ts]

    given intEncoder: JsonEncoder[Int] with
        override inline def encode(x: Int)(using JsonDateFormat): JsonNode = JsonNode.Num(x)

    given longEncoder: JsonEncoder[Long] with
        override inline def encode(x: Long)(using JsonDateFormat): JsonNode = JsonNode.Num(x)

    given floatEncoder: JsonEncoder[Float] with
        override inline def encode(x: Float)(using JsonDateFormat): JsonNode = JsonNode.Num(x)

    given doubleEncoder: JsonEncoder[Double] with
        override inline def encode(x: Double)(using JsonDateFormat): JsonNode = JsonNode.Num(x)

    given decimalEncoder: JsonEncoder[BigDecimal] with
        override inline def encode(x: BigDecimal)(using JsonDateFormat): JsonNode = JsonNode.Num(x)

    given stringEncoder: JsonEncoder[String] with
        override inline def encode(x: String)(using JsonDateFormat): JsonNode = JsonNode.Str(x)

    given booleanEncoder: JsonEncoder[Boolean] with
        override inline def encode(x: Boolean)(using JsonDateFormat): JsonNode = JsonNode.Bool(x)

    given dateEncoder: JsonEncoder[Date] with
        override inline def encode(x: Date)(using dataFormat: JsonDateFormat): JsonNode =
            val formatter = new SimpleDateFormat(dataFormat.format)
            JsonNode.Str(formatter.format(x))

    given localDateEncoder: JsonEncoder[LocalDate] with
        override inline def encode(x: LocalDate)(using dateFormat: JsonDateFormat): JsonNode =
            val formatter = DateTimeFormatter.ofPattern(dateFormat.format)
            JsonNode.Str(x.format(formatter))

    given localDateTimeEncoder: JsonEncoder[LocalDateTime] with
        override inline def encode(x: LocalDateTime)(using dateFormat: JsonDateFormat): JsonNode =
            val formatter = DateTimeFormatter.ofPattern(dateFormat.format)
            JsonNode.Str(x.format(formatter))

    given optionEncoder[T](using e: JsonEncoder[T]): JsonEncoder[Option[T]] with
        override inline def encode(x: Option[T])(using JsonDateFormat): JsonNode = x match
            case None => JsonNode.Null
            case Some(value) => e.encode(value)

    given listEncoder[T](using e: JsonEncoder[T]): JsonEncoder[List[T]] with
        override inline def encode(x: List[T])(using JsonDateFormat): JsonNode = 
            JsonNode.Array(x.map(e.encode))

    inline given namedTupleEncoder[N <: Tuple, V <: Tuple]: JsonEncoder[NamedTuple.NamedTuple[N, V]] =
        val names = fetchNames[N]
        val instances = summonInstances[V]
        newEncoderNamedTuple(names, instances)

    private def newEncoderNamedTuple[N <: Tuple, V <: Tuple](names: List[String], instances: List[JsonEncoder[?]]): JsonEncoder[NamedTuple.NamedTuple[N, V]] =
        new JsonEncoder[NamedTuple.NamedTuple[N, V]]:
            override def encode(x: NamedTuple.NamedTuple[N, V])(using JsonDateFormat): JsonNode =
                val data = x.toTuple.productIterator.toList
                val nodes = data.zip(instances).map: (v, i) =>
                    i.asInstanceOf[JsonEncoder[Any]].encode(v)
                JsonNode.Object(names.zip(nodes).toMap)
    
    inline given derived[T](using m: Mirror.Of[T]): JsonEncoder[T] =
        ${ jsonEncoderMacro[T] }

    private def jsonEncoderMacro[T: Type](using q: Quotes): Expr[JsonEncoder[T]] =
        import q.reflect.*

        val mirror: Expr[Mirror.Of[T]] = Expr.summon[Mirror.Of[T]].get

        mirror match
            case '{ $m: Mirror.ProductOf[T] { type MirroredElemTypes = EmptyTuple; type MirroredLabel = label } } =>
                val nameExpr = Expr(TypeRepr.of[label].show.replaceAll("\"", ""))
                '{
                    new JsonEncoder[T]:
                        override def encode(x: T)(using JsonDateFormat): JsonNode =
                            JsonNode.Str($nameExpr)
                }
            case '{ $m: Mirror.ProductOf[T] { type MirroredElemTypes = elementTypes } } =>
                val symbol = TypeRepr.of[T].typeSymbol
                val fields = symbol.declaredFields
                val fieldTypes = fetchTypes[elementTypes]
                def fetchExprs(x: Expr[T], format: Expr[JsonDateFormat]): Expr[List[(String, JsonNode)]] =
                    val fieldList = scala.collection.mutable.ListBuffer[Expr[(String, JsonNode)]]()
                    for (f, typ) <- fields.zip(fieldTypes) do
                        val annotations = f.annotations
                        if !annotations.exists:
                            case Apply(Select(New(TypeIdent("jsonIgnore")), _), _)  => true
                            case _ => false
                        then
                            val alias = annotations.find:
                                case Apply(Select(New(TypeIdent("jsonAlias")), _), _)  => true
                                case _ => false
                            match
                                case Some(Apply(Select(New(TypeIdent(_)), _), Literal(v) :: Nil)) =>
                                    Option(v.value.toString)
                                case _ => None
                            val n = Expr(alias.getOrElse(f.name))
                            val addExpr = typ match
                                case '[t] =>
                                    val showType = TypeRepr.of[t].show
                                    val summonExpr = Expr.summon[JsonEncoder[t]] match
                                        case None => report.errorAndAbort(s"Couble not create a json encoder for field (${f.name}: $showType). It's possible to fix this by adding: given JsonEncoder[$showType].")
                                        case Some(s) => s
                                    val expr = Select.unique(x.asTerm, f.name).asExprOf[t]
                                    '{ $n -> $summonExpr.encode($expr)(using $format) }
                            fieldList.addOne(addExpr)
                    Expr.ofList(fieldList.toList)
                '{
                    new JsonEncoder[T]:
                        override def encode(x: T)(using f: JsonDateFormat): JsonNode =
                            val fields = ${
                                fetchExprs('x, 'f)
                            }
                            JsonNode.Object(fields.toMap)
                }
            case '{ $m: Mirror.SumOf[T] { type MirroredElemTypes = elementTypes; type MirroredElemLabels = elementLabels } } =>
                '{
                    val s = $m
                    val names = fetchNames[s.MirroredElemLabels]
                    val instances = summonInstances[s.MirroredElemTypes]

                    new JsonEncoder[T]:
                        override def encode(x: T)(using JsonDateFormat): JsonNode =
                            val ord = s.ordinal(x)
                            x match
                                case p: Product if p.productArity == 0 => JsonNode.Str(names(ord))
                                case _ =>
                                    val instance = instances(ord)
                                    val node = instance.asInstanceOf[JsonEncoder[Any]].encode(x)
                                    JsonNode.Object(Map(names(ord) -> node))
                }