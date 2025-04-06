package sqala.data.json

import sqala.data.util.{fetchNames, fetchTypes}

import java.time.{LocalDate, LocalDateTime, LocalTime}
import java.time.format.DateTimeFormatter
import scala.NamedTuple.NamedTuple
import scala.compiletime.*
import scala.deriving.Mirror
import scala.quoted.*

trait JsonEncoder[T]:
    def encode(x: T)(using JsonDateFormat): String

object JsonEncoder:
    inline def summonInstances[T <: Tuple]: List[JsonEncoder[?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => summonInline[JsonEncoder[t]] :: summonInstances[ts]

    given intEncoder: JsonEncoder[Int] with
        override inline def encode(x: Int)(using JsonDateFormat): String =
            x.toString

    given longEncoder: JsonEncoder[Long] with
        override inline def encode(x: Long)(using JsonDateFormat): String =
            x.toString

    given floatEncoder: JsonEncoder[Float] with
        override inline def encode(x: Float)(using JsonDateFormat): String =
            x.toString

    given doubleEncoder: JsonEncoder[Double] with
        override inline def encode(x: Double)(using JsonDateFormat): String =
            x.toString

    given decimalEncoder: JsonEncoder[BigDecimal] with
        override inline def encode(x: BigDecimal)(using JsonDateFormat): String =
            x.toString

    given stringEncoder: JsonEncoder[String] with
        override inline def encode(x: String)(using JsonDateFormat): String =
            val builder = new StringBuilder()
            builder.append("\"")
            for c <- x.toCharArray do
                if c == '"' then
                    builder.append("\\")
                builder.append(c)
            builder.append("\"")
            builder.toString

    given booleanEncoder: JsonEncoder[Boolean] with
        override inline def encode(x: Boolean)(using JsonDateFormat): String =
            if x then "true" else "false"

    given localDateEncoder: JsonEncoder[LocalDate] with
        override inline def encode(x: LocalDate)(using dateFormat: JsonDateFormat): String =
            val formatter = DateTimeFormatter.ofPattern(dateFormat.format)
            LocalDateTime.of(x, LocalTime.MIN).format(formatter)

    given localDateTimeEncoder: JsonEncoder[LocalDateTime] with
        override inline def encode(x: LocalDateTime)(using dateFormat: JsonDateFormat): String =
            val formatter = DateTimeFormatter.ofPattern(dateFormat.format)
            x.format(formatter)

    given listEncoder[T](using e: JsonEncoder[T]): JsonEncoder[List[T]] with
        override inline def encode(x: List[T])(using JsonDateFormat): String =
            x.map(i => e.encode(i)).mkString("[", ", ", "]")

    given optionEncoder[T](using e: JsonEncoder[T]): JsonEncoder[Option[T]] with
        override inline def encode(x: Option[T])(using JsonDateFormat): String = x match
            case None => "null"
            case Some(value) => e.encode(value)

    inline given namedTupleEncoder[N <: Tuple, V <: Tuple]: JsonEncoder[NamedTuple.NamedTuple[N, V]] =
        val names = fetchNames[N]
        val instances = summonInstances[V]
        newEncoderNamedTuple(names, instances)

    private def newEncoderNamedTuple[N <: Tuple, V <: Tuple](
        names: List[String],
        instances: List[JsonEncoder[?]]
    ): JsonEncoder[NamedTuple.NamedTuple[N, V]] =
        new JsonEncoder[NamedTuple.NamedTuple[N, V]]:
            override def encode(x: NamedTuple.NamedTuple[N, V])(using JsonDateFormat): String =
                val data = x.toTuple.productIterator.toList
                val nodes = data.zip(instances).map: (v, i) =>
                    i.asInstanceOf[JsonEncoder[Any]].encode(v)
                names.zip(nodes).map((k, v) => s"\"$k\": $v").mkString("{", ", ", "}")

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
                        override def encode(x: T)(using JsonDateFormat): String =
                            $nameExpr
                }
            case '{ $m: Mirror.ProductOf[T] { type MirroredElemTypes = elementTypes } } =>
                val symbol = TypeRepr.of[T].typeSymbol
                val fields = symbol.declaredFields
                val fieldTypes = fetchTypes[elementTypes]
                def fetchExprs(x: Expr[T], format: Expr[JsonDateFormat]): Expr[List[(String, String)]] =
                    val fieldList = scala.collection.mutable.ListBuffer[Expr[(String, String)]]()
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
                                        case None => report.errorAndAbort(s"Cannot create a json encoder for field (${f.name}: $showType). It's possible to fix this by adding: given JsonEncoder[$showType].")
                                        case Some(s) => s
                                    val expr = Select.unique(x.asTerm, f.name).asExprOf[t]
                                    '{ $n -> $summonExpr.encode($expr)(using $format) }
                            fieldList.addOne(addExpr)
                    Expr.ofList(fieldList.toList)
                '{
                    new JsonEncoder[T]:
                        override def encode(x: T)(using f: JsonDateFormat): String =
                            val fields = ${
                                fetchExprs('x, 'f)
                            }
                            fields.map((k, v) => s"\"$k\": $v").mkString("{", ", ", "}")
                }
            case '{ $m: Mirror.SumOf[T] { type MirroredElemTypes = elementTypes; type MirroredElemLabels = elementLabels } } =>
                '{
                    val s = $m
                    val names = fetchNames[s.MirroredElemLabels]
                    val instances = summonInstances[s.MirroredElemTypes]

                    new JsonEncoder[T]:
                        override def encode(x: T)(using JsonDateFormat): String =
                            val ord = s.ordinal(x)
                            x match
                                case p: Product if p.productArity == 0 => s"\"${names(ord)}\""
                                case _ =>
                                    val instance = instances(ord)
                                    val node = instance.asInstanceOf[JsonEncoder[Any]].encode(x)
                                    s"{\"${names(ord)}\": $node}"
                }