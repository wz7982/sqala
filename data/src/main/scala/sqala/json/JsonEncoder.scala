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
    def encode(x: T)(using JsonDateFormat): String

object JsonEncoder:
    inline def summonInstances[T <: Tuple]: List[JsonEncoder[?]] =
        inline erasedValue[T] match
            case _: EmptyTuple => Nil
            case _: (t *: ts) => summonInline[JsonEncoder[t]] :: summonInstances[ts]

    given intEncoder: JsonEncoder[Int] with
        override inline def encode(x: Int)(using JsonDateFormat): String = x.toString

    given longEncoder: JsonEncoder[Long] with
        override inline def encode(x: Long)(using JsonDateFormat): String = x.toString

    given floatEncoder: JsonEncoder[Float] with
        override inline def encode(x: Float)(using JsonDateFormat): String = x.toString

    given doubleEncoder: JsonEncoder[Double] with
        override inline def encode(x: Double)(using JsonDateFormat): String = x.toString

    given decimalEncoder: JsonEncoder[BigDecimal] with
        override inline def encode(x: BigDecimal)(using JsonDateFormat): String = x.toString

    given stringEncoder: JsonEncoder[String] with
        override inline def encode(x: String)(using JsonDateFormat): String = "\"" + x + "\""

    given booleanEncoder: JsonEncoder[Boolean] with
        override inline def encode(x: Boolean)(using JsonDateFormat): String = x.toString

    given dateEncoder: JsonEncoder[Date] with
        override inline def encode(x: Date)(using dataFormat: JsonDateFormat): String =
            val formatter = new SimpleDateFormat(dataFormat.format)
            formatter.format(x)

    given localDateEncoder: JsonEncoder[LocalDate] with
        override inline def encode(x: LocalDate)(using dateFormat: JsonDateFormat): String =
            val formatter = DateTimeFormatter.ofPattern(dateFormat.format)
            x.format(formatter)

    given localDateTimeEncoder: JsonEncoder[LocalDateTime] with
        override inline def encode(x: LocalDateTime)(using dateFormat: JsonDateFormat): String =
            val formatter = DateTimeFormatter.ofPattern(dateFormat.format)
            x.format(formatter)

    given optionEncoder[T](using e: JsonEncoder[T]): JsonEncoder[Option[T]] with
        override inline def encode(x: Option[T])(using JsonDateFormat): String = x match
            case None => "null"
            case Some(value) => e.encode(value)

    given listEncoder[T](using e: JsonEncoder[T]): JsonEncoder[List[T]] with
        override inline def encode(x: List[T])(using JsonDateFormat): String = 
            x.map(e.encode).mkString("[", ", ", "]")

    // private def newEncoderNamedTuple[N <: Tuple, V <: Tuple](names: List[String], instances: List[JsonEncoder[?]]): JsonEncoder[NamedTuple.NamedTuple[N, V]] =
    //     new JsonEncoder[NamedTuple.NamedTuple[N, V]]:
    //         override def encode(x: NamedTuple.NamedTuple[N, V])(using JsonDateFormat): JsonNode =
    //             val data = x.toTuple.productIterator.toList
    //             val nodes = data.zip(instances).map: (v, i) =>
    //                 i.asInstanceOf[JsonEncoder[Any]].encode(v)
    //             JsonNode.Object(names.zip(nodes).toMap)

    // inline given namedTupleEncoder[N <: Tuple, V <: Tuple]: JsonEncoder[NamedTuple.NamedTuple[N, V]] =
    //     val names = fetchNames[N]
    //     val instances = summonInstances[V]
    //     newEncoderNamedTuple(names, instances)
    
    // private def newEncoderProduct[T](names: List[String], metaData: JsonMetaData, instances: List[JsonEncoder[?]]): JsonEncoder[T] =
    //     val aliasNameMap = metaData.fieldNames
    //         .zip(metaData.aliasNames)
    //         .toMap
    //         .map((k, v) => k -> v.headOption.getOrElse(k))

    //     val ignoreMap = metaData.fieldNames
    //         .zip(metaData.ignore)
    //         .toMap

    //     new JsonEncoder[T]:
    //         override def encode(x: T)(using JsonDateFormat): JsonNode =
    //             val data = names
    //                 .zip(instances)
    //                 .zip(x.asInstanceOf[Product].productIterator)
    //                 .map(i => (i._1._1, i._1._2, i._2))
    //                 .filter((n, _, _) => !ignoreMap(n))
    //                 .map: (n, i, v) =>
    //                     val alias = aliasNameMap(n)
    //                     alias -> i.asInstanceOf[JsonEncoder[Any]].encode(v)
    //             JsonNode.Object(data.toMap)

    // private def newEncoderSum[T](s: Mirror.SumOf[T], names: List[String], instances: List[JsonEncoder[?]]): JsonEncoder[T] =
    //     new JsonEncoder[T]:
    //         override def encode(x: T)(using JsonDateFormat): JsonNode =
    //             val ord = s.ordinal(x)
    //             x match
    //                 case p: Product if p.productArity == 0 => JsonNode.Str(names(ord))
    //                 case _ => 
    //                     val instance = instances(ord)
    //                     val node = instance.asInstanceOf[JsonEncoder[Any]].encode(x)
    //                     JsonNode.Object(Map(names(ord) -> node))
    
    inline given derived[T](using m: Mirror.Of[T]): JsonEncoder[T] =
        ${ jsonEncoderMacro[T] }
        // val names = fetchNames[m.MirroredElemLabels]
        // val instances = summonInstances[m.MirroredElemTypes]

        // inline m match
        //     case _: Mirror.ProductOf[T] =>
        //         val metaData = jsonMetaDataMacro[T]
        //         newEncoderProduct[T](names, metaData, instances)
        //     case s: Mirror.SumOf[T] =>
        //         newEncoderSum[T](s, names, instances)

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
                val names = symbol.declaredFields.map: f =>
                    Expr(f.name)
                val fieldTypes = fetchTypes[elementTypes]
                def fetchExprs(x: Expr[T], format: Expr[JsonDateFormat]): Expr[List[(String, String)]] =
                    val fields = names.zip(fieldTypes).map: (n, typ) =>
                        typ match
                            case '[t] =>
                                val summonExpr = Expr.summon[JsonEncoder[t]].get
                                val expr = Select.unique(x.asTerm, n.value.get).asExprOf[t]
                                '{ $n -> $summonExpr.encode($expr)(using $format) }
                    Expr.ofList(fields)
                '{
                    new JsonEncoder[T]:
                        override def encode(x: T)(using f: JsonDateFormat): String =
                            val fields = ${
                                fetchExprs('x, 'f)
                            }
                            fields.map((n, v) => s""""$n": $v""").mkString("{", ", ", "}")
                }
                        
                // val exprs = names.zip(fieldTypes).map: (n, typ) =>
                //     typ match
                //         case '[t] =>
                //             val summonExpr = Expr.summon[JsonEncoder[t]].get
                //             val expr = '{ $summonExpr.encode(???) }
            // case '{ $m: Mirror.ProductOf[T] { type MirroredElemTypes = elementTypes } } =>
            //     val elemTypes = fetchTypes[elementTypes]
            //     val tpr = TypeRepr.of[T]
            //     val types = tpr match
            //         case AppliedType(_, ts) => ts
            //         case _ => Nil
            //     val symbol = tpr.typeSymbol
            //     val ctor = symbol.primaryConstructor
            //     val exprs = elemTypes.map:
            //         case '[t] =>
            //             val summonExpr = Expr.summon[DefaultValue[t]].get
            //             '{ $summonExpr.defaultValue }
            //     '{
            //         new DefaultValue[T]:
            //             override def defaultValue: T = 
            //                 ${
            //                     if types.isEmpty then
            //                         New(Inferred(tpr)).select(ctor).appliedToArgs(exprs.map(_.asTerm)).asExprOf[T]
            //                     else
            //                         New(Inferred(tpr)).select(ctor).appliedToTypes(types).appliedToArgs(exprs.map(_.asTerm)).asExprOf[T]
            //                 }
            //     }
            // case '{ $m: Mirror.SumOf[T] { type MirroredElemTypes = elementTypes } } =>
            //     val typ = fetchTypes[elementTypes].head
            //     val expr = typ match
            //         case '[t] => 
            //             val summonExpr = Expr.summon[DefaultValue[t]].get
            //             '{ $summonExpr.defaultValue }.asExprOf[T]
            //     '{
            //         new DefaultValue[T]:
            //             override def defaultValue: T = $expr
            //     }