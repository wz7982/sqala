package sqala.static.metadata

import scala.quoted.{Expr, Quotes, Type}

trait ViewMapping[T, R]:
    def mapToList(data: List[T]): List[R]

    def mapToOption(data: List[T]): Option[R]

    def mapToObject(data: List[T]): R

object ViewMapping:
    inline given derived[T, R]: ViewMapping[T,R] =
        ${ derivedImpl[T, R] }

    def derivedImpl[T: Type, R: Type](using q: Quotes): Expr[ViewMapping[T, R]] =
        import q.reflect.*

        val tp = TypeRepr.of[T]
        val isTuple = tp.asType match
            case '[scala.NamedTuple.AnyNamedTuple] => true
            case _ => false

        def collectNames(t: Type[?]): List[String] =
            t match
                case '[x *: xs] =>
                    val name = TypeRepr.of[x] match
                        case c: ConstantType => c.constant.value.toString
                    name :: collectNames(Type.of[xs])
                case '[EmptyTuple] => Nil


        val sourceFieldNames =
            if isTuple then
                tp.asType match
                    case '[t] =>
                        collectNames(tp.typeArgs(0).asType)
            else Nil
        val tpr = TypeRepr.of[R]
        val sym = tpr.typeSymbol
        val annotations = sym.annotations
        val (prefix, key) = annotations.collectFirst:
            case Apply(
                Select(New(TypeIdent("view")), _), 
                NamedArg("prefix", Literal(StringConstant(p))) ::
                NamedArg("key", Literal(StringConstant(k))) :: Nil
            ) => 
                (p, k)
        .get
        val keyWithPrefix = prefix + key.capitalize
        val dataExpr =
            (data: Expr[List[T]]) =>
                val grouping = (datum: Expr[T]) =>
                    if isTuple then
                        val index = sourceFieldNames.indexOf(keyWithPrefix)
                        val indexExpr = Expr(index)
                        tp.typeArgs(1).asType match
                            case '[type t <: Tuple; t] =>
                                '{
                                    $datum.asInstanceOf[t].apply($indexExpr)
                                }
                    else
                        Select.unique(datum.asTerm, keyWithPrefix).asExpr
                '{
                    $data.groupBy(datum => 
                        ${
                            grouping('datum)
                        }
                    ).values.toList
                }
        val ctor = sym.primaryConstructor
        val eles = sym.declaredFields

        val elesInfo: collection.mutable.LinkedHashMap[String, (Expr[List[T]] => (q.reflect.Term, q.reflect.Term), Boolean)] =
            collection.mutable.LinkedHashMap()
        
        eles.foreach: e =>
            val eleName = e.name
            val eleAnnotations = e.annotations

            val nested = eleAnnotations.exists:
                case Apply(
                    Select(New(TypeIdent("nested")), _), 
                    _
                ) => true
                case _ => false

            if nested then
                val nestedType = tpr.memberType(e).asType
                val tpe = nestedType match
                    case '[List[t]] => Type.of[t]
                    case '[Option[t]] => Type.of[t]
                    case _ => nestedType
                val summonExprOption =
                    tpe match
                        case '[v] =>
                            Expr.summon[ViewMapping[T, v]]
                val summonExpr = summonExprOption.get
                nestedType match
                    case '[List[t]] =>
                        elesInfo.addOne(
                            eleName,
                            (
                                (data: Expr[List[T]]) =>
                                    (
                                        '{
                                            $summonExpr.mapToList($data)
                                        }.asTerm,
                                        '{ None }.asTerm
                                    )
                                ,
                                true
                            )
                        )
                    case '[Option[t]] =>
                        elesInfo.addOne(
                            eleName,
                            (
                                (data: Expr[List[T]]) =>
                                    (
                                        '{
                                            $summonExpr.mapToOption($data)
                                        }.asTerm,
                                        '{ None }.asTerm
                                    )
                                ,
                                true
                            )
                        )
                    case _ =>
                        elesInfo.addOne(
                            eleName,
                            (
                                (data: Expr[List[T]]) =>
                                    (
                                        '{
                                            $summonExpr.mapToObject($data)
                                        }.asTerm,
                                        '{ None }.asTerm
                                    )
                                ,
                                true
                            )
                        )
            else
                val fieldType = tpr.memberType(e).asType

                val derivedInfo = eleAnnotations.collectFirst:
                    case
                        Apply(
                            _,
                            NamedArg("source", Literal(StringConstant(s))) ::
                            NamedArg("mapper", term) :: Nil
                        )
                    => 
                        (s, term)

                derivedInfo match
                    case None => 
                        val eleNameWithPrefix =
                            prefix + eleName.capitalize
                        elesInfo.addOne(
                            eleName,
                            (
                                (data: Expr[List[T]]) =>
                                    val select = 
                                        if isTuple then
                                            val index = sourceFieldNames.indexOf(eleNameWithPrefix)
                                            val indexExpr = Expr(index)
                                            tp.typeArgs(1).asType match
                                                case '[type t <: Tuple; t] =>
                                                    '{
                                                        $data.head.asInstanceOf[t].apply($indexExpr)
                                                    }.asTerm
                                        else
                                            Select.unique('{$data.head}.asTerm, eleNameWithPrefix)
                                    (
                                        fieldType match
                                            case '[Option[t]] =>
                                                select
                                            case '[t] =>
                                                select.tpe.asType match
                                                    case '[Option[t]] =>
                                                        '{ ${select.asExprOf[Option[t]]}.get }.asTerm
                                                    case '[t] =>
                                                        select
                                        ,
                                        select
                                    )
                                ,
                                false
                            )
                        )
                    case Some(source, derivedMapper) =>
                        val sourceInfo = elesInfo(source)
                        elesInfo.addOne(
                            eleName,
                            (
                                (data: Expr[List[T]]) =>
                                    val fieldInfo = sourceInfo._1(data)
                                    (
                                        fieldType match
                                            case '[r] =>
                                                val sourceExpr = fieldInfo._1.asExpr
                                                '{ ${derivedMapper.asExpr}.asInstanceOf[Function[Any, r]].apply($sourceExpr) }.asTerm
                                        ,
                                        fieldInfo._2
                                    )
                                ,
                                false
                            )
                        )

        val mapper = elesInfo.values.toList
        
        '{
            new ViewMapping[T, R]:
                def mapToList(data: List[T]): List[R] =
                    ${
                        dataExpr('data)
                    }.filter: row =>
                        val currentFields = ${
                            Expr.ofList(mapper.filter(!_._2).map(m => m._1('row)._2.asExpr))
                        }
                        !currentFields.forall(_ == None)
                    .map: row =>
                        ${
                            val exprs = mapper.map(m => m._1('row)._1)
                            New(Inferred(tpr)).select(ctor).appliedToArgs(exprs).asExprOf[R]
                        }

                def mapToOption(data: List[T]): Option[R] =
                    ${
                        dataExpr('data)
                    }.map: row =>
                        val currentFields = ${
                            Expr.ofList(mapper.filter(!_._2).map(m => m._1('row)._2.asExpr))
                        }
                        if currentFields.forall(_ == None) then None
                        else
                            Some(
                                ${
                                    val exprs = mapper.map(m => m._1('row)._1)
                                    New(Inferred(tpr)).select(ctor).appliedToArgs(exprs).asExprOf[R]
                                }
                            )
                    .head

                def mapToObject(data: List[T]): R =
                    ${
                        dataExpr('data)
                    }.map: row =>
                        ${
                            val exprs = mapper.map(m => m._1('row)._1)
                            New(Inferred(tpr)).select(ctor).appliedToArgs(exprs).asExprOf[R]
                        }
                    .head
        }