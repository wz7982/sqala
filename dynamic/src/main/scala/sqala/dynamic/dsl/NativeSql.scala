package sqala.dynamic.dsl

import scala.collection.mutable.ArrayBuffer

case class NativeSql(sql: String, args: Array[Any]):
    def +(that: NativeSql): NativeSql =
        NativeSql(sql + that.sql, args.appendedAll(that.args))

    def +(s: String): NativeSql =
        NativeSql(sql + s, args)

extension (s: StringContext)
    def sql(args: Any*): NativeSql =
        val strings = s.parts.iterator
        val argArray = ArrayBuffer[Any]()
        val argIterator = args.iterator
        val builder = new StringBuilder(strings.next())
        while strings.hasNext do
            val arg = argIterator.next()
            arg match
                case l: List[_] => 
                    builder.append(l.map(_ => "?").mkString("(", ", ", ")"))
                    argArray.appendAll(l)
                case _ => 
                    builder.append("?")
                    argArray.append(arg)
            builder.append(strings.next())
        NativeSql(builder.toString, argArray.toArray)