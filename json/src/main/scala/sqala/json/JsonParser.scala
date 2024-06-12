package sqala.json

import scala.language.experimental.saferExceptions
import scala.util.parsing.combinator.lexical.StdLexical
import scala.util.parsing.combinator.syntactical.StandardTokenParsers
import scala.util.parsing.input.CharArrayReader.EofCh

class JsonParser extends StandardTokenParsers:
    class SqlLexical extends StdLexical:
        override protected def processIdent(name: String): Token =
            val upperCased = name.toUpperCase
            if reserved.contains(upperCased) then Keyword(upperCased) else Identifier(name)

        override def token: Parser[Token] =
            identChar ~ rep(identChar | digit) ^^ {
                case first ~ rest => processIdent((first :: rest).mkString(""))
            } |
            rep1(digit) ~ opt('.' ~> rep(digit)) ^^ {
                case i ~ None => NumericLit(i.mkString(""))
                case i ~ Some(d) => NumericLit(i.mkString("") + "." + d.mkString(""))
            } |
            '"' ~ rep(chrExcept('"', '\n', EofCh)) ~ '"' ^^ {
                case _ ~ chars ~ _ => StringLit(chars.mkString(""))
            } |
            EofCh ^^^ EOF |
            delim |
            failure("illegal character")

    override val lexical: SqlLexical = new SqlLexical

    lexical.reserved.addAll(List("TRUE", "FALSE", "NULL"))

    lexical.delimiters.addAll(List("{", "}", "[", "]", "\"", ":", ",", "."))

    def literal: Parser[JsonNode] =
        numberLiteral |
        stringLiteral |
        booleanLiteral |
        nullLiteral |
        objectLiteral |
        arrayLiteral

    def numberLiteral: Parser[JsonNode] =
        numericLit ^^ (i => JsonNode.NumberLiteral(BigDecimal(i)))

    def stringLiteral: Parser[JsonNode] =
        stringLit ^^ (i => JsonNode.StringLiteral(i))

    def booleanLiteral: Parser[JsonNode] =
        "TRUE" ^^ (_ => JsonNode.BooleanLiteral(true)) |
        "FALSE" ^^ (_ => JsonNode.BooleanLiteral(false))

    def nullLiteral: Parser[JsonNode] =
        "NULL" ^^ (_ => JsonNode.NullLiteral)

    def attr: Parser[(String, JsonNode)] =
        stringLiteral ~ ":" ~ literal ^^ {
            case k ~ _ ~ v => k.asInstanceOf[JsonNode.StringLiteral].string -> v
        }

    def objectLiteral: Parser[JsonNode] =
        "{" ~> repsep(attr, ",") <~ "}" ^^ (items => JsonNode.Object(items.toMap))

    def arrayLiteral: Parser[JsonNode] =
        "[" ~> repsep(literal, ",") <~ "]" ^^ (items => JsonNode.Vector(items))

    def parse(text: String): JsonNode throws JsonDecodeException = 
        phrase(literal)(new lexical.Scanner(text)) match
            case Success(result, _) => result
            case e => throw JsonDecodeException(e.toString)