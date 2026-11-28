package sqala.codegen

import java.util.Locale
import scala.collection.mutable.ListBuffer

object Naming {
    private val scalaKeywords: Set[String] = Set(
        "abstract", "case", "catch", "class", "def", "do", "else", "enum",
        "export", "extends", "false", "final", "finally", "for", "given",
        "if", "implicit", "import", "lazy", "match", "new", "null", "object",
        "override", "package", "private", "protected", "return", "sealed",
        "super", "then", "this", "throw", "trait", "true", "try", "type",
        "val", "var", "while", "with", "yield", "extension", "using",
        "inline", "opaque", "open", "transparent", "infix", "derives", "end"
    )

    /**
     * Scala class names are always PascalCase; `strategy` describes how the
     * database table name is written, so the generator can split it into words.
     */
    def tableClassName(raw: String, strategy: TableNaming): String = {
        val splitCamel = strategy == TableNaming.CamelCase || strategy == TableNaming.PascalCase
        val base = words(raw, splitCamel).map(pascalWord).mkString
        val nonEmpty = if (base.isEmpty) "Table" else base
        val withLetter = if (nonEmpty.head.isDigit) "_" + nonEmpty else nonEmpty
        if (scalaKeywords.contains(withLetter)) withLetter + "Table" else withLetter
    }

    def fieldName(raw: String): String = {
        val base = words(raw, splitCamel = true).zipWithIndex.map { case (word, index) =>
            if (index == 0) lowerWord(word) else pascalWord(word)
        }.mkString
        val nonEmpty = if (base.isEmpty) "column" else base
        if (nonEmpty.head.isDigit) "_" + nonEmpty else nonEmpty
    }

    def escapeField(name: String): String = {
        if (scalaKeywords.contains(name)) "`" + name + "`" else name
    }

    def snakeCase(name: String): String = {
        val builder = new StringBuilder
        var i = 0
        while (i < name.length) {
            val c = name.charAt(i)
            if (c.isUpper) {
                if (i > 0 && (name.charAt(i - 1).isLower || name.charAt(i - 1).isDigit)) {
                    builder.append('_')
                }
                builder.append(c.toLower)
            } else {
                builder.append(c)
            }
            i += 1
        }
        builder.toString
    }

    private def words(raw: String, splitCamel: Boolean): Seq[String] = {
        raw.split("[^A-Za-z0-9]+").filter(_.nonEmpty).flatMap { chunk =>
            if (splitCamel) splitCamelWord(chunk) else Seq(chunk)
        }.toSeq
    }

    private def splitCamelWord(word: String): Seq[String] = {
        val parts = ListBuffer.empty[String]
        val current = new StringBuilder
        var i = 0
        while (i < word.length) {
            val c = word.charAt(i)
            val previous = if (i > 0) word.charAt(i - 1) else '\u0000'
            val next = if (i + 1 < word.length) word.charAt(i + 1) else '\u0000'
            val boundary =
                current.nonEmpty && c.isUpper && (
                    previous.isLower || previous.isDigit ||
                        (previous.isUpper && next.isLower)
                )
            if (boundary) {
                parts += current.toString
                current.clear()
            }
            current += c
            i += 1
        }
        if (current.nonEmpty) {
            parts += current.toString
        }
        parts.toSeq
    }

    private def pascalWord(word: String): String = {
        if (word.isEmpty) {
            word
        } else if (word.length > 1 && word.forall(_.isUpper)) {
            word.head.toUpper + word.tail.toLowerCase(Locale.ROOT)
        } else {
            word.head.toUpper + word.tail
        }
    }

    private def lowerWord(word: String): String = {
        if (word.isEmpty) {
            word
        } else if (word.length > 1 && word.forall(_.isUpper)) {
            word.toLowerCase(Locale.ROOT)
        } else {
            word.head.toLower + word.tail
        }
    }
}
