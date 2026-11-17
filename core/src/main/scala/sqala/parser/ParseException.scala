package sqala.parser

/**
 * Thrown when SQL text cannot be parsed.
 *
 * `offset` is the zero-based character offset, `line` and `column` are
 * one-based, and `sourceLine` is the original line containing the error.
 *
 * The rendered message always has the same shape:
 * {{{
 * <detail> (line <line>, column <column>)
 * <sourceLine>
 * <caret>
 * }}}
 */
final class ParseException(
    val detail: String,
    val offset: Int,
    val line: Int,
    val column: Int,
    val sourceLine: String
) extends Exception(ParseException.render(detail, line, column, sourceLine))

object ParseException:
    def at(message: String, source: String, offset: Int, line: Int, column: Int): ParseException =
        new ParseException(message, offset, line, column, sourceLineAt(source, offset))

    private def sourceLineAt(source: String, offset: Int): String =
        val safeOffset =
            if offset < 0 then 0
            else if offset > source.length then source.length
            else offset
        val start = source.lastIndexOf('\n', safeOffset - 1) + 1
        val end =
            source.indexOf('\n', safeOffset) match
                case -1 => source.length
                case index => index
        source.substring(start, end)

    private def render(detail: String, line: Int, column: Int, sourceLine: String): String =
        val padding = " " * (if column <= 0 then 0 else column - 1)
        s"$detail (line $line, column $column)\n$sourceLine\n$padding^"