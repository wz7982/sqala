package sqala.printer

/**
 * Thrown when an AST node cannot be rendered as valid SQL.
 *
 * This signals a defect in the tree rather than in the dialect: a name that
 * cannot be written as an identifier, or a value that has no SQL literal form.
 * Printing never silently rewrites such input, because a mangled name or a
 * substituted value would change the meaning of the statement without telling
 * the caller.
 *
 * @param reason a description of the node that could not be printed
 */
final class PrintException(val reason: String) extends Exception(reason)