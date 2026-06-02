package sqala.printer

/**
 * H2 dialect printer.
 *
 * Inherits all default SQL standard behavior from `SqlPrinter` without modification.
 *
 * @param standardEscapeStrings `true` treats backslashes literally;
 *                              `false` uses backslashes as escape characters.
 */
class H2Printer(override val standardEscapeStrings: Boolean) extends SqlPrinter(standardEscapeStrings)