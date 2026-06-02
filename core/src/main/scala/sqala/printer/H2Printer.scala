package sqala.printer

/**
 * H2 dialect printer.
 */
class H2Printer(override val standardEscapeStrings: Boolean) extends SqlPrinter(standardEscapeStrings)