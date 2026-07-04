import sqala.printer.StandardSqlPrinter

def createSql(f: StandardSqlPrinter => Unit): String =
    val printer = StandardSqlPrinter(true)
    f(printer)
    printer.sql