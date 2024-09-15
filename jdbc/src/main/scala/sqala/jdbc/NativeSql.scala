package sqala.jdbc

import scala.collection.mutable.ArrayBuffer
import scala.language.unsafeNulls
import scala.quoted.{Expr, Quotes, Varargs}

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

case class StaticNativeSql[R](sql: String, args: Array[Any])

extension (inline s: StringContext)
    transparent inline def staticSql(inline args: Any*)(using inline c: JdbcTestConnection): Any =
        ${ sqlMacro('s, 'args, 'c) }

def sqlMacro(s: Expr[StringContext], args: Expr[Seq[Any]], c: Expr[JdbcTestConnection])(using q: Quotes): Expr[Any] =
    import q.reflect.*

    val types = args match
        case Varargs(exprs) => 
            exprs.map: e =>
                val term = e.asTerm.underlying
                term.tpe.widen.asType
    val argTypes = types.iterator
    val strings = s.value.get.parts.iterator
    val testSqlBuilder = new StringBuilder(strings.next())
    while strings.hasNext do
        val tpe = argTypes.next()
        tpe match
            case '[List[t]] => testSqlBuilder.append("(?, ?)")
            case _ => testSqlBuilder.append("?")
        testSqlBuilder.append(strings.next())
    val testSql = testSqlBuilder.toString

    val conn = c.value.get

    Class.forName(conn.driverClassName)

    var connection: java.sql.Connection = null
    var stmt: java.sql.PreparedStatement = null
    var explainStmt: java.sql.PreparedStatement = null

    val errorMessage = try
        connection = java.sql.DriverManager.getConnection(conn.url, conn.user, conn.password)
        explainStmt = connection.prepareStatement("EXPLAIN " + testSql)
        var index = 1
        for t <- types do
            t match
                case '[List[t]] =>
                    val expr = Expr.summon[SetDefaultValue[t]].get
                    expr.value.get
                    expr.value.get.setValue(explainStmt, index)
                    index += 1
                    expr.value.get.setValue(explainStmt, index)
                case '[t] =>
                    val expr =Expr.summon[SetDefaultValue[t]].get
                    expr.value.get
                    expr.value.get.setValue(explainStmt, index)
            index += 1
        explainStmt.execute()
        None
    catch
        case e: Exception => 
            Some(e.getMessage())
    finally
        if explainStmt != null then explainStmt.close()

    errorMessage match
        case Some(e) => report.error(e)
        case _ =>

    val info = try
        stmt = connection.prepareStatement(testSql)
        val metaData = stmt.getMetaData()
        for i <- 1 to metaData.getColumnCount() yield
            val columnLabel = metaData.getColumnLabel(i)
            val columnType = metaData.getColumnType(i)
            val columnNullable = metaData.isNullable(i) != java.sql.ResultSetMetaData.columnNoNulls

            import java.sql.Types.*

            val tpr = (columnType, columnNullable) match
                case (SMALLINT, true) => TypeRepr.of[Option[Int]]
                case (SMALLINT, false) => TypeRepr.of[Int]
                case (INTEGER, true) => TypeRepr.of[Option[Int]]
                case (INTEGER, false) => TypeRepr.of[Int]
                case (BIGINT, true) => TypeRepr.of[Option[Long]]
                case (BIGINT, false) => TypeRepr.of[Long]
                case (FLOAT, true) => TypeRepr.of[Option[Float]]
                case (FLOAT, false) => TypeRepr.of[Float]
                case (DOUBLE, true) => TypeRepr.of[Option[Double]]
                case (DOUBLE, false) => TypeRepr.of[Double]
                case (DECIMAL, true) => TypeRepr.of[Option[BigDecimal]]
                case (DECIMAL, false) => TypeRepr.of[BigDecimal]
                case (BOOLEAN, true) => TypeRepr.of[Option[Boolean]]
                case (BOOLEAN, false) => TypeRepr.of[Boolean]
                case (VARCHAR | CHAR | NVARCHAR | LONGVARCHAR | LONGNVARCHAR, true) => TypeRepr.of[Option[String]]
                case (VARCHAR | CHAR | NVARCHAR | LONGVARCHAR | LONGNVARCHAR, false) => TypeRepr.of[String]
                case (DATE | TIME | TIMESTAMP, true) => TypeRepr.of[Option[java.util.Date]]
                case (DATE | TIME | TIMESTAMP, false) => TypeRepr.of[java.util.Date]
                case (_, true) => TypeRepr.of[Option[Any]]
                case (_, false) => TypeRepr.of[Any]

            columnLabel -> tpr
    finally
        if stmt != null then stmt.close()
        if connection != null then connection.close()

    var refinement = Refinement(TypeRepr.of[Record], info.head._1, info.head._2)
    for i <- info.tail do
        refinement = Refinement(refinement, i._1, i._2)

    refinement.asType match
        case '[r] =>
            '{
                val strings = $s.parts.iterator
                val argArray = ArrayBuffer[Any]()
                val argIterator = $args.iterator
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

                StaticNativeSql[r](builder.toString, argArray.toArray)
            }