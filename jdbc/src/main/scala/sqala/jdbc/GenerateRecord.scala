package sqala.jdbc

import sqala.dynamic.dsl.NativeSql

import java.sql.Connection
import scala.language.unsafeNulls
import scala.quoted.*

private[sqala] object GenerateRecord:
    private[sqala] 
    transparent inline def run[Url <: String, Username <: String, Password <: String, Driver <: String](
        conn: Connection,
        exec: Connection => NativeSql => List[Map[String, Any]],
        inline sql: NativeSql
    ): Any =
        ${ runMacro[Url, Username, Password, Driver]('conn, 'exec, 'sql) }

    private[sqala] 
    def runMacro[Url <: String, Username <: String, Password <: String, Driver <: String](
        conn: Expr[Connection],
        exec: Expr[Connection => NativeSql => List[Map[String, Any]]],
        sql: Expr[NativeSql]
    )(using 
        q: Quotes,
        typeUrl: Type[Url],
        typeUsername: Type[Username],
        typePassword: Type[Password],
        typeDriver: Type[Driver]
    ): Expr[Any] =
        import q.reflect.*

        val url = TypeRepr.of[Url] match
            case ConstantType(StringConstant(v)) => v
            case _ => report.errorAndAbort("Unable to obtain database information at compile time.")

        val username = TypeRepr.of[Username] match
            case ConstantType(StringConstant(v)) => v
            case _ => report.errorAndAbort("Unable to obtain database information at compile time.")

        val password = TypeRepr.of[Password] match
            case ConstantType(StringConstant(v)) => v
            case _ => report.errorAndAbort("Unable to obtain database information at compile time.")

        val driver = TypeRepr.of[Driver] match
            case ConstantType(StringConstant(v)) => v
            case _ => report.errorAndAbort("Unable to obtain database information at compile time.")

        def removeInlined(term: Term): Term =
            term match
                case Inlined(None, Nil, t) => removeInlined(t)
                case _ => term

        val sqlTerm = removeInlined(sql.asTerm)

        val (snippets, argTerms) = sqlTerm match
            case Apply(Apply(Ident("sql"), Apply(_, Typed(Repeated(snippetTerms, _), _) :: Nil) :: Nil), Typed(Repeated(argTerms, _), _) :: Nil) => 
                val snippets = snippetTerms.map:
                    case Literal(StringConstant(v)) => v
                (snippets, argTerms)

        val argTypes = argTerms.map: t =>
            t.tpe.widen.asType

        val argTypeIterator = argTypes.iterator
        val snippetIterator = snippets.iterator

        val testSqlBuilder = new StringBuilder(snippetIterator.next())
        
        while snippetIterator.hasNext do
            val tpe = argTypeIterator.next()
            tpe match
                case '[List[t]] => testSqlBuilder.append("(?, ?)")
                case _ => testSqlBuilder.append("?")
            testSqlBuilder.append(snippetIterator.next())

        val testSql = testSqlBuilder.toString

        Class.forName(driver)

        var connection: java.sql.Connection = null
        var stmt: java.sql.PreparedStatement = null
        var explainStmt: java.sql.PreparedStatement = null

        val errorMessage = try
            connection = java.sql.DriverManager.getConnection(url, username, password)
            explainStmt = connection.prepareStatement("EXPLAIN " + testSql)
            var index = 1

            for t <- argTypes do
                t match
                    case '[List[t]] =>
                        val expr = Expr.summon[SetDefaultValue[t]].get
                        expr.value.get
                        expr.value.get.setValue(explainStmt, index)
                        index += 1
                        expr.value.get.setValue(explainStmt, index)
                    case '[t] =>
                        val expr = Expr.summon[SetDefaultValue[t]].get
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
                    case (SMALLINT, true) => 
                        TypeRepr.of[Option[Int]]
                    case (SMALLINT, false) => 
                        TypeRepr.of[Int]
                    case (INTEGER, true) => 
                        TypeRepr.of[Option[Int]]
                    case (INTEGER, false) => 
                        TypeRepr.of[Int]
                    case (BIGINT, true) => 
                        TypeRepr.of[Option[Long]]
                    case (BIGINT, false) => 
                        TypeRepr.of[Long]
                    case (FLOAT, true) => 
                        TypeRepr.of[Option[Float]]
                    case (FLOAT, false) => 
                        TypeRepr.of[Float]
                    case (DOUBLE, true) => 
                        TypeRepr.of[Option[Double]]
                    case (DOUBLE, false) => 
                        TypeRepr.of[Double]
                    case (DECIMAL | NUMERIC, true) => 
                        TypeRepr.of[Option[BigDecimal]]
                    case (DECIMAL | NUMERIC, false) => 
                        TypeRepr.of[BigDecimal]
                    case (BOOLEAN, true) => 
                        TypeRepr.of[Option[Boolean]]
                    case (BOOLEAN, false) => 
                        TypeRepr.of[Boolean]
                    case (VARCHAR | CHAR | NVARCHAR | LONGVARCHAR | LONGNVARCHAR, true) => 
                        TypeRepr.of[Option[String]]
                    case (VARCHAR | CHAR | NVARCHAR | LONGVARCHAR | LONGNVARCHAR, false) => 
                        TypeRepr.of[String]
                    case (DATE, true) => 
                        TypeRepr.of[Option[java.util.Date]]
                    case (DATE, false) => 
                        TypeRepr.of[java.util.Date]
                    case (TIMESTAMP, true) => 
                        TypeRepr.of[Option[java.util.Date]]
                    case (TIMESTAMP, false) => 
                        TypeRepr.of[java.util.Date]
                    case (_, true) => 
                        TypeRepr.of[Option[Any]]
                    case (_, false) => 
                        TypeRepr.of[Any]

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
                    val records: List[r] = 
                        $exec($conn)($sql).map(i => new Record(i).asInstanceOf[r])
                    records
                }