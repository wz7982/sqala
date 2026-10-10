import sqala.static.dsl.*
import sqala.metadata.*

case class A(x: Int, y: String)

object Test:
    def main(args: Array[String]): Unit =
        val q =
            from(A.join(A.join(A).on((a2, a3) => a2.x == a3.x)).on((a1, a2, a3) => a1.x == a3.x))
                .filter((a1, a2, a3) => a1.x === 10)

                
        val qq = from(A).filter: a =>
            val expr = s"".asExpr
            a.x === 1



        // val q =
        //     from(A)
        //         .filter(a => a.x == from(A).filter(aa => aa.x == a.x).map(aa => a.x).take(1))
                // .map(a => from(A).map(aa => count()).take(1))


        println(q.sql(PostgresqlDialect, true))
