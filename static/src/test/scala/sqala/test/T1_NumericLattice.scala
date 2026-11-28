package sqala.test

import sqala.static.dsl.*
import sqala.test.StaticFixture.expect

/**
 * The numeric result lattice. `BigDecimal` beats `Double` beats `Float`
 * beats `Long` beats `Int`, an optional operand makes the result optional,
 * and the functions that always return a fixed SQL type are checked
 * against that type rather than the operand type.
 */
class T1_NumericLattice extends munit.FunSuite:
    test("addition picks the wider operand type"):
        from(Nums).map(n => expect[Int](n.i + n.i))
        from(Nums).map(n => expect[Long](n.i + n.l))
        from(Nums).map(n => expect[Long](n.l + n.i))
        from(Nums).map(n => expect[Float](n.i + n.f))
        from(Nums).map(n => expect[Float](n.f + n.i))
        from(Nums).map(n => expect[Double](n.i + n.d))
        from(Nums).map(n => expect[Double](n.d + n.i))
        from(Nums).map(n => expect[BigDecimal](n.i + n.bd))
        from(Nums).map(n => expect[BigDecimal](n.bd + n.i))
        from(Nums).map(n => expect[Float](n.l + n.f))
        from(Nums).map(n => expect[Double](n.f + n.d))
        from(Nums).map(n => expect[BigDecimal](n.d + n.bd))

    test("subtraction picks the wider operand type"):
        from(Nums).map(n => expect[Int](n.i - n.i))
        from(Nums).map(n => expect[Long](n.i - n.l))
        from(Nums).map(n => expect[Float](n.l - n.f))
        from(Nums).map(n => expect[Double](n.f - n.d))
        from(Nums).map(n => expect[BigDecimal](n.bd - n.d))

    test("multiplication picks the wider operand type"):
        from(Nums).map(n => expect[Int](n.i * n.i))
        from(Nums).map(n => expect[Long](n.i * n.l))
        from(Nums).map(n => expect[Float](n.i * n.f))
        from(Nums).map(n => expect[Double](n.i * n.d))
        from(Nums).map(n => expect[BigDecimal](n.i * n.bd))

    test("a literal widens the result just like a column does"):
        from(Nums).map(n => expect[Int](n.i + 1))
        from(Nums).map(n => expect[Long](n.i + 1L))
        from(Nums).map(n => expect[Float](n.i + 1.0f))
        from(Nums).map(n => expect[Double](n.i + 1.0))
        from(Nums).map(n => expect[BigDecimal](n.i + BigDecimal(1)))

    test("an optional operand on either side makes the result optional"):
        from(Nums).map(n => expect[Option[Int]](n.oi + n.i))
        from(Nums).map(n => expect[Option[Int]](n.i + n.oi))
        from(Nums).map(n => expect[Option[Int]](n.oi + n.oi))
        from(Nums).map(n => expect[Option[Double]](n.oi + n.od))
        from(Nums).map(n => expect[Option[Double]](n.od + n.d))
        from(Nums).map(n => expect[Option[BigDecimal]](n.oi + n.bd))
        from(Nums).map(n => expect[Option[Long]](n.oi - n.l))
        from(Nums).map(n => expect[Option[Float]](n.oi * n.f))

    test("an optional literal makes the result optional"):
        from(Nums).map(n => expect[Option[Int]](n.i + Option(1)))
        from(Nums).map(n => expect[Option[Long]](n.oi + 1L))

    test("division and modulo always yield an optional decimal"):
        from(Nums).map(n => expect[Option[BigDecimal]](n.i / n.i))
        from(Nums).map(n => expect[Option[BigDecimal]](n.d / n.d))
        from(Nums).map(n => expect[Option[BigDecimal]](n.i % n.i))
        from(Nums).map(n => expect[Option[BigDecimal]](mod(n.i, n.i)))
        from(Nums).map(n => expect[Option[BigDecimal]](round(n.d, 2)))

    test("unary signs and abs keep the operand type"):
        from(Nums).map(n => expect[Int](+n.i))
        from(Nums).map(n => expect[Int](-n.i))
        from(Nums).map(n => expect[BigDecimal](-n.bd))
        from(Nums).map(n => expect[Option[Int]](-n.oi))
        from(Nums).map(n => expect[Int](abs(n.i)))
        from(Nums).map(n => expect[Option[Double]](abs(n.od)))

    test("floor and ceil yield an optional decimal regardless of the operand"):
        from(Nums).map(n => expect[Option[BigDecimal]](floor(n.i)))
        from(Nums).map(n => expect[Option[BigDecimal]](floor(n.d)))
        from(Nums).map(n => expect[Option[BigDecimal]](ceil(n.bd)))
        from(Nums).map(n => expect[Option[BigDecimal]](ceil(n.od)))

    test("the floating point functions yield an optional decimal"):
        from(Nums).map(n => expect[Option[BigDecimal]](sin(n.i)))
        from(Nums).map(n => expect[Option[BigDecimal]](sqrt(n.d)))
        from(Nums).map(n => expect[Option[BigDecimal]](power(n.i, 2)))
        from(Nums).map(n => expect[Option[BigDecimal]](ln(n.f)))

    test("count yields a long and sum and avg an optional decimal"):
        from(Nums).map(_ => expect[Long](count()))
        from(Nums).map(n => expect[Long](count(n.i)))
        from(Nums).map(n => expect[Long](count(n.oi)))
        from(Nums).map(n => expect[Long](countDistinct(n.i)))
        from(Nums).map(n => expect[Option[BigDecimal]](sum(n.i)))
        from(Nums).map(n => expect[Option[BigDecimal]](sum(n.bd)))
        from(Nums).map(n => expect[Option[BigDecimal]](avg(n.i)))
        from(Nums).map(n => expect[Option[BigDecimal]](stddevPop(n.d)))

    test("max and min keep the operand type but make it optional"):
        from(Nums).map(n => expect[Option[Int]](max(n.i)))
        from(Nums).map(n => expect[Option[Int]](min(n.i)))
        from(Nums).map(n => expect[Option[Int]](max(n.oi)))
        from(Nums).map(n => expect[Option[BigDecimal]](min(n.bd)))
        from(Texts).map(t => expect[Option[String]](max(t.s)))
        from(Texts).map(t => expect[Option[String]](anyValue(t.s)))

    test("a window function keeps the type of its aggregate"):
        from(Nums).map(_ => expect[Long](count().over()))
        from(Nums).map(_ => expect[Long](rowNumber().over()))
        from(Nums).map(n => expect[Option[BigDecimal]](sum(n.i).over()))
        from(Nums).map(n => expect[Option[Int]](max(n.i).over()))

    test("numeric operators and functions reject non-numeric operands"):
        assert(compileErrors("""from(Texts).map(t => t.s + t.s)""").startsWith("error: Types java.lang.String and java.lang.String cannot be added."))
        assert(compileErrors("""from(Texts).map(t => t.s - t.s)""").startsWith("error: Types java.lang.String and java.lang.String cannot be subtracted."))
        assert(compileErrors("""from(Texts).map(t => t.s * t.s)""").startsWith("error: The arguments type should be numeric, but got java.lang.String."))
        assert(compileErrors("""from(Texts).map(t => t.s / t.s)""").startsWith("error: The arguments type should be numeric, but got java.lang.String."))
        assert(compileErrors("""from(Texts).map(t => abs(t.s))""").startsWith("error: The arguments type should be numeric, but got java.lang.String."))
        assert(compileErrors("""from(Texts).map(t => sum(t.s))""").startsWith("error: The arguments type should be numeric, but got java.lang.String."))
        assert(compileErrors("""from(Nums).map(n => n.i + "x")""").startsWith("error: Types scala.Int and java.lang.String cannot be added."))