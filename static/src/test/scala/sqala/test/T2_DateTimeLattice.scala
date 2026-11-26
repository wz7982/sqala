package sqala.test

import java.time.{LocalDate, LocalDateTime, LocalTime, OffsetDateTime, OffsetTime}
import sqala.metadata.Interval
import sqala.static.dsl.*
import sqala.test.StaticFixture.expect

/**
 * The date and time result lattice. Adding an interval promotes a date to
 * a local date-time and a local time to an offset time; subtracting two
 * dates or two times yields an interval. An optional operand makes the
 * result optional.
 */
class T2_DateTimeLattice extends munit.FunSuite:
    test("adding an interval promotes the operand to its offset-aware form"):
        from(Times).map(t => expect[LocalDateTime](t.dt + StaticFixture.interval))
        from(Times).map(t => expect[OffsetDateTime](t.ts + StaticFixture.interval))
        from(Times).map(t => expect[OffsetDateTime](t.lts + StaticFixture.interval))
        from(Times).map(t => expect[OffsetTime](t.tm + StaticFixture.interval))
        from(Times).map(t => expect[OffsetTime](t.ltm + StaticFixture.interval))

    test("an optional operand makes the promoted result optional"):
        from(Times).map(t => expect[Option[LocalDateTime]](t.odt + StaticFixture.interval))
        from(Times).map(t => expect[Option[OffsetDateTime]](t.ots + StaticFixture.interval))
        from(Times).map(t => expect[Option[OffsetTime]](t.otm + StaticFixture.interval))
        from(Times).map(t => expect[Option[LocalDateTime]](t.dt + t.oiv))
        from(Times).map(t => expect[Option[OffsetDateTime]](t.lts + t.oiv))

    test("subtracting an interval promotes the operand the same way"):
        from(Times).map(t => expect[LocalDateTime](t.dt - StaticFixture.interval))
        from(Times).map(t => expect[OffsetDateTime](t.ts - StaticFixture.interval))
        from(Times).map(t => expect[OffsetTime](t.tm - StaticFixture.interval))
        from(Times).map(t => expect[Option[LocalDateTime]](t.odt - StaticFixture.interval))

    test("subtracting two dates or two date-times yields an interval"):
        from(Times).map(t => expect[Interval](t.dt - t.dt))
        from(Times).map(t => expect[Interval](t.ts - t.ts))
        from(Times).map(t => expect[Interval](t.lts - t.lts))
        from(Times).map(t => expect[Interval](t.dt - t.ts))
        from(Times).map(t => expect[Interval](t.ts - t.dt))
        from(Times).map(t => expect[Option[Interval]](t.odt - t.dt))
        from(Times).map(t => expect[Option[Interval]](t.dt - t.ots))

    test("subtracting two times yields an interval"):
        from(Times).map(t => expect[Interval](t.tm - t.tm))
        from(Times).map(t => expect[Interval](t.ltm - t.ltm))
        from(Times).map(t => expect[Interval](t.tm - t.ltm))
        from(Times).map(t => expect[Option[Interval]](t.otm - t.tm))

    test("the current date and time functions have fixed types"):
        from(Times).map(_ => expect[LocalDate](currentDate()))
        from(Times).map(_ => expect[OffsetDateTime](currentTimestamp()))
        from(Times).map(_ => expect[LocalDateTime](localTimestamp()))
        from(Times).map(_ => expect[OffsetTime](currentTime()))
        from(Times).map(_ => expect[LocalTime](localTime()))

    test("extracting a date part yields an optional decimal"):
        from(Times).map(t => expect[Option[BigDecimal]](t.dt.year))
        from(Times).map(t => expect[Option[BigDecimal]](t.dt.month))
        from(Times).map(t => expect[Option[BigDecimal]](t.dt.day))
        from(Times).map(t => expect[Option[BigDecimal]](t.ts.hour))
        from(Times).map(t => expect[Option[BigDecimal]](t.tm.minute))
        from(Times).map(t => expect[Option[BigDecimal]](t.ts.second))
        from(Times).map(t => expect[Option[BigDecimal]](t.iv.day))

    test("a conditional widens date operands to the wider representation"):
        from(Times).map(t => expect[LocalDate](caseWhen(t.dt > t.dt)(t.dt).otherwise(t.dt)))
        from(Times).map(t => expect[OffsetDateTime](caseWhen(t.dt > t.dt)(t.dt).otherwise(t.ts)))
        from(Times).map(t => expect[OffsetDateTime](caseWhen(t.dt > t.dt)(t.ts).otherwise(t.dt)))
        from(Times).map(t => expect[LocalDateTime](caseWhen(t.dt > t.dt)(t.lts).otherwise(t.dt)))
        from(Times).map(t => expect[Option[OffsetDateTime]](caseWhen(t.dt > t.dt)(t.odt).otherwise(t.ts)))

    test("a conditional mixes a date or time with a string"):
        from(Times).map(t => expect[LocalDate](caseWhen(t.dt > t.dt)(t.dt).otherwise("2026-01-01")))
        from(Times).map(t => expect[LocalDate](caseWhen(t.dt > t.dt)("2026-01-01").otherwise(t.dt)))
        from(Times).map(t => expect[OffsetDateTime](caseWhen(t.dt > t.dt)(t.ts).otherwise("2026-01-01")))
        from(Times).map(t => expect[OffsetTime](caseWhen(t.dt > t.dt)(t.tm).otherwise("12:00:00")))
        from(Times).map(t => expect[LocalTime](caseWhen(t.dt > t.dt)(t.ltm).otherwise("12:00:00")))

    test("date and time families do not mix in arithmetic"):
        assert(compileErrors("""from(Times).map(t => t.dt + t.dt)""").nonEmpty)
        assert(compileErrors("""from(Times).map(t => t.dt - t.tm)""").nonEmpty)
        assert(compileErrors("""from(Times).map(t => t.tm - t.dt)""").nonEmpty)
        assert(compileErrors("""from(Times).map(t => t.dt + 1)""").nonEmpty)
        assert(compileErrors("""from(Texts).map(t => t.s + StaticFixture.interval)""").nonEmpty)

    test("a date part cannot be extracted from a plain string or number"):
        assert(compileErrors("""from(Texts).map(t => t.s.year)""").nonEmpty)
        assert(compileErrors("""from(Nums).map(n => n.i.day)""").nonEmpty)
        assert(compileErrors("""from(Times).map(t => t.dt.hour)""").nonEmpty)
        assert(compileErrors("""from(Times).map(t => t.tm.year)""").nonEmpty)