package sqala.parser

import sqala.ast.expr.{SqlIntervalField, SqlTimeUnit}

/**
 * Internal failure of a literal validation. It is converted to a positioned
 * ParseException]by Parser.
 */
private[parser] final class LiteralValidationException(val reason: String)
    extends Exception(reason)

/**
 * Hand-written validators for SQL date, time, timestamp, and interval
 * literals. No `java.time` parser is used; only character-level checks.
 */
private[parser] object LiteralValidator:
    def validateDate(value: String): Unit =
        val parts = split(value, '-', 3, "YYYY-MM-DD")
        val year = fixedDigits(parts(0), 4, "year")
        val month = range(fixedDigits(parts(1), 2, "month"), 1, 12, "month")
        val day = fixedDigits(parts(2), 2, "day")
        range(day, 1, daysInMonth(year, month), "day")

    def validateTime(value: String, withTimeZone: Boolean): Unit =
        val zoneIndex = value.indexWhere(c => c == '+' || c == '-')
        val zone = if zoneIndex < 0 then None else Some(value.substring(zoneIndex))
        if withTimeZone && zone.isEmpty then fail("time zone is required")
        if !withTimeZone && zone.isDefined then fail("time zone is not allowed")

        val timeText = if zoneIndex < 0 then value else value.substring(0, zoneIndex)
        val timePart = timeText.trim
        if timePart.isEmpty then fail("missing time value")
        if timePart.exists(_.isWhitespace) then fail("unexpected whitespace in time value")

        val parts = split(timePart, ':', 3, "HH:MM:SS")
        range(fixedDigits(parts(0), 2, "hour"), 0, 23, "hour")
        range(fixedDigits(parts(1), 2, "minute"), 0, 59, "minute")
        validateSeconds(parts(2), requireRange = true)

        zone.foreach(validateTimeZone)

    def validateTimestamp(value: String, withTimeZone: Boolean): Unit =
        val separator = value.indexWhere(c => c == ' ' || c == 'T' || c == 't')
        if separator < 0 then
            fail("expected 'YYYY-MM-DD HH:MM:SS' or 'YYYY-MM-DDTHH:MM:SS'")
        val datePart = value.substring(0, separator)
        val timePart = value.substring(separator + 1)
        if timePart.isEmpty then fail("missing time value")
        validateDate(datePart)
        validateTime(timePart, withTimeZone)

    def validateInterval(value: String, field: SqlIntervalField): Unit =
        val body = stripSign(value)
        if body.isEmpty then fail("missing interval value")
        field match
            case SqlIntervalField.Single(unit) =>
                validateSingleInterval(body, unit)
            case SqlIntervalField.To(start, end) =>
                validateIntervalRange(body, start, end)

    private def validateSingleInterval(body: String, unit: SqlTimeUnit): Unit =
        unit match
            case SqlTimeUnit.Second =>
                validateSeconds(body, requireRange = false)
            case SqlTimeUnit.Year =>
                unsignedInt(body, "year")
            case SqlTimeUnit.Month =>
                unsignedInt(body, "month")
            case SqlTimeUnit.Day =>
                unsignedInt(body, "day")
            case SqlTimeUnit.Hour =>
                unsignedInt(body, "hour")
            case SqlTimeUnit.Minute =>
                unsignedInt(body, "minute")
            case SqlTimeUnit.UnsafeCustom(_) =>
                fail("unsupported interval unit")

    private def validateIntervalRange(body: String, start: SqlTimeUnit, end: SqlTimeUnit): Unit =
        (start, end) match
            case (SqlTimeUnit.UnsafeCustom(_), _) | (_, SqlTimeUnit.UnsafeCustom(_)) =>
                fail("unsupported interval unit")

            case (SqlTimeUnit.Year, SqlTimeUnit.Month) =>
                val parts = split(body, '-', 2, "YEAR TO MONTH")
                unsignedInt(parts(0), "year")
                range(unsignedInt(parts(1), "month"), 0, 11, "month")

            case (SqlTimeUnit.Day, SqlTimeUnit.Hour) =>
                val parts = split(body, ' ', 2, "DAY TO HOUR")
                unsignedInt(parts(0), "day")
                range(unsignedInt(parts(1), "hour"), 0, 23, "hour")

            case (SqlTimeUnit.Day, SqlTimeUnit.Minute) =>
                val parts = split(body, ' ', 2, "DAY TO MINUTE")
                unsignedInt(parts(0), "day")
                val time = split(parts(1), ':', 2, "DAY TO MINUTE")
                range(unsignedInt(time(0), "hour"), 0, 23, "hour")
                range(unsignedInt(time(1), "minute"), 0, 59, "minute")

            case (SqlTimeUnit.Day, SqlTimeUnit.Second) =>
                val parts = split(body, ' ', 2, "DAY TO SECOND")
                unsignedInt(parts(0), "day")
                val time = split(parts(1), ':', 3, "DAY TO SECOND")
                range(unsignedInt(time(0), "hour"), 0, 23, "hour")
                range(unsignedInt(time(1), "minute"), 0, 59, "minute")
                validateSeconds(time(2), requireRange = true)

            case (SqlTimeUnit.Hour, SqlTimeUnit.Minute) =>
                val parts = split(body, ':', 2, "HOUR TO MINUTE")
                unsignedInt(parts(0), "hour")
                range(unsignedInt(parts(1), "minute"), 0, 59, "minute")

            case (SqlTimeUnit.Hour, SqlTimeUnit.Second) =>
                val parts = split(body, ':', 3, "HOUR TO SECOND")
                unsignedInt(parts(0), "hour")
                range(unsignedInt(parts(1), "minute"), 0, 59, "minute")
                validateSeconds(parts(2), requireRange = true)

            case (SqlTimeUnit.Minute, SqlTimeUnit.Second) =>
                val parts = split(body, ':', 2, "MINUTE TO SECOND")
                unsignedInt(parts(0), "minute")
                validateSeconds(parts(1), requireRange = true)

            case _ =>
                fail(
                    s"unsupported interval field combination " +
                    s"${timeUnitName(start)} TO ${timeUnitName(end)}"
                )

    private def validateSeconds(text: String, requireRange: Boolean): Unit =
        val parts = text.split("\\.", -1).toList
        if parts.length > 2 then fail("seconds must have at most one fractional part")
        val seconds = unsignedInt(parts(0), "second")
        if requireRange then range(seconds, 0, 59, "second")
        if parts.length == 2 then
            if parts(1).isEmpty || !parts(1).forall(_.isDigit) then
                fail("fractional seconds must contain digits")

    private def validateTimeZone(zone: String): Unit =
        if zone.isEmpty || (zone.charAt(0) != '+' && zone.charAt(0) != '-') then
            fail("time zone must start with '+' or '-'")
        val body = zone.substring(1)
        if body.isEmpty then fail("time zone offset must have an hour")
        val colon = body.indexOf(':')
        val hourText =
            if colon < 0 then body
            else body.substring(0, colon)
        val minuteText =
            if colon < 0 then
                "0"
            else
                if body.indexOf(':', colon + 1) >= 0 then
                    fail("expected '+HH' or '+HH:MM'")
                body.substring(colon + 1)
        val hour = range(unsignedInt(hourText, "time zone hour"), 0, 14, "time zone hour")
        val minute = range(unsignedInt(minuteText, "time zone minute"), 0, 59, "time zone minute")
        if hour == 14 && minute != 0 then fail("time zone offset cannot exceed 14:00")

    private def fixedDigits(text: String, length: Int, label: String): Int =
        if text.length != length || !text.forall(_.isDigit) then
            fail(s"$label must be $length digit(s)")
        text.toInt

    private def unsignedInt(text: String, label: String): Int =
        if text.isEmpty || !text.forall(_.isDigit) then
            fail(s"$label must be a non-negative integer")
        try text.toInt
        catch
            case _: NumberFormatException => fail(s"$label is too large")

    private def range(value: Int, min: Int, max: Int, label: String): Int =
        if value < min || value > max then
            fail(s"$label must be between $min and $max")
        value

    private def daysInMonth(year: Int, month: Int): Int =
        month match
            case 1 | 3 | 5 | 7 | 8 | 10 | 12 => 31
            case 4 | 6 | 9 | 11 => 30
            case 2 => if isLeapYear(year) then 29 else 28
            case _ => 31

    private def isLeapYear(year: Int): Boolean =
        year % 4 == 0 && (year % 100 != 0 || year % 400 == 0)

    private def stripSign(text: String): String =
        if text.startsWith("+") || text.startsWith("-") then text.substring(1) else text

    private def split(text: String, separator: Char, count: Int, shape: String): List[String] =
        val parts =
            text.split(java.util.regex.Pattern.quote(separator.toString), -1).toList
        if parts.length != count then fail(s"expected $shape")
        parts

    private def timeUnitName(unit: SqlTimeUnit): String =
        unit match
            case SqlTimeUnit.Year => "YEAR"
            case SqlTimeUnit.Month => "MONTH"
            case SqlTimeUnit.Day => "DAY"
            case SqlTimeUnit.Hour => "HOUR"
            case SqlTimeUnit.Minute => "MINUTE"
            case SqlTimeUnit.Second => "SECOND"
            case SqlTimeUnit.UnsafeCustom(_) => fail("unsupported interval unit")

    private def fail(reason: String): Nothing =
        throw new LiteralValidationException(reason)
