package sqala.metadata

import java.time.*

trait SqlNumber[T]

object SqlNumber:
    given int: SqlNumber[Int]()

    given long: SqlNumber[Long]()

    given float: SqlNumber[Float]()

    given double: SqlNumber[Double]()

    given decimal: SqlNumber[BigDecimal]()

    given option[T: SqlNumber]: SqlNumber[Option[T]]()

trait SqlDateTime[T]

object SqlDateTime:
    given localDate: SqlDateTime[LocalDate]()

    given localDateTime: SqlDateTime[LocalDateTime]()

    given offsetDateTime: SqlDateTime[OffsetDateTime]()

    given option[T: SqlDateTime]: SqlDateTime[Option[T]]()

trait SqlTime[T]

object SqlTime:
    given localTime: SqlTime[LocalTime]()

    given offsetTime: SqlTime[OffsetTime]()

    given option[T: SqlTime]: SqlTime[Option[T]]()

trait SqlString[T]

object SqlString:
    given string: SqlString[String]()

    given option[T: SqlString]: SqlString[Option[T]]()

trait SqlBoolean[T]

object SqlBoolean:
    given boolean: SqlBoolean[Boolean]()

    given option[T: SqlBoolean]: SqlBoolean[Option[T]]()

trait SqlJson[T]

object SqlJson:
    given json: SqlJson[Json]()

    given option[T: SqlJson]: SqlJson[Option[T]]()

trait SqlVector[T]

object SqlVector:
    given vector: SqlVector[Vector]()

    given option[T: SqlVector]: SqlVector[Option[T]]()

trait SqlSpatial[T]

opaque type Json = String

object Json:
    def apply(value: String): Json = value

opaque type Interval = Long

opaque type Vector = String

object Vector:
    def apply(value: String): Vector = value