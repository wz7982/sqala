package sqala.codegen

import java.sql.Types
import java.util.Locale

final case class MappedType(scalaType: String, warning: Option[String] = None)

object TypeMapper {
    private val standardTextTypes: Set[String] = Set(
        "char", "varchar", "bpchar", "text", "tinytext", "mediumtext",
        "longtext", "enum", "set", "uuid", "citext", "json", "jsonb"
    )

    def map(column: ColumnSchema, overrides: Map[String, String]): Either[String, MappedType] = {
        val typeName = column.typeName.toLowerCase(Locale.ROOT)
        val overrideType = overrides.get(typeName).orElse(overrides.get(column.typeName))
        overrideType match {
            case Some(scalaType) => Right(MappedType(scalaType))
            case None => mapByJdbcType(column, typeName)
        }
    }

    private def mapByJdbcType(column: ColumnSchema, typeName: String): Either[String, MappedType] = {
        column.jdbcType match {
            case Types.BIT | Types.BOOLEAN =>
                Right(MappedType("Boolean"))
            case Types.TINYINT | Types.SMALLINT | Types.INTEGER =>
                Right(MappedType("Int"))
            case Types.BIGINT =>
                Right(MappedType("Long"))
            case Types.REAL =>
                Right(MappedType("Float"))
            case Types.FLOAT | Types.DOUBLE =>
                Right(MappedType("Double"))
            case Types.NUMERIC | Types.DECIMAL =>
                Right(MappedType("BigDecimal"))
            case Types.CHAR | Types.VARCHAR | Types.LONGVARCHAR | Types.NCHAR |
                 Types.NVARCHAR | Types.LONGNVARCHAR | Types.CLOB | Types.NCLOB =>
                mapTextType(typeName)
            case Types.DATE if typeName == "year" =>
                Right(MappedType("Int"))
            case Types.DATE =>
                Right(MappedType("java.time.LocalDate"))
            case Types.TIME =>
                Right(MappedType("java.time.LocalTime"))
            case Types.TIME_WITH_TIMEZONE =>
                Right(MappedType("java.time.OffsetTime"))
            case Types.TIMESTAMP if typeName.contains("tz") || typeName.contains("with time zone") =>
                Right(MappedType("java.time.OffsetDateTime"))
            case Types.TIMESTAMP =>
                Right(MappedType("java.time.LocalDateTime"))
            case Types.TIMESTAMP_WITH_TIMEZONE =>
                Right(MappedType("java.time.OffsetDateTime"))
            case Types.ARRAY =>
                Left("array types are not supported")
            case Types.BINARY | Types.VARBINARY | Types.LONGVARBINARY | Types.BLOB =>
                Left("binary/blob types are not supported")
            case Types.OTHER =>
                mapOtherType(typeName)
            case _ =>
                Left("type '" + typeName + "' is not supported")
        }
    }

    private def mapTextType(typeName: String): Either[String, MappedType] = {
        typeName match {
            case "json" | "jsonb" =>
                Right(MappedType("sqala.metadata.Json"))
            case "enum" | "set" =>
                Right(MappedType(
                    "String",
                    Some("enum column mapped to String; change it to a Scala enum manually")
                ))
            case "uuid" =>
                Right(MappedType(
                    "String",
                    Some("uuid column mapped to String; change it if you provide a UUID encoder")
                ))
            case _ if !standardTextTypes.contains(typeName) =>
                Right(MappedType(
                    "String",
                    Some("custom type '" + typeName + "' mapped to String; change it manually if needed")
                ))
            case _ =>
                Right(MappedType("String"))
        }
    }

    private def mapOtherType(typeName: String): Either[String, MappedType] = {
        if (typeName == "json" || typeName == "jsonb") {
            Right(MappedType("sqala.metadata.Json"))
        } else if (typeName == "uuid") {
            Right(MappedType(
                "String",
                Some("uuid column mapped to String; change it if you provide a UUID encoder")
            ))
        } else if (typeName.contains("timestamptz") || typeName.contains("timestamp with time zone")) {
            Right(MappedType("java.time.OffsetDateTime"))
        } else if (typeName.contains("timetz") || typeName.contains("time with time zone")) {
            Right(MappedType("java.time.OffsetTime"))
        } else if (!typeName.contains("geometry") && !typeName.contains("geography")) {
            Right(MappedType(
                "String",
                Some("custom type '" + typeName + "' mapped to String; change it manually if needed")
            ))
        } else {
            Left("spatial type '" + typeName + "' is not supported")
        }
    }
}
