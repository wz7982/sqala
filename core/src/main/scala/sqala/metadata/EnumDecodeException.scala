package sqala.metadata

/**
 * Thrown when a database value cannot be mapped back to an enum case.
 *
 * This indicates a mismatch between the stored data and the enum
 * declaration, for example a value added by a migration but not yet
 * reflected in the Scala enum, or a column without a matching check
 * constraint.
 *
 * @param enumName the fully qualified name of the enum type
 * @param value    the value read from the database
 */
final class EnumDecodeException(
    val enumName: String,
    val value: Any
) extends Exception(
    s"Unknown value '$value' for enum $enumName."
)