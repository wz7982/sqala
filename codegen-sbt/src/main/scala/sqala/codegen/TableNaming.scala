package sqala.codegen

/**
 * Naming style of database table names.
 *
 * The generated Scala class name is always PascalCase. This setting only tells
 * the generator how to split the raw table name into words.
 */
sealed trait TableNaming

object TableNaming {
    /**
     * Tables are named like `user_profile`.
     */
    case object SnakeCase extends TableNaming

    /**
     * Tables are named like `userProfile`.
     */
    case object CamelCase extends TableNaming

    /**
     * Tables are named like `UserProfile`.
     */
    case object PascalCase extends TableNaming

    /**
     * Tables are named like `user-profile`.
     */
    case object KebabCase extends TableNaming
}
