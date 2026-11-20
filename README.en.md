# sqala

[简体中文](./README.md) | English

sqala is a type-safe SQL query library designed for Scala 3. It combines the expressiveness of Scala with the power of SQL, and uses Scala 3's powerful type system to keep database operations efficient and safe. Its name comes from the combination of Scala and SQL.

Documentation: https://wz7982.github.io/sqala-doc/en/

With sqala, you can:

1. **Build queries in an object-oriented, collection-like way**: the type-safe query builder filters out most invalid SQL at compile time and infers the result type automatically:

    ```scala
    case class User(id: Int, name: String)

    val query =
        from(User)
            .filter(u => u.id === 1)
            .map(u => u.name)

    // Result type: List[String]
    val result = db.fetch(query)
    ```

2. **Manage queries with Scala 3 named tuples**: no need to predefine DTOs for projections or join results, and no `Map[String, Any]`. Returned fields are accessed with `.` and remain type-safe:

    ```scala
    val query =
        from(User).map(u => (id = u.id))

    // Result type: List[(id: Int)]
    val result = db.fetch(query)

    for r <- result do
        println(r.id)
    ```

3. **Type-safe primary key lookups**: querying by primary key needs no DSL. The parameter type is read from the entity's configuration, and the query is created for you:

    ```scala
    // Result type: Option[User]
    val result = db.findByPrimaryKey[User](1)
    ```

4. **Extreme type safety**: sqala models SQL semantics with the type system, so its type checks catch not only simple errors such as unknown fields or incompatible types on either side of a comparison operator, but also deeper semantic errors:

    ```scala
    val q1 =
        // Compile error: Aggregate functions are not allowed in filter.
        from(A).filter(a => a.x > count())

    val q2 =
        // Compile error: Ungrouped column references must appear in the groupBy clause
        // or be used in an aggregate function.
        from(A).map(a => (a.x, count()))

    val q3 =
        // Compile error: More than one row returned by a subquery used as an expression.
        from(A).map(a => from(A).map(_.a))

    val q4 =
        // Compile error: Aggregate function calls cannot be nested.
        from(A).map(a => count(sum(a.x)))

    // ...and more
    ```

5. **Dynamic queries**: sqala provides dynamic query support for scenarios such as drag-and-drop reporting platforms:

    ```scala
    val query =
        select(column("b").as("col1"), column("c").as("col2"))
            .from(table("t"))
            .where(column("a") === value(1))
    ```

6. **SQL parser**: the `dynamic` module includes a hand-written recursive descent SQL parser with no third-party parser dependency. `unsafeExpr` parses SQL fragments into expressions, which makes migrating existing SQL or handling dynamic query fragments easier (note: only syntax is validated, not semantics, and untrusted input must never be spliced in).

7. **Strong batch decoding performance**: Scala 3's `inline` capability generates decoding code at compile time. For 10,000-row batch queries, decoding throughput is close to hand-written JDBC (about 94%) and is typically higher than reflection/proxy-based frameworks such as MyBatis, Hibernate, and jOOQ.

8. **Transparency**: sqala is not a traditional ORM. The query DSL maps almost 1:1 to the generated query, with no black-box behaviour such as N + 1 queries, so performance is predictable.

9. **Multi-dialect support**: PostgreSQL, MySQL, Oracle and other dialects are supported. The same query expression generates different SQL when a different dialect parameter is passed in, and the query documentation lists the tested support level of the latest version of each database in detail.

10. **Powerful query features**: beyond CRUD, sqala supports multidimensional grouping, `LATERAL` subqueries, recursive queries, row pattern recognition, JSON tables, property graph queries and other advanced features. From simple scenarios to complex data analysis, sqala provides a one-stop solution.

11. **Specialized domain support**: spatial operations defined by the `ISO/IEC 13249` standard are supported, providing professional support for geographic information computation.

12. **Standards compatibility**: the built-in abstract syntax tree and generic SQL generator are compatible with `ISO/IEC 9075` SQL 2023 and the `ISO/IEC 13249` SQL spatial extension, so supporting more database dialects takes only a small amount of code.

13. **Zero dependencies**: sqala's core modules depend only on Scala and the Java standard library, with no third-party dependencies. Only optional integration modules such as `sqala-slf4j` and `sqala-spring-boot-starter` bring in the corresponding frameworks when used.