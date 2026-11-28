package sqala.test

import sqala.metadata.*

case class SimpleUser(id: Int, userName: String)

@table("USERS")
case class AnnotatedUser(
    @primaryKey @autoInc @column("ID") id: Int,
    @column("USER_NAME") userName: String
)

@table("MEMBERSHIP")
case class CompositeKey(
    @primaryKey groupId: Int,
    @primaryKey userId: Int,
    remark: String
)

case class GeneratedId(@primaryKey @autoInc id: Int, name: String)

class M2_TableMacro extends munit.FunSuite:
    test("table name defaults to snake_case"):
        assertEquals(TableMacro.tableName[SimpleUser], "simple_user")

    test("table annotation overrides the name"):
        assertEquals(TableMacro.tableName[AnnotatedUser], "USERS")
        assertEquals(TableMacro.tableName[CompositeKey], "MEMBERSHIP")

    test("column names default to snake_case"):
        val m = TableMacro.tableMetaData[SimpleUser]
        assertEquals(m.tableName, "simple_user")
        assertEquals(m.fields.toList, List("id", "userName"))
        assertEquals(m.columns.toList, List("id", "user_name"))
        assertEquals(m.primaryKeyFields, Nil)
        assertEquals(m.incrementField, None)

    test("column annotation overrides the column name but not the field name"):
        val m = TableMacro.tableMetaData[AnnotatedUser]
        assertEquals(m.fields.toList, List("id", "userName"))
        assertEquals(m.columns.toList, List("ID", "USER_NAME"))

    test("primary key and auto increment annotations are collected"):
        val m = TableMacro.tableMetaData[AnnotatedUser]
        assertEquals(m.primaryKeyFields, List("id"))
        assertEquals(m.incrementField, Some("id"))

    test("composite primary keys keep declaration order"):
        val m = TableMacro.tableMetaData[CompositeKey]
        assertEquals(m.primaryKeyFields, List("groupId", "userId"))
        assertEquals(m.incrementField, None)

    test("FetchCompanion resolves the companion class metadata"):
        val c = FetchCompanion.derived[AnnotatedUser.type]
        assertEquals(c.metaData.tableName, "USERS")
        assertEquals(c.metaData.columns.toList, List("ID", "USER_NAME"))

    test("FetchPrimaryKey builds a select for a single key"):
        val f = FetchPrimaryKey.derived[AnnotatedUser]
        val q = f.createQueryTree(Seq(1), false)
        val printed = createSql(_.printQuery(q))
        assertEquals(printed, "SELECT\n    \"ID\",\n    \"USER_NAME\"\nFROM\n    \"USERS\"\nWHERE\n    \"ID\" = 1")

    test("FetchPrimaryKey builds a delete for a single key"):
        val f = FetchPrimaryKey.derived[AnnotatedUser]
        val printed = createSql(_.printStatement(f.createDeleteTree(Seq(1), false)))
        assertEquals(printed, "DELETE FROM \"USERS\" WHERE \"ID\" = 1")

    test("FetchPrimaryKey builds a composite key predicate"):
        val f = FetchPrimaryKey.derived[CompositeKey]
        val printed = createSql(_.printQuery(f.createQueryTree(Seq((2, 3)), false)))
        assert(printed.contains("\"group_id\" = 2"), printed)
        assert(printed.contains("\"user_id\" = 3"), printed)
        assert(printed.contains("AND"), printed)

    test("FetchPrimaryKey combines multiple key tuples with OR"):
        val f = FetchPrimaryKey.derived[AnnotatedUser]
        val printed = createSql(_.printQuery(f.createQueryTree(Seq(1, 2), false)))
        assert(printed.contains("OR"), printed)

    test("FetchPrimaryKey honors the inlined flag"):
        val f = FetchPrimaryKey.derived[AnnotatedUser]
        assertEquals(
            createSql(_.printQuery(f.createQueryTree(Seq(1), true))),
            createSql(_.printQuery(f.createQueryTree(Seq(1), false)))
        )

    test("bindGeneratedPrimaryKey replaces the autoInc field"):
        val entity = GeneratedId(0, "a")
        val bound = InsertMacro.bindGeneratedPrimaryKey(42L, entity)
        assertEquals(bound, GeneratedId(42, "a"))
        assertEquals(entity, GeneratedId(0, "a"))