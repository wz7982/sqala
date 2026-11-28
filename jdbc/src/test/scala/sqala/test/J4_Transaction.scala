package sqala.test

import sqala.jdbc.*
import sqala.test.JdbcFixture.*

/**
 * Transaction semantics: commit, rollback, nested blocks joining the outer
 * transaction, savepoints, and isolation level handling.
 */
abstract class J4_TransactionBase extends munit.FunSuite:
    def db: JdbcContext

    def reset(): Unit

    override def beforeAll(): Unit =
        reset()

    test("transaction commits on success"):
        db.deleteByPrimaryKey[JUser](91)
        db.executeTransaction:
            db.insert(JUser(91, "Tx", 1))
        assertEquals(db.findByPrimaryKey[JUser](91), Some(JUser(91, "Tx", 1)))

    test("transaction rolls back on failure"):
        db.deleteByPrimaryKey[JUser](90)
        intercept[RuntimeException]:
            db.executeTransaction:
                db.insert(JUser(90, "Tx", 1))
                throw new RuntimeException("boom")
        assertEquals(db.findByPrimaryKey[JUser](90), None)

    test("nested transaction joins the outer one"):
        db.deleteByPrimaryKey[JUser](92)
        db.deleteByPrimaryKey[JUser](93)
        intercept[RuntimeException]:
            db.executeTransaction:
                db.insert(JUser(92, "Outer", 1))
                db.executeTransaction:
                    db.insert(JUser(93, "Inner", 1))
                    throw new RuntimeException("inner boom")
        assertEquals(db.findByPrimaryKey[JUser](92), None)
        assertEquals(db.findByPrimaryKey[JUser](93), None)

    test("uncommitted rows are visible inside the transaction"):
        db.deleteByPrimaryKey[JUser](94)
        db.executeTransaction:
            db.insert(JUser(94, "Vis", 1))
            assertEquals(db.findByPrimaryKey[JUser](94), Some(JUser(94, "Vis", 1)))
        assertEquals(db.findByPrimaryKey[JUser](94), Some(JUser(94, "Vis", 1)))

    test("named savepoint rolls back only later writes"):
        db.deleteByPrimaryKey[JUser](95)
        db.deleteByPrimaryKey[JUser](96)
        db.executeTransaction:
            val tx = db.currentTransaction
            db.insert(JUser(95, "Keep", 1))
            val sp = tx.createSavepoint("sp1")
            db.insert(JUser(96, "Drop", 1))
            tx.rollbackToSavepoint(sp)
            tx.releaseSavepoint(sp)
        assertEquals(db.findByPrimaryKey[JUser](95), Some(JUser(95, "Keep", 1)))
        assertEquals(db.findByPrimaryKey[JUser](96), None)

    test("unnamed savepoint behaves the same"):
        db.deleteByPrimaryKey[JUser](97)
        db.deleteByPrimaryKey[JUser](98)
        db.executeTransaction:
            val tx = db.currentTransaction
            db.insert(JUser(97, "Keep", 1))
            val sp = tx.createSavepoint()
            db.insert(JUser(98, "Drop", 1))
            tx.rollbackToSavepoint(sp)
        assertEquals(db.findByPrimaryKey[JUser](97), Some(JUser(97, "Keep", 1)))
        assertEquals(db.findByPrimaryKey[JUser](98), None)

    test("isolation level block commits"):
        db.deleteByPrimaryKey[JUser](99)
        db.executeTransactionWithIsolation(TransactionIsolation.ReadCommitted):
            db.insert(JUser(99, "Iso", 1))
        assertEquals(db.findByPrimaryKey[JUser](99), Some(JUser(99, "Iso", 1)))

    test("isolation block nested in a transaction still commits"):
        db.deleteByPrimaryKey[JUser](100)
        db.executeTransaction:
            db.executeTransactionWithIsolation(TransactionIsolation.Serializable):
                db.insert(JUser(100, "Join", 1))
        assertEquals(db.findByPrimaryKey[JUser](100), Some(JUser(100, "Join", 1)))


/**
 * PostgreSQL exposes the effective transaction isolation level through
 * `show transaction_isolation`, so the level application can be verified.
 */
class J4_Transaction_Postgresql extends J4_TransactionBase:
    override def db: JdbcContext =
        pgContext

    override def reset(): Unit =
        runPgDdl(userDdl*)

    private def currentIsolation: String =
        db.fetchTo[String](new NativeSql("show transaction_isolation", Array.empty)).head

    test("read-committed block applies its level"):
        db.executeTransactionWithIsolation(TransactionIsolation.ReadCommitted):
            assertEquals(currentIsolation, "read committed")

    test("isolation level is ignored inside an existing transaction"):
        db.deleteByPrimaryKey[JUser](101)
        db.executeTransaction:
            db.executeTransactionWithIsolation(TransactionIsolation.Serializable):
                assertEquals(currentIsolation, "read committed")
                db.insert(JUser(101, "JoinIso", 1))
        assertEquals(db.findByPrimaryKey[JUser](101), Some(JUser(101, "JoinIso", 1)))


/**
 * MySQL only applies `setTransactionIsolation` to the next transaction and
 * the session variable does not reflect it mid-transaction, so the suites
 * only pin the commit behavior and the session default here.
 */
class J4_Transaction_Mysql extends J4_TransactionBase:
    override def db: JdbcContext =
        mysqlContext

    override def reset(): Unit =
        runMysqlDdl(userDdl*)

    test("session isolation defaults to repeatable read"):
        val level = db.fetchTo[String](new NativeSql("select @@transaction_isolation", Array.empty)).head
        assertEquals(level.toUpperCase.replace('-', '_'), "REPEATABLE_READ")
