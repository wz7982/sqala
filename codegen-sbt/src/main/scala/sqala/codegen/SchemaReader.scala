package sqala.codegen

import java.sql.{Connection, ResultSet}
import java.util.Locale
import scala.collection.mutable.ListBuffer

object SchemaReader {
    def read(connection: Connection, config: CodegenConfig): Seq[TableSchema] = {
        val metadata = connection.getMetaData
        val (catalog, schema) = location(connection, config)
        val wanted = config.tables.map(_.toLowerCase(Locale.ROOT)).toSet

        val tableNames = ListBuffer.empty[String]
        val tables = metadata.getTables(catalog, schema, "%", Array("TABLE"))
        try {
            while (tables.next()) {
                val name = tables.getString("TABLE_NAME")
                if (name != null && (wanted.isEmpty || wanted.contains(name.toLowerCase(Locale.ROOT)))) {
                    tableNames += name
                }
            }
        } finally {
            tables.close()
        }

        tableNames.toSeq.sorted.map { tableName =>
            val columns = readColumns(metadata, catalog, schema, tableName)
            val primaryKeys = readPrimaryKeys(metadata, catalog, schema, tableName)
            TableSchema(tableName, columns, primaryKeys)
        }
    }

    private def location(connection: Connection, config: CodegenConfig): (String, String) = {
        val url = config.url.toLowerCase(Locale.ROOT)
        if (url.startsWith("jdbc:mysql:")) {
            val catalog = config.schema.orElse(Option(connection.getCatalog)).orNull
            (catalog, null)
        } else if (url.startsWith("jdbc:postgresql:")) {
            (null, config.schema.getOrElse("public"))
        } else {
            (null, config.schema.orNull)
        }
    }

    private def readColumns(
        metadata: java.sql.DatabaseMetaData,
        catalog: String,
        schema: String,
        tableName: String
    ): Seq[ColumnSchema] = {
        val buffer = ListBuffer.empty[ColumnSchema]
        val columns = metadata.getColumns(catalog, schema, tableName, "%")
        try {
            while (columns.next()) {
                buffer += ColumnSchema(
                    name = columns.getString("COLUMN_NAME"),
                    jdbcType = columns.getInt("DATA_TYPE"),
                    typeName = Option(columns.getString("TYPE_NAME")).getOrElse(""),
                    nullable = "YES".equalsIgnoreCase(columns.getString("IS_NULLABLE")),
                    autoIncrement = "YES".equalsIgnoreCase(columns.getString("IS_AUTOINCREMENT")),
                    generated = "YES".equalsIgnoreCase(columns.getString("IS_GENERATEDCOLUMN")),
                    size = intOrZero(columns, "COLUMN_SIZE"),
                    decimalDigits = intOrZero(columns, "DECIMAL_DIGITS"),
                    ordinal = intOrZero(columns, "ORDINAL_POSITION")
                )
            }
        } finally {
            columns.close()
        }
        buffer.toSeq.sortBy(_.ordinal)
    }

    private def readPrimaryKeys(
        metadata: java.sql.DatabaseMetaData,
        catalog: String,
        schema: String,
        tableName: String
    ): Set[String] = {
        val buffer = ListBuffer.empty[(Int, String)]
        val keys = metadata.getPrimaryKeys(catalog, schema, tableName)
        try {
            while (keys.next()) {
                val column = keys.getString("COLUMN_NAME")
                if (column != null) {
                    buffer += ((intOrZero(keys, "KEY_SEQ"), column))
                }
            }
        } finally {
            keys.close()
        }
        buffer.sortBy(_._1).map(_._2).toSet
    }

    private def intOrZero(resultSet: ResultSet, column: String): Int = {
        val value = resultSet.getInt(column)
        if (resultSet.wasNull()) 0 else value
    }
}
