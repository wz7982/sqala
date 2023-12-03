package sqala.compiletime.macros

import sqala.compiletime.TableMetaData

inline def tableNameMacro[T <: Product]: String = ${ tableNameMacroImpl[T] }

transparent inline def tableInfoMacro[T <: Product]: Any = ${ tableInfoMacroImpl[T] }

transparent inline def tableAliasMacro[T <: Product](inline name: String): Any = ${ tableAliasMacroImpl[T]('name) }

inline def exprMetaDataMacro[T](inline name: String): String = ${ exprMetaDataMacroImpl[T]('name) }

inline def columnsMetaDataMacro[T]: List[(String, String)] = ${ columnsMetaDataMacroImpl[T] }

inline def tableMetaDataMacro[T <: Product]: TableMetaData = ${ tableMetaDataMacroImpl[T] }