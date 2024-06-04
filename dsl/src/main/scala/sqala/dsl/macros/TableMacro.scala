package sqala.dsl.macros

import sqala.dsl.TableMetaData

inline def tableNameMacro[T]: String = ${ tableNameMacroImpl[T] }

inline def tableMetaDataMacro[T]: TableMetaData = ${ tableMetaDataMacroImpl[T] }