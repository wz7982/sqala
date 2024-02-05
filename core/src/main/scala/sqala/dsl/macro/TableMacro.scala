package sqala.dsl.`macro`

import sqala.dsl.TableMetaData

inline def tableNameMacro[T]: String = ${ tableNameMacroImpl[T] }

inline def tableMetaDataMacro[T]: TableMetaData = ${ tableMetaDataMacroImpl[T] }