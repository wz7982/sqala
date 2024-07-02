# sqala

sqala是一个基于Scala3的SQL查询库，得名于Scala和SQL的结合，sqala是一个轻量级的查询库，除Scala和Java官方库外没有第三方依赖。

得益于Scala3强大的类型系统，sqala支持类型安全的查询构建，可以通过Scala3代码以面向对象的方式安全构建复杂查询（支持连接、子查询、与内存中的集合互操作、递归查询和复杂投影操作）。并可以使用JDBC安全地将查询结果反序列化回对象。

sqala支持将数据对象转换为UPDATE、INSERT、DELETE等操作，而无需编写样板代码。

另外，sqala还内置了对于数据中台场景的支持，基于sqala提供的SQL AST（抽象语法树）与SQL Parser，可以根据运行时的信息灵活地动态构造复杂查询，为动态建立数据报表等场景提供坚实的基础，而无需使用不安全且编码复杂的字符串拼接。

sqala支持多种数据库方言的生成，包括：MySQL、PostgreSQL、Oracle、MSSQL、Sqlite、DB2等，只需要传入不同的数据库类型，即可将同一个查询进行方言转换（其中MySQL和PostgreSQL为第一优先级支持）。

sqala的json模块支持任意ADT（代数数据类型）对象生成JSON；以及JSON反序列化到任意ADT对象功能。映射过程使用Scala3的类型类推导能力生成代码，无反射开销且类型安全。

文档地址：

https://wz7982.github.io/sqala-doc/
