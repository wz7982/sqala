package sqala.static.dsl.table

// TODO 炸列 要比标准操作多一个groupBy 要加一个PivotContext
// TODO 最后要加一个转成SubQueryTable操作
// .pivot(t =>
//    t.groupBy(b = t.b, c = t.c)
//    .agg(s = sum(t.a), c = count())
//    .`for`(
//      t.d.within(q1 = "Q1", q2 = "Q2"),
//      t.e.within(cn = "CN", us = "US")
//    )
//)
// 炸成有字段(b, c, s_q1_cn, s_q2_cn, s_q1_us, s_q2_us, c...) 一共十个字段