def doIncrement(extra:Int): Int => Int = (a: Int) => a + extra

val incr10 = doIncrement(10)
val incre990 = doIncrement(990)
incr10(10)
incre990(990)