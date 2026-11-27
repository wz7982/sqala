package sqala.test

import scala.language.unsafeNulls
import java.lang.reflect.*
import java.nio.file.{Files, Path}
import scala.jdk.CollectionConverters.*

/**
 * Reflective builder for every sqala.ast enum case.
 *
 * Enum cases cannot be created through their constructors on the JVM, so
 * instances are allocated without running constructors and every declared
 * field is filled: String fields receive unique taint markers, enums and
 * nested classes recurse with a depth limit, numerics get neutral values.
 */
object TaintFactory:
    private var counter = 0
    private val markerPattern = "SLT"

    def reset(): Int =
        counter = 0
        counter

    def current(): Int = counter

    private def salt(): String =
        counter += 1
        s"$markerPattern$counter"

    /** Walk the ast sources and return every enum class by package+name. */
    def enumClasses(root: Path): List[Class[?]] =
        val files = Files.walk(root).iterator.asScala.filter(Files.isRegularFile(_)).toList
        files.flatMap { f =>
            val text = new String(Files.readAllBytes(f))
            val pkg = "(?m)^package (\\S+)".r.findFirstMatchIn(text).map(_.group(1)).getOrElse("")
            "(?m)^enum (\\w+)".r.findAllMatchIn(text).map(m => s"$pkg.${m.group(1)}")
        }.distinct.map(name => Class.forName(name)).sortBy(_.getName)

    private def rawOf(t: Type): Class[?] = t match
        case c: Class[?] => c
        case p: ParameterizedType => p.getRawType.asInstanceOf[Class[?]]
        case g: GenericArrayType => Array.newInstance(rawOf(g.getGenericComponentType), 0).getClass
        case _ => classOf[Object]

    private def innerOf(t: Type, n: Int): Type = t match
        case p: ParameterizedType if p.getActualTypeArguments.length > n => p.getActualTypeArguments()(n)
        case _ => classOf[Object]

    def mk(t: Type, depth: Int): AnyRef =
        val cls = rawOf(t)
        val cn = cls.getName
        if cls == classOf[String] then salt()
        else if cn == "java.lang.Integer" || cn == "int" then java.lang.Integer.valueOf(1)
        else if cn == "java.lang.Long" || cn == "long" then java.lang.Long.valueOf(1L)
        else if cn == "java.lang.Short" || cn == "short" then java.lang.Short.valueOf(1.toShort)
        else if cn == "java.lang.Byte" || cn == "byte" then java.lang.Byte.valueOf(1.toByte)
        else if cn == "java.lang.Double" || cn == "double" then java.lang.Double.valueOf(1.0)
        else if cn == "java.lang.Float" || cn == "float" then java.lang.Float.valueOf(1.0f)
        else if cn == "java.lang.Boolean" || cn == "boolean" then java.lang.Boolean.valueOf(false)
        else if cn == "java.lang.Character" || cn == "char" then java.lang.Character.valueOf('x')
        else if cls == classOf[scala.math.BigInt] then scala.math.BigInt(1)
        else if cls == classOf[scala.math.BigDecimal] then scala.math.BigDecimal(1)
        else if cn == "scala.Option" then
            if depth > 0 then Some(mk(innerOf(t, 0), depth - 1)) else None
        else if cn == "sqala.util.NonEmptyList" then
            if depth > 0 then
                val head = mk(innerOf(t, 0), depth - 1)
                Class.forName("sqala.util.NonEmptyList").getConstructors.head.newInstance(head, Nil).asInstanceOf[AnyRef]
            else null
        else if cn == "scala.collection.immutable.List" || cn == "scala.collection.Seq" ||
                cn == "scala.collection.immutable.Seq" || cn == "scala.collection.immutable.LinearSeq" then
            if depth > 0 then List(mk(innerOf(t, 0), depth - 1)) else Nil
        else if cn == "scala.collection.immutable.Vector" then
            if depth > 0 then Vector(mk(innerOf(t, 0), depth - 1)) else Vector.empty
        else if cls.isArray then
            if depth > 0 then
                val arr = java.lang.reflect.Array.newInstance(cls.getComponentType, 1)
                java.lang.reflect.Array.set(arr, 0, mk(cls.getComponentType, depth - 1))
                arr
            else null
        else if cls.isEnum then enumSample(cls, depth)
        else
            try allocate(cls, depth) catch case _: Throwable => null

    def enumSample(cls: Class[?], depth: Int): AnyRef =
        val companion = Class.forName(cls.getName + "$").getField("MODULE$").get(null)
        val singletons = companion.getClass.getDeclaredFields.filter(_.getType == cls)
        if singletons.nonEmpty then
            singletons.head.setAccessible(true)
            singletons.head.get(companion)
        else if depth > 0 then allInstances(cls, depth).headOption.map(_.instance).orNull
        else null

    final case class Built(caseName: String, instance: AnyRef, markerFrom: Int, markerTo: Int)

    def allInstances(cls: Class[?], depth: Int): List[Built] =
        val companion = Class.forName(cls.getName + "$").getField("MODULE$").get(null)
        val singles = companion.getClass.getDeclaredFields.filter(_.getType == cls).map { f =>
            f.setAccessible(true)
            Built(f.getName, f.get(companion), counter, counter)
        }.toList
        val params = cls.getDeclaredClasses.filterNot(_.getSimpleName.contains("$")).toList.flatMap { c =>
            val from = counter
            try Built(c.getSimpleName, allocate(c, depth), from, counter) :: Nil
            catch
                case ex: Throwable =>
                    val cause = if ex.isInstanceOf[InvocationTargetException] then ex.getCause else ex
                    println(s"CFAIL ${cls.getSimpleName}.${c.getSimpleName}|${cause.getClass.getSimpleName}|${Option(cause.getMessage).map(_.take(90)).getOrElse("")}")
                    Nil
        }
        singles ::: params

    private lazy val unsafe: sun.misc.Unsafe =
        val f = classOf[sun.misc.Unsafe].getDeclaredField("theUnsafe")
        f.setAccessible(true)
        f.get(null).asInstanceOf[sun.misc.Unsafe]

    /** Bypasses the enum-constructor restriction and fills every declared field. */
    private def allocate(cls: Class[?], depth: Int): AnyRef =
        val inst = unsafe.allocateInstance(cls)
        var c: Class[?] = cls
        while c != null && c != classOf[Object] do
            for fld <- c.getDeclaredFields
                if !Modifier.isStatic(fld.getModifiers) && !(c == classOf[java.lang.Enum[?]] && Set("name", "ordinal").contains(fld.getName)) do
                fld.setAccessible(true)
                val v =
                    if fld.getType == java.lang.Integer.TYPE then java.lang.Integer.valueOf(100)
                    else if fld.getType == java.lang.Long.TYPE then java.lang.Long.valueOf(1L)
                    else if fld.getType == java.lang.Boolean.TYPE then java.lang.Boolean.valueOf(false)
                    else if fld.getType == java.lang.Double.TYPE then java.lang.Double.valueOf(1.0)
                    else if fld.getType == java.lang.Float.TYPE then java.lang.Float.valueOf(1.0f)
                    else if fld.getType == java.lang.Short.TYPE then java.lang.Short.valueOf(1.toShort)
                    else if fld.getType == java.lang.Byte.TYPE then java.lang.Byte.valueOf(1.toByte)
                    else if fld.getType == java.lang.Character.TYPE then java.lang.Character.valueOf('x')
                    else mk(fld.getGenericType, depth)
                if v != null then
                    try fld.set(inst, v) catch case _: Throwable => ()
            c = c.getSuperclass
        inst

