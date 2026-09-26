package sqala.util

/**
 * A non-empty list of elements.
 *
 * This is a simple wrapper around a head element and a tail list,
 * ensuring that there is always at least one element.
 */
case class NonEmptyList[+A](head: A, tail: List[A]):
    /**
     * Convert to a regular list.
     */
    def toList: List[A] =
        head :: tail

    /**
     * Map over the elements.
     */
    def map[B](f: A => B): NonEmptyList[B] =
        NonEmptyList(f(head), tail.map(f))

    /**
     * Get the size of the list.
     */
    def size: Int =
        1 + tail.size

    /**
     * Group the elements into sublists of size `n`.
     */
    def grouped(n: Int): NonEmptyList[NonEmptyList[A]] =
        require(n > 0, "n must be greater than 0")
        def loop(remaining: List[A],
           current: List[A],
           acc: List[NonEmptyList[A]]
        ): List[NonEmptyList[A]] =
            remaining match
                case Nil =>
                    if current.isEmpty then acc.reverse
                    else
                        val nel = NonEmptyList(current.head, current.tail)
                        (nel :: acc).reverse
                case h :: t =>
                    val newCurrent = h :: current
                        if newCurrent.length == n then
                            val group = newCurrent.reverse
                            loop(t, Nil, NonEmptyList(group.head, group.tail) :: acc)
                        else
                            loop(t, newCurrent, acc)

        val groups = loop(toList, Nil, Nil)
        val first :: rest = groups.runtimeChecked
        NonEmptyList(first, rest)

    /**
     * Prepend an element to the list.
     */
    def ::[B >: A](elem: B): NonEmptyList[B] =
        NonEmptyList(elem, toList)

    /**
     * Append an element to the list.
     */
    def :+[B >: A](elem: B): NonEmptyList[B] =
        NonEmptyList(head, tail :+ elem)

    /**
     * Concatenate two non-empty lists.
     */
    def ++[B >: A](other: NonEmptyList[B]): NonEmptyList[B] =
        NonEmptyList(head, tail ++ other.toList)

object NonEmptyList:
    /**
     * Create a non-empty list from a head and a tail.
     */
    def apply[A](head: A, tail: NonEmptyList[A]): NonEmptyList[A] =
        NonEmptyList(head, tail.toList)

    extension [A](list: List[A])
        /**
         * Convert to a non-empty list.
         */
        def toNonEmptyList: NonEmptyList[A] =
            NonEmptyList(list.head, list.tail)