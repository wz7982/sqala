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

object NonEmptyList:
    extension [A](list: List[A])
        /**
         * Convert to a non-empty list.
         */
        def toNonEmptyList: NonEmptyList[A] =
            NonEmptyList(list.head, list.tail)