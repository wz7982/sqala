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

    /**
     * Concatenate two lists.
     */
    def ++[B >: A](other: Seq[B]): NonEmptyList[B] =
        NonEmptyList(head, tail ++ other.toList)

    /**
     * Zip two non-empty lists together.
     */
    def zip[B](other: NonEmptyList[B]): NonEmptyList[(A, B)] =
        val zippedHead = (head, other.head)
        val zippedTail = tail.zip(other.tail)
        NonEmptyList(zippedHead, zippedTail)

    /**
     * Zip the elements with their indices.
     */
    def zipWithIndex: NonEmptyList[(A, Int)] =
        val indexedHead = (head, 0)
        val indexedTail = tail.zipWithIndex.map: (elem, idx) =>
            (elem, idx + 1)
        NonEmptyList(indexedHead, indexedTail)

    /**
     * Get the indices of the elements.
     */
    def indices: NonEmptyList[Int] =
        NonEmptyList(0, (1 until size).toList)

object NonEmptyList:
    /**
     * Create a non-empty list from a head and a tail.
     */
    def apply[A](head: A, tail: NonEmptyList[A]): NonEmptyList[A] =
        NonEmptyList(head, tail.toList)