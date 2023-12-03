package sqala.util

trait Functor[F[_]]:
    extension [A, B] (x: F[A])
        def map(f: A => B): F[B]

trait Monad[F[_]] extends Functor[F]:
    def pure[A](x: A): F[A]

    extension [A, B] (x: F[A])
        def flatMap(f: A => F[B]): F[B]