package fs2

import org.scalatest.prop.CommonGenerators._
import org.scalatest.prop._

trait MiscellaneousGenerators {

  implicit val throwableGenerator: Generator[Throwable] =
    specificValue(new Err)

  implicit def arrayGenerator[A: Generator: reflect.ClassTag]: Generator[Array[A]] =
    lists[A].map(_.toArray)
}
