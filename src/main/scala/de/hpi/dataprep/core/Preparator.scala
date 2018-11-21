package de.hpi.dataprep.core

trait Preparator[E, A] {
	protected def checkPrerequisites(config: Map[String, String])(data: Set[Metadata]): Option[E]
	protected def executePreparator(config: Map[String, String])(data: A): A

	def execute(config: Map[String, String])(metadata: Set[Metadata], data: A): Either[E, A] = {
		checkPrerequisites(config)(metadata) match {
			case Some(errors) => Left(errors)
			case None => Right(executePreparator(config)(data))
		}
	}
}

object Preparator {
	def execute[E, A](config: Map[String, String])(metadata: Set[Metadata], data: A)(implicit p: Preparator[E, A]): Either[E, A] =
		p.execute(config)(metadata, data)
}
