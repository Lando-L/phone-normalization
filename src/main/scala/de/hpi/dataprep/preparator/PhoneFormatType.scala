package de.hpi.dataprep.preparator

import de.hpi.dataprep.core.Metadata

case class PhoneFormatType(format: String) extends Metadata {
	def name: String = PhoneFormatType.name
}

object PhoneFormatType {
	val name: String = "phoneFormatType"

	def fromString(number: String): Option[PhoneFormatType] = {
		Some(PhoneFormatType("ddmmyyyy"))
	}

	def transform(from: PhoneFormatType, to: PhoneFormatType)(number: String): String = {
		(from, to) match {
			case (PhoneFormatType("mmddyyyy"), PhoneFormatType("ddmmyyyy")) => number.slice(2,4) + number.take(2) + number.drop(4)
			case _ => number
		}
	}
}
