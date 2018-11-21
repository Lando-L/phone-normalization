package de.hpi.dataprep.preparator

import de.hpi.dataprep.core.{Metadata, Preparator}
import org.apache.spark.sql.{DataFrame, Encoder}
import org.apache.spark.sql.functions._

object PhoneNormalizationPreparator {
	def changePhoneFormat(attribute: String, sourceFormat: PhoneFormatType, targetFormat: PhoneFormatType)(implicit encoder: Encoder[String]): Preparator[List[String], DataFrame] =
		new Preparator[List[String], DataFrame] {
			override protected def checkPrerequisites(config: Map[String, String])(data: Set[Metadata]): Option[List[String]] = {
				if (!data.contains(sourceFormat)) {
					Some(List("Metadata does not contain the right source format"))
				} else {
					None
				}
			}

			override protected def executePreparator(config: Map[String, String])(data: DataFrame): DataFrame = {
				data.withColumn(
					attribute,
					udf { number: String =>
						PhoneFormatType.transform(sourceFormat, targetFormat)(number)
					}.apply(data(attribute))
				)
			}
		}

	def changePhoneFormat(attribute: String, targetFormat: PhoneFormatType)(implicit encoder: Encoder[String]): Preparator[List[String], DataFrame] =
		new Preparator[List[String], DataFrame] {
			override protected def checkPrerequisites(config: Map[String, String])(data: Set[Metadata]): Option[List[String]] = {
				None
			}

			override protected def executePreparator(config: Map[String, String])(data: DataFrame): DataFrame = {
				data.withColumn(
					attribute,
					udf { number: String =>
						PhoneFormatType
							.fromString(number)
							.map(format => PhoneFormatType.transform(format, targetFormat)(number))
					}.apply(data(attribute))
				)
			}
		}
}
