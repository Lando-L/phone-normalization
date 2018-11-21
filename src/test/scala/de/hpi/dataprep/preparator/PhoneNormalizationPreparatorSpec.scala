package de.hpi.dataprep.preparator

import de.hpi.dataprep.core.{Metadata, Preparator}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FlatSpec, Matchers}

class PhoneNormalizationPreparatorSpec extends FlatSpec with Matchers {
	"changePhoneFormat" should "create a preparator normalizing phone data given the attribute, source format and the target format" in {
		val sparkSession = SparkSession
  		.builder()
  		.appName("testing-suite")
  		.master("local")
  		.getOrCreate()

		import sparkSession.implicits._

		val attribute: String = "phone"
		val sourceFormat: PhoneFormatType = PhoneFormatType("mmddyyyy")
		val targetFormat: PhoneFormatType = PhoneFormatType("ddmmyyyy")

		val metadata: Set[Metadata] = Set(sourceFormat)
		val data: DataFrame = Seq("12211990", "04011996").toDF(attribute)
		val expected = Set("21121990", "01041996")

		val preparator: Preparator[List[String], DataFrame] = PhoneNormalizationPreparator.changePhoneFormat(attribute, sourceFormat, targetFormat)

		val result = preparator.execute(Map())(metadata, data).map(_.as[String].collect().toSet)

		result.isRight shouldBe true
		result foreach { _ shouldEqual expected }
	}
}
