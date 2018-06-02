package org.toan.assignment

import java.time.LocalDate
import org.scalatest.FunSuite

/**
  * Create by Toan Le on 6/2/2018
  */
class AlgorithmHelpersTestSuite extends FunSuite {

  test("phone number has only one activation record") {
    val data = List(
      generateActivationLog("0987000008", "2016-01-30", None)
    )

    assert(LocalDate.parse("2016-01-30") === AlgorithmHelpers.getActualActivation(data))
  }

  test("phone number has multiple activation records that are the same owner") {
    val data = List(
      generateActivationLog("0987000001","2016-12-01",None),
      generateActivationLog("0987000001","2016-03-01",Some("2016-12-01")),
      generateActivationLog("0987000001","2016-01-01",Some("2016-03-01"))
    )
    assert(LocalDate.parse("2016-01-01") === AlgorithmHelpers.getActualActivation(data))
  }

  test("phone number has multiple activation records that caused by different owners") {
    val data = List(
      generateActivationLog("0987000008","2018-01-30",None),
      generateActivationLog("0987000008","2016-09-30",Some("2017-09-30")),
      generateActivationLog("0987000008","2017-10-30",Some("2018-01-30")),
      generateActivationLog("0987000008","2013-09-01",Some("2014-09-01")),
      generateActivationLog("0987000008","2010-12-01",Some("2013-09-01")),
      generateActivationLog("0987000008","2015-03-01",Some("2016-09-30"))
    )
    assert(LocalDate.parse("2017-10-30") === AlgorithmHelpers.getActualActivation(data))
  }

  private def generateActivationLog(number: String, activationDate: String, deactivationDate: Option[String]): ActivationLog =
    ActivationLog(number, LocalDate.parse(activationDate), deactivationDate match {
      case Some(x) => Some(LocalDate.parse(x))
      case _ => None
    })
}
