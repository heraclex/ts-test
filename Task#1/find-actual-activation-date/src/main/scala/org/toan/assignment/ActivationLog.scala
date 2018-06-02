package org.toan.assignment

import java.time.LocalDate

/**
  * Create by Toan Le on 5/29/2018
  */
case class ActivationLog(phoneNumber: String, activationDate: LocalDate, deActivationDate: Option[LocalDate]) {}
