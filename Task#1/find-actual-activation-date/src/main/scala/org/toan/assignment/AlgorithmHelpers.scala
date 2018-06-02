package org.toan.assignment

import java.time.LocalDate

/**
  * Create by Toan Le on 5/31/2018
  */
object AlgorithmHelpers {
  def getActualActivation(phoneActivationLogs: Iterable[ActivationLog]): LocalDate = {

    // sort decs by activation date
    val sorted = phoneActivationLogs.toList.sortWith((x, y) => x.activationDate.compareTo(y.activationDate) > 0)

    // Find the first interrupt point
    def findActualActivation(accLog: ActivationLog, remaining: List[ActivationLog]): ActivationLog =
      if (remaining.nonEmpty && accLog.activationDate.equals(remaining.head.deActivationDate.get))
        findActualActivation(remaining.head, remaining.tail)
      else
        accLog

    findActualActivation(sorted.head, sorted.tail).activationDate
  }
}
