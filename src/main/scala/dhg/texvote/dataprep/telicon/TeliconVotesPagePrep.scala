package dhg.texvote.dataprep.telicon

import dhg.util.CollectionUtil._
import dhg.util.FileUtil
import dhg.util.FileUtil._
import au.com.bytecode.opencsv.CSVWriter
import java.io.BufferedWriter
import java.io.FileWriter
import dhg.texvote.dataprep.telicon.TeliconDataprepHelper._

object TeliconVotesPagePrep {

  case class VoteName(session: String, bill: String, motion: String) {
    override def toString = session + " - " + bill + " - " + motion
  }

  /**
   * @return Map[BillName, Map[MotionName, Vote]]
   */
  def getVotesPage(memnum: String, session: String): Map[VoteName, String] = {
    val BillLineRe = """  ((HB|SB|HR|SR|HCR|SCR|HJR|SJR) \d+\t.*)""".r
    val VoteLineRe = """ \t([YNAEPCX])\t[YN]\t\d\d/\d\d/\d\d\t(.+)""".r

    val page = File(VotesDir + "%03d_%s.txt".format(memnum.toInt, session)).readLines.toVector

    if (page(0) == "Invalid Parameters")
      Map.empty

    else {
      val headerLines = page.indexOf(" \tVote	Maj	Date	Number	Vote Description") + 1
      assert(page.size > 12, "In [%s %s]".format(memnum, session))
      assert(page(page.length - 12) == " \t Votes 	 	 Percent \t ", page(page.length - 12))

      val votes =
        page.drop(headerLines).dropRight(12)
          .foldLeft((None: Option[String], List[(VoteName, String)]())) {
            case ((Some(bill), votes), line) =>
              line match {
                case BillLineRe(newBill, _) => (Some(newBill), votes)
                case VoteLineRe(vote, motion) => (Some(bill), (VoteName(session, bill, motion), vote) :: votes)
              }
            case ((None, Nil), BillLineRe(newBill, _)) => (Some(newBill), Nil)
          }._2

      votes.toMap
    }
  }

}
