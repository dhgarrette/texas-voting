package dhg.texvote.dataprep.telicon

import opennlp.scalabha.util.CollectionUtil._
import opennlp.scalabha.util.CollectionUtils._
import opennlp.scalabha.util.FileUtils
import opennlp.scalabha.util.FileUtils._
import java.io.File
import au.com.bytecode.opencsv.CSVWriter
import java.io.BufferedWriter
import java.io.FileWriter
import dhg.texvote.dataprep.telicon.TeliconDataprepHelper._

object TeliconLegislatorPagePrep {

  case class LegislatorPage(
    memnum: String,
    session: String,
    name: String,
    features: Map[String, Option[String]])

  def getLegislatorPage(memnum: String, session: String) = {
    val pageLines = getPageLines(memnum, session)
    val NameRe = """(.+) \(([A-Z])\)""".r

    //legislatorPage.zipWithIndex.foreach { case (l, i) => println(i + " " + l) }

    pageLines(0) match {
      case "Member Information" =>
        val (chamber, shift) = pageLines(3) match { case "House of" => ("House", 0); case "Senate" => ("Senate", -1) }
        val NameRe(name, party) = pageLines(6 + shift)

        Some(LegislatorPage(
          memnum,
          session,
          normalizeName(name),
          Map(
            "party" -> Some(party),
            "chamber" -> Some(chamber),
            "dob" -> findInfo("Date of Birth", pageLines).map(normalizeDate),
            "degrees" -> findInfo("Degrees", pageLines),
            "church" -> findInfo("Church Affiliation", pageLines))))

      case "Unknown Member" => None
    }
  }

  def findInfo(name: String, page: Vector[String]) = {
    page.find(_.startsWith(name)).map(_.drop(name.size + 1).trim)
  }

  def getPageLines(memnum: String, session: String) = {
    val page = readLines(LegislatorDir + "%03d_%s.txt".format(memnum.toInt, session), "latin1").map(_.trim).toVector

    val x1 =
      if (memnum.toInt == 41 && session == "74R" && page(3) == "41") {
        val newpart = Vector("House of", "Representatives", "District 41")
        page.take(3) ++ newpart ++ page.drop(4)
      }
      else if (memnum.toInt == 63 && session == "73R" && page(3) == "2") {
        val newpart = Vector("House of", "Representatives", "District 2")
        page.take(3) ++ newpart ++ page.drop(4)
      }
      else page

    x1
  }

}
