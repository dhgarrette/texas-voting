package dhg.texvote.dataprep

import opennlp.scalabha.util.CollectionUtil._
import opennlp.scalabha.util.CollectionUtils._
import opennlp.scalabha.util.FileUtils._
import java.io.File

object TeliconCsvPrep {

  val legislatorDir = "data/scraped/legislator_pages/"
  val votesDir = "data/scraped/votes_pages/"

  val FilenameRe = """(\d{3})_(\d\d.)\.txt""".r
  val NameRe = """(.+) \(([A-Z])\)""".r

  def main(args: Array[String]) {

    def getMemnumSessions(dirname: String) =
      new File(legislatorDir).listFiles.map(_.getName).collect { case FilenameRe(memnum, session) => (memnum, session) }.toSet.unzip

    val (allMemnums, allSessions) = getMemnumSessions(legislatorDir)
    val (checkMemnums, checkSessions) = getMemnumSessions(votesDir); assert(allMemnums.toSet == checkMemnums.toSet); assert(allSessions.toSet == checkSessions.toSet)

    def findInfo(name: String, page: Vector[String]) =
      page.find(_.startsWith(name)).map(_.drop(name.size + 1).trim).orElse(Some("-NONE-"))

    val data =
      for (memnum <- allMemnums; session <- allSessions) yield {

        //        val memnum = "41"
        //        val session = "74R"

        //        println(memnum)
        //        println(session)

        val legislatorPage = getLegislatorPage(memnum, session)

        //legislatorPage.zipWithIndex.foreach { case (l, i) => println(i + " " + l) }

        legislatorPage(0) match {
          case "Member Information" =>
            val (chamber, shift) = legislatorPage(3) match { case "House of" => ("House", 0); case "Senate" => ("Senate", -1) }
            val NameRe(name, party) = legislatorPage(6 + shift)

            Some(
              memnum,
              session,
              name,
              Map(
                "party" -> Some(party),
                "chamber" -> Some(chamber),
                "dob" -> findInfo("Date of Birth", legislatorPage),
                "degrees" -> findInfo("Degrees", legislatorPage),
                "church" -> findInfo("Church Affiliation", legislatorPage)))

          case "Unknown Member" => None
        }
      }

    val byName = data.flatten
      .groupBy(_._3)
      .mapVals { entries =>
        val allInfo = entries.map(_._4)
        val sessionsByMemnum = entries.map { case (memnum, session, _, _) => (memnum, session) }.groupByKey
        val info = allInfo.flatMap(_.keySet).mapTo(k => allInfo.flatMap(_(k)))
        allInfo.map(m => (m("party"), m("chamber"), m("dob"), m("degrees"), m("church")))
        (sessionsByMemnum, info)
      }

    for ((name, (sessionsByMemnum, info)) <- byName.toVector.sortBy(n => (n._1.split(" ").last, n._1))) {
      println("%s %s".format(name, sessionsByMemnum))
      info foreach println
      println
    }

    //readLines("data/scraped/votes_pages/%03d_%s.txt".format(memnum, session)).toVector

  }

  def getLegislatorPage(memnum: String, session: String) = {
    val page = readLines(legislatorDir + "%03d_%s.txt".format(memnum.toInt, session)).map(_.trim).toVector

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
