package dhg.texvote.dataprep

import opennlp.scalabha.util.CollectionUtil._
import opennlp.scalabha.util.CollectionUtils._
import opennlp.scalabha.util.FileUtils._
import java.io.File

object TeliconCsvPrep {

  def main(args: Array[String]) {

    val legislatorDir = "data/scraped/legislator_pages/"
    val votesDir = "data/scraped/votes_pages/"

    val FilenameRe = """(\d{3})_(\d\d.)\.txt""".r
    val NameRe = """(.+) \(([A-Z])\)""".r

    def getMemnumSessions(dirname: String) =
      new File(legislatorDir).listFiles.map(_.getName).collect { case FilenameRe(memnum, session) => (memnum, session) }.toSet.unzip

    val (allMemnums, allSessions) = getMemnumSessions(legislatorDir)

    {
      val (checkMemnums, checkSessions) = getMemnumSessions(votesDir)
      assert(allMemnums.toSet == checkMemnums.toSet)
      assert(allSessions.toSet == checkSessions.toSet)
    }

    def findInfo(name: String, page: Vector[String]) =
      page.find(_.startsWith(name)).map(_.drop(name.size + 1).trim)

    val data =
      for (memnum <- allMemnums; session <- allSessions) yield {

        //    val memnum = 1
        //    val session = "721"

        println(memnum)
        println(session)

        val legislatorPage = readLines(legislatorDir + "%03d_%s.txt".format(memnum.toInt, session)).map(_.trim).toVector

        //legislatorPage.zipWithIndex.foreach { case (l, i) => println(i + " " + l) }

        assert(legislatorPage(0) == "Member Information")
        val (chamber, shift) = legislatorPage(3) match { case "House of" => ("House", 0); case "Senate" => ("Senate", -1) }
        val NameRe(name, party) = legislatorPage(6 + shift)

        val dob = findInfo("Date of Birth", legislatorPage)
        val degrees = findInfo("Degrees", legislatorPage)
        val church = findInfo("Church Affiliation", legislatorPage)

        //    println(name)
        //    println(party)
        //    println(chamber)
        //    println(dob)
        //    println(degrees)
        //    println(church)

        (memnum, session, name, party, chamber, dob, degrees, church)
      }

    val names = data.map(_._3)
    println(names.size)

    //readLines("data/scraped/votes_pages/%03d_%s.txt".format(memnum, session)).toVector

  }

}
