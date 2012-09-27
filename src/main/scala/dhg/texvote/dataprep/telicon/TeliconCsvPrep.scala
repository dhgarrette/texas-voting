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
import dhg.texvote.dataprep.telicon.TeliconVotesPagePrep._

object TeliconCsvPrep {

  val LegislatorDir = "data/scraped/legislator_pages/"
  val VotesDir = "data/scraped/votes_pages/"
  val PopulationDir = "data/scraped/population_pages/"
  val EduEmployDir = "data/scraped/edu_employ_pages/"
  val IncomeHousingDir = "data/scraped/income_housing_pages/"

  val FilenameRe = """(\d{3})_(\d\d.)\.txt""".r
  val NameRe = """(.+) \(([A-Z])\)""".r

  val NoneString = "-NONE-"

  def main(args: Array[String]) {

    val (allMemnums, allSessions) = getMemnumSessions(LegislatorDir, "txt")

    { val (checkMemnums, checkSessions) = getMemnumSessions(VotesDir, "txt"); assert(allMemnums.toSet == checkMemnums.toSet); assert(allSessions.toSet == checkSessions.toSet) }
    { val (checkMemnums, checkSessions) = getMemnumSessions(PopulationDir, "xml"); assert(allMemnums.toSet == checkMemnums.toSet, "%s != %s".format(allMemnums, checkMemnums)); assert(Set("82R") == checkSessions.toSet) }
    { val (checkMemnums, checkSessions) = getMemnumSessions(EduEmployDir, "xml"); assert(allMemnums.toSet == checkMemnums.toSet); assert(Set("82R") == checkSessions.toSet) }
    { val (checkMemnums, checkSessions) = getMemnumSessions(IncomeHousingDir, "xml"); assert(allMemnums.toSet == checkMemnums.toSet); assert(Set("82R") == checkSessions.toSet) }

    val legislatorsAndVotesBySession =
      for (
        memnum <- allMemnums;
        session <- allSessions;
        legPage <- TeliconLegislatorPagePrep.getLegislatorPage(memnum, session);
        votes = TeliconVotesPagePrep.getVotesPage(memnum, session)
      ) yield { (legPage, votes) }

    //data.flatten.map(_._3).groupBy(n => getLastName(n)).mapVals(_.toSet).toVector.sortBy(_._1).filter { case (k, vs) => vs.size > 1 } foreach println

    val legislators =
      legislatorsAndVotesBySession
        .groupBy(_._1.name)
        .map {
          case (name, entriesUnderSameName) =>
            val allLegInfo = entriesUnderSameName.map(_._1.features)
            val sessionsByMemnum = entriesUnderSameName.map(_._1).map(leg => (leg.memnum, leg.session)).groupByKey
            val info = allLegInfo.flatMap(_.keys).mapTo(k => allLegInfo.map(_(k))).toMap
            val votes = entriesUnderSameName.map(_._2).flatten.toMap
            LegislatorInfo(name, sessionsByMemnum, info, votes)
        }

    //val BillNameSortCriteriaRe = """(.{3}) - (.{2}.?) (\d+)\t.* - (.+)\t.*""".r
    //BillNameSortCriteriaRe(session, billType, billNum, motionId)
    val BillRe = """(.{2}.?) (\d+)\t.*""".r
    val MotionRe = """(.+)\t.*""".r
    val voteNames =
      legislators.flatMap(_.votes.map(_._1)).toSet.toVector.sortBy {
        case VoteName(session, BillRe(billType, billNum), MotionRe(motionId)) =>
          (session.replace('R', '0'), billType, billNum.toInt, motionId)
      }
    FileUtils.writeUsing("data/allVoteNames.txt") { f => voteNames.foreach(s => f.write(s + "\n")) }

    //val groupedSessions = List(("allSessions", allSessions)) // all sessions in one CSV
    //val groupedSessions = allSessions.groupBy(_.take(2)) // each session, by first two digits, in its own CSV
    val groupedSessions = allSessions.groupBy(identity) // each session ID in its own CSV

    for ((sessionGroupSuffix, sessionGroup) <- groupedSessions) {
      val usedVoteNames = voteNames.filter(voteName => sessionGroup.exists(voteName.session.startsWith))

      val infoItems = Vector("party", "chamber", "dob", "degrees", "church")
      FileUtils.using(new CSVWriter(new BufferedWriter(new FileWriter("data/clean/voting_data-%s.csv".format(sessionGroupSuffix))))) { f =>
        f.writeNext((("name" +: "member numbers and sessions" +: infoItems) ++ usedVoteNames.map(_.toString)).toArray)
        for (LegislatorInfo(name, sessionsByMemnum, info, votes) <- legislators.toVector.sortBy(leg => (leg.lastName, leg.name))) {
          val sessions = sessionsByMemnum.map { case (k, v) => "%s -> [%s]".format(k, v.toVector.sorted.mkString(",")) }.mkString(", ")
          val items = infoItems.map(item => info(item).toVector.sorted.map(_.getOrElse("-None-")).mkString("; "))
          val voteItems = usedVoteNames.map(votes.getOrElse(_, ""))
          f.writeNext(((name +: sessions +: items) ++ voteItems).toArray)
        }
      }
    }
  }

  case class LegislatorInfo(
    name: String,
    sessionsByMemnum: Map[String, Set[String]],
    info: Map[String, Set[Option[String]]],
    votes: Map[VoteName, String]) {

    def lastName = {
      val x0 = if (name.endsWith(".")) name.dropRight(1) else name
      val x1 = if (x0.toLowerCase.endsWith(" jr")) x0.dropRight(3) else x0
      val x2 = if (x1.toLowerCase.endsWith(" iii")) x1.dropRight(3) else x1
      val x3 = if (x2.endsWith(",")) x2.dropRight(1) else x2
      x3.split(" ").last
    }
  }

}
