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
import dhg.texvote.dataprep.telicon.TeliconConstituentPagesPrep._

/**
 * sbt "run-main dhg.texvote.dataprep.telicon.TeliconCsvPrep"
 */
object TeliconCsvPrep {

  def main(args: Array[String]) {

    val (allMemnums, allSessions) = getMemnumSessions(LegislatorDir, "txt")

    { val (checkMemnums, checkSessions) = getMemnumSessions(VotesDir, "txt"); assert(allMemnums.toSet == checkMemnums.toSet); assert(allSessions.toSet == checkSessions.toSet) }
    { val (checkMemnums, checkSessions) = getMemnumSessions(PopulationDir, "xml"); assert(allMemnums.toSet == checkMemnums.toSet, "%s != %s".format(allMemnums, checkMemnums)); assert(Set("82R") == checkSessions.toSet) }
    { val (checkMemnums, checkSessions) = getMemnumSessions(EduEmployDir, "xml"); assert(allMemnums.toSet == checkMemnums.toSet); assert(Set("82R") == checkSessions.toSet) }
    { val (checkMemnums, checkSessions) = getMemnumSessions(IncomeHousingDir, "xml"); assert(allMemnums.toSet == checkMemnums.toSet); assert(Set("82R") == checkSessions.toSet) }

    //val groupedSessions = List(("allSessions", allSessions)) // all sessions in one CSV
    val groupedSessions = allSessions.groupBy(_.take(2)) // each session, by first two digits, in its own CSV
    //val groupedSessions = allSessions.groupBy(identity) // each session ID in its own CSV

    for ((sessionGroupSuffix, sessionGroup) <- groupedSessions) {

      val pageInfoBySession =
        for (
          memnum <- allMemnums;
          session <- sessionGroup;
          legPage <- TeliconLegislatorPagePrep.getLegislatorPage(memnum, session);
          votes = TeliconVotesPagePrep.getVotesPage(memnum, session)
        ) yield { (legPage, votes) }

      //data.flatten.map(_._3).groupBy(n => getLastName(n)).mapVals(_.toSet).toVector.sortBy(_._1).filter { case (k, vs) => vs.size > 1 } foreach println

      val legislators =
        pageInfoBySession
          .groupBy(_._1.name) // group by the legislator name
          .map {
            case (name, entriesUnderSameName) =>
              val allLegInfo = entriesUnderSameName.map(_._1.features)

              val sessionsByMemnum = entriesUnderSameName.map(_._1).map(leg => (leg.memnum, leg.session)).groupByKey
              if (sessionsByMemnum.size > 1) println("%s %s".format(name, sessionsByMemnum))

              val info = allLegInfo.flatMap(_.keys).mapTo(k => allLegInfo.map(_(k))).toMap
              val votes = entriesUnderSameName.map(_._2).flatten.toMap

              val (earliestMemnum, earliestSession) = earliestMemnumAndSession(sessionsByMemnum)
              val constituentPage = getConstitutentPages(earliestMemnum, earliestSession)
              LegislatorInfo(name, sessionsByMemnum, info, votes, constituentPage)
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

      val usedVoteNames = voteNames.filter(voteName => sessionGroup.exists(voteName.session.startsWith))

      val infoItems = Vector("party", "chamber", "dob", "degrees", "church")
      val constituentItems = Vector(
        "pctForeignBorn",
        "pctNoncitizen",
        "pctRural",
        "pctSingleParentFamilies",
        "pctBachelorsDegree",
        "pctLivingInPoverty",
        "pctWhite")
      FileUtils.using(new CSVWriter(new BufferedWriter(new FileWriter("data/clean/voting_data-%s.csv".format(sessionGroupSuffix))))) { f =>
        f.writeNext((("name" +: "member numbers and sessions" +: infoItems) ++ constituentItems ++ usedVoteNames.map(_.toString)).toArray)
        for (LegislatorInfo(name, sessionsByMemnum, info, votes, constituentInfo) <- legislators.toVector.sortBy(leg => (leg.lastName, leg.name))) {
          val sessions = sessionsByMemnum.map { case (k, v) => "%s -> [%s]".format(k, v.toVector.sorted.mkString(",")) }.mkString(", ")
          val infoData = infoItems.map(item => info(item).toVector.sorted.map(_.getOrElse("-None-")).mkString("; "))
          val constituentData = constituentItems.map(constituentInfo)
          val voteItems = usedVoteNames.map(votes.getOrElse(_, ""))
          f.writeNext(((name +: sessions +: infoData) ++ constituentData ++ voteItems).toArray)
        }
      }
    }
  }

  case class LegislatorInfo(
    name: String,
    sessionsByMemnum: Map[String, Set[String]],
    info: Map[String, Set[Option[String]]],
    votes: Map[VoteName, String],
    constituentInfo: Map[String, String]) {

    def lastName = {
      val x0 = if (name.endsWith(".")) name.dropRight(1) else name
      val x1 = if (x0.toLowerCase.endsWith(" jr")) x0.dropRight(3) else x0
      val x2 = if (x1.toLowerCase.endsWith(" iii")) x1.dropRight(3) else x1
      val x3 = if (x2.endsWith(",")) x2.dropRight(1) else x2
      x3.split(" ").last
    }
  }

  def earliestMemnumAndSession(sessionsByMemnum: Map[String, Set[String]]) = {
    val ungrouped = sessionsByMemnum.ungroup
    val (earliestMemnum, earliestSession) = ungrouped
      .mapVals(_.replace('R', '0'))
      .minBy { case (memnum, session) => session }
    (earliestMemnum, earliestSession)
  }

}
