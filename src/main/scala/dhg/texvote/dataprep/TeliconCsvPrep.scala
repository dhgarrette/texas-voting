package dhg.texvote.dataprep

import opennlp.scalabha.util.CollectionUtil._
import opennlp.scalabha.util.CollectionUtils._
import opennlp.scalabha.util.FileUtils
import opennlp.scalabha.util.FileUtils._
import java.io.File
import au.com.bytecode.opencsv.CSVWriter
import java.io.BufferedWriter
import java.io.FileWriter

object TeliconCsvPrep {

  val legislatorDir = "data/scraped/legislator_pages/"
  val votesDir = "data/scraped/votes_pages/"

  val FilenameRe = """(\d{3})_(\d\d.)\.txt""".r
  val NameRe = """(.+) \(([A-Z])\)""".r

  val NoneString = "-NONE-"

  def main(args: Array[String]) {

    def getMemnumSessions(dirname: String) =
      new File(legislatorDir).listFiles.map(_.getName).collect { case FilenameRe(memnum, session) => (memnum, session) }.toSet.unzip

    val (allMemnums, allSessions) = getMemnumSessions(legislatorDir)
    val (checkMemnums, checkSessions) = getMemnumSessions(votesDir); assert(allMemnums.toSet == checkMemnums.toSet); assert(allSessions.toSet == checkSessions.toSet)

    def findInfo(name: String, page: Vector[String]) =
      page.find(_.startsWith(name)).map(_.drop(name.size + 1).trim)

    val data =
      for (memnum <- allMemnums; session <- allSessions) yield {

        //        val memnum = "41"
        //        val session = "74R"

        // println("%s %s".format(memnum, session))

        val legislatorPage = getLegislatorPage(memnum, session)

        //legislatorPage.zipWithIndex.foreach { case (l, i) => println(i + " " + l) }

        legislatorPage(0) match {
          case "Member Information" =>
            val (chamber, shift) = legislatorPage(3) match { case "House of" => ("House", 0); case "Senate" => ("Senate", -1) }
            val NameRe(name, party) = legislatorPage(6 + shift)

            val votes = getVotesPage(memnum, session)

            Some(
              memnum,
              session,
              normalizeName(name),
              Map(
                "party" -> Some(party),
                "chamber" -> Some(chamber),
                "dob" -> findInfo("Date of Birth", legislatorPage).map(normalizeDate),
                "degrees" -> findInfo("Degrees", legislatorPage),
                "church" -> findInfo("Church Affiliation", legislatorPage)),
              votes)

          case "Unknown Member" => None
        }
      }

    //data.flatten.map(_._3).groupBy(n => getLastName(n)).mapVals(_.toSet).toVector.sortBy(_._1).filter { case (k, vs) => vs.size > 1 } foreach println

    val byName = data.flatten
      .groupBy(_._3)
      .mapVals { entries =>
        val allInfo = entries.map(_._4)
        val sessionsByMemnum = entries.map { case (memnum, session, _, _, _) => (memnum, session) }.groupByKey
        val info = allInfo.flatMap(_.keys).mapTo(k => allInfo.map(_(k))).toMap
        val votes = entries.map(_._5).flatten.ungroup.map { case (bill, (motion, vote)) => (bill + " - " + motion, vote) }.toMap
        (sessionsByMemnum, info, votes)
      }

    val BillNameSortCriteriaRe = """(.{3}) - (.{2}.?) (\d+)\t.* - (.+)\t.*""".r
    val voteNames = byName.flatMap(_._2._3.keys).toSet.toVector.sortBy {
      case BillNameSortCriteriaRe(session, billType, billNum, motionId) =>
        (session.replace('R', '0'), billType, billNum.toInt, motionId)
    }
    //FileUtils.writeUsing("data/allVoteNames.txt") { f => voteNames.foreach(s => f.write(s + "\n")) }

    //val groupedSessions = List(("allSessions", allSessions)) // all sessions in one CSV
    //val groupedSessions = allSessions.groupBy(_.take(2)) // each session, by first two digits, in its own CSV
    val groupedSessions = allSessions.groupBy(identity) // each session ID in its own CSV

    for ((sessionGroupSuffix, sessionGroup) <- groupedSessions) {
      val usedVoteNames = voteNames.filter(voteName => sessionGroup.exists(voteName.startsWith))

      val infoItems = Vector("party", "chamber", "dob", "degrees", "church")
      FileUtils.using(new CSVWriter(new BufferedWriter(new FileWriter("data/clean/voting_data-%s.csv".format(sessionGroupSuffix))))) { f =>
        f.writeNext((("name" +: "member numbers and sessions" +: infoItems) ++ usedVoteNames).toArray)
        for ((name, (sessionsByMemnum, info, votes)) <- byName.toVector.sortBy(n => (getLastName(n._1), n._1))) {
          val sessions = sessionsByMemnum.map { case (k, v) => "%s -> [%s]".format(k, v.toVector.sorted.mkString(",")) }.mkString(", ")
          val items = infoItems.map(item => info(item).toVector.sorted.map(_.getOrElse("-None-")).mkString("; "))
          val voteItems = usedVoteNames.map(votes.getOrElse(_, ""))
          f.writeNext(((name +: sessions +: items) ++ voteItems).toArray)
        }
      }
    }
  }

  def getLegislatorPage(memnum: String, session: String) = {
    val page = readLines(legislatorDir + "%03d_%s.txt".format(memnum.toInt, session), "latin1").map(_.trim).toVector

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

  /**
   * @return Map[BillName, Map[MotionName, Vote]]
   */
  def getVotesPage(memnum: String, session: String) = {
    val BillLineRe = """  ((HB|SB|HR|SR|HCR|SCR|HJR|SJR) \d+\t.*)""".r
    val VoteLineRe = """ \t([YNAEPCX])\t[YN]\t\d\d/\d\d/\d\d\t(.+)""".r

    val page = readLines(votesDir + "%03d_%s.txt".format(memnum.toInt, session)).toVector

    if (page(0) == "Invalid Parameters")
      Map[String, Map[String, String]]()

    else {
      val headerLines = page.indexOf(" \tVote	Maj	Date	Number	Vote Description") + 1
      assert(page.size > 12, "In [%s %s]".format(memnum, session))
      assert(page(page.length - 12) == " \t Votes 	 	 Percent \t ", page(page.length - 12))

      page.drop(headerLines).dropRight(12).foldLeft(
        List[(String, Map[String, String])]()) {
          case (((curBill, votes) :: z), line) =>
            line match {
              case BillLineRe(bill, _) =>
                (session + " - " + bill, Map[String, String]()) :: (curBill, votes) :: z
              case VoteLineRe(vote, motion) =>
                (curBill, votes.updated(motion, vote)) :: z
            }
          case (Nil, BillLineRe(bill, _)) =>
            (session + " - " + bill, Map[String, String]()) :: Nil
        }.toMap
    }
  }

  def getLastName(name: String): String = {
    val x0 = if (name.endsWith(".")) name.dropRight(1) else name
    val x1 = if (x0.toLowerCase.endsWith(" jr")) x0.dropRight(3) else x0
    val x2 = if (x1.toLowerCase.endsWith(" iii")) x1.dropRight(3) else x1
    val x3 = if (x2.endsWith(",")) x2.dropRight(1) else x2
    x3.split(" ").last
  }

  def normalizeName(name: String) = {
    name match {
      case "Alma A. Allen" => "Alma Allen"
      case "Roberto R. Alonzo" => "Roberto Alonzo"
      case "Charles Doc Anderson" => "Charles (Doc) Anderson"
      case "Ken Armbrister" | "Kenneth L. (Ken) Armbrister" => "Kenneth Armbrister"
      case "Kevin E. Bailey" => "Kevin Bailey"
      case "David E. Bernsen" => "David Bernsen"
      case "Charles (Layton) Black" => "Layton Black"
      case "Elton L. Bomer" => "Elton Bomer"
      case "Fred M. Bosse" => "Fred Bosse"
      case "Dan H. Branch" => "Dan Branch"
      case "Kenneth (Kim) Brimer" => "Kim Brimer"
      case "Betty J. Brown" => "Betty Brown"
      case "J. E. (Buster) Brown" | "J.E. (Buster) Brown" | "J.E. \"Buster\" Brown" => "Buster Brown"
      case "David H. Cain" => "David Cain"
      case "William \"Bill\" A. Callegari" | "William A. (Bill) Callegari" => "Bill Callegari"
      case "Ben D. Campbell" => "Ben Campbell"
      case "Jaime L. Capelo" | "Jaime L. Capelo, Jr." => "Jaime Capelo"
      case "Bill G. Carter" => "Bill Carter"
      case "Warren D. Chisum" => "Warren Chisum"
      case "Garnet F. Coleman" => "Garnet Coleman"
      case "Karyne J. Conley" | "Karyne Jones Conley" => "Karyne Conley"
      case "John R. Cook" => "John Cook"
      case "Robert (Robby) L. Cook" | "Robert Laird Cook, III" | "Robert L. (Robby) Cook" => "Robby Cook"
      case "Frank J. Corte Jr." | "Frank Corte, Jr." | "Frank J. Corte, Jr." | "Frank Corte, Jr." => "Frank Corte"
      case "Thomas R. Craddick" => "Tom Craddick"
      case "C. Brandon Creighton" => "Brandon Creighton"
      case "Henry R. Cuellar" => "Henry Cuellar"
      case "John E. Davis" => "John Davis"
      case "Homer Pete Dear" => "Homer Dear"
      case "Wilhelmina R. Delco" => "Wilhelmina Delco"
      case "Dianne White Delisi" => "Dianne Delisi"
      case "Joseph D. Deshotel" | "Joseph \"Joe\" D. Deshotel" => "Joe Deshotel"
      case "Bob Deuell" => "Robert Deuell"
      case "Dawnna Dukes" => "Dawna Dukes"
      case "Robert L. Duncan" => "Robert Duncan"
      case "Harold Dutton Jr." | "Harold V. Dutton Jr." | "Harold V. Dutton, Jr." | "Harold Dutton, Jr." => "Harold Dutton"
      case "Robert Earley" => "Robert A. Earley"
      case "Harryette B. Ehrhardt" => "Harryette Ehrhardt"
      case "Dan P. Ellis" => "Dan Ellis"
      case "Rodney Glenn Ellis" | "Rodney G. Ellis" => "Rodney Ellis"
      case "Juan M. Escobar" => "Juan Escobar"
      case "Craig L. Estes" => "Craig Estes"
      case "David L. Farabee" => "David Farabee"
      case "Jose Farias" => "Joe Farias"
      case "Jessica Cristina Farrar" => "Jessica Farrar"
      case "Charles A. Finnell" => "Charles Finnell"
      case "Ismael \"Kino\" Flores" | "Ismael (Kino) Flores" => "Kino Flores"
      case "Stephen J. Frost" | "Stephen James Frost" => "Stephen Frost"
      case "Pete P. Gallego" => "Pete Gallego"
      case "Mario V. Gallegos, Jr." | "Mario V. Gallegos" | "Mario Gallegos, Jr." => "Mario Gallegos"
      case "Michael L. Galloway" => "Mike Galloway"
      case "Eddie De La Garza" => "Eddie de la Garza"
      case "Charlie Geren" => "Charles Geren"
      case "Bob D. Glaze" => "Bob Glaze"
      case "Patrick \"Pat\" Haggerty" | "Patrick (Pat) Haggerty" | "Patrick B. Haggerty" => "Pat Haggerty"
      case "Mike (Tuffy) Hamilton" | "Mike Tuffy Hamilton" => "Mike Hamilton"
      case "Richard \"Rick\" L. Hardcastle" | "Richard L. (Rick) Hardcastle" => "Rick Hardcastle"
      case "Christopher (Chris) Harris" | "Christopher J. (Chris) Harris" => "Chris Harris"
      case "Will Ford Hartnett" => "Will Hartnett"
      case "Talmadge L. Heflin" => "Talmadge Heflin"
      case "Glenn Hegar, Jr." => "Glenn Hegar"
      case "Ana E. Hernandez" => "Ana Hernandez"
      case "Allen Ross Hightower, Jr." | "Allen Ross Hightower" => "Allen Hightower"
      case "Paul J. Hilbert" => "Paul Hilbert"
      case "Juan (Chuy) Hinojosa" => "Juan Hinojosa"
      case "Ruben W. Hope, Jr." | "Ruben Hope, Jr." => "Ruben Hope"
      case "Charles (Chuck) Hopson" => "Chuck Hopson"
      case "Todd A. Hunter" => "Todd Hunter"
      case "Eddie Lucio, III" => "Eddie Lucio III"
      case "Samuel William Hudson, III" | "Samuel W. Hudson, III" => "Samuel W. Hudson, III"
      case "Carl H. Isett" => "Carl Isett"
      case "James M. (Mike) Jackson" => "Mike Jackson"
      case "Delwin L. Jones" => "Delwin Jones"
      case "Jesse W. Jones" => "Jesse Jones"
      case "Robert A. Junell" => "Robert Junell"
      case "James L. (Jim) Keffer" => "Jim Keffer"
      case "William (Bill) Keffer" => "Bill Keffer"
      case "Susan L. King" => "Susan King"
      case "Tracy O. King" => "Tracy King"
      case "Lois W. Kolkhorst" => "Lois Kolkhorst"
      case "James E. (Pete) Laney" | "James (Pete) E. Laney" => "James (Pete) Laney"
      case "David McQuade Leibowitz" => "David Leibowitz"
      case "Ronald E. (Ron) Lewis" | "Ron E. Lewis" => "Ron Lewis"
      case "Jon S. Lindsay" => "Jon Lindsay"
      case "Elizabeth (Libby) Linebarger" => "Libby Linebarger"
      case "John Amos Longoria" | "John A. Longoria" => "John Longoria"
      case "Eduardo A. (Eddie) Lucio, Jr." | "Eduardo (Eddie) Lucio, Jr." => "Eddie Lucio, Jr."
      case "Jerry A. Madden" => "Jerry Madden"
      case "Frank Lloyd Madla" | "Frank L. Madla, Jr." | "Frank L. Madla" => "Frank Madla"
      case "Ken E. Marchant" | "Kenny Marchant" => "Ken Marchant"
      case "Armando \"Mando\" Martinez" | "Armando (Mando) Martinez" => "Armando Alfonso Martinez"
      case "Trey Martinez Fischer" => "Trey Martinez-Fischer"
      case "Ruth Jones McClendon" => "Ruth McClendon"
      case "Nancy H. McDonald" => "Nancy McDonald"
      case "Michael J. (Mike) Moncrief" => "Mike Moncrief"
      case "Joe E. Moreno" => "Joe Moreno"
      case "Paul Cruz Moreno" | "Paul C. Moreno" => "Paul Moreno"
      case "Geanie W. Morrison" => "Geanie Morrison"
      case "Eliott Naishtat" => "Elliott Naishtat"
      case "Joe M. Nixon" => "Joe Nixon"
      case "Richard (Rick) Noriega" | "Richard Noriega" => "Rick Noriega"
      case "William K. (Keith) Oakley" => "Keith Oakley"
      case "Stephen E. (Steve) Ogden" | "Stephen Ogden" | "Steven Eugene Ogden" => "Steve Ogden"
      case "Rene O. Oliveira" => "Rene Oliveira"
      case "Dora F. Olivo" => "Dora Olivo"
      case "Solomon P. Ortiz, Jr." => "Solomon Ortiz, Jr."
      case "Carl A. Parker" => "Carl Parker"
      case "Jerry E. Patterson" => "Jerry Patterson"
      case "L.P. (Pete) Patterson" => "L.P. Pete Patterson"
      case "Allen D. Place, Jr." => "Allen Place, Jr."
      case "Joe C. Pickett" | "Joseph (Joe) C. Pickett" | "Joseph Pickett" => "Joe Pickett"
      case "Robert R. Puente" => "Robert Puente"
      case "Inocente (Chente) Quintanilla" | "Chente Quintanillia" => "Chente Quintanilla"
      case "Irma L. Rangel" => "Irma Rangel"
      case "Aaron Pena, Jr." => "Aaron Pena"
      case "Richard Pena Raymond" | "Richard E. Raymond" => "Richard Raymond"
      case "Arthur C. (Art) Reyna" => "Arthur (Art) Reyna"
      case "Allan B. Ritter" | "Allen Ritter" => "Allan Ritter"
      case "Ciro D. Rodriguez" => "Ciro Rodriguez"
      case "Patrick M. Rose" => "Patrick Rose"
      case "Margaret Ann (Peggy) Rosson" => "Peggy Rosson"
      case "Jimmy Dean (Jim) Rudd" => "Jim D. Rudd"
      case "Paul L. Sadler" => "Paul Sadler"
      case "Sue A. Schechter" => "Sue Schechter"
      case "Eugene (Gene) T. Seaman" => "Gene Seaman"
      case "Curtis Lee Seidlits, Jr." => "Curtis L. Seidlits, Jr."
      case "David M. Sibley" => "David Sibley"
      case "William M. (Bill) Sims" => "Bill Sims"
      case "John H. Shields" => "John Shields"
      case "Burt R. Solomons" | "Burt P. Solomons" => "Burt Solomons"
      case "Mark W. Stiles" => "Mark Stiles"
      case "Joe R. Straus, III" => "Joe Straus"
      case "David A. Swinford" => "David Swinford"
      case "Robert E. Talton" => "Robert Talton"
      case "Barry B. Telford" => "Barry Telford"
      case "Dale B. Tillery" => "Dale Tillery"
      case "Carlos F. Truan" => "Carlos Truan"
      case "James W. (Jim) Turner" => "Jim Turner"
      case "Robert (Bob) Turner" | "Robert R. (Bob) Turner" => "Bob Turner"
      case "D. R. Tom Uher" | "D.R. (Tom) Uher" => "D. R. (Tom) Uher"
      case "Carlos (Charlie) Uresti" | "Carlos I. Uresti" => "Carlos Uresti"
      case "Michael (Mike) Villarreal" => "Mike Villarreal"
      case "Jack C. Vowell" => "Jack Vowell"
      case "Gary L. Walker" => "Gary Walker"
      case "Kirk P. Watson" => "Kirk Watson"
      case "Randy K. Weber" => "Randy Weber"
      case "Jeffrey E. (Jeff) Wentworth" => "Jeff Wentworth"
      case "G.E. \"Buddy\" West" | "G.E. (Buddy) West" | "G.E. Buddy West" | "George (Buddy) West" | "George E. (Buddy) West" => "Buddy West"
      case "Royce B. West" => "Royce West"
      case "Thomas D. \"Tommy\" Williams" | "Thomas (Tommy) Williams" | "Tommy Williams" => "Thomas Williams"
      case "Richard F. (Ric) Williamson" => "Ric Williamson"
      case "Miguel (Mike) Wise" => "Miguel David Wise"
      case "Steven Wolens" => "Steve Wolens"
      case "Gerald V. (Jerry) Yost" => "Jerry Yost"
      case "Zeb D. Zbranek" => "Zeb Zbranek"
      case "William (Bill) Zedler" => "Bill Zedler"
      case _ => name
    }
  }

  def normalizeDate(dateString: String) = {
    val DateRe = """(\d\d?)(-|/)(\d\d?)(-|/)(\d\d(\d\d)?)""".r
    val YearRe = """(\d{4})""".r
    dateString match {
      case "10/5/XX" | "10-5" | "10/5/" => "10/5"
      case "1/1" => dateString
      case "11/ /39" => dateString
      case "11/07/" => dateString
      case "11/09" => dateString
      case "6-29" => dateString
      case "02265430" => "2/26/1954"
      case "Dec. 27" | "12/27/" => "12/27"
      case DateRe(m, _, d, _, y, _) =>
        val yi = y.toInt
        "%s/%s/%s".format(m.toInt, d.toInt, if (yi < 100) yi + 1900 else yi)
      case YearRe(y) => y
      case _ => println(dateString); dateString
    }
  }

}
