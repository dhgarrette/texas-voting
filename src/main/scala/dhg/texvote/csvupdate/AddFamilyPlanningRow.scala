package dhg.texvote.csvupdate

import au.com.bytecode.opencsv.CSVReader
import java.io.FileReader
import dhg.util.FileUtil._

object AddFamilyPlanningRow {

  val familyPlanningBillsString = """
    72R - SB 722
	73R - HB 1510
	73R - HB 1
	74R - SB 673
	74R - HB 1409
	74R - SB 1
	74R - HB 1
	75R - SB 407
	75R - SB 86
	75R - HB 2856
	75R - SB 585
	75R - SB 54
	75R - SB 163
	75R - HB 723
	76R - HB 3155
	76R - SB 188
	76R - SB 30
	76R - HB 91
	76R - SB 1232
	76R - HB 1764
	77R - SB 8
	77R - SB 532
	77R - SB 812
	77R - SB 1156
	77R - SB 269
	77R - HB 757
	77R - HB 2382
	77R - HB 2260
	77R - SB 580
	78R - HB 15
	78R - HB 341
	78R - SB 331
	78R - SB 804
	78R - SB 162
	78R - SB 319
	78R - HB 1
	78R - HB 2292
	79R - HB 1485
	79R - HB 1535
	79R - SB 747
	79R - SB 826
	79R - SB 1150
	79R - SB 1001
	79R - SB 1
	80R - SB 110
	80R - SB 143
	80R - SB 785
	80R - SB 920
	80R - SB 1696
	80R - HB 1098
	80R - HB 1379
	80R - HB 1
	80R - HB 15
	81R - SB 182
	81R - SB 1886
	81R - SB 1098
	81R - HB 281
	82R - HB 15
	82R - SB 16
	82R - HB 1983
	82R - HB 2636
	82R - HB 1
	82R - HB 13
	821 - SB 7
    """

  def main(args: Array[String]) {

    val familyPlanningBills = familyPlanningBillsString.trim.split("\n").map(_.trim).toVector
    val FnRe = """voting_data-(\d\d)\.csv""".r
    //val FnRe = """(.*)""".r
    val found = scala.collection.mutable.Set[(String, String)]()
    for (
      file <- File("data/voting_data_by_session_clean").listFiles.sortBy(_.name);
      FnRe(sessionNum) = file.name
    ) {
      println(sessionNum)

      val reader: CSVReader = new CSVReader(new FileReader(file))
      val topline = reader.readNext
      reader.close()

      val binary =
        topline.drop(14).map { col =>
          //if (col.startsWith("77R - SB 812")) println(col)
          if (familyPlanningBills.exists {
            bill =>
              if (col.startsWith(bill + "\t")) {
                if (col.startsWith("82R - HB 16")) println((bill, col))
                found += ((bill, col))
                true
              }
              else false
          }) 1
          else 0
        }

      val line = (Vector.fill(14)("") ++ binary).mkString(", ")

      writeUsing(File("data/voting_data_by_session_clean_familyplanning", file.name)) { f =>
        val lineItr = file.readLines
        f.writeLine(lineItr.next)
        f.writeLine(line)
        lineItr.foreach(f.writeLine)
      }

      println(line)
      println
    }

    println(familyPlanningBills.sorted)
    println(found.map(_._1).toVector.sorted)
    val notFound = (familyPlanningBills.toSet -- found.map(_._1))
    println(notFound.toVector.sorted)
    println(notFound.size)

  }

}
