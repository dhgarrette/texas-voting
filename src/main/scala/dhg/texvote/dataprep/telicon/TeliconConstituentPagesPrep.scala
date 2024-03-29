package dhg.texvote.dataprep.telicon

import dhg.util.CollectionUtil._
import dhg.util.FileUtil
import dhg.util.FileUtil._
import au.com.bytecode.opencsv.CSVWriter
import java.io.BufferedWriter
import java.io.FileWriter
import dhg.texvote.dataprep.telicon.TeliconDataprepHelper._

object TeliconConstituentPagesPrep {

  //	1. From "Population and Household Profile Analysis": 
  //	
  //		% foreign born
  //		% non-citizen
  //		% rural 
  //		% single parent families 
  //	
  //	2. From "Education and Employment Profile Analysis":
  //	
  //		% with a bachelor's degree
  //	
  //	3. From "Income and Housing Profile Analysis": 
  //	
  //		% living in poverty 
  //	
  //	4. PDF file "Population Analysis"
  //
  //		% white

  val PctRe = """(\d?\d\.\d)%?""".r
  def findInfo(name: String, page: Vector[String], col: Int) = {
    page.map(_.trim)
      .find(_.startsWith(name))
      .map { line =>
        val PctRe(v) = line.drop(name.size).trim.split("\\s+")(col)
        v
      }
  }

  def getConstitutentPages(memnum: String, session: String): Map[String, String] = {
    val fileSession = "82R" // We need this because only 82R has constituency info

    val xmlColumn = if (session.take(2).toInt >= 78) 1 else 4

    val populationPage = File(PopulationDir + "%03d_%s.xml".format(memnum.toInt, fileSession), "latin1").readLines.toVector
    val Some(pctForeignBorn) = findInfo("FOREIGN BORN", populationPage, xmlColumn)
    val Some(pctNoncitizen) = findInfo("NONCITIZEN", populationPage, xmlColumn)
    val Some(pctRural) = findInfo("RURAL POPULATION", populationPage, xmlColumn)
    val Some(pctSingleParentFamilies) = findInfo("SINGLE-PARENT FAMILIES", populationPage, xmlColumn)

    val eduEmployPage = File(EduEmployDir + "%03d_%s.xml".format(memnum.toInt, fileSession), "latin1").readLines.toVector
    val Some(pctBachelorsDegree) = findInfo("BACHELOR'S DEGREE OR HIGHER (Age 25+)", eduEmployPage, xmlColumn)

    val incomeHousingPage = File(IncomeHousingDir + "%03d_%s.xml".format(memnum.toInt, fileSession), "latin1").readLines.toVector
    val Some(pctLivingInPoverty) = findInfo("POPULATION LIVING IN POVERTY", incomeHousingPage, xmlColumn)

    val raceDemoPage = File(RaceDemoDir + "%03d_%s.txt".format(memnum.toInt, fileSession), "latin1").readLines.toVector
    val Some(pctWhite) = findInfo("DISTRICT %s Total:".format((memnum.toInt - 1) % 150 + 1), raceDemoPage, 6)

    Map(
      "pctForeignBorn" -> pctForeignBorn,
      "pctNoncitizen" -> pctNoncitizen,
      "pctRural" -> pctRural,
      "pctSingleParentFamilies" -> pctSingleParentFamilies,
      "pctBachelorsDegree" -> pctBachelorsDegree,
      "pctLivingInPoverty" -> pctLivingInPoverty,
      "pctWhite" -> pctWhite)
  }

}
