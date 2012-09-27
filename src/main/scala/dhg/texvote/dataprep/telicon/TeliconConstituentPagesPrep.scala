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

  def getConstitutentPages(memnum: String): Map[String, String] = {
    val session = "82R"

    val populationPage = readLines(PopulationDir + "%03d_%s.xml".format(memnum.toInt, session), "latin1").toVector
    val Some(pctForeignBorn) = findInfo("FOREIGN BORN", populationPage, 1)
    val Some(pctNoncitizen) = findInfo("NONCITIZEN", populationPage, 1)
    val Some(pctRural) = findInfo("RURAL POPULATION", populationPage, 1)
    val Some(pctSingleParentFamilies) = findInfo("SINGLE-PARENT FAMILIES", populationPage, 1)

    val eduEmployPage = readLines(EduEmployDir + "%03d_%s.xml".format(memnum.toInt, session), "latin1").toVector
    val Some(pctBachelorsDegree) = findInfo("BACHELOR'S DEGREE OR HIGHER (Age 25+)", eduEmployPage, 1)

    val incomeHousingPage = readLines(IncomeHousingDir + "%03d_%s.xml".format(memnum.toInt, session), "latin1").toVector
    val Some(pctLivingInPoverty) = findInfo("POPULATION LIVING IN POVERTY", incomeHousingPage, 1)

    val raceDemoPage = readLines(RaceDemoDir + "%03d_%s.txt".format(memnum.toInt, session), "latin1").toVector
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
