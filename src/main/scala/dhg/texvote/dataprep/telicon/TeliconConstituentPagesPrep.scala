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

  val PctRe = """(\d?\d\.\d)%""".r
  def findInfo(name: String, page: Vector[String]) = {
    page.map(_.trim)
      .find(_.startsWith(name))
      .map { line =>
        val PctRe(v) = line.drop(name.size).trim.split("\\s+")(1)
        v
      }
  }

  def getConstitutentPages(memnum: String): Map[String, String] = {
    val session = "82R"

    val populationPage = readLines(PopulationDir + "%03d_%s.xml".format(memnum.toInt, session)).toVector
    val Some(pctForeignBorn) = findInfo("FOREIGN BORN", populationPage)
    val Some(pctNoncitizen) = findInfo("NONCITIZEN", populationPage)
    val Some(pctRural) = findInfo("RURAL POPULATION", populationPage)
    val Some(pctSingleParentFamilies) = findInfo("SINGLE-PARENT FAMILIES", populationPage)

    val eduEmployPage = readLines(EduEmployDir + "%03d_%s.xml".format(memnum.toInt, session)).toVector
    val Some(pctBachelorsDegree) = findInfo("BACHELOR'S DEGREE OR HIGHER (Age 25+)", eduEmployPage)

    val incomeHousingPage = readLines(IncomeHousingDir + "%03d_%s.xml".format(memnum.toInt, session)).toVector
    val Some(pctLivingInPoverty) = findInfo("POPULATION LIVING IN POVERTY", incomeHousingPage)

    Map(
      "pctForeignBorn" -> pctForeignBorn,
      "pctNoncitizen" -> pctNoncitizen,
      "pctRural" -> pctRural,
      "pctSingleParentFamilies" -> pctSingleParentFamilies,
      "pctBachelorsDegree" -> pctBachelorsDegree,
      "pctLivingInPoverty" -> pctLivingInPoverty)
  }

}
