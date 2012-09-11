package dhg.texvote.scrape

import com.gargoylesoftware.htmlunit.WebClient
import com.gargoylesoftware.htmlunit.html._
import opennlp.scalabha.util.CollectionUtil._
import opennlp.scalabha.util.CollectionUtils._
import opennlp.scalabha.util.FileUtils._
import java.io.File
import org.apache.log4j.Logger
import org.apache.log4j.Level

/**
 * www.telicon.com
 */
object TeliconScraper {

  def main(args: Array[String]) {
    
    val (username, password) =
      args.toSeq match {
        case Seq(username, password) => (username, password)
      }
    
    Logger.getRootLogger.setLevel(Level.INFO)

    val webClient: WebClient = new WebClient()
    webClient.setJavaScriptEnabled(false)

    // Get the first page
    val page1: HtmlPage = webClient.getPage("http://www.telicon.com/www/tx/index.htm")

    // Get the form that we are dealing with and within that form, 
    // find the submit button and the field that we want to change.

    // <FORM NAME="Login" METHOD="GET" ACTION="/htbin/web_logintx.com" target="_top">
    //   <table border=0 width=100% cellspacing=0 cellpadding=0>
    //     <tr width=100%><td align=left valign=middle><img src="/www/temp/teliconhead.gif"></td>
    //       <td><font color="#000066"><b><i>Innovative legislative information for Texas</i></b></font></td>
    //       <td align=right valign=top width=400><font face=verdana size=2>
    //         <b>Username: </b>
    //         <INPUT TYPE="text" NAME="UWB1734S" SIZE=7 MAXLENGTH=10>&nbsp;&nbsp;
    //         <b>Password: </b>
    //         <INPUT TYPE="password" NAME="P91Z2637" SIZE=7 MAXLENGTH=10>&nbsp;&nbsp;
    //         <input type="hidden" NAME="TIDTXZR" VALUE="">
    //         <INPUT TYPE="submit" VALUE="Login"><br><br>
    //         <font color="#000066">Contact us at 512.476.7596 or <a href="mailto:cdent@telicon.com">info@telicon.com</a></font></font></td>
    //       <td width=10>&nbsp;</td>
    //     </tr>
    //   </table>
    // </form>

    val form: HtmlForm = page1.getFormByName("Login")

    val uidField: HtmlTextInput = form.getInputByName("UWB1734S")
    val pwdField: HtmlPasswordInput = form.getInputByName("P91Z2637")
    val button: HtmlSubmitInput = form.getInputByValue("Login")

    // Change the value of the text field
    uidField.setValueAttribute(username)
    pwdField.setValueAttribute(password)

    // Now submit the form by clicking the button and get back the second page.
    val page2: HtmlPage = button.click()

    //
    //
    //

    //val sessionInformationPage: HtmlPage = webClient.getPage("http://www.telicon.com/htbin/web_info99N.com?Session=82R&IOPT=3")

    //<OPTION          VALUE=821>821 - May 31, 2011 
    //<OPTION SELECTED VALUE=82R>82R - January 11, 2011 
    //<OPTION          VALUE=811>811 - July 1, 2009 
    //<OPTION          VALUE=81R>81R - January 13, 2009 
    //<OPTION          VALUE=80R>80R - January 9, 2007 
    //<OPTION          VALUE=793>793 - April 17, 2006 
    //<OPTION          VALUE=792>792 - July 21, 2005 
    //<OPTION          VALUE=791>791 - June 21, 2005 
    //<OPTION          VALUE=79R>79R - January 11, 2005 
    //<OPTION          VALUE=784>784 - April 20, 2004 
    //<OPTION          VALUE=783>783 - September 15, 2003 
    //<OPTION          VALUE=782>782 - July 28, 2003 
    //<OPTION          VALUE=781>781 - June 30, 2003 
    //<OPTION          VALUE=78R>78R - January 14, 2003 
    //<OPTION          VALUE=77R>77R - January 9, 2001 
    //<OPTION          VALUE=76R>76R - January 12, 1999 
    //<OPTION          VALUE=75R>75R - January 14, 1997 
    //<OPTION          VALUE=74R>74R - January 10, 1995 
    //<OPTION          VALUE=73R>73R - January 12, 1993 
    //<OPTION          VALUE=72R>72R - January 8, 1991 
    //<OPTION          VALUE=721>721 - July 15, 1991 
    //<OPTION          VALUE=722>722 - August 19, 1991 
    //<OPTION          VALUE=723>723 - January 2, 1992 
    //<OPTION          VALUE=724>724 - November 10, 1992

    val allSessions = Vector("821", "82R", "811", "81R", "80R", "793", "792", "791", "79R", "784", "783", "782", "781", "78R", "77R", "76R", "75R", "74R", "73R", "72R", "721", "722", "723", "724")
    val allMemnums = (1 to 181) // 150 House districts and 31 Senate districts

    val dataDir = new File("data"); if (!dataDir.exists) dataDir.mkdir
    val scrapedDir = new File("data/scraped"); if (!scrapedDir.exists) scrapedDir.mkdir
    val legislatorDir = new File("data/scraped/legislator_pages"); if (!legislatorDir.exists) legislatorDir.mkdir
    val votesDir = new File("data/scraped/votes_pages"); if (!votesDir.exists) votesDir.mkdir

    allMemnums.foreach { memnum =>
      allSessions.foreach { session =>
        val legislatorPage: HtmlPage = webClient.getPage("http://www.telicon.com/htbin/web_member.com?Memnum=%s&Session=%s".format(memnum, session))
        writeUsing("data/scraped/legislator_pages/%03d_%s.txt".format(memnum, session))(_.write(legislatorPage.asText))
        writeUsing("data/scraped/legislator_pages/%03d_%s.xml".format(memnum, session))(_.write(legislatorPage.asXml))

        val votesPage: HtmlPage = webClient.getPage("http://www.telicon.com/htbin/web_memvot.com?mvses=%s&mvmem=TX%s&X5=ALL&XD=&XL=N".format(session, memnum))
        writeUsing("data/scraped/votes_pages/%03d_%s.txt".format(memnum, session))(_.write(votesPage.asText))
        writeUsing("data/scraped/votes_pages/%03d_%s.xml".format(memnum, session))(_.write(votesPage.asXml))
      }
    }

    webClient.closeAllWindows()

  }

}
