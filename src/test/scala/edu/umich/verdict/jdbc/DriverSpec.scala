package edu.umich.verdict.jdbc

import java.sql.{SQLException, DriverManager}

import edu.umich.verdict.VerdictFlatSpec

class DriverSpec extends VerdictFlatSpec{
  "JDBC Driver" should "be loaded and recognize Verdict URLs" in {
    Class.forName("edu.umich.verdict.jdbc.Driver")
    try {
    println("err1")
      DriverManager.getConnection("jdbc:verdict:baddbms://bigdata.eecs.umich.edu:21050/default")
    } catch {
      case e: SQLException => e.getMessage.contains("baddbms is not a supported DBMS") shouldBe true
    }
  }

  it should " not recognize non-Verdict URLs" in {
    Class.forName("edu.umich.verdict.jdbc.Driver")
    try {
    println("err2")
      DriverManager.getConnection("jdbc:verdicts:baddbms://bigdata.eecs.umich.edu:21050/default")
    } catch {
      case e: SQLException => e.getMessage.contains("baddbms is not a supported DBMS") shouldBe false
    }
  }
}
