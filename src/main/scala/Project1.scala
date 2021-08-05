import org.apache.spark.sql.SparkSession
import scala.Console._

object Project1 {
  def main(args: Array[String]): Unit = {


    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\winutils\\hadoop-3.0.0")
    var choice : String = ""
    val spark = SparkSession
      .builder
      .appName("p1 project")
      .config("spark.master", "local")
      .config("spark.sql.warehouse.dir","hdfs://localhost:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")


    spark.sparkContext.setLogLevel("Off")

    spark.sql("drop table bev_branch")
    spark.sql("drop table bev_cust")
    spark.sql("create table if not exists bev_branch(bev string, branch string) row format delimited fields terminated by ','");
    spark.sql("create table if not exists bev_cust(bev string, cust_co int) row format delimited fields terminated by ','");
    spark.sql("load data local inpath 'input/p1data/branch' into table bev_branch")
    spark.sql("load data local inpath 'input/p1data/customer' into table bev_cust" )

    printMenu
    choice = readLine
    while(choice != "Q" && choice != "q") {

      scenarioTaskHandler(spark, choice)
      println
      printMenu
      choice = readLine
    }

    println
    println("Demo Finished")
    println("Exiting")
    Thread.sleep(2000)

  }

  def printMenu: Unit = {
    println
    println("Please Select One of the Problem Scenarios Below")
    println("Problem Scenario [1]")
    println("Problem Scenario [2]")
    println("Problem Scenario [3]")
    println("Problem Scenario [4]")
    println("Problem Scenario [5]")
    println("Problem Scenario [6]")
    println("Quit Program [Q]")
    println
    print("Please enter one of the options above: ")
  }

  def scenarioTaskHandler(spark: SparkSession, choice: String): Unit = {
    choice match {
      case "1" => {
        println
        println("Scenario 1: ")
        println("Total # of customers for branch1")
        spark.sql("select branch, sum(cust_co) as branch1_consumers from bev_branch bb left join bev_cust bc on bb.bev = bc.bev where branch = 'Branch1' group by branch").show
        Thread.sleep(2000)
        println
        println("Total # of customers for branch2")
        spark.sql("select branch, sum(cust_co) as branch2_consumers from bev_branch bb left join bev_cust bc on bb.bev = bc.bev where branch = 'Branch2' group by branch").show
        println
        Thread.sleep(3000)

      }
      case "2" => {
        println
        println("Scenario 2:")
        println("Beverages available in Branch1, Branch8, and Branch10")
        spark.sql("select distinct(bev) from bev_branch where branch = 'Branch8' or branch like 'Branch1%'").show
        Thread.sleep(2000)
        println
        println("Common beverages avalailable in Branch4, Branch7")
        spark.sql("select distinct * from (select bev from bev_branch where branch = 'Branch7') b7 inner join (select bev from bev_branch where branch = 'Branch4') b4 using(bev)").show
        Thread.sleep(3000)

      }
      case "3" => {
        println
        println("Scenario 3:")
        println("Most consumed beverage in branch 1")
        spark.sql("select bev_cust.bev, sum(cust_co) as total from bev_branch left join bev_cust on bev_branch.bev = bev_cust.bev where branch = 'Branch1' group by bev_cust.bev order by total desc limit 1").show
        Thread.sleep(2000)
        println
        println("Least consumed beverage in branch 2")
        spark.sql("select bev_cust.bev, sum(cust_co) as total from bev_branch left join bev_cust on bev_branch.bev = bev_cust.bev where branch = 'Branch2'  group by bev_cust.bev order by total asc limit 1").show
        Thread.sleep(3000)

      }
      case "4" => {
        println
        println("Scenario 3:")
        println("Creating View 'common_4_7'")
        Thread.sleep(1000)
        spark.sql("create view if not exists common_4_7 as select distinct * from (select bev from bev_branch where branch = 'Branch7') b7 inner join (select bev from bev_branch where branch = 'Branch4') b4 using(bev)")
        println("Selecting * from view 'common_4_7")
        Thread.sleep(2000)
        spark.sql("select * from common_4_7").show
        Thread.sleep(3000)
        //spark.sql("select count(*) from bev_branch as num_branch_rows").show()

      }
      case "5" => {
        println
        println("Scenario 5:")
        println("Alter table properties to add comment")
        println
        Thread.sleep(2000)
        println("Printing Current Table Details: ")
        Thread.sleep(2000)
        spark.sql("describe table bev_cust").show
        Thread.sleep(2000)
        println
        println("Changing Table Comments...")
        Thread.sleep(2000)
        spark.sql("alter table bev_cust change bev bev string comment 'Beverage Comment!'")
        spark.sql("describe table bev_cust").show
        Thread.sleep(2000)
        println
        println("Changing Table Comments...")
        Thread.sleep(2000)
        spark.sql("alter table bev_cust change bev bev string comment 'New Bev Comment!'")
        spark.sql("describe table bev_cust").show
        Thread.sleep(3000)
      }

      case "6" => {
        println
        println("Scenario 6:")
        println("Remove Row(s) from Any of the Previous Scenarios:")
        Thread.sleep(2000)
        println("Reprinting Beverages Available at Branch 1, Branch 8, Branch 10: ")
        println
        spark.sql("create table if not exists scenario_table (bev string) row format delimited fields terminated by ','")
        spark.sql("insert overwrite table scenario_table select distinct(bev) from bev_branch where branch = 'Branch8' or branch like 'Branch1%'")
        spark.sql("select * from scenario_table").show
        Thread.sleep(2000)

        spark.sql("insert overwrite table scenario_table select * from scenario_table where bev != 'MED_Espresso'")
        spark.sql("select * from scenario_table").show
        
        Thread.sleep(2000)


      }

      case _ => {
        println
        println("Invalid Choice: Please Choose Another Option: ")
      }
    }

  }

}
