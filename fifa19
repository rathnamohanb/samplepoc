import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.max
import org.apache.log4j.LogManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SQLContext

object Test {
 def main(args:Array[String])
 {

case class FIFA19(ID:Int, Name:String, Age:String, Photo:String, Nationality:String,	Flag:String,Overall:Int,	Potential:Int,	Club:String,	
                    Club Logo:String,Value:String,	Wage:String,Special:Int,Preferred Foot:Int,	International Reputation:Int,	Weak Foot:Int,	
                    Skill Moves:Int,	Work Rate:String,	Body Type:String,	Real Face:String,	Position:String,	Jersey Number:Int,	Joined:String,
                    Loaned From:String,	Contract Valid Until:Int,	Height:Int	Weight:Int,	LS:Int,	ST:Int,	RS:Int,	LW:Int,	LF:Int,	CF:Int,	RF:Int,	                           RW:Int,	LAM:Int,	CAM:Int,	RAM:Int,	LM:Int,	LCM:Int,	CM:Int,	RCM:Int,	RM:Int,	LWB:Int,	LDM:Int,	CDM:Int,	                         RDM:Int,	RWB:Int,	LB:Int,	LCB:Int,	CB:Int,	RCB:Int,	RB:Int,	Crossing:Int,	Finishing:Int,	HeadingAccuracy:Int,	                           ShortPassing:Int,	Volleys:Int,	Dribbling:Int,	Curve:Int,	FKAccuracy:Int,	LongPassing:Int,	BallControl:Int,	                                 Acceleration:Int,	SprintSpeed:Int,	Agility:Int,	Reactions:Int,	Balance:Int, ShotPower:Int,	Jumping:Int,	Stamina:Int,	                       Strength:Int,	LongShots:Int,	Aggression:Int,	Interceptions:Int,	Positioning:Int,	Vision:Int,	Penalties:Int,	Composure:Int,	                     Marking:Int,	StandingTackle:Int,	SlidingTackle:Int,	GKDiving:Int,	GKHandling:Int,	GKKicking:Int,	GKPositioning:Int,	                               GKReflexes:Int,	Release Clause:Int)

  
    val conf = new SparkConf()
    var listDS = new ListBuffer[RDD[String]]
    val sparkConf = new SparkConf().setAppName("FIFA")
    val sc = new SparkContext(sparkConf)
   // val sqlContext: SQLContext = new HiveContext(sc)					
val FIFA_Data = sc.textFile("/FileStore/tables/data.csv")
import spark.implicits._

val convert = Seq(FIFA19[FIFA_Data])
val convert1 = convert.registerTempTable( "fifa" )

//*******************************Which club has the most number of left footed midfielders under 30 years of age?******************//
val query1 = (""" WITH LEFT_FOOTER AS (
SELECT
Club,
count(distinct id) as Player_Count
FROM  fifa
WHERE
AGE < 30
AND Preferred_Foot='Left'
AND Position IN 
(
'RM','RWM','LCM','CM','RCM','CM','LM','LWM','CDM'
)
Group By
Club
)
SELECT
Club
FROM LEFT_FOOTER
WHERE Player_Count=(SELECT MAX(Player_Count) FROM LEFT_FOOTER) """)

query.show()

//************************** Which team has the most expensive squad value in the world? ***************//

val query2 = (""" WITH club_agg as (
SELECT
Club,
SUM(VALUE) AS Squad_Value,
SUM(WAGE) AS Wage
FROM
fifa
GROUP BY
Club
)
SELECT 
Club 
FROM 
club_agg
Where squad_value=(SELECT MAX(squad_value) FROM club_agg)""")
query2.show()

//**************************** Does that team also have the largest wage bill ? *******************///

val query3= (""" WITH club_agg as (
SELECT
Club,
SUM(VALUE) AS Squad_Value,
SUM(WAGE) AS Wage
FROM
fifa
GROUP BY
Club
)
SELECT 
Club 
FROM 
club_agg
Where Wage=(SELECT MAX(Wage) FROM club_agg)""")
query3.show()

//********************** Which position pays the highest wage in average?*************************//

val query 4= (""" with position_wage as 
(
SELECT
position,
AVG(wage) as avg_wage
FROM fifa
GROUP BY position
)
SELECT 
position 
from
position_wage where
avg_wage=(select avg_wage from position_wage)""")
 query 4.show()

 
 //****************What makes a good Striker (ST)? Share 5 attributes which are most relevant to becoming a top striker ******///
 
 val query 5 = (""" select * from fifa 
 where 
 (
 ST IN
 (
 select TOP (5) ST
 from fifa as fifa 1
 group by ST
 order by ST DESC
 )
 )
 """)
 
 query 5.show()
 
 }
 
 
 //*************************What makes a goalkeeper great? Share 4 attributes which are most relevant to becoming a good goalkeeper? ******///
 
 val query 6 = (""" with goalkeeper as 
(
SELECT
Name,
GKDiving,
GKHandling,
GKKicking,
GKPositioning,
GKReflexes
FROM fifa
where ( 
GKDiving IN 
( 
SELECT 
top (4) 
from 
goalkeeper where
GKDiving=(select GKDiving from goalkeeper)""")
 
 }
 
 """
