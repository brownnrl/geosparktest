package threaded_track

import java.nio.file.Paths

import com.databricks.spark.avro._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.sql.expressions.{Window, WindowSpec}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.sql._
import org.datasyslab.geospark.serde.GeoSparkKryoRegistrator
import org.datasyslab.geosparksql.utils.GeoSparkSQLRegistrator

object GeoSparkTestGeoJSON {

  def dfZipWithIndex(
                      df: DataFrame,
                      offset: Int = 1,
                      colName: String = "id",
                      inFront: Boolean = true
                    ) : DataFrame = {
    df.sqlContext.createDataFrame(
      df.rdd.zipWithIndex.map(ln =>
        Row.fromSeq(
          (if (inFront) Seq(ln._2 + offset) else Seq())
            ++ ln._1.toSeq ++
            (if (inFront) Seq() else Seq(ln._2 + offset))
        )
      ),
      StructType(
        (if (inFront) Array(StructField(colName,LongType,nullable = false)) else Array[StructField]())
          ++ df.schema.fields ++
          (if (inFront) Array[StructField]() else Array(StructField(colName,LongType,nullable = false)))
      )
    )
  }

  def unionTwoDataframes(df1 : DataFrame, df2 : DataFrame) : DataFrame = {
    val cols1 = df1.columns.toSet
    val cols2 = df2.columns.toSet
    val total = cols1 ++ cols2

    def expr(myCols: Set[String], allCols: Set[String]) = {
      allCols.toList.map {
        case x if myCols.contains(x) => col(x)
        case x => lit(null).as(x)
      }
    }

    df1.select(expr(cols1, total):_*).union(df2.select(expr(cols2, total):_*))
  }

  def filterColumns(df: DataFrame, cols: Seq[String]): DataFrame = {
    df.select(cols.head, cols.tail: _*)
  }

  def getSparkSession(runLocal: Boolean, appName: String): SparkSession = {
    val sparkBuilder = SparkSession.builder()
    val sparkBuilderMaster = if (runLocal) {
      sparkBuilder.master("local[*]").appName(appName).config("geospark.join.numpartition", "1")
    } else {
      sparkBuilder.appName(appName)
    }
    val sparkSession = sparkBuilderMaster.
      config("spark.serializer",classOf[KryoSerializer].getName).
      config("spark.kryo.registrator", classOf[GeoSparkKryoRegistrator].getName).
      config("geospark.global.index", "true").
      getOrCreate()
    GeoSparkSQLRegistrator.registerAll(sparkSession.sqlContext)
    sparkSession
  }

  def getPointDF(sparkSession: SparkSession, tt_df : DataFrame) : DataFrame = {
      tt_df.createOrReplaceTempView("tt_df")
      var pointDF = sparkSession.sql("select ST_Point(cast(tt_df.longitude as Decimal(24,20)), cast(tt_df.latitude as Decimal(24,20))) AS lonlat_point," +
      "tt_id, time, latitude, longitude, pressure_altitude" +
      " FROM tt_df")
      pointDF = pointDF.withColumn("timestamp", from_unixtime(col("time")))
      pointDF.createOrReplaceTempView("pointDF")
      pointDF
  }


  def getThreadedTrackDF (runAll: Boolean, sparkSession: SparkSession, ttFilePath: String): DataFrame  = {
    val tt_df: DataFrame = (if(runAll) {
      sparkSession.read.avro(ttFilePath)
    } else {
      sparkSession.read.avro(ttFilePath)
        .filter(col("tt_id").isin(
          "2017030100001328_2.3.0-F",
          "2017030101686679_2.3.0-F",
          "2017030101686656_2.3.0-F"
        ))
    }).select("tt_id", "threaded_track")
      .withColumn("threaded_track", explode(col("threaded_track")))
      .select("tt_id", "threaded_track.*")
      .select("tt_id", "time", "latitude", "longitude", "pressure_altitude")
    tt_df
  }

  def getCsvTTDF(sparkSession: SparkSession, csvFilePath: String): DataFrame = {
    sparkSession.read.format("csv").option("header", value = true).load(csvFilePath)
  }

  def loadFavDF(sparkSession: SparkSession, json_file_path : String) : DataFrame = {
    var favGeoJsDF = sparkSession.read.format("csv").option("header", value = true)
      .option("delimiter", "|")
      .load(json_file_path)
    favGeoJsDF
  }

  def convertFavToGeoJson(sparkSession: SparkSession, loadedDF: DataFrame) : DataFrame = {
      loadedDF.createOrReplaceTempView("fav_geojs")
      var favGeoJsDF = sparkSession.sql("select Facility, FavID, cast(fav_geojs.AltLow as Integer) as min_alt," +
        "cast(fav_geojs.AltHigh as Integer) as max_alt, GeoJSON, Inclusion FROM fav_geojs ")
      favGeoJsDF = favGeoJsDF.withColumn("max_alt", col("max_alt"))
        .withColumn("FavID", concat(col("Facility"), col("FavID")))
      favGeoJsDF.createOrReplaceTempView("fav_geojs")
      val favDF = sparkSession.sql("select FavID, min_alt, max_alt, Inclusion, ST_GeomFromGeoJSON(fav_geojs.GeoJSON) AS polygon FROM fav_geojs")
      favDF.createOrReplaceTempView("favDF")
      favDF
  }

  def calculateOverlap(sparkSession: SparkSession): DataFrame = {
    filterColumns(sparkSession.sql(
      "select * FROM pointDF, favDF WHERE " +
      "(pointDF.pressure_altitude >= favDF.min_alt) " +
      "AND (pointDF.pressure_altitude < favDF.max_alt) " +
      "AND ST_Within(pointDF.lonlat_point, favDF.polygon)"),
      Seq(
        "tt_id",
        "time",
        "timestamp",
        "latitude",
        "longitude",
        "pressure_altitude",
        "min_alt",
        "max_alt",
        "FavID",
        "Inclusion"))
  }

  def filterInclusionExclusionOverlap(overlapDF: DataFrame) : DataFrame = {
    val favWindow = Window.partitionBy("tt_id", "time", "FavID")
    overlapDF.withColumn("count", count("tt_id")
      .over(favWindow))
      .filter(col("count") === 1)
      .drop(col("count"))
      .orderBy(col("tt_id"), col("time"))
  }


  def getMinMaxPointsFromOverlapIndex(sparkSession: SparkSession, overlapIndexed: DataFrame) : DataFrame = {
      overlapIndexed.createOrReplaceTempView("overlapIndexed")
      sparkSession.sql("SELECT tt.* FROM overlapIndexed tt " +
        "INNER JOIN (SELECT tt_id, min(tt_id_msgid) as min_msgid, max(tt_id_msgid) as max_msgid " +
        "FROM overlapIndexed GROUP BY tt_id) tt_min_max " +
        "ON (tt.tt_id_msgid = tt_min_max.min_msgid and tt.tt_id = tt_min_max.tt_id) OR " +
        "   (tt.tt_id_msgid = tt_min_max.max_msgid and tt.tt_id = tt_min_max.tt_id)")
  }


  def leadOneOver(win_func: WindowSpec)(name: String, alias_postfix : String): Column = {
    lead(name,1,null).over(win_func).alias(name + alias_postfix)
  }

  def subWithNulls(colName1 : String, colName2: String) = {
    when(col(colName1).isNull || col(colName2).isNull, lit(-1)).otherwise(col(colName1)-col(colName2))
  }

  def getTransitions(overlapIndexed : DataFrame): DataFrame = {
    import overlapIndexed.sqlContext.implicits._
    val win_func : WindowSpec = Window.partitionBy("tt_id").orderBy("tt_id_msgid")

    def leadOne: (String, String) => Column = leadOneOver(win_func)(_, _)

    val transitions = overlapIndexed.select(
      col("tt_id_msgid"),
      leadOne("tt_id_msgid", "_1"),
      col("tt_id"),
      leadOne("tt_id", "_1"),
      col("time"),
      leadOne("time", "_1"),
      col("timestamp"),
      leadOne("timestamp", "_1"),
      col("FavID"),
      leadOne("FavID", "_1")
    ).filter($"tt_id" === $"tt_id_1" and $"FavID" =!= $"FavID_1")
      .withColumn("delta", subWithNulls("time_1", "time"))

    transitions

  }

  def getUnionMinMaxAndTransitions(minMaxValues: DataFrame,
                                   transitions: DataFrame): DataFrame = {
    import transitions.sqlContext.implicits._
    val win_func : WindowSpec =
      Window.partitionBy("tt_id").orderBy("tt_id_msgid")

    def leadOne: (String, String) => Column = leadOneOver(win_func)(_, _)

    val unionMinMaxTransitions =
      unionTwoDataframes(minMaxValues, transitions).select(
        col("tt_id_msgid"),
        leadOne("tt_id_msgid", "_1"),
        leadOne("tt_id_msgid_1", "_2"),
        col("tt_id"),
        leadOne("tt_id", "_1"),
        leadOne("tt_id_1", "_2"),
        col("time"),
        leadOne("time", "_1"),
        leadOne("time_1", "_2"),
        col("timestamp"),
        leadOne("timestamp", "_1"),
        col("FavID"),
        leadOne("FavID", "_1"),
        leadOne("FavID_1", "_2")
      ).filter(not ($"FavID_1".isNull and $"FavID_1_2".isNull))
        .select(
          col("tt_id"),
          col("FavID_1").alias("FavID"),
          col("timestamp").alias("entering"),
          col("timestamp_1").alias("leaving")
        )

    unionMinMaxTransitions

  }


  def main(args: Array[String]) {
    val log = Logger.getLogger("org")
    log.setLevel(Level.WARN)
    for(i <- args.indices) {
      print(args(i) + " ")
    }
    println()
    val runLocal = !args.contains("submit")
    val runAll = args.contains("runAll")

    val now = System.currentTimeMillis

    val sparkSession = getSparkSession(runLocal, "HF_FAV_INTERSECTION")

    val ttFileName : String = "ThreadedTrack_2.3.0-F_ASSOCIATED_20170301_part-00080.avro"

    val ttFilePath: String = if (runLocal) {
      Paths.get("data", ttFileName).toAbsolutePath.toString
    } else {
      "hdfs:///data/derived/tt/2.3.0-F/ThreadedTrack/2017/03/01/" + ttFileName
    }

    var GEO_JSON_FAV_PATH : String = if (runLocal) {
      Paths.get("data", "FAVs_GeoJSON.csv").toAbsolutePath.toString
    } else {
      "hdfs:///user/n.brown/ZAB_FAVs_GeoJSON.csv"
    }

    /*

    STEP ONE:

    We read in the threaded track data in it's native Avro format.

    We extract out those bits of information that we are interested in
    (threaded track id, lat/lon, pressure altitude, and time)

     */
    //val ttDF = getThreadedTrackDF(runAll, sparkSession, ttFilePath)

    /*

    STEP ONE (continued)

    If we're just doing some sample / test processing, we can extract
    a subset of this data and save it to a CSV file for examination.

     */
    val ttDF = getCsvTTDF(sparkSession,
      Paths.get( "data", "out_big.csv").toAbsolutePath.toString).filter(
      col("tt_id").isin(
        "ac96b6",
        "aa56da",
        "a2e5de"))
    println("Threaded Track DF Schema")
    ttDF.printSchema()


    /*
    STEP TWO:

    The data that we've extracted from the AVRO threaded track is not yet
    in a format that we can use GeoSpark's underlying spatial operations
    with, we need to use the basic datatypes of strings and numbers and
    create object provided by the framework for processing.

     */

    val pointDF = getPointDF(sparkSession, ttDF)

    pointDF.printSchema()
    // NOTE: Repartitioning large datasets is an important step.
    // Too few partitions, and you will not take advantage of
    // your cluster, and too many and you'll spend too much time
    // communicating the information between executors (a process
    // called shuffling) and not enough time performing computation.
    pointDF.repartition(200)
    pointDF.show(10)

    /*
    STEP THREE:

    Now that we have points, we need the fixed airspace volumes to which
    we want to find the points within those volumes (let's call that
    "overlap").

    Here, we do the same process of converting basic datatypes into
    datatypes that can be processed.
     */

    val dfFromLoad = loadFavDF(sparkSession, GEO_JSON_FAV_PATH)
    println("DataFrame of FAVs from load")
    dfFromLoad.printSchema()
    dfFromLoad.show(10)
    val favDF = convertFavToGeoJson(sparkSession, dfFromLoad)
    favDF.sort("FavID").show
    favDF.explain()

    /*
    STEP FOUR:

    In this step, we calculate where the overlap exists between points and
    the fixed airspace volumes they are within.

     */

    val overlapDF = calculateOverlap(sparkSession)
    println("OVERLAP DF")
    overlapDF.explain()
    overlapDF.show(5)


    /*
    STEP FIVE:

    Some airspace volumes are "inclusion", which means they are additive.

    We add the groups of these volumes together to define the aggregate volume.

    Some airspace volumes also define "exclusions".  These are subtractive,
    meaning that if a point belongs to an inclusion space, but also belongs to
    an exclusion of the same airspace volume then we exclude that point from
    overlap with the airspace volume.

    The typical example is a terminal airspace volume "embedded" in an ARTCC
    controlled airspace (or some portion thereof).

    In this step, we calculate where the overlap exists between points and
    the fixed airspace volumes they are within.

     */

    val overlapInclusionDF = filterInclusionExclusionOverlap(overlapDF)
    println("OVERLAP INCLUSION DF")
    overlapInclusionDF.show(5)

    /*
    STEP SIX:

    We want to find the "transition" points, where the target (represented by
    a threaded track id) exits one Fixed Airspace Volume and enters another.

    In order to find these transition points, we use a trick by which we
    add an index to each row of our dataframe.  This adds the index to the
    classified position hits of each target (the overlap).

     */

    val overlapIndexed = dfZipWithIndex(overlapInclusionDF, colName = "tt_id_msgid")
    println("OVERLAP INCLUSION DF WITH INDEX")
    overlapIndexed.show(5)

    /*
    STEP SEVEN:

    While finding transition points permits us to find when the target traverses from
    one airspace volume to the next, it unfortunately doesn't allow us to find where
    the target stops and ends.  This is as there is no "previous" row for the first hit,
    and no "next" row for the last hit.

    What is needed to to calculate the minimum and maximum indexed value for each target
    and include them as a separate dataframe to be combined with our transitions.

     */

    val minMaxValues = getMinMaxPointsFromOverlapIndex(sparkSession, overlapIndexed)

    writeAndReplace("minMaxValues.csv", minMaxValues, runLocal)
    writeAndReplace("overlapIndexed.csv", overlapIndexed, runLocal)
    println("Overlap paritions: " + overlapIndexed.rdd.getNumPartitions)

    /*
    STEP EIGHT:

    To find the transition points, we use a windowing technique to combine one row
    of a dataframe with it's subsequent row, and this permits us to perform a row-by-row
    comparison without losing the ability to process these rows in parallel.

     */
    val transitions = getTransitions(overlapIndexed)

    writeAndReplace("transitions.csv", transitions, runLocal)
    println("Transition paritions: " + transitions.rdd.getNumPartitions)

    /*
    STEP NINE:

    The final step is to union the dataframe of transitions with the first and last points
    of each target in the threaded track.

     */
    val unionMinMaxTransitions = getUnionMinMaxAndTransitions(minMaxValues, transitions)

    writeAndReplace("unionsMinMaxTransitions.csv", unionMinMaxTransitions, runLocal)

    println(System.currentTimeMillis - now)
  }

  def writeAndReplace(location: String, df : DataFrame, runLocal: Boolean): Unit = {
    if (runLocal) {
      df.repartition(1)
        .write.mode(SaveMode.Overwrite).option("header", "true").csv(
        Paths.get("data", "out", location).toAbsolutePath.toString)
    } else {
      df.repartition(1)
        .write.mode(SaveMode.Overwrite).option("header", "true").csv("hdfs:///user/n.brown/" + location)
    }

  }

}
