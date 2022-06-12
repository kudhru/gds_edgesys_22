package syn

import ch.cern.sparkmeasure.StageMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object SimulateGDS5P3 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Geo-Distributed Sampling Phase 3")
      .getOrCreate()

    // Read the user inputs into appropriate variables
    val inputFilePrefix: String = args(0)
    val numTables: Int = args(1).toInt
    val numPartitions: Int = args(2).toInt
    val samplingFraction: Double = args(3).toDouble
    val outputFilePrefix: String = args(4)
    val partitionId: Int = args(5).toInt
    val freqCountFilePrefix: String = args(6)
    val stageMetricsFile = args(7)

    val stageMetrics = StageMetrics(spark)

    stageMetrics.begin()


    // Read the table partitions into dataframes
    var tableDFs: Seq[DataFrame] = Seq()
    for (tableId: Int <- 0 until numTables) {
      val inputFilePath = f"$inputFilePrefix%s_t_$tableId%d_p_$partitionId%d_of_$numPartitions%d.csv"
      val df = spark.read.format("csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("delimiter", ",")
        .schema(SyntheticSchema.table_schema_five_string)
        .load(inputFilePath)
        .withColumnRenamed("value_one", f"value_one_$tableId%d")
        .withColumnRenamed("value_two", f"value_two_$tableId%d")
        .withColumnRenamed("value_three", f"value_three_$tableId%d")
        .withColumnRenamed("value_four", f"value_four_$tableId%d")
        .withColumnRenamed("value_five", f"value_five_$tableId%d")
      tableDFs = tableDFs :+ df
    }

    // Read the table frequency counts
    var perTableFreqCountDFs: Seq[DataFrame] = Seq()
    for (tableId: Int <- 0 until numTables) {
      val freqCountFilePath = f"$freqCountFilePrefix%s_sf_$samplingFraction%f_t_$tableId%d_gds5_p2.csv"
      val df = spark.read.format("csv")
        .option("header", "false")
        .option("inferSchema", "false")
        .option("delimiter", ",")
        .schema(SyntheticSchema.freq_table_schema)
        .load(freqCountFilePath)
        .withColumnRenamed("count", f"count_$tableId%d")
      perTableFreqCountDFs = perTableFreqCountDFs :+ df
    }

    // compute the weights for sampling
    // TODO: This only works for join involving two tables.
    //  Need to discuss with some database/spark expert how to compute it in an efficient manner for more tables.
    import spark.implicits._
    var perTableWeightMap: Seq[RDD[(String, GenericRowWithSchema)]] = Seq()
    for (tableId: Int <- 0 until numTables) {
      val otherTableId = numTables - 1 - tableId
      perTableWeightMap = perTableWeightMap :+
        perTableFreqCountDFs(otherTableId).rdd
          .keyBy(Row => Row.getAs[String]("key"))
          .asInstanceOf[RDD[(String, GenericRowWithSchema)]]
    }

    // use the weights to sample from each partition of each table
    for (tableId: Int <- 0 until numTables) {
      val outputFilePath = f"$outputFilePrefix%s_sf_$samplingFraction%f_t_$tableId%d_p_$partitionId%d_of_$numPartitions%d_gds5_p3.csv"
      val tableSampledRDD: RDD[(String, GenericRowWithSchema, Int)] = tableDFs(tableId)
        .rdd.keyBy(Row => Row.getAs[String]("key")).asInstanceOf[RDD[(String, GenericRowWithSchema)]]
        .join(perTableWeightMap(tableId))
        .mapPartitionsWithIndex(SamplingUtils.getSamplingFunc(samplingFraction), preservesPartitioning = true)
        .asInstanceOf[RDD[(String, GenericRowWithSchema, Int)]]
      tableSampledRDD
        .map(element =>
          (element._1,
            element._2.get(1).asInstanceOf[String],
            element._2.get(2).asInstanceOf[String],
            element._2.get(3).asInstanceOf[String],
            element._2.get(4).asInstanceOf[String],
            element._2.get(5).asInstanceOf[String],
            element._3,
          )
        ).toDF(
        "key",
        f"value_one_$tableId%d", f"value_two_$tableId%d", f"value_three_$tableId%d", f"value_four_$tableId%d", f"value_five_$tableId%d",
        "count")
        .write.format("csv")
        .option("header", "false")
        .mode(SaveMode.Overwrite)
        .save(outputFilePath)
    }

    stageMetrics.end()
    stageMetrics.createStageMetricsDF("PerfStageMetrics")
    val aggMetricsDF = stageMetrics.aggregateStageMetrics("PerfStageMetrics")
    stageMetrics.saveData(aggMetricsDF, stageMetricsFile, saveMode = "overwrite")

    spark.stop()
  }
}
