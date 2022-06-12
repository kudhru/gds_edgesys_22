package syn

import ch.cern.sparkmeasure.StageMetrics
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object SimulateGDS5P1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Geo-Distributed Sampling Phase 1")
      .getOrCreate()

    // Read the user inputs into appropriate variables
    val inputFilePrefix: String = args(0)
    val numTables: Int = args(1).toInt
    val numPartitions: Int = args(2).toInt
    val samplingFraction: Double = args(3).toDouble
    val outputFilePrefix: String = args(4)
    val partitionId: Int = args(5).toInt
    val stageMetricsFile: String = args(6)

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

    // Compute the frequency counts of the joining columns
    for (tableId: Int <- 0 until numTables) {
      val outputFilePath = f"$outputFilePrefix%s_sf_$samplingFraction%f_t_$tableId%d_p_$partitionId%d_of_$numPartitions%d_gds5_p1.csv"
      tableDFs(tableId)
        .groupBy("key")
        .agg(count("*").as(f"count_$tableId%d"))
        .select("key", f"count_$tableId%d")
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
