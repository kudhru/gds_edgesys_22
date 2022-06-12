package syn

import ch.cern.sparkmeasure.StageMetrics
import org.apache.spark.sql.functions.sum
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}


object SimulateGDS5P2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Geo-Distributed Sampling Phase 2")
      .getOrCreate()

    // Read the user inputs into appropriate variables
    val inputFilePrefix: String = args(0)
    val numTables: Int = args(1).toInt
    val numPartitions: Int = args(2).toInt
    val samplingFraction: Double = args(3).toDouble
    val outputFilePrefix: String = args(4)
    val stageMetricsFile = args(5)

    val stageMetrics = StageMetrics(spark)

    stageMetrics.begin()

    // Read the table partitions into dataframes
    var tableDFs: Seq[Seq[DataFrame]] = Seq()
    for (tableId: Int <- 0 until numTables) {
      var partitionDFs: Seq[DataFrame] = Seq()
      for (partitionId: Int <- 0 until numPartitions) {
        val inputFilePath = f"$inputFilePrefix%s_sf_$samplingFraction%f_t_$tableId%d_p_$partitionId%d_of_$numPartitions%d_gds5_p1.csv"
        val df = spark.read.format("csv")
          .option("header", "false")
          .option("inferSchema", "false")
          .option("delimiter", ",")
          .schema(SyntheticSchema.freq_table_schema)
          .load(inputFilePath)
          .withColumnRenamed("count", f"count_$tableId%d")
        partitionDFs = partitionDFs :+ df
      }
      tableDFs = tableDFs :+ partitionDFs
    }

    // Compute the per table frequency counts
    for (tableId: Int <- 0 until numTables) {
      var tableFreqCount: DataFrame = tableDFs(tableId).head
      for (partitionId: Int <- 1 until numPartitions) {
        tableFreqCount = tableFreqCount.union(tableDFs(tableId)(partitionId))
      }
      val outputFilePath = f"$outputFilePrefix%s_sf_$samplingFraction%f_t_$tableId%d_gds5_p2.csv"
      tableFreqCount
        .groupBy("key")
        .agg(sum(f"count_$tableId%d").as(f"count_$tableId%d"))
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
