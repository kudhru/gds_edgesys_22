package syn

import ch.cern.sparkmeasure.StageMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.{SaveMode, SparkSession}


object SimulateGDS5P4 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Geo-Distributed Sampling Phase 4")
      .getOrCreate()

    // Read the user inputs into appropriate variables
    val inputFilePrefix: String = args(0)
    val numTables: Int = args(1).toInt
    val numPartitions: Int = args(2).toInt
    val outputFile: String = args(3)
    val samplingFraction: Double = args(4).toDouble
    val stageMetricsFile: String = args(5)

    val stageMetrics = StageMetrics(spark)

    stageMetrics.begin()

    // Read the sampled table partitions into dataframes
    var sampledRDDs: Seq[RDD[(String, GenericRowWithSchema)]] = Seq()
    for (tableId: Int <- 0 until numTables) {
      var perPartitionSampledRDDs: Seq[RDD[(String, GenericRowWithSchema)]] = Seq()
      for (partitionId: Int <- 0 until numPartitions) {
        val inputFilePath = f"$inputFilePrefix%s_sf_$samplingFraction%f_t_$tableId%d_p_$partitionId%d_of_$numPartitions%d_gds5_p3.csv"
        val partitionSampledRDD: RDD[(String, GenericRowWithSchema)] = spark.read.format("csv")
          .option("header", "false")
          .option("inferSchema", "false")
          .option("delimiter", ",")
          .schema(SyntheticSchema.table_schema_five_string_sampled)
          .load(inputFilePath)
          .withColumnRenamed("value_one", f"value_one_$tableId%d")
          .withColumnRenamed("value_two", f"value_two_$tableId%d")
          .withColumnRenamed("value_three", f"value_three_$tableId%d")
          .withColumnRenamed("value_four", f"value_four_$tableId%d")
          .withColumnRenamed("value_five", f"value_five_$tableId%d")
          .withColumnRenamed("count", f"count_$tableId%d")
          .select("key",
            f"value_one_$tableId%d",
            f"value_two_$tableId%d",
            f"value_three_$tableId%d",
            f"value_four_$tableId%d",
            f"value_five_$tableId%d",
            f"count_$tableId%d")
          .rdd.keyBy(Row => Row.getAs[String]("key"))
          .asInstanceOf[RDD[(String, GenericRowWithSchema)]]
        perPartitionSampledRDDs = perPartitionSampledRDDs :+ partitionSampledRDD
      }
      var tableSampledRDD: RDD[(String, GenericRowWithSchema)] = perPartitionSampledRDDs.head
      for (partitionId: Int <- 1 until numPartitions) {
        tableSampledRDD = tableSampledRDD.union(perPartitionSampledRDDs(partitionId))
      }
      sampledRDDs = sampledRDDs :+ tableSampledRDD
    }

    // use the sampled data to construct the final sampled join
    //TODO: This works only for two tables at present.

    import spark.implicits._
    val sampledJoin = sampledRDDs.head.cogroup(sampledRDDs(1)).flatMapValues { pair =>
      val iterator1 = pair._1.iterator.flatMap { item =>
        val tuple = (
          item.get(1).asInstanceOf[String],
          item.get(2).asInstanceOf[String],
          item.get(3).asInstanceOf[String],
          item.get(4).asInstanceOf[String],
          item.get(5).asInstanceOf[String],
        )
        Iterator.fill(item.get(6).asInstanceOf[Int])(tuple)
      }

      val iterator2 = pair._2.iterator.flatMap { item =>
        val tuple = (
          item.get(1).asInstanceOf[String],
          item.get(2).asInstanceOf[String],
          item.get(3).asInstanceOf[String],
          item.get(4).asInstanceOf[String],
          item.get(5).asInstanceOf[String],
        )
        Iterator.fill(item.get(6).asInstanceOf[Int])(tuple)
      }
      iterator1.zip(iterator2)
    }.map(element =>
      (element._1,
        element._2._1._1,
        element._2._1._2,
        element._2._1._3,
        element._2._1._4,
        element._2._1._5,
        element._2._2._1,
        element._2._2._2,
        element._2._2._3,
        element._2._2._4,
        element._2._2._5)
    ).toDF(
      "key",
      "value_one_1", "value_two_1", "value_three_1", "value_four_1", "value_five_1",
      "value_one_2", "value_two_2", "value_three_2", "value_four_2", "value_five_2",
    )

    sampledJoin
      .write.format("csv")
      .option("header", "false")
      .mode(SaveMode.Overwrite)
      .save(outputFile)

    stageMetrics.end()
    stageMetrics.createStageMetricsDF("PerfStageMetrics")
    val aggMetricsDF = stageMetrics.aggregateStageMetrics("PerfStageMetrics")
    stageMetrics.saveData(aggMetricsDF, stageMetricsFile, saveMode = "overwrite")

    spark.stop()
  }
}
