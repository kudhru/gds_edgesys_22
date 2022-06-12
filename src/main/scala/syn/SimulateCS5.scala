package syn

import ch.cern.sparkmeasure.StageMetrics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.count
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object SimulateCS5 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Simulate Centralized Sampling")
      .getOrCreate()

    // Read the user inputs into appropriate variables
    val inputFilePrefix: String = args(0)
    val numTables: Int = args(1).toInt
    val numPartitions: Int = args(2).toInt
    val samplingFraction: Double = args(3).toDouble
    val outputFile: String = args(4)
    val stageMetricsFile: String = args(5)

    val stageMetrics = StageMetrics(spark)

    stageMetrics.begin()

    // Read the table partitions into dataframes
    var tableDFs: Seq[DataFrame] = Seq()
    for (tableId: Int <- 0 until numTables) {
      var partitionDFs: Seq[DataFrame] = Seq()
      for (partitionId: Int <- 0 until numPartitions) {
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
        partitionDFs = partitionDFs :+ df
      }
      var tableDF: DataFrame = partitionDFs.head
      for (partitionId: Int <- 1 until numPartitions) {
        tableDF = tableDF.union(partitionDFs(partitionId))
      }
      tableDFs = tableDFs :+ tableDF
    }

    // Compute the frequency counts of the joining columns
    var perTableFreqCountDFs: Seq[DataFrame] = Seq()
    for (tableId: Int <- 0 until numTables) {
      perTableFreqCountDFs = perTableFreqCountDFs :+ tableDFs(tableId)
        .groupBy("key")
        .agg(count("*").as(f"count_$tableId%d"))
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
    var sampledRDDs: Seq[RDD[(String, (String, GenericRowWithSchema, Int))]] = Seq()
    for (tableId: Int <- 0 until numTables) {
      sampledRDDs = sampledRDDs :+ tableDFs(tableId)
        .rdd.keyBy(Row => Row.getAs[String]("key")).asInstanceOf[RDD[(String, GenericRowWithSchema)]]
        .join(perTableWeightMap(tableId))
        .mapPartitionsWithIndex(SamplingUtils.getSamplingFunc(samplingFraction), preservesPartitioning = true)
        .asInstanceOf[RDD[(String, GenericRowWithSchema, Int)]]
        .keyBy(Row => Row._1)
    }

    // use the sampled data to construct the final sampled join
    //TODO: This works only for two tables at present.
    val sampledJoin = sampledRDDs.head.cogroup(sampledRDDs(1)).flatMapValues { pair =>
      val iterator1 = pair._1.iterator.flatMap { item =>
        val tuple = (
          item._2.get(1).asInstanceOf[String],
          item._2.get(2).asInstanceOf[String],
          item._2.get(3).asInstanceOf[String],
          item._2.get(4).asInstanceOf[String],
          item._2.get(5).asInstanceOf[String],
        )
        Iterator.fill(item._3)(tuple)
      }

      val iterator2 = pair._2.iterator.flatMap { item =>
        val tuple = (
          item._2.get(1).asInstanceOf[String],
          item._2.get(2).asInstanceOf[String],
          item._2.get(3).asInstanceOf[String],
          item._2.get(4).asInstanceOf[String],
          item._2.get(5).asInstanceOf[String],
        )
        Iterator.fill(item._3)(tuple)
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
