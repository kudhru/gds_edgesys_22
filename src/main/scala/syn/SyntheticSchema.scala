package syn

import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructType}

object SyntheticSchema {
  val freq_table_schema = new StructType()
    .add("key", StringType, true)
    .add("count", LongType, true)

  val table_schema_five_string = new StructType()
    .add("key", StringType, true)
    .add("value_one", StringType, true)
    .add("value_two", StringType, true)
    .add("value_three", StringType, true)
    .add("value_four", StringType, true)
    .add("value_five", StringType, true)

  val table_schema_five_string_sampled = new StructType()
    .add("key", StringType, true)
    .add("value_one", StringType, true)
    .add("value_two", StringType, true)
    .add("value_three", StringType, true)
    .add("value_four", StringType, true)
    .add("value_five", StringType, true)
    .add("count", IntegerType, true)
}
