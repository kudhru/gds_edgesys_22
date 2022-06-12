import random
import string
import sys

import numpy as np
from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Generate Data Uniform") \
    .config("spark.driver.memory", "16g") \
    .config("spark.driver.maxResultSize", "0") \
    .getOrCreate()


# synthetic_table_schema = StructType([
#     StructField("key", IntegerType(), True),
#     StructField("value", DoubleType(), True),
# ])

def generate_zipf_uniform_input_filename_per_partition(num_keys, min_rpk, max_rpk, zipf_param, ds_id,
                                                       file_system_prefix, overlap_fraction, partition, num_partitions,
                                                       nnjc_size):
    synthetic_file = file_system_prefix + "synthetic_file_string_n_{0}_rpk_{1}_{2}_zipf_{3}_overlap_{4}_nnjc_{5}_t_{6}_p_{7}_of_{8}.csv".format(
        num_keys,
        min_rpk,
        max_rpk,
        zipf_param,
        overlap_fraction,
        nnjc_size,
        ds_id,
        partition,
        num_partitions)
    return synthetic_file


def generate_and_write_data_zipf_for_first_table(num_keys, keys, min_rpk, max_rpk, zipf_param, ds_id,
                                                 file_system_prefix, overlap_fraction, partition, num_partitions,
                                                 nnjc_size):
    rpks = np.random.default_rng(12345).zipf(zipf_param, num_keys)
    keys = spark.sparkContext.parallelize(zip(keys, rpks))

    def generate_records_per_key(key, num_records):
        values1 = ["".join(random.choices(string.ascii_letters, k=nnjc_size)) for _ in range(num_records)]
        values2 = ["".join(random.choices(string.ascii_letters, k=nnjc_size)) for _ in range(num_records)]
        values3 = ["".join(random.choices(string.ascii_letters, k=nnjc_size)) for _ in range(num_records)]
        values4 = ["".join(random.choices(string.ascii_letters, k=nnjc_size)) for _ in range(num_records)]
        values5 = ["".join(random.choices(string.ascii_letters, k=nnjc_size)) for _ in range(num_records)]
        return [
            (key, value1, value2, value3, value4, value5)
            for value1, value2, value3, value4, value5 in zip(values1, values2, values3, values4, values5)
        ]

    value_tuples = keys.flatMap(lambda key: generate_records_per_key(key[0], int(key[1])))
    synthetic_trace_df = spark.createDataFrame(
        value_tuples,
        ['key', 'valueOne', 'valueTwo', 'valueThree', 'valueFour', 'valueFive']
    )

    synthetic_file = generate_zipf_uniform_input_filename_per_partition(num_keys, min_rpk, max_rpk, zipf_param, ds_id,
                                                                        file_system_prefix,
                                                                        overlap_fraction, partition, num_partitions,
                                                                        nnjc_size)
    print('file write started...')
    synthetic_trace_df.write.csv(synthetic_file, mode='overwrite')
    print('file write ended...')
    return synthetic_trace_df, synthetic_file


def generate_and_write_data_uniform_for_second_table(num_keys, keys, min_rpk, max_rpk, zipf_param, ds_id,
                                                     file_system_prefix, overlap_fraction, partition, num_partitions,
                                                     nnjc_size):
    min_rpk_per_partition = max(int(min_rpk / num_partitions), 1)
    max_rpk_per_partition = max(int(max_rpk / num_partitions), 1)
    rpks = np.random.uniform(min_rpk_per_partition, max_rpk_per_partition, len(keys))
    keys = spark.sparkContext.parallelize(zip(keys, rpks))

    def generate_records_per_key(key, num_records):
        values1 = ["".join(random.choices(string.ascii_letters, k=nnjc_size)) for _ in range(num_records)]
        values2 = ["".join(random.choices(string.ascii_letters, k=nnjc_size)) for _ in range(num_records)]
        values3 = ["".join(random.choices(string.ascii_letters, k=nnjc_size)) for _ in range(num_records)]
        values4 = ["".join(random.choices(string.ascii_letters, k=nnjc_size)) for _ in range(num_records)]
        values5 = ["".join(random.choices(string.ascii_letters, k=nnjc_size)) for _ in range(num_records)]
        return [
            (key, value1, value2, value3, value4, value5)
            for value1, value2, value3, value4, value5 in zip(values1, values2, values3, values4, values5)
        ]

    value_tuples = keys.flatMap(lambda key: generate_records_per_key(key[0], int(key[1])))
    synthetic_trace_df = spark.createDataFrame(
        value_tuples,
        ['key', 'valueOne', 'valueTwo', 'valueThree', 'valueFour', 'valueFive']
    )

    synthetic_file = generate_zipf_uniform_input_filename_per_partition(num_keys, min_rpk, max_rpk, zipf_param, ds_id,
                                                                        file_system_prefix,
                                                                        overlap_fraction, partition, num_partitions,
                                                                        nnjc_size)
    print('file write started...')
    synthetic_trace_df.write.csv(synthetic_file, mode='overwrite')
    print('file write ended...')
    return synthetic_trace_df, synthetic_file


num_keys = int(sys.argv[1])
min_rpk = int(sys.argv[2])
max_rpk = int(sys.argv[3])
zipf_param = float(sys.argv[4])
overlap_fraction = float(sys.argv[5])
num_partitions = int(sys.argv[6])
nnjc_size = int(sys.argv[7])
file_system_prefix = sys.argv[8]

num_overlapping_keys = int(num_keys * overlap_fraction)
num_non_overlapping_keys = num_keys - num_overlapping_keys
overlapping_keys = ["".join(random.choices(string.ascii_letters, k=16)) for _ in range(num_overlapping_keys)]
non_overlapping_keys_ds_2 = ["".join(random.choices(string.ascii_letters, k=16)) for _ in
                             range(num_non_overlapping_keys)]
non_overlapping_keys_ds_1 = ["".join(random.choices(string.ascii_letters, k=16)) for _ in
                             range(num_non_overlapping_keys)]
keys_ds_1 = overlapping_keys + non_overlapping_keys_ds_1
keys_ds_2 = overlapping_keys + non_overlapping_keys_ds_2

for partition in range(num_partitions):
    table_one_df, table_one_file = generate_and_write_data_zipf_for_first_table(
        num_keys, keys_ds_1, min_rpk, max_rpk, zipf_param, 0, file_system_prefix, overlap_fraction, partition,
        num_partitions, nnjc_size
    )
    table_two_df, table_two_file = generate_and_write_data_uniform_for_second_table(
        num_keys, keys_ds_2, min_rpk, max_rpk, zipf_param, 1, file_system_prefix, overlap_fraction, partition, num_partitions,
        nnjc_size
    )
