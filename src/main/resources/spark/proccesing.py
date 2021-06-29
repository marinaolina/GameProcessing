from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from urllib.request import urlopen
import args as A
import config as C
import utils as U
import sys
import gzip
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType


# Passed parameters
REPORT = A.get_report_name(sys.argv[1])
DATE = A.get_date(sys.argv[2])
FORMAT = A.get_format(sys.argv[3])
# REPORT = A.get_report_name('input')
# DATE = A.get_date('2017-02-01')
# FORMAT = A.get_format('json')


def transform(self, f):
    return f(self)


DataFrame.transform = transform

sc = SparkContext(appName="json.processing")
LOGGER = sc._jvm.org.apache.log4j.LogManager.getLogger(__name__)
LOGGER.info("pyspark script logger initialized")


def main():
    spark = (
        SparkSession
            .builder
            .master("local")
            .config("hive.exec.dynamic.partition", "true")
            .config("hive.exec.dynamic.partition.mode", "nonstrict")
            .config("spark.sql.session.timeZone", "Africa/Abidjan")
            .enableHiveSupport()
            .getOrCreate()
    )

    schema = StructType([
        StructField("user", IntegerType(), nullable=False),
        StructField("ts", DoubleType(), nullable=False),
        StructField("context", StructType([
            StructField('hard', IntegerType(), False),
            StructField('soft', IntegerType(), False),
            StructField('level', IntegerType(), False)
            ]), False),
        StructField("ip", StringType(), nullable=False),
        StructField("row_text", StringType(), nullable=True)
                         ])

    data = gzip.decompress(urlopen(C.FILE_CALL(REPORT, DATE, FORMAT)).read())
    rdd = spark.sparkContext.parallelize(data.decode('utf-8').split('\n'))
    df = (
        spark
        .read
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "row_text")
        .schema(schema)
        .json(rdd)
        .transform(U.add_exception_column(
            exception_column_name=C.ERROR_TEXT,
            malformed_column_names=C.MALFORMED_COLUMN_NAMES,
            malformed_column_error=C.MALFORMED_COLUMN_ERROR,
            timestamp_column_names=C.TIMESTAMP_COLUMN_NAMES,
            timestamp_column_error=C.TIMESTAMP_COLUMN_ERROR
        ))
        .cache()
    )

    df.show()
    # print(df.count())
    df.printSchema()

    # Save error table
    (df
        .filter(F.col(C.ERROR_TEXT).isNotNull())
        .withColumn(C.API_REPORT, F.lit(REPORT))
        .withColumn(C.API_DATE, F.lit(DATE))
        .transform(U.cast_cols_as_date([C.API_DATE]))
        .withColumn(C.RECORD_CONCATENATED, F.concat_ws("|", *[C.RECORD_CONCATENATED, "user", "ts", "context.hard", "context.soft", "context.level", "ip"]))
        .withColumn(C.INS_TS, F.current_timestamp())
        .select(list(C.FIELDS_PREPARED_ERROR.keys()))
        .write.saveAsTable(name=C.TABLE_PREPARED_ERROR, format="parquet", mode="append")
    )

    # Save target table
    (df
        .filter(F.col(C.ERROR_TEXT).isNull())
        .transform(U.cast_cols_as_ts(C.TIMESTAMP_COLUMN_NAMES))
        .transform(U.cast_cols_as_integer(C.INTEGER_COLUMN_NAMES))
        .select(list(C.FIELDS_PREPARED_TARGET.keys()))
        .write.saveAsTable(name=C.TABLE_PREPARED_TARGET, format="parquet", mode="append")
    )

    spark.sql("select * from data_error").show(truncate=False)
    spark.sql("select * from report_input").show(truncate=False)

    sc.stop()


if __name__ == "__main__":
    main()

