from pyspark.sql import SparkSession, dataframe
from pyspark.sql.functions import (
    col,
    lit,
    avg,
    when,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    IntegerType,
    FloatType,
    StringType,
)

path = "books.csv"
schema = StructType(
    [
        StructField("bookID", StringType(), True),
        StructField("title", StringType(), True),
        StructField("authors", StringType(), True),
        StructField("average_rating", FloatType(), True),
        StructField("isbn", StringType(), True),
        StructField("isbn13", IntegerType(), True),
        StructField("language_code", StringType(), True),
        StructField("num_pages", IntegerType(), True),
        StructField("ratings_count", IntegerType(), True),
        StructField("text_reviews_count", IntegerType(), True),
        StructField("publication_date", StringType(), True),
        StructField("publisher", StringType(), True),
    ]
)


class BeelineTA():
    def __init__(self):
        self.spark_session = SparkSession.builder.master("local[*]").appName("beeline").getOrCreate()

    def task_1(self):
        return self.spark_session.read.csv(
            path=path,
            schema=schema,
            header=True,
        )

    def task_2(self, df: dataframe):
        return df.schema

    def task_3(self, df: dataframe):
        return df.count()

    def task_4(self, df: dataframe):
        return df.filter(col("average_rating") > 4.5).show()

    def task_5(self, df: dataframe):
        return df.agg(avg(col("average_rating"))).alias("avg_raiting").show()

    def task_6(self, df: dataframe):
        return (
            df.withColumn(
                "range",
                when(
                    (col("average_rating") >= 0) & (col("average_rating") <= 1), lit("0-1")
                ).otherwise(
                    when(
                        (col("average_rating") > 1) & (col("average_rating") <= 2),
                        lit("1-2"),
                    ).otherwise(
                        when(
                            (col("average_rating") > 2) & (col("average_rating") <= 3),
                            lit("2-3"),
                        ).otherwise(
                            when(
                                (col("average_rating") > 3) & (col("average_rating") <= 4),
                                lit("3-4"),
                            ).otherwise(
                                when(
                                    (col("average_rating") > 4)
                                    & (col("average_rating") <= 5),
                                    lit("4-5"),
                                ).otherwise(lit("Other"))
                            )
                        )
                    )
                ),
            )
            .groupBy("range")
            .count()
            .sort("range")
            .show()
        )


test = BeelineTA()
df = test.task_1()
test.task_2(df)
test.task_3(df)
test.task_4(df)
test.task_5(df)
test.task_6(df)