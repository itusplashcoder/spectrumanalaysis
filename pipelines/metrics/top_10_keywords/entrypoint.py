from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, lower, col, count, row_number
from pyspark.sql.window import Window
from pyspark.ml.feature import StopWordsRemover


def top_10_keywords_by_country(spark):
    # Read the fact_news table
    fact_news_df = spark.table("fact_news")
    tokenized_df = fact_news_df.withColumn(
        "word", explode(split(lower(col("news_text")), "\\W+"))
    )
    stop_words = StopWordsRemover.loadDefaultStopWords("english")
    stop_words_df = spark.createDataFrame(
        [(word,) for word in stop_words], ["stop_word"]
    )
    filtered_df = tokenized_df.join(
        stop_words_df, tokenized_df.word == stop_words_df.stop_word, "left_anti"
    )

    # Group by country and word, then count occurrences
    keyword_freq_df = filtered_df.groupBy("country_id", "word").agg(
        count("word").alias("frequency")
    )
    keyword_freq_df = keyword_freq_df.filter(keyword_freq_df.word != "")
    window_spec = Window.partitionBy("country_id").orderBy(col("frequency").desc())
    ranked_df = keyword_freq_df.withColumn("rank", row_number().over(window_spec))
    top_keywords_df = ranked_df.filter(col("rank") <= 10).drop("rank")

    # Save the resulting DataFrame to a Delta table
    top_keywords_df.write.mode("overwrite").format("delta").saveAsTable(
        "trending_keywords_by_country"
    )


def top_10_keywords_by_category(spark):
    # Read the fact_news table
    fact_news_df = spark.table("fact_news")
    tokenized_df = fact_news_df.withColumn(
        "word", explode(split(lower(col("news_text")), "\\W+"))
    )
    stop_words = StopWordsRemover.loadDefaultStopWords("english")
    stop_words_df = spark.createDataFrame(
        [(word,) for word in stop_words], ["stop_word"]
    )
    filtered_df = tokenized_df.join(
        stop_words_df, tokenized_df.word == stop_words_df.stop_word, "left_anti"
    )

    # Group by category and word, then count occurrences
    keyword_freq_df = filtered_df.groupBy("category_id", "word").agg(
        count("word").alias("frequency")
    )
    keyword_freq_df = keyword_freq_df.filter(keyword_freq_df.word != "")
    window_spec = Window.partitionBy("category_id").orderBy(col("frequency").desc())
    ranked_df = keyword_freq_df.withColumn("rank", row_number().over(window_spec))
    top_keywords_by_category_df = ranked_df.filter(col("rank") <= 10).drop("rank")
    top_keywords_by_category_df.write.mode("overwrite").format("delta").saveAsTable(
        "trending_keywords_by_category"
    )


if __name__ == "__main__":
    #Create session
    spark = SparkSession.builder.appName("Top10Keywords").getOrCreate()
    top_10_keywords_by_category(spark)
    top_10_keywords_by_country(spark)
