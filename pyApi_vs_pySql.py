from docx import Document
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower, regexp_replace, trim
import time
from itertools import combinations_with_replacement

#########################################
'''Convert .docx to .txt'''
#########################################
doc = Document("ModernOperatingSystems.docx")  # Load the .docx file

# All non-empty paragraphs are joined into one string along with line breaks
full_text = "\n".join([para.text for para in doc.paragraphs if para.text.strip() != ""])

# Cleaned text is saved to a .txt file
with open("ModernOperatingSystems_clean.txt", "w", encoding="utf-8") as f:
    f.write(full_text)

print("YES: .docx converted to .txt")


#########################################
''' implement Spark'''
#########################################
#  Spark application name is set
'''spark = SparkSession.builder \
    .appName("mileStoneFinal") \
    .getOrCreate()  # Starting the Spark session'''
    

spark = SparkSession.builder \
    .appName("mileStoneFinal") \
    .config("spark.sql.inMemoryColumnarStorage.enabled", "false") \
    .config("spark.sql.cbo.enabled", "false") \
    .config("spark.sql.autoBroadcastJoinThreshold", "-1") \
    .config("spark.sql.adaptive.enabled", "false") \
    .config("spark.sql.shuffle.partitions", "8") \
    .getOrCreate()

# Clear any cached data
spark.catalog.clearCache()



#########################################
'''Load, preprocess text'''
#########################################
# cleaned text file is read into DataFrame
textDF = spark.read.text("ModernOperatingSystems_clean.txt").withColumnRenamed("value", "line")

# make lowercase, removing punctuation 
cleanedDF = textDF.select(
    regexp_replace(lower(col("line")), r"[^a-z\s]", "").alias("cleanLines")
)

# each line is split into words
tokenized_df = cleanedDF.select(
    split(trim(col("cleanLines")), r"\s+").alias("words")
)

# filter out blanks and also flattening words into individual rows 
words_df = tokenized_df.select(
    explode(col("words")).alias("word")
).filter(col("word") != "")

# Make SQL-accessible table
words_df.createOrReplaceTempView("wordTable_temp")
tokenized_df.createOrReplaceTempView("lineTable_temp")


print("YES: Text loaded and preprocessed")


#########################################
'''1: Total Word Count for pyspark/SQL'''
#########################################
print("\n--- Total Word Count ---")
print()

startPYSPARK = time.time()
pySparkWordCount = words_df.count()  # Count total words (API)
pysparkTime = round(time.time() - startPYSPARK, 4)
print("API total words:", pySparkWordCount)
print("API time:", pysparkTime, "seconds")
print()

startSQL = time.time()
sqlWordCount = spark.sql("SELECT COUNT(*) AS total_words FROM wordTable_temp")
sqlCountVal = sqlWordCount.collect()[0]["total_words"]  # Extract value from row
print("SQL total words:", sqlCountVal)
sqlTime = round(time.time() - startSQL, 4)
print("SQL time:", sqlTime, "seconds")

print()
# Compare results to verfiy both methods accomplishing the same goal
if sqlCountVal == pySparkWordCount:
    print(f"YES!: Word counts match! Total words: {sqlCountVal}")
else:
    print(f"NO!: Mismatch! SQL: {sqlCountVal}, API: {pySparkWordCount}")

print()
print("_____________________________________________________")
print()
print()

#########################################
''' 2 : Word Frequency pySpark vs SQL'''
#########################################
print("\n--- Word Frequency(API) ---")

startPYSPARK = time.time()
# Grouping by word then count frequency and then order by most frequent
pysparkFrequency = words_df.groupBy("word") \
    .count() \
    .withColumnRenamed("count", "frequency") \
    .orderBy(col("frequency").desc()) \
    .limit(20)

pysparkFrequency.show(truncate=False)
pysparkFrequencyTime = round(time.time() - startPYSPARK, 4)
print("API frequency time:", pysparkFrequencyTime, "seconds")
print()
print()

print("\n--- Word Frequency(SQL) ---")
startSQL = time.time()
# SQL version for word frequency
sqlFrequency = spark.sql("""
    SELECT word, COUNT(*) AS frequency
    FROM wordTable_temp

    GROUP BY word
    ORDER BY frequency DESC
    LIMIT 20
""")

sqlFrequency.show(truncate=False)
sqlFrequencyTime = round(time.time() - startSQL, 4)
print("SQL frequency time:", sqlFrequencyTime, "seconds")
print()

# Compare top 20 frequencies for API and SQL to verfiy both methods accomplishing the same goal
sqlRes = [(row["word"], row["frequency"]) for row in sqlFrequency.collect()]
pysparkRes = [(row["word"], row["frequency"]) for row in pysparkFrequency.collect()]

if sqlRes == pysparkRes:
    print("YES!: SQL and API word frequency results MATCH for top 20 words.")
else:
    print("NO!: SQL and API word frequency results DO NOT match!")
    print("SQL Top 20:", sqlRes)
    print("API Top 20:", pysparkRes)

print("_____________________________________________________")   
print()
print()   

##########################################
''' 3 : Word Pairs, pySpark vs SQL'''
##########################################

print("\n--- Word Pairs (PySpark API + SQL, Unordered) ---")

from pyspark.sql.functions import udf, explode
from pyspark.sql.types import ArrayType, StringType

#  unordered word pairs are generated
def generate_pairs(words):
    words = sorted(set(words))  # Unique and sorted words
    return [f"{w1}|{w2}" for w1, w2 in combinations_with_replacement(words, 2)]

# UDF is created to use with PySpark and SQL
getPairs = udf(generate_pairs, ArrayType(StringType()))
spark.udf.register("generate_pairs_udf", getPairs)

startPYSPARK = time.time()

# Pairs from each line are generated and exploded into individual rows
pySparkPair = tokenized_df.withColumn("pairs", getPairs(col("words"))) \
                          .select(explode(col("pairs")).alias("pair"))

# Top 20 word pairs are counted and sorted
pysparkPairFreq = pySparkPair.groupBy("pair") \
                             .count() \
                             .orderBy(col("count").desc()) \
                             .limit(20)

print("\nTop 20 Word Pairs (API):")
pysparkPairFreq.show(truncate=False)
pysparkPairTime = round(time.time() - startPYSPARK, 4)
print("API word pair time:", pysparkPairTime, "seconds")
print()
print()

startSQL = time.time()

# Create exploded word pair table using UDF for SQL version
spark.sql("""
    CREATE OR REPLACE TEMP VIEW pair_table AS
    SELECT explode(generate_pairs_udf(words)) AS pair
    FROM lineTable_temp
""")


# Count and order top 20 word pairs for SQL version
sqlPairFreq = spark.sql("""
    SELECT pair, COUNT(*) AS count
    FROM pair_table
    GROUP BY pair
    ORDER BY count DESC
    LIMIT 20
""")

print("\nTop 20 Word Pairs (SQL):")
sqlPairFreq.show(truncate=False)
sqlPairTime = round(time.time() - startSQL, 4)
print("SQL word pair time:", sqlPairTime, "seconds")

# API and SQL results are compared
pysparkPairRes = sorted([(row["pair"], row["count"]) for row in pysparkPairFreq.collect()])
sqlPairRes = sorted([(row["pair"], row["count"]) for row in sqlPairFreq.collect()])

if pysparkPairRes == sqlPairRes:
    print("YES!: Word pair results MATCH between API and SQL.")
else:
    print("NO!: Word pair results DO NOT match!")
    print("API Top 20:", pysparkPairRes)
    print("SQL Top 20:", sqlPairRes)


'''

#  loop through 100 times and stop when API wins the race/ is faster
def generate_pairs(words):
    words = sorted(set(words))
    return [f"{w1}|{w2}" for w1, w2 in combinations_with_replacement(words, 2)]

getPairs = udf(generate_pairs, ArrayType(StringType()))

for i in range(1, 101):  # Run up to 100 times
    print(f"\nRun {i}")

    try:
        spark.stop()
    except:
        pass

    spark = SparkSession.builder.appName("mileStoneFinal").getOrCreate()
    spark.udf.register("generate_pairs_udf", getPairs)

    textDF = spark.read.text("ModernOperatingSystems_clean.txt").withColumnRenamed("value", "line")
    cleanedDF = textDF.select(
        regexp_replace(lower(col("line")), r"[^a-z\s]", "").alias("cleanLines")
    )
    tokenized_df = cleanedDF.select(
        split(trim(col("cleanLines")), r"\s+").alias("words")
    )
    tokenized_df.createOrReplaceTempView("lineTable")

    # --- API ---
    startPYSPARK = time.time()
    pySparkPair = tokenized_df.withColumn("pairs", getPairs(col("words"))) \
                              .select(explode(col("pairs")).alias("pair"))

    pysparkPairFreq = pySparkPair.groupBy("pair") \
                                 .count() \
                                 .orderBy(col("count").desc()) \
                                 .limit(20)
    pysparkPairFreq.collect()
    pysparkPairTime = round(time.time() - startPYSPARK, 4)

    # --- SQL ---
    startSQL = time.time()
    spark.sql("""
        CREATE OR REPLACE TEMP VIEW pair_table AS
        SELECT explode(generate_pairs_udf(words)) AS pair
        FROM lineTable
    """)

    sqlPairFreq = spark.sql("""
        SELECT pair, COUNT(*) AS count
        FROM pair_table
        GROUP BY pair
        ORDER BY count DESC
        LIMIT 20
    """)
    sqlPairFreq.collect()
    sqlPairTime = round(time.time() - startSQL, 4)

    print("API word pair time:", pysparkPairTime, "seconds")
    print("SQL word pair time:", sqlPairTime, "seconds")

    if pysparkPairTime < sqlPairTime:
        print(f"\n YES!!!! API wins on run {i}!")
        print(f"API: {pysparkPairTime} sec | SQL: {sqlPairTime} sec")
        break
else:
    print("\n NO API win in 100 runs.")'''




print("------------------------------------------------")


#############################################################################
'''Export CSV'''                                                           
#############################################################################

from pyspark.sql import Row

print("\n--- Results to CSV Files ---")

# SQL and API total word counts are exported
sql_wordcount_df = spark.createDataFrame([Row(source="SQL", total_words=sqlCountVal)])
api_wordcount_df = spark.createDataFrame([Row(source="API", total_words=pySparkWordCount)])
sql_wordcount_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("sql_word_count.csv")
api_wordcount_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("api_word_count.csv")

# Top word frequency results are exported
sqlFrequency.coalesce(1).write.mode("overwrite").option("header", "true").csv("sql_word_frequency.csv")
pysparkFrequency.coalesce(1).write.mode("overwrite").option("header", "true").csv("api_word_frequency.csv")

# Top word pairs are exported
sqlPairFreq.coalesce(1).write.mode("overwrite").option("header", "true").csv("sql_word_pairs.csv")
pysparkPairFreq.coalesce(1).write.mode("overwrite").option("header", "true").csv("api_word_pairs.csv")

print("\n--- Saving Runtime Comparison to CSV ---")

# Combine all runtimes into one DF
runtime_data = [
    Row(task="word_count", method="SQL", runtime_seconds=sqlTime),
    Row(task="word_count", method="API", runtime_seconds=pysparkTime),
    Row(task="word_frequency", method="SQL", runtime_seconds=sqlFrequencyTime),
    Row(task="word_frequency", method="API", runtime_seconds=pysparkFrequencyTime),
    Row(task="word_pairs", method="SQL", runtime_seconds=sqlPairTime),
    Row(task="word_pairs", method="API", runtime_seconds=pysparkPairTime),
]

# Runtime comparison are exported
runtime_df = spark.createDataFrame(runtime_data)
runtime_df.coalesce(1).write.mode("overwrite").option("header", "true").csv("runtime_comparison.csv")

print("results exported to CSV.")
