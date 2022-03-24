from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = SparkSession \
  .builder \
  .master('yarn') \
  .appName('spark-bigquery-demo') \
  .getOrCreate()

# Use the Cloud Storage bucket for temporary BigQuery export data used
# by the connector.
BUCKET = "movielens-dataset-de"
PROJECT = "zoomcamp-de-project-1"
spark.conf.set('temporaryGcsBucket', BUCKET)
#spark.conf.set('project', "zoomcamp-de-project-1")

# Load data from BigQuery.
movies = (spark.read.format('bigquery') 
          .option('table', f'{PROJECT}:movielens_dataset.movies') 
          .load()
)
#words.createOrReplaceTempView('words')

ratings = (spark.read.format('bigquery') 
          .option('table', f'{PROJECT}:movielens_dataset.ratings') 
          .load()
)

movies_ratings = (movies.join(ratings, on='movieId', how='inner')
                .select(
                  F.col('movieId').alias('movieId'),
                  F.col('title').alias('title'),
                  F.col('rating').alias('rating')
                )
                .groupBy('movieId', 'title')
                .agg({'rating':'avg'}).withColumnRenamed('avg(rating)', 'avgRating')
)

# Saving the data to BigQuery
(movies_ratings.write.format('bigquery')
  .mode('overwrite')
  .option('table', 'movielens_dataset.movies_ratngs') 
  .save()
)
