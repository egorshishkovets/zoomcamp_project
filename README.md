### Zoomcamp project
Movilens dataset is represent ratings movies from huge ammount of users. That's why we can build ratings distrubution.

In this project have been used:
* Docker
* Airflow
* GCP
* PySpark


### GCP
1. Create an account on GCP
1. Setup a new project and write down the Project ID. If you don't want change something in source code use `zoomcamp-de-project` name for your project
1. Setup a service account for this project and download the JSON authentication key files. Put it to `/.google/credentials/google_credentials.json`
1. Assign the following IAM Roles to the Service Account: Owner
  Unfortunatly I haven't enoughth time to go deeper at GCP role model. In this project we will create GCP cluster and it's possible with this role
1. Create bucket in Cloud Storage with name `movielens-dataset-de` (You can use another one name, but make sure you made also changes in the source code)
2. Go to Biq Query and create dataset with name `movielens_dataset` for `zoomcamp-de-project-1` project

### Data ingestion
1. Go to /airflow folder using bash. After execute next commands:
2. ```docker-compose build```
4.   ```docker-compose up airflow-init```
4.  ```docker-compose up```
5.  Browse to `localhost:8080`. Username and password are both `airflow` 
6.  Trigger `movielens_dag`
  * This dag upload movielens dataset from `https://files.grouplens.org/datasets/movielens/ml-latest.zip` to your Big Query
  * for `tags.csv`, `ratings.csv` creating partitioned and clusterd table. Partition via timestamp column, that truncated to monthly format. Also clustered by `userID`, `movieId`.
  * `links.csv`, `movies.csv`,`genome-scores.csv` clustered by `movieId`. This made for more efficient join with `ratings` BQ tables
7. Trigger `batch_processing_dags`
  * This dag will create `movies_ratings` table in BQ via PySpark job submiting to cluster that also will create and drop while dag working
  * Movies ratings is average through all ratings that represent in current dataset

### Dashboard
There are two tiles on dashboard
1. Distribution of movies ratings
2. Count of rated movies and users that give their marks per month
<img width="764" alt="Снимок экрана 2022-03-24 в 15 04 33" src="https://user-images.githubusercontent.com/15835468/159936031-b0c6726d-3def-409e-83a6-96e813286584.png">


