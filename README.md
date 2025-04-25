# NBA Data Analysis Pipeline

A pet project focused on loading, merging, and auto-detecting data types from the [Kaggle NBA dataset](https://www.kaggle.com/datasets/wyattowalsh/basketball). 

This dataset is updated daily and includes:
- 30 teams
- 4800+ players
- 65,000+ games (every game since the inaugural 1946-47 NBA season)
- Box Scores for over 95% of all games
- Play-by-Play game data with 13M+ rows of Play-by-Play data in all!

The project is deployed using three virtual machines on Google Cloud Platform (GCP): one for the ClickHouse server, another for the PostgreSQL server, and a third for the main application server. BigQuery is used as a native service within GCP.

The project includes:
- Automated type inference and preprocessing for clean data ingestion
- Exploratory Data Analysis (EDA) with histograms, box plots, and correlation heatmaps
- Data normalization to improve distribution shapes (Yeo-Jonson transformation)
- Feature selection and identification of important variables
- Preparation for future predictive modeling tasks

Automatic schema detection is supported when uploading Kaggle CSV files directly to each of the three services: ClickHouse, PostgreSQL, and BigQuery, using pandas. For large tables, chunking and appending to a file on a remote drive were used to avoid out-of-memory errors.

Database mirroring was enabled between PostgreSQL and ClickHouse to synchronize changes between the databases. This was achieved using the PostgreSQL table engine in ClickHouse. For more details, refer to the [official ClickHouse documentation](https://clickhouse.com/docs/integrations/postgresql). 



