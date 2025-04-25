# NBA Data Analysis Pipeline

A pet project focused on loading, merging, and auto-detecting data types from the [Kaggle NBA dataset]([url](https://www.kaggle.com/datasets/wyattowalsh/basketball)). 
The project is deployed using three virtual machines created on Google Cloud Platofrm (GCP). One for clickhouse server, another for postgresql server, and the last one for the main server. BigQuery is enabled natively in GCP. 

The project includes:
- Automated type inference and preprocessing for clean data ingestion
- Exploratory Data Analysis (EDA) with histograms, box plots, and correlation heatmaps
- Data normalization to improve distribution shapes (Yeo-Jonson transformation)
- Feature selection and identification of important variables
- Preparation for future predictive modeling tasks

Automatic schema detection is supported when uploading Kaggle CSV files directly to each of the three services: ClickHouse, PostgreSQL, and BigQuery, using pandas. For large tables, chunking and appending to a file on a remote drive were used to avoid out-of-memory errors.

Database mirroring was enabled between PostgreSQL and ClickHouse to synchronize changes between the databases. This was achieved using the PostgreSQL table engine in ClickHouse. For more details, refer to the [official ClickHouse documentation]([url](https://clickhouse.com/docs/integrations/postgresql)). 


This dataset is updated daily and includes:
- 30 teams
- 4800+ players
- 65,000+ games (every game since the inaugural 1946-47 NBA season)
- Box Scores for over 95% of all games
- Play-by-Play game data with 13M+ rows of Play-by-Play data in all!
