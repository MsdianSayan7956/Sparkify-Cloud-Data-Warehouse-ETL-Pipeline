### Data Warehouse ETL Pipeline for Sparkify 
#### 



###### 1. Project Overview

---



This project builds an ETL pipeline for Sparkify, a music streaming startup, to migrate their data processing to the cloud. The pipeline extracts data from JSON files in Amazon S3, stages it in Amazon Redshift, and transforms it into a star schema optimized for analytics on song plays.


###### 2\. Purpose of the Database



The primary purpose of this data warehouse is to optimize queries for song play analysis. Sparkify's analytics team needs to understand user behavior to drive business decisions. The raw data, stored as millions of JSON files in S3, is inefficient and difficult to query.

By creating a star schema, we provide a clean, denormalized structure that allows the analytics team to easily answer questions like:

What are our most popular songs and artists?

During which times of day, week, or month is user activity the highest?

Which subscription level (paid vs. free) generates more listening activity?

Are there geographic trends in our user base's listening habits?

This database serves as the single source of truth for user activity analysis, enabling fast, simple, and powerful analytical queries.





###### 3\. Database Schema Design



A star schema was chosen for this project because it is the industry standard for data warehousing and is highly optimized for the OLAP (Online Analytical Processing) queries that the analytics team will perform. Its simple structure (a central fact table linked to multiple dimension tables) allows for faster query performance and easier-to-understand queries compared to a normalized schema.



The schema consists of one fact table and four dimension tables:



Fact Table



songplays: This is the central table of our schema. Each row represents a single song play event, capturing transactional data.

songplay\_id (PRIMARY KEY, IDENTITY), start\_time, user\_id, level, song\_id, artist\_id, session\_id, location, user\_agent.



Dimension Tables



users: Stores descriptive information about each user in the app.

user\_id (PRIMARY KEY), first\_name, last\_name, gender, level.

songs: Stores metadata for each song in the music database.

song\_id (PRIMARY KEY), title, artist\_id, year, duration.

artists: Stores descriptive information about each artist.

artist\_id (PRIMARY KEY), name, location, latitude, longitude.

time: Stores timestamps broken down into useful units, allowing for easy time-based analysis.

start\_time (PRIMARY KEY), hour, day, week, month, year, weekday.

Design Justification

Distribution Style: Dimension tables (users, songs, artists, time) use DISTSTYLE ALL because they are relatively small and frequently joined. This ensures a copy of these tables exists on every node, eliminating data shuffling during joins. The songplays fact table uses user\_id as its DISTKEY to co-locate user song play data on the same node, optimizing joins with the users table.

Sort Key: The songplays and time tables use start\_time as their SORTKEY. This physically orders the data on disk by time, dramatically speeding up queries that filter on a time range (e.g., "show me all songs played last month").





###### 4\. The ETL Pipeline



The ETL process is orchestrated by the Python scripts and leverages the power of Amazon Redshift for scalable data processing.



Extract: The process begins with the raw data sources in S3:

s3://udacity-dend/song-data: JSON files with song and artist metadata.

s3://udacity-dend/log-data: JSON files with user activity logs.

Stage (Load into Staging): Using Redshift's highly efficient COPY command, the raw JSON data is bulk-loaded into two staging tables (staging\_events and staging\_songs).

This step is incredibly fast and scalable.

Staging tables provide a temporary holding area that decouples the extraction process from the final transformation, allowing for data validation and cleaning if needed.

Transform and Load (into Analytics Tables): Once the data is staged, a series of SQL INSERT INTO ... SELECT statements are executed to transform and load the data into the final star schema tables.

The songplays fact table is populated by joining staging\_events and staging\_songs to find the correct song\_id and artist\_id for each event.

Dimension tables (users, songs, artists, time) are populated by selecting distinct records from the staging tables.





###### 5\. Project File Structure



create\_tables.py: This script connects to the Redshift cluster, drops all existing tables to ensure a clean slate, and then creates the staging and star schema tables. It should be run first to initialize the database.



etl.py: This is the main ETL script. It orchestrates the entire pipeline: loading data from S3 into the staging tables and then transforming and inserting that data into the final analytics tables.



sql\_queries.py: A centralized Python module containing all the SQL statements used in the project. This includes DROP, CREATE, COPY, and INSERT queries, which keeps the main scripts clean and focused on orchestration.



dwh.cfg: A configuration file that stores the Redshift cluster endpoint, database credentials, IAM role ARN, and S3 paths. This keeps sensitive information separate from the source code.



README.md: This file, providing comprehensive documentation for the project.

sample\_analytics.sql : This file demonstrates the power of the ETL pipeline by showcasing real-world analytical queries that business users would run.

