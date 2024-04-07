# Project Radical Sparks
## Online Extremism Detection Using Big Data & AI

![image](https://github.com/wbcmthh42/bead_pyspark/assets/104043746/59747138-fc44-4bf2-aa77-f2d63ff428d5)

![image](https://github.com/wbcmthh42/bead_pyspark/assets/104043746/b7fcc649-7aa6-4a69-bddd-a7749b44d50f)

### Data Sources:
Data will primarily be derived from social media posts, initially focusing on posts from Reddit for the proof-of-concept (POC). Reddit, a globally popular social news platform has become increasingly popular amongst the younger generation5 which makes it a suitable data source for this project.

### I.Reddit API: 
The project will make use of the Reddit API to access and retrieve text posts from various subreddits related to Singapore. In addition, Python Reddit API Wrapper (PRAW), which is a Python package that allows for simple access to Reddit's API will also be used.

### II.Simulated Data: 
To address scenarios where there may be limited or even no user posts relating to radical activities/ideology found during the data collection period, the team may look into injecting simulated radical posts to fine-tune the model and test for performance.

### Data Storage:
The main data type consists of text strings and the stored data will be retrieved frequently for analysis. Therefore, an optimal choice for database storage would be a columnar database, designed to efficiently handle reads and writes. This type of database stores data in columns rather than rows, which aligns well with the requirements of frequent data retrieval.

In this scenario, the team will explore storing the data in a Parquet file format, which offers several advantages, including efficient compression and columnar storage. With accessibility in mind, the team will strive to utilise cloud services for storage, otherwise, the data will be stored on a local system.

### Data Retrieval and Ingestion:
Data retrieval can be done through a batch processing job cadence (either daily or more frequently when required) to collect recent posts and comments using the Reddit API. Following data retrieval through batch processing, the information undergoes preprocessing before being stored in the Parquet file format, as previously mentioned.

### Data Analysis:
The stored data is then retrieved and processed with PySpark for analysis which could include:

#### I.Text processing
#### II.Classification of data into either radical or non-radical posts by running the text through suitable LLMs to create labels for each post.
#### III.Using a suitable dashboard to output selected metrics on the findings after the text has been processed and classified.
