# ApiMongoManager

Manage a MongoClient for use with ApiObjects

## Configuration

Uses [ConfigProvider](https://github.com/kscarr73/ConfigProvider_jre21)

- MONGO_URL: The URL for the Mongo Connection
- MONGO_DB:  The Database to Connect to

## Usage

```java
ApiMongoManager mongoDb = ApiMongoManager.getInstance();


```