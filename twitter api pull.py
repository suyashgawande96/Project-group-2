# Databricks notebook source
#importing libraries

import http.client
import json
from pyspark.sql.functions import *

# COMMAND ----------

# for accessing blob

spark.conf.set(
    "fs.azure.account.key.hpiprojectblob01.dfs.core.windows.net",
    dbutils.secrets.get(scope="hpi-project-kv-01", key="hpi-project-blob-accesskey"))

# COMMAND ----------

# pulling data from API 

woeid_list = [1,
              23424848, #india
              23424922, #pakistan
              23424960, #thailand
              23424948  #singapore
             ]

for woeid in woeid_list:
    conn = http.client.HTTPSConnection("api.twitter.com")
    payload = ''
    headers = {
      'Authorization': 'Bearer {}'.format(dbutils.secrets.get(scope="hpi-project-kv-01", key="hpi-project-twitter-bearer-token")),
      'Cookie': 'guest_id=v1%3A167151438540552949; guest_id_ads=v1%3A167151438540552949; guest_id_marketing=v1%3A167151438540552949; personalization_id="v1_thS4QWw6QqAwrlLgvxmL+w=="'
    }
    conn.request("GET", "/1.1/trends/place.json?id={}".format(woeid), payload, headers)
    res = conn.getresponse()
    data = res.read()
    # print(data.decode("utf-8"))
    
    # tranforming the data into dataframe
    jsonResponse = json.loads(data.decode('utf-8'))
    df = spark.createDataFrame(jsonResponse)
    
    # writing the DF into parquet (bronze)
    
    df.write.format("parquet").mode("append").save("abfss://raw-data-zone@hpiprojectblob01.dfs.core.windows.net/twitter-temp/")

# COMMAND ----------

df = spark.read.parquet('abfss://raw-data-zone@hpiprojectblob01.dfs.core.windows.net/twitter-temp/')

df2 = df.select(df.as_of.alias("pulled_at"), df.created_at, df.locations[0].alias('location'), explode(df.trends).alias('data'))

# COMMAND ----------

df3 = (df2
       .withColumn('woeid', df2.location['woeid'])
       .withColumn('country', df2.location['name'])
       .withColumn('trend_name', df2.data['name'])
       .withColumn('search_url', df2.data['url'])
       .withColumn('is_promoted', df2.data['promoted_content'])
       .withColumn('tweet_volume', df2.data['tweet_volume'])
       .drop('data')
       .drop('location'))

df3.write.format("parquet").mode("overwrite").save("abfss://raw-data-zone@hpiprojectblob01.dfs.core.windows.net/twitter-silver/")

# COMMAND ----------

dbutils.fs.rm('abfss://raw-data-zone@hpiprojectblob01.dfs.core.windows.net/twitter-temp/', recurse=True)

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC SELECT count(*) FROM parquet.`abfss://raw-data-zone@hpiprojectblob01.dfs.core.windows.net/twitter-silver/`
