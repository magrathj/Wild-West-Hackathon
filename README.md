# Wild-West-Hackathon
Databricks &amp; Blueprint Hackathon



### Challenge Details

You are a data engineer for a sporting goods store.  They want to send the data from their sales system to THE CLOUD.  A developer on the sales system has written an application to send real time data in addition to the initial load from the sales system to an **Azure Event Hub**.  Utilizing the **Capture** feature within Azure Event Hub, that data is being delivered directly to **Azure Data Lake Storage**.  Unfortunately, the raw data is not very useable.  It will be your job to *munge* the data into a useable format and land the data into **Databricks Delta** tables.  Once you have figured out how to work with the data, you will need to go a step further and make sure that the data in Delta is always kept up to date with something called **Structured Streaming**.  The streaming data is all either Inserts or Updates.  You won't have to worry about deletes, but you will need to **merge** updated records into the Delta tables to represent current state.

 You have the choice of working in **Python** or **Scala** for this challenge.  One thing to keep in mind is that this is a *canned* hackathon.  We provide you with the starting point and also have a specific expectation of the ending point.  You are responsible for everything in between.  The starting point is the "Hackathon Start Up" notebook in either the Python or Scala folder.  The expectation for the ending point is streaming data into Delta tables created in the "Build Delta Tables" notebook.  In that notebook you will create the Database for your team, and the code for creating one of the tables is provided.  You will need to figure out the schemas for the remaining tables as part of the discovery process and populate the appropriate cells.



Set up databricks cli

```
    databricks configure --token --profile hackathon
```

View workspace

```
    databricks workspace list --profile hackathon
```