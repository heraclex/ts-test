# telecom-data-pipeline
Contains spark apps to form the pipelines to process telecom data.
# Usage
## Build
Build this project with Maven by:

```mvn clean package```

## Submit
Inside the ```/target``` folder you will find the result fat jar called ```telecom-data-pipeline-0.0.1-SNAPSHOT-jar-with-depencencies.jar```. In order to launch the Spark job use this command in a shell with a configured Spark environment:
### CallHistoryTransformerApp

    spark-submit --class org.duong.assignment.CallHistoryTransformerApp \
      --master yarn-cluster \
      telecom-data-pipeline-1.0-SNAPSHOT-jar-with-dependencies.jar \
      rawInputPath \
      outputPath

The parameter ```rawInputPath``` is location of CDR files (`/raw/<date>/call_history/`)
The parameter ```outputpath``` is the path to write the output CSV file (`/input/<date>/call_history/,`).

### CallHistoryAggregatorApp

    spark-submit --class org.duong.assignment.CallHistoryAggregatorApp \
      --master yarn-cluster \
      telecom-data-pipeline-1.0-SNAPSHOT-jar-with-dependencies.jar \
      inputPath \
      outputPath

The parameter ```rawInputPath``` is location of transformed CSV files (`/input/<date>/call_history/`)
The parameter ```outputpath``` is the path to write the output aggregation data CSV file (`/aggr/<date>/call_history/,`).
