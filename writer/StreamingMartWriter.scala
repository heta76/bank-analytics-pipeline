package writer

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.streaming.StreamingQuery

class StreamingMartWriter {

  def write(df: DataFrame): StreamingQuery = {

    df.writeStream
      .format("parquet")
      .option(
        "path",
        "hdfs://namenode:9000/marts/card_activity_streaming"
      )
      .option(
        "checkpointLocation",
        "hdfs://namenode:9000/checkpoints/card_activity_streaming"
      )
      .outputMode("append")
      .start()

  }

}