package com.etl.load

import com.etl.config.SinkConfig
import org.apache.spark.sql.DataFrame

object Loader {
  @transient lazy val logger = org.apache.log4j.Logger.getLogger(getClass.getName)

  def load(df: DataFrame, config: SinkConfig): Unit = {
    logger.info(s"Loading data to ${config.path} with format ${config.format} and coalesce ${config.coalesceNum}")
    
    val coalescedDf = if (config.coalesceNum > 0) {
      df.coalesce(config.coalesceNum)
    } else {
      df
    }

    coalescedDf.write
      .mode(config.mode)
      .format(config.format)
      .save(config.path)
      
    logger.info("Data load completed successfully")
  }
}
