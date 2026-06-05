package com.etl.extract

import com.etl.config.SourceConfig
import org.apache.spark.sql.{DataFrame, SparkSession}

object Extractor {
  @transient lazy val logger = org.apache.log4j.Logger.getLogger(getClass.getName)

  def extract(spark: SparkSession, config: SourceConfig): DataFrame = {
    logger.info(s"Extracting data from ${config.path} with format ${config.format}")
    
    val df = config.format.toLowerCase match {
      case "parquet" => spark.read.parquet(config.path)
      case "csv"     => spark.read.options(config.options).csv(config.path)
      case "json"    => spark.read.options(config.options).json(config.path)
      case "orc"     => spark.read.orc(config.path)
      case other     => throw new IllegalArgumentException(s"Unsupported format: $other")
    }
    
    // ✅ Chỉ log schema, KHÔNG gọi count() — giữ nguyên lazy evaluation
    logger.info(s"Extraction configured. Schema: ${df.schema.simpleString}")
    df
  }
}
