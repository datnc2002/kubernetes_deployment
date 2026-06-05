package com.etl

import com.etl.config.ETLConfig
import com.etl.extract.Extractor
import com.etl.transform.Transformer
import com.etl.load.Loader
import org.apache.spark.sql.SparkSession
import scala.util.{Failure, Success}

object SparkETLApp {
  @transient lazy val logger = org.apache.log4j.Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      logger.error("Usage: SparkETLApp <path-to-etl-config.json>")
      throw new IllegalArgumentException("Usage: SparkETLApp <path-to-etl-config.json>")
    }

    val configPath = args(0)
    logger.info(s"Starting Spark ETL Job with config: $configPath")

    ETLConfig.load(configPath) match {
      case Success(config) =>
        logger.info(s"Config loaded successfully for app: ${config.appName}")
        logger.info(s"Transform mode: ${config.transformMode}")

        val spark = SparkSession.builder()
          .appName(config.appName)
          // Bật Dynamic Partition Overwrite → ghi đè an toàn, đảm bảo Idempotency
          .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
          .getOrCreate()

        try {
          // 1. Extract: Đọc dữ liệu chính (Fact table) từ source
          val rawDf = Extractor.extract(spark, config.source)
          
          // 1b. Extract các source phụ (Dimension tables) nếu có
          val extraDfs = config.extraSources.map { case (name, srcConfig) =>
            logger.info(s"Extracting extra source: $name")
            name -> Extractor.extract(spark, srcConfig)
          }
          
          // 2. Transform: Chạy transform theo mode (sql hoặc custom)
          val transformedDf = Transformer.transform(
            spark,
            rawDf,
            config.transformMode,
            config.transformSQL,
            config.transformName,
            extraDfs,
            config.dlqPath
          )
          
          // 3. Load: Ghi kết quả ra sink (GCS/HDFS/local)
          Loader.load(transformedDf, config.sink)
          
          logger.info(s"Spark ETL Job [${config.appName}] completed successfully")
        } catch {
          case e: Exception =>
            logger.error("Error occurred during ETL pipeline execution", e)
            throw e
        } finally {
          spark.stop()
        }

      case Failure(e) =>
        logger.error(s"Failed to load config from $configPath", e)
        throw new RuntimeException(s"Failed to load config from $configPath: ${e.getMessage}", e)
    }
  }
}
