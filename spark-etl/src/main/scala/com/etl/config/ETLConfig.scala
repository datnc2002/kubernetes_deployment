package com.etl.config

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import scala.util.Try
import scala.io.Source

case class SourceConfig(path: String, format: String, options: Map[String, String] = Map.empty)
case class SinkConfig(path: String, format: String, mode: String, coalesceNum: Int = 1)

/**
 * ETLConfig hỗ trợ 2 chế độ transform:
 *
 * 1. transformMode = "sql"    → dùng transformSQL (câu SQL thuần)
 *    → Ưu điểm: chỉ cần sửa file JSON, không cần compile lại code
 *
 * 2. transformMode = "custom" → dùng transformName (tên hàm DataFrame đã đăng ký)
 *    → Ưu điểm: dùng full sức mạnh DataFrame API (UDF, Window, etc.)
 *
 * Hỗ trợ multi-source: ngoài source chính (bảng Fact), có thể thêm
 * extraSources (VD: bảng Dimension Zone CSV) để join.
 */
case class ETLConfig(
  appName: String,
  transformMode: String = "sql",              // "sql" hoặc "custom"
  transformSQL: Option[String] = None,        // Câu Spark SQL (dùng khi mode = "sql")
  transformName: Option[String] = None,       // Tên hàm custom (dùng khi mode = "custom")
  source: SourceConfig,                       // Source chính (Fact table)
  extraSources: Map[String, SourceConfig] = Map.empty,  // Sources phụ (Dimension tables)
  sink: SinkConfig,
  dlqPath: Option[String] = None              // Đường dẫn Dead Letter Queue cho data lỗi
)

object ETLConfig {
  @transient lazy val logger = org.apache.log4j.Logger.getLogger(getClass.getName)

  def load(configPath: String): Try[ETLConfig] = {
    Try {
      logger.info(s"Loading config from path: $configPath")
      val source = Source.fromFile(configPath)
      val jsonString = try source.mkString finally source.close()

      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)
      val config = mapper.readValue(jsonString, classOf[ETLConfig])

      // Validate: kiểm tra mode hợp lệ
      config.transformMode.toLowerCase match {
        case "sql" =>
          require(config.transformSQL.isDefined && config.transformSQL.get.nonEmpty,
            "transformMode='sql' yêu cầu trường transformSQL không được rỗng")
        case "custom" =>
          require(config.transformName.isDefined && config.transformName.get.nonEmpty,
            "transformMode='custom' yêu cầu trường transformName không được rỗng")
        case other =>
          throw new IllegalArgumentException(
            s"transformMode='$other' không hợp lệ. Chỉ chấp nhận 'sql' hoặc 'custom'")
      }

      // Validate: kiểm tra mode write hợp lệ
      val validModes = Set("overwrite", "append", "ignore", "errorifexists")
      require(validModes.contains(config.sink.mode.toLowerCase),
        s"sink.mode='${config.sink.mode}' không hợp lệ. Chỉ chấp nhận: ${validModes.mkString(", ")}")

      config
    }
  }
}
