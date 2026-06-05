package com.etl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
 * Transformer hỗ trợ 2 chế độ:
 *
 * Mode "sql":
 *   - Tạo temp view "raw_data" từ DataFrame đầu vào
 *   - Chạy câu Spark SQL từ config
 *   - Ưu điểm: chỉ cần sửa file JSON → không cần compile lại
 *   - Lưu ý: Spark SQL dùng cùng Catalyst Optimizer với DataFrame API
 *            → hiệu năng TƯƠNG ĐƯƠNG, không bị chậm hơn
 *
 * Mode "custom":
 *   - Gọi hàm DataFrame đã đăng ký trong CustomTransforms
 *   - Ưu điểm: dùng full DataFrame API (UDF, Window, nhiều bước)
 *   - extraDfs: Map chứa các DataFrame phụ (Dimension), sẽ được truyền cho hàm custom
 */
object Transformer {
  @transient lazy val logger = org.apache.log4j.Logger.getLogger(getClass.getName)

  def transform(
    spark: SparkSession,
    df: DataFrame,
    mode: String,
    sql: Option[String],
    customName: Option[String],
    extraDfs: Map[String, DataFrame] = Map.empty,
    dlqPath: Option[String] = None
  ): DataFrame = {

    logger.info(s"Starting transformation with mode: $mode")

    val result = mode.toLowerCase match {

      case "sql" =>
        val sqlQuery = sql.getOrElse(
          throw new IllegalArgumentException("transformSQL is required for sql mode"))

        // Đăng ký DataFrame thành bảng tạm "raw_data" để SQL truy vấn được
        df.createOrReplaceTempView("raw_data")

        // Đăng ký thêm các extra DataFrames nếu có
        extraDfs.foreach { case (name, extraDf) =>
          extraDf.createOrReplaceTempView(name)
          logger.info(s"Registered extra DataFrame as temp view: $name")
        }

        logger.info(s"Executing Spark SQL: $sqlQuery")
        spark.sql(sqlQuery)

      case "custom" =>
        val transformName = customName.getOrElse(
          throw new IllegalArgumentException("transformName is required for custom mode"))

        logger.info(s"Executing custom DataFrame transform: $transformName")

        // Lấy hàm transform từ registry — truyền kèm extraDfs và dlqPath
        val transformFn = CustomTransforms.get(transformName, extraDfs, dlqPath)
        transformFn(df)

      case other =>
        throw new IllegalArgumentException(s"Unknown transform mode: $other")
    }

    logger.info(s"Transformation completed. Output schema: ${result.schema.simpleString}")
    result
  }
}
