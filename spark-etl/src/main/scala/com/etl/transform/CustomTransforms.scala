package com.etl.transform

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

/**
 * Registry các hàm DataFrame transform cho mode "custom".
 *
 * ── Bài toán TLC HVFHV 2021 ──
 * Hàm chính: "tlc_hvfhv_kpi"
 *   - Cleansing + Dead Letter Queue (DLQ)
 *   - Broadcast Join với bảng Zone (Dimension)
 *   - Tính 5 KPIs nghiệp vụ, ghi mỗi KPI ra thư mục riêng
 *   - Return: KPI đầu tiên (total_trips_by_zone) làm output chính
 */
object CustomTransforms {
  @transient lazy val logger = org.apache.log4j.Logger.getLogger(getClass.getName)

  /**
   * Trả về hàm transform (DataFrame => DataFrame) dựa trên tên đăng ký.
   *
   * @param name     Tên hàm transform
   * @param extraDfs Map chứa các DataFrame phụ (VD: "dimension" -> zone CSV)
   * @param dlqPath  Đường dẫn ghi Dead Letter Queue (data lỗi)
   */
  def get(
    name: String,
    extraDfs: Map[String, DataFrame] = Map.empty,
    dlqPath: Option[String] = None
  ): DataFrame => DataFrame = {

    logger.info(s"Loading custom transform: $name")

    name match {
      // ─────────────────────────────────────────────────────────
      // TLC HVFHV 2021: Xử lý 5 KPIs nghiệp vụ
      // ─────────────────────────────────────────────────────────
      case "tlc_hvfhv_kpi" => factDf => {
        val spark = factDf.sparkSession

        // ── Bước 1: Data Cleansing + DLQ ──
        val goodDF = factDf.filter(
          col("trip_miles") > 0 &&
          col("trip_time") > 0 &&
          col("base_passenger_fare") >= 0
        )

        val badDF = factDf.filter(
          col("trip_miles") <= 0 ||
          col("trip_time") <= 0 ||
          col("base_passenger_fare") < 0
        )

        // Ghi data lỗi vào DLQ nếu có cấu hình đường dẫn
        dlqPath.foreach { path =>
          val badCount = badDF.count()
          val totalCount = factDf.count()
          val errorRate = if (totalCount > 0) badCount.toDouble / totalCount * 100 else 0

          logger.info(s"DLQ: $badCount invalid records (${f"$errorRate%.2f"}% of total)")

          if (badCount > 0) {
            badDF.write.mode("append").parquet(path)
            logger.info(s"DLQ data written to: $path")
          }

          // Cảnh báo nếu tỷ lệ lỗi > 1%
          if (errorRate > 1.0) {
            logger.warn(s"⚠ ALERT: Error rate ${f"$errorRate%.2f"}% exceeds 1% threshold!")
          }
        }

        // ── Bước 2: Broadcast Join với bảng Zone ──
        val enrichedDF = extraDfs.get("dimension") match {
          case Some(zoneDf) =>
            logger.info("Performing Broadcast Hash Join with Zone lookup table")
            goodDF.join(
              broadcast(zoneDf),
              goodDF("PULocationID") === zoneDf("LocationID"),
              "left"
            )
          case None =>
            logger.warn("No dimension source provided — skipping Zone join")
            goodDF
        }

        // Lấy basePath từ sink — lấy phần cha để ghi 5 thư mục con
        // VD: sink.path = gs://bucket/output/kpi/ → basePath = gs://bucket/output/kpi
        // Các KPI sẽ ghi ra: basePath/total_trips_by_zone/, basePath/avg_wait_time/, ...

        // ── Bước 3: Tính 5 KPIs ──

        // KPI 1: Total Trips by Pickup Zone (Mật độ thị trường)
        val kpi1 = enrichedDF
          .groupBy("Borough", "Zone")
          .agg(count("*").alias("total_trips"))
          .orderBy(desc("total_trips"))

        logger.info("KPI 1 — Total Trips by Zone: computed")

        // KPI 2: Average Wait Time (Chất lượng dịch vụ)
        val kpi2 = enrichedDF
          .withColumn("wait_time_minutes",
            (unix_timestamp(col("pickup_datetime")) - unix_timestamp(col("request_datetime"))) / 60.0
          )
          .filter(col("wait_time_minutes") >= 0 && col("wait_time_minutes") < 120) // lọc outlier
          .groupBy("Borough")
          .agg(
            avg("wait_time_minutes").alias("avg_wait_minutes"),
            count("*").alias("trip_count")
          )

        logger.info("KPI 2 — Average Wait Time: computed")

        // KPI 3: Driver Pay vs Passenger Fare (Tài chính)
        val kpi3 = enrichedDF
          .withColumn("trip_date", to_date(col("pickup_datetime")))
          .groupBy("trip_date")
          .agg(
            sum("driver_pay").alias("total_driver_pay"),
            sum("base_passenger_fare").alias("total_passenger_fare"),
            count("*").alias("trip_count")
          )
          .withColumn("driver_share_pct",
            round(col("total_driver_pay") / col("total_passenger_fare") * 100, 2)
          )
          .orderBy("trip_date")

        logger.info("KPI 3 — Driver Pay vs Fare: computed")

        // KPI 4: Average Trip Speed (Hiệu năng vận hành — đo tình trạng kẹt xe)
        val kpi4 = enrichedDF
          .withColumn("speed_mph",
            col("trip_miles") / (col("trip_time") / 3600.0)
          )
          .filter(col("speed_mph") > 0 && col("speed_mph") < 100) // lọc giá trị phi lý
          .groupBy("Borough")
          .agg(
            avg("speed_mph").alias("avg_speed_mph"),
            count("*").alias("trip_count")
          )

        logger.info("KPI 4 — Average Trip Speed: computed")

        // KPI 5: Shared Ride Match Rate (Hiệu suất thuật toán ghép chuyến)
        val kpi5 = enrichedDF
          .filter(col("shared_request_flag") === "Y")
          .groupBy("Borough")
          .agg(
            count("*").alias("shared_requests"),
            sum(when(col("shared_match_flag") === "Y", 1).otherwise(0)).alias("shared_matches")
          )
          .withColumn("match_rate_pct",
            round(col("shared_matches") / col("shared_requests") * 100, 2)
          )

        logger.info("KPI 5 — Shared Ride Match Rate: computed")

        // ── Ghi 5 KPI ra 5 thư mục riêng biệt (cùng bucket) ──
        // Lấy basePath từ spark config (được truyền từ SparkETLApp)
        val basePath = spark.conf.get("spark.etl.kpi.basePath", "")

        if (basePath.nonEmpty) {
          kpi1.coalesce(1).write.mode("overwrite").parquet(s"$basePath/total_trips_by_zone")
          kpi2.coalesce(1).write.mode("overwrite").parquet(s"$basePath/avg_wait_time")
          kpi3.coalesce(1).write.mode("overwrite").parquet(s"$basePath/driver_pay_vs_fare")
          kpi4.coalesce(1).write.mode("overwrite").parquet(s"$basePath/avg_trip_speed")
          kpi5.coalesce(1).write.mode("overwrite").parquet(s"$basePath/shared_ride_match_rate")
          logger.info(s"All 5 KPIs written to: $basePath/")
        } else {
          logger.warn("spark.etl.kpi.basePath not set — KPIs only returned, not written separately")
        }

        // Return KPI 1 làm output chính (sẽ được Loader ghi vào sink.path)
        kpi1
      }

      // ─────────────────────────────────────────────────────────
      // Ví dụ: Tính tăng trưởng doanh thu theo năm (YoY Growth)
      // ─────────────────────────────────────────────────────────
      case "revenue_yoy_growth" => df => {
        val windowSpec = Window.partitionBy("product").orderBy("report_month")
        df
          .withColumn("prev_year_revenue",
            lag("revenue", 12).over(windowSpec))
          .withColumn("yoy_growth_pct",
            when(col("prev_year_revenue").isNotNull && col("prev_year_revenue") =!= 0,
              round((col("revenue") - col("prev_year_revenue")) / col("prev_year_revenue") * 100, 2)
            ).otherwise(lit(null)))
          .filter(col("prev_year_revenue").isNotNull)
      }

      // ─────────────────────────────────────────────────────────
      // (THÊM CHỈ TIÊU MỚI Ở ĐÂY)
      // case "ten_chi_tieu" => df => { ... }
      // ─────────────────────────────────────────────────────────

      case unknown =>
        throw new IllegalArgumentException(
          s"Custom transform '$unknown' chưa được đăng ký trong CustomTransforms. " +
          s"Hãy thêm case '$unknown' vào hàm get().")
    }
  }
}
