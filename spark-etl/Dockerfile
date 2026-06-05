# Stage 1: Build the Spark ETL Fat JAR using Maven
FROM maven:3.8.7-eclipse-temurin-11 AS builder
WORKDIR /build

# Copy pom.xml and download dependencies
COPY pom.xml .
RUN mvn dependency:go-offline -B

# Copy the source code and build the fat JAR
COPY src ./src
RUN mvn clean package -DskipTests

# Stage 2: Runtime image using official Apache Spark for K8s
FROM apache/spark:3.4.4-scala2.12-java11-ubuntu

# Chuyển sang user root chỉ để cài đặt các package phụ thuộc và thư mục
USER root
RUN mkdir -p /opt/spark/work-dir/jars && \
    mkdir -p /opt/spark/work-dir/config

# Copy file JAR từ quá trình maven build ở stage 1
COPY --from=builder /build/target/spark-etl-1.0.0.jar /opt/spark/work-dir/jars/spark-etl.jar


# Tải sẵn GCS Connector (nếu bạn cần đọc/ghi S3/GCS trong code)
ADD https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.16/gcs-connector-hadoop3-2.2.16.jar /opt/spark/jars/

# Phân quyền lại cho user spark (uid 185 là chuẩn của image apache/spark)
RUN chown -R 185:185 /opt/spark/work-dir/jars && \
    chown -R 185:185 /opt/spark/work-dir/config

# Quay lại user non-root để chạy pod trên K8s
USER 185

WORKDIR /opt/spark/work-dir
