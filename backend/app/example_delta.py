from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

def run_delta_example():
    # Tạo SparkSession
    builder = SparkSession.builder \
        .appName("DeltaLakeExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Tạo dữ liệu mẫu
    data = [{"id": 1, "name": "Alice", "age": 30},
            {"id": 2, "name": "Bob", "age": 25}]
    df = spark.createDataFrame(data)

    # Lưu vào Delta Table
    delta_path = "/app/delta-table"
    df.write.format("delta").mode("overwrite").save(delta_path)

    # Đọc dữ liệu từ Delta Table
    delta_df = spark.read.format("delta").load(delta_path)
    return delta_df.toPandas().to_dict(orient="records")

def run_delta_update_example():
    # Tạo SparkSession
    builder = SparkSession.builder \
        .appName("DeltaLakeUpdateExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Đường dẫn Delta Table
    delta_path = "/app/delta-table"

    # Tạo dữ liệu ban đầu
    initial_data = [{"id": 1, "name": "Alice", "age": 30},
                    {"id": 2, "name": "Bob", "age": 25}]
    df = spark.createDataFrame(initial_data)
    df.write.format("delta").mode("overwrite").save(delta_path)

    # Thêm dữ liệu mới
    new_data = [{"id": 3, "name": "Charlie", "age": 35}]
    new_df = spark.createDataFrame(new_data)
    new_df.write.format("delta").mode("append").save(delta_path)

    # Đọc dữ liệu sau khi thêm
    delta_df = spark.read.format("delta").load(delta_path)
    return delta_df.toPandas().to_dict(orient="records")

def run_delta_versioning_example():
    # Tạo SparkSession
    builder = SparkSession.builder \
        .appName("DeltaLakeVersioningExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Đường dẫn Delta Table
    delta_path = "/app/delta-table"

    # Tạo dữ liệu ban đầu
    initial_data = [{"id": 1, "name": "Alice", "age": 30},
                    {"id": 2, "name": "Bob", "age": 25}]
    df = spark.createDataFrame(initial_data)
    df.write.format("delta").mode("overwrite").save(delta_path)

    # Thêm dữ liệu mới
    updated_data = [{"id": 1, "name": "Alice", "age": 31},
                    {"id": 3, "name": "Charlie", "age": 35}]
    updated_df = spark.createDataFrame(updated_data)
    updated_df.write.format("delta").mode("overwrite").save(delta_path)

    # Đọc phiên bản cũ
    old_version_df = spark.read.format("delta").option("versionAsOf", 0).load(delta_path)

    # Đọc phiên bản mới
    new_version_df = spark.read.format("delta").load(delta_path)

    return {
        "old_version": old_version_df.toPandas().to_dict(orient="records"),
        "new_version": new_version_df.toPandas().to_dict(orient="records")
    }

def run_delta_delete_example():
    # Tạo SparkSession
    builder = SparkSession.builder \
        .appName("DeltaLakeDeleteExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Đường dẫn Delta Table
    delta_path = "/app/delta-table"

    # Tạo dữ liệu ban đầu
    initial_data = [{"id": 1, "name": "Alice", "age": 30},
                    {"id": 2, "name": "Bob", "age": 25},
                    {"id": 3, "name": "Charlie", "age": 35}]
    df = spark.createDataFrame(initial_data)
    df.write.format("delta").mode("overwrite").save(delta_path)

    # Xóa dữ liệu với điều kiện
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, delta_path)
    delta_table.delete("age > 30")

    # Đọc dữ liệu sau khi xóa
    delta_df = spark.read.format("delta").load(delta_path)
    return delta_df.toPandas().to_dict(orient="records")

def run_delta_complex_example():
    # Tạo SparkSession
    builder = SparkSession.builder \
        .appName("DeltaLakeComplexExample") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    # Đường dẫn Delta Table
    delta_path = "/app/delta-complex-table"

    # Tạo dữ liệu mẫu lớn
    large_data = [{"id": i, "name": f"User{i}", "age": (i % 50) + 20, "score": (i % 100) * 1.5} for i in range(1, 1001)]
    large_df = spark.createDataFrame(large_data)
    large_df.write.format("delta").mode("overwrite").save(delta_path)

    # Cập nhật một số bản ghi
    from delta.tables import DeltaTable
    delta_table = DeltaTable.forPath(spark, delta_path)
    delta_table.update(condition="age > 40", set={"score": "score + 10"})

    # Xóa bản ghi có điều kiện
    delta_table.delete("score < 50")

    # Đọc dữ liệu cuối cùng
    final_df = spark.read.format("delta").load(delta_path)

    return final_df.toPandas().to_dict(orient="records")
