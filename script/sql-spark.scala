

/*
  Steps in this script:
  1) Small test SQL demo (keeps as example)
  2) Drop `dod` column from patients and write Parquet
  3) Filter emar (remove empty medication) and write Parquet
  4) Clean emar_detail by merging NULL-parent fallback values into non-null child rows
     and write the cleaned result to Parquet
  5) Write omr.csv and services.csv as Parquet
*/

import org.apache.spark.sql.Column
import org.apache.spark.sql.functions._

// -----------------------------
// 0) Helpful helper: normalize empty strings -> null
// -----------------------------
def nullifyEmpty(c: Column) = when(trim(c) === "" || c.isNull, lit(null).cast("string")).otherwise(c)

// -----------------------------
// 1) Test SQL example
// -----------------------------
// val testDf = spark.read.option("header", "true").csv("hdfs://namenode:8020/input/localfile.csv")
// testDf.createOrReplaceTempView("mytable")
// val result = spark.sql("SELECT category, SUM(price * quantity) AS total_sales FROM mytable GROUP BY category")
// result.show(10, false)

// -----------------------------
// 2) Drop dod column from patients
// -----------------------------
val patients = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://namenode:8020/input/patients.csv")
val patientsNoDod = patients.drop("dod")
patientsNoDod.write.mode("overwrite").parquet("hdfs://namenode:8020/input/patients_filtered")

println("[Script]: patients filtered.")

// -----------------------------
// 3) Filter columns from emar and remove empty medication
// -----------------------------
val emarCols = Seq("subject_id", "hadm_id", "emar_id", "charttime", "medication", "event_txt")
val emarRaw = spark.read.option("header", "true").csv("hdfs://namenode:8020/input/emar.csv").select(emarCols.map(c => col(c)): _*)
val emarFiltered = emarRaw.filter(col("medication").isNotNull && trim(col("medication")) =!= "")
emarFiltered.write.mode("overwrite").parquet("hdfs://namenode:8020/input/emar_filtered")

println("[Script]: emar filtered.")

// -----------------------------
// 4) Filter emar_detail: 
// - Remove emar_ids with only one NULL-parent_field_ordinal; 
// - Fuse infusion_rate and infusion_rate_unit to dose_given and dose_given_unit; 
// - Remove rows with NULL-product_code or NULL-dose_given
// -----------------------------
val emarDetailCols = Seq("subject_id","emar_id","parent_field_ordinal","dose_due","dose_due_unit","dose_given","dose_given_unit","product_amount_given","product_unit","product_code","infusion_rate","infusion_rate_unit")
// Read, normalize empties, and select only desired columns
val emarDetailRaw = spark.read.option("header", "true").option("inferSchema", "false").csv("hdfs://namenode:8020/input/emar_detail.csv").select(emarDetailCols.map(c => col(c)): _*)
val df = emarDetailRaw
val dfNorm = df.select(df.columns.map(c => nullifyEmpty(col(c)).as(c)): _*)

// Identify emar_ids that have at least one non-null parent_field_ordinal row
val emarWithNonNull = dfNorm.filter($"parent_field_ordinal".isNotNull).select($"emar_id").distinct()

// Separate NULL-parent rows and non-null-detail rows
val nullParent = dfNorm.filter($"parent_field_ordinal".isNull)
.select($"emar_id".as("n_emar_id"), $"dose_due".as("n_dose_due"), $"dose_due_unit".as("n_dose_due_unit"), $"infusion_rate".as("n_infusion_rate"), $"infusion_rate_unit".as("n_infusion_rate_unit"), $"dose_given".as("n_dose_given"), $"dose_given_unit".as("n_dose_given_unit"))

val nonNullDetail = dfNorm.filter($"parent_field_ordinal".isNotNull)

// Keep only non-null-detail rows whose emar_id exists (drop emar_ids that only had a NULL-parent stub)
val nonNullFiltered = nonNullDetail.join(emarWithNonNull, Seq("emar_id"))

// Left-join the null-parent fallback info onto non-null rows by emar_id and coalesce fields in one pass
val joined = nonNullFiltered.join(nullParent, nonNullFiltered("emar_id") === nullParent("n_emar_id"), "left")

val filled = joined
.withColumn("dose_given_f", coalesce(col("infusion_rate"), col("n_infusion_rate"), col("dose_given"), col("n_dose_given")))
.withColumn("dose_given_unit_f", coalesce(col("infusion_rate_unit"), col("n_infusion_rate_unit"), col("dose_given_unit"), col("n_dose_given_unit")))
.withColumn("dose_due_f", coalesce(col("dose_due"), col("n_dose_due")))
.withColumn("dose_due_unit_f", coalesce(col("dose_due_unit"), col("n_dose_due_unit")))
.select(
col("subject_id"),
col("emar_id"),
col("parent_field_ordinal"),
col("dose_due_f").as("dose_due"),
col("dose_due_unit_f").as("dose_due_unit"),
col("dose_given_f").as("dose_given"),
col("dose_given_unit_f").as("dose_given_unit"),
col("product_amount_given"),
col("product_unit"),
col("product_code")
)
// filter out rows missing product_code or dose_given (empty or null)
.filter(col("product_code").isNotNull && trim(col("product_code")) =!= "" && col("dose_given").isNotNull && trim(col("dose_given")) =!= "")


// 6) Write the cleaned result back to HDFS as Parquet
filled.write.mode("overwrite").parquet("hdfs://namenode:8020/input/emar_detail_cleaned")

// -----------------------------
// 5) Write omr.csv and services.csv as Parquet
// -----------------------------

val omr = spark.read.option("header", "true").option("inferSchema", "false").csv("hdfs://namenode:8020/input/omr.csv")
omr.write.mode("overwrite").parquet("hdfs://namenode:8020/input/omr_filtered")

println("[Script]: omr written.")

val services = spark.read.option("header", "true").option("inferSchema", "false").csv("hdfs://namenode:8020/input/services.csv")
services.write.mode("overwrite").parquet("hdfs://namenode:8020/input/services_filtered")

println("[Script]: services written.")
println("[Script]: Script completed.")
