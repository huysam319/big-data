import org.apache.spark.sql.functions.{col, count, countDistinct, sum, min, max, desc, avg}

// Configure Spark to reduce warnings and improve performance
spark.conf.set("spark.sql.adaptive.enabled", "true")
spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")
spark.conf.set("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionFactor", "5")
spark.conf.set("spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes", "256MB")

// Set log level to reduce warnings
spark.sparkContext.setLogLevel("ERROR")

println("=== MIMIC-IV MEDICATION RECOMMENDER ===")

// Read Parquet files from HDFS
var diagnoses_df: org.apache.spark.sql.DataFrame = null
var prescriptions_df: org.apache.spark.sql.DataFrame = null
var icd_descriptions_df: org.apache.spark.sql.DataFrame = null
var patients_df: org.apache.spark.sql.DataFrame = null
var admissions_df: org.apache.spark.sql.DataFrame = null
var services_df: org.apache.spark.sql.DataFrame = null

try {
  println("Reading Parquet files from HDFS...")
  
  diagnoses_df = spark.read.parquet("hdfs://namenode:8020/data/diagnoses_icd.parquet")
    .select("subject_id", "hadm_id", "icd_code", "icd_version")
  prescriptions_df = spark.read.parquet("hdfs://namenode:8020/data/prescriptions.parquet")
    .select("subject_id", "hadm_id", "drug", "drug_type")
  icd_descriptions_df = spark.read.parquet("hdfs://namenode:8020/data/d_icd_diagnoses.parquet")
    .select("icd_code", "icd_version", "long_title")
  patients_df = spark.read.parquet("hdfs://namenode:8020/data/patients.parquet")
    .select("subject_id", "gender", "anchor_age", "dod")
  admissions_df = spark.read.parquet("hdfs://namenode:8020/data/admissions.parquet")
    .select("hadm_id", "admittime", "dischtime", "admission_type", "insurance")
  services_df = spark.read.parquet("hdfs://namenode:8020/data/services.parquet")
    .select("hadm_id", "curr_service")
  
  println("✓ All Parquet files loaded successfully!")
  
  // Display basic information
  println("\n=== Dataset Information ===")
  println(s"Diagnoses: ${diagnoses_df.count()} rows")
  println(s"Prescriptions: ${prescriptions_df.count()} rows")
  println(s"ICD Descriptions: ${icd_descriptions_df.count()} rows")
  println(s"Patients: ${patients_df.count()} rows")
  println(s"Admissions: ${admissions_df.count()} rows")
  println(s"Services: ${services_df.count()} rows")
} catch {
  case e: Exception =>
    println(s"✗ Error reading Parquet files: ${e.getMessage}")
}

// Data Cleaning
if (diagnoses_df != null && prescriptions_df != null && patients_df != null && icd_descriptions_df != null && admissions_df != null && services_df != null) {
  println("\n=== Data Cleaning ===")
  
  // Clean data with optimized dropDuplicates using keys
  val diagnoses_clean = diagnoses_df
    .filter(col("subject_id").isNotNull && col("hadm_id").isNotNull && col("icd_code").isNotNull)
    .filter(col("icd_code") =!= "" && col("icd_code") =!= "NULL")
    .dropDuplicates("subject_id", "hadm_id", "icd_code", "icd_version")
  
  val prescriptions_clean = prescriptions_df
    .filter(col("subject_id").isNotNull && col("hadm_id").isNotNull)
    .filter(col("drug").isNotNull && col("drug") =!= "" && col("drug") =!= "NULL")
    .dropDuplicates("subject_id", "hadm_id", "drug", "drug_type")
  
  val patients_clean = patients_df
    .filter(col("subject_id").isNotNull)
    .filter(col("gender").isNotNull && col("gender") =!= "" && col("gender") =!= "NULL")
    .filter(col("anchor_age").isNotNull && col("anchor_age") >= 0 && col("anchor_age") <= 150)
    .dropDuplicates("subject_id")
  
  val icd_descriptions_clean = icd_descriptions_df
    .filter(col("icd_code").isNotNull && col("icd_code") =!= "" && col("icd_code") =!= "NULL")
    .dropDuplicates("icd_code", "icd_version")
  
  val admissions_clean = admissions_df
    .filter(col("hadm_id").isNotNull)
    .dropDuplicates("hadm_id")
  
  val services_clean = services_df
    .filter(col("hadm_id").isNotNull)
    .dropDuplicates("hadm_id", "curr_service")
  
  println(s"✓ Data cleaning completed:")
  println(s"  - Diagnoses: ${diagnoses_clean.count()} rows")
  println(s"  - Prescriptions: ${prescriptions_clean.count()} rows")
  println(s"  - Patients: ${patients_clean.count()} rows")
  println(s"  - ICD Descriptions: ${icd_descriptions_clean.count()} rows")
  println(s"  - Admissions: ${admissions_clean.count()} rows")
  println(s"  - Services: ${services_clean.count()} rows")
  
  // Join and Analysis using SQL
  println("\n=== Medication Analysis ===")
  
  // Register cleaned data as temporary views
  diagnoses_clean.createOrReplaceTempView("diagnoses_clean")
  prescriptions_clean.createOrReplaceTempView("prescriptions_clean")
  patients_clean.createOrReplaceTempView("patients_clean")
  icd_descriptions_clean.createOrReplaceTempView("icd_descriptions_clean")
  admissions_clean.createOrReplaceTempView("admissions_clean")
  services_clean.createOrReplaceTempView("services_clean")
  // Join data using SQL with sampling to avoid memory issues
  println("Performing join with sampling to avoid memory issues...")
  
  // Sample data to avoid OutOfMemoryError (use 10% of data for testing)
  val diagnoses_sample = diagnoses_clean.sample(0.1, seed = 42)
  val prescriptions_sample = prescriptions_clean.sample(0.1, seed = 42)
  
  // Register sampled data
  diagnoses_sample.createOrReplaceTempView("diagnoses_sample")
  prescriptions_sample.createOrReplaceTempView("prescriptions_sample")
  admissions_clean.createOrReplaceTempView("admissions_clean")
  services_clean.createOrReplaceTempView("services_clean")
  val joined_data = spark.sql("""
    SELECT 
      d.icd_code, 
      d.hadm_id, 
      p.drug, 
      icd_desc.long_title as diagnosis_description, 
      adm.admission_type, 
      adm.insurance, 
      s.curr_service as department,
      adm.admittime,
      adm.dischtime
    FROM diagnoses_sample d
    JOIN prescriptions_sample p ON d.hadm_id = p.hadm_id
    LEFT JOIN icd_descriptions_clean icd_desc ON d.icd_code = icd_desc.icd_code AND d.icd_version = icd_desc.icd_version
    LEFT JOIN admissions_clean adm ON d.hadm_id = adm.hadm_id
    LEFT JOIN services_clean s ON d.hadm_id = s.hadm_id
  """)
  
  println(s"✓ Joined data (sampled): ${joined_data.count()} records")
  
  // Register joined data for ranking
  joined_data.createOrReplaceTempView("joined_data")
  
  // Calculate medication frequency using SQL
  val medication_frequency = spark.sql("""
    SELECT 
      icd_code, 
      diagnosis_description, 
      drug,
      department,
      admission_type,
      COUNT(*) as prescription_count,
      COUNT(DISTINCT hadm_id) as unique_admission_count
    FROM joined_data
    GROUP BY icd_code, diagnosis_description, drug, department, admission_type
  """)
  
  println(s"✓ Medication frequency calculated: ${medication_frequency.count()} combinations")
  
  // Register for ranking
  medication_frequency.createOrReplaceTempView("medication_frequency")
  
  // Add ranking using SQL
  val medication_ranking = spark.sql("""
    SELECT 
      icd_code, 
      diagnosis_description, 
      drug, 
      department,
      admission_type,
      prescription_count, 
      unique_admission_count,
           row_number() OVER (PARTITION BY icd_code ORDER BY prescription_count DESC) as rank
    FROM medication_frequency
  """)
  
  println("✓ Medication ranking completed!")
  
  // Get top 5 medications per diagnosis
  val top_medications = medication_ranking.filter(col("rank") <= 5)
  
  println("\n=== TOP MEDICATIONS PER DIAGNOSIS ===")
  top_medications.orderBy("icd_code", "rank").show(20, truncate = false)
  
  // Summary statistics
  println("\n=== SUMMARY STATISTICS ===")
  medication_frequency.groupBy("department","admission_type")
    .agg(sum(col("unique_admission_count")).alias("unique_admission_count"), sum(col("prescription_count")).alias("total_prescriptions_count")).show(20, truncate = false)
  
  // Top diagnoses by prescription count
  println("\n=== TOP DIAGNOSES BY PRESCRIPTION COUNT ===")
  val top_diagnoses = spark.sql("""
    SELECT icd_code, diagnosis_description, SUM(prescription_count) as total_prescriptions
    FROM medication_frequency
    GROUP BY icd_code, diagnosis_description
    ORDER BY total_prescriptions DESC
    LIMIT 10
  """)
  top_diagnoses.show(10, truncate = false)
  
  // Top medications overall
  println("\n=== TOP MEDICATIONS OVERALL ===")
  val top_meds_overall = spark.sql("""
    SELECT drug, department, admission_type, SUM(prescription_count) as total_prescriptions
    FROM medication_frequency
    GROUP BY drug, department, admission_type
    ORDER BY total_prescriptions DESC
    LIMIT 10
  """)
  top_meds_overall.show(10, truncate = false)
  
  // Create final recommendation table with percentage calculation
  println("\n=== CREATING RECOMMENDATION TABLE ===")
  
  // Add percentage calculation using SQL
  val recommendation_table = spark.sql("""
    SELECT icd_code, diagnosis_description, drug, department, admission_type, prescription_count, unique_admission_count, rank,
           ROUND(prescription_count * 100.0 / SUM(prescription_count) OVER (PARTITION BY icd_code), 2) as percentage_within_diagnosis
    FROM (
      SELECT icd_code, diagnosis_description, drug, department, admission_type, prescription_count, unique_admission_count,
             row_number() OVER (PARTITION BY icd_code ORDER BY prescription_count DESC) as rank
      FROM medication_frequency
    ) ranked
    WHERE rank <= 5
  """)
  
  // Add diagnosis statistics
  val diagnosis_stats = spark.sql("""
    SELECT icd_code, diagnosis_description, department, admission_type,
           COUNT(drug) as total_different_meds,
           SUM(prescription_count) as total_prescriptions
    FROM medication_frequency
    GROUP BY icd_code, diagnosis_description, department, admission_type
  """)
  
  // Join recommendation table with diagnosis stats
  val final_recommendation_table = recommendation_table.join(
    diagnosis_stats, 
    Seq("icd_code", "department", "admission_type", "diagnosis_description"), 
    "left"
  )
  
  // Cache the final result
  final_recommendation_table.cache()
  
  println(s"✓ Recommendation table created: ${final_recommendation_table.count()} recommendations")
  
  // Show sample results
  println("\n=== SAMPLE RECOMMENDATIONS ===")
  final_recommendation_table.show(20, truncate = false)
  
  // Save results to HDFS
  println("\n=== SAVING RESULTS ===")
  try {
    val hdfs_output_path = "hdfs://namenode:8020/processed/medication_recommendations"
    
    final_recommendation_table.write
      .mode("overwrite")
      .option("compression", "snappy")
      .parquet(hdfs_output_path)
    
    println(s"✓ Successfully saved to HDFS: $hdfs_output_path")
    
    // Show summary statistics
    println("\n=== FINAL SUMMARY STATISTICS ===")
    val unique_diagnoses = final_recommendation_table.select("icd_code").distinct().count()
    val unique_medications = final_recommendation_table.select("drug").distinct().count()
    val avg_meds_per_diagnosis = final_recommendation_table.groupBy("icd_code").count().agg(avg("count")).collect()(0)(0)
    
    println(s"Total unique diagnoses: $unique_diagnoses")
    println(s"Total unique medications: $unique_medications")
    println(s"Average medications per diagnosis: ${avg_meds_per_diagnosis}")
    
  } catch {
    case e: Exception =>
      println(s"Error saving results: ${e.getMessage}")
      println("Trying alternative save method...")
      
      try {
        val hdfs_output_path = "hdfs://namenode:8020/processed/medication_recommendations_csv"
        final_recommendation_table.write
          .mode("overwrite")
          .option("header", "true")
          .csv(hdfs_output_path)
        println(s"✓ Successfully saved as CSV: $hdfs_output_path")
      } catch {
        case e2: Exception =>
          println(s"Failed to save results: ${e2.getMessage}")
      }
  }
  
  // Clean up memory
  println("\n=== CLEANING UP ===")
  try {
    final_recommendation_table.unpersist()
    medication_frequency.unpersist()
    println("✓ Cached DataFrames cleared")
  } catch {
    case e: Exception =>
      println(s"Warning: Could not unpersist DataFrames: ${e.getMessage}")
  }
  
  println("\n✓ Analysis completed successfully!")
  
} else {
  println("✗ Cannot proceed - DataFrames not loaded successfully")
}

println("\n✓ Script completed successfully!")
