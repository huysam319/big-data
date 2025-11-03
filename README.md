# MIMIC-IV Big Data Analytics Project

## 🏥 Project Overview
This project analyzes MIMIC-IV (Medical Information Mart for Intensive Care) dataset using Apache Spark and Hadoop for medication recommendation system.

## 🚀 Quick Start

### 1. Start the Environment
```bash
# Start all services (Hadoop + Spark)
docker-compose up -d

# Check if services are running
docker-compose ps
```

### 2. Data Processing Pipeline

#### Step 1: Convert CSV.gz to Parquet
```bash
# Convert all CSV.gz files to Parquet format
./script/convert_to_parquet_only.sh

# Or convert individual files
python3 script/convert_csv_to_parquet_chunked.py data/diagnoses_icd.csv.gz data/parquet/diagnoses_icd.parquet
```

#### Step 2: Upload to HDFS
```bash
# Upload Parquet files to HDFS
./script/upload_to_hdfs.sh
```

#### Step 3: Test Data Quality
```bash
# Test all Parquet files
make run SCRIPT=quick_test.scala
```

### 3. Run Analytics

#### Medication Recommendation Analysis
```bash
# Run the main analysis script
make run SCRIPT=main.scala

# Or run with specific script
make run SCRIPT=main_fixed.scala
```

## 📁 Project Structure

```
big-data/
├── docker-compose.yml          # Docker services configuration
├── Makefile                   # Build automation
├── script/                    # Analysis scripts
│   ├── main.scala            # Main medication recommendation analysis
│   ├── convert_csv_to_parquet_chunked.py  # CSV to Parquet converter
│   ├── convert_to_parquet_only.sh        # Batch conversion script
│   └── upload_to_hdfs.sh     # HDFS upload script
├── data/                     # Data directory
│   ├── *.csv.gz             # Original MIMIC-IV data files
│   └── parquet/              # Converted Parquet files
└── hadoop_config/            # Hadoop configuration files
```

## 🔧 Available Commands

### Docker Management
```bash
# Start services
docker-compose up -d

# Stop services
docker-compose down

# View logs
docker-compose logs -f

# Check service status
docker-compose ps
```

### Data Processing
```bash
# Convert CSV to Parquet
make convert

# Upload to HDFS
make upload
```

### Spark Analytics
```bash
# Run Scala script
make run SCRIPT=main.scala

# List available data 
make list
```

## 📊 Dataset Information

### MIMIC-IV Tables Used
- **diagnoses_icd**: 5.7M records - Patient diagnoses with ICD codes
- **prescriptions**: 7.9M records - Medication prescriptions
- **patients**: 337K records - Patient demographics
- **d_icd_diagnoses**: 112K records - ICD code descriptions
- **admissions**: 544K records - Hospital admissions
- **services**: 555K records - Hospital services

### Data Quality
- ✅ All Parquet files tested and working
- ✅ 100% success rate on data validation
- ✅ Low null rates (< 0.1% for key columns)
- ✅ Proper data relationships maintained

## 🎯 Analysis Results

### Medication Recommendation System
- **42,213** medication recommendations generated
- **10,262** unique diagnoses analyzed
- **1,172** unique medications recommended
- **4.11** average medications per diagnosis

### Top Diagnoses by Prescription Count
1. Essential hypertension: 8,174 prescriptions
2. Hyperlipidemia: 7,980 prescriptions
3. Hypertension: 6,519 prescriptions

### Top Medications Overall
1. Sodium Chloride 0.9% Flush: 19,653 prescriptions
2. Acetaminophen: 16,305 prescriptions
3. Heparin: 14,092 prescriptions

## 🔍 Web Interfaces

- **Spark UI**: http://localhost:4040
- **Hadoop NameNode**: http://localhost:9870
- **Hadoop DataNode**: http://localhost:9864

## 🛠️ Technical Stack

- **Apache Spark 3.5.0**: Distributed data processing
- **Apache Hadoop 3.3.4**: Distributed storage (HDFS)
- **Scala 2.12**: Primary programming language
- **Python 3**: Data conversion scripts
- **Docker**: Containerization
- **Parquet**: Columnar storage format

## 📈 Performance Optimizations

### Memory Management
- Sampling (10%) to avoid OutOfMemoryError
- Optimized dropDuplicates with keys
- SQL-based joins for better performance
- Memory cleanup after processing

### Spark Configuration
- Adaptive Query Execution enabled
- Kryo serializer for better performance
- Optimized partition sizes
- Skew join handling

## 🚨 Troubleshooting

### Common Issues
1. **OutOfMemoryError**: Use sampling or increase memory
2. **HDFS Connection**: Check Docker network connectivity
3. **Parquet Files**: Verify file integrity with test scripts

### Debug Commands
```bash
# Check Docker containers
docker ps

# Check HDFS status
docker exec namenode hdfs dfsadmin -report

# Check Spark logs
docker logs spark-master
```

## 📝 Notes

- All scripts are optimized for the MIMIC-IV dataset
- Data processing includes comprehensive cleaning and validation
- Results are saved to HDFS for further analysis
- Memory management is crucial for large datasets

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test with the provided scripts
5. Submit a pull request

## 📄 License

This project is for educational and research purposes using the MIMIC-IV dataset.