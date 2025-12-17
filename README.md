# Big Data Processing: Comparative Study & ETL Pipeline

A comprehensive big data project featuring performance benchmarking across Apache Spark, Tez, and MapReduce, along with a production-ready ETL pipeline orchestrated with Apache Airflow.

---

## 📋 Overview

This project provides two main components:

1. **Performance Benchmarking Suite**: Compare execution times and resource utilization across different big data processing engines
2. **Production ETL Pipeline**: Docker-based data pipeline using Apache Spark and Airflow for scalable data processing

## 🏗️ Project Structure

```
Mini_projet_Bigdata/
├── étude_comparative_tez_spark_mr/    # Benchmarking suite
│   ├── csv_convert_to_orc.py          # CSV to ORC converter
│   ├── hive_mapreduce_benchmark.sh    # Hive with MapReduce tests
│   ├── hive_tez_benchmark.sh          # Hive with Tez tests
│   ├── pig_mapreduce_benchmark.pig    # Pig script for MapReduce
│   ├── pig_mapreduce_benchmark.sh     # Pig benchmark runner
│   └── spark_bechmark.py              # Spark performance tests
│
└── Pipline_etl_spark_airflow/         # ETL pipeline
    ├── docker-compose.yml             # Service orchestration
    ├── Dockerfile                     # Custom Airflow image
    ├── requirements.txt               # Python dependencies
    ├── dags/                          # Airflow DAG definitions
    ├── airflow/dags/                  # Additional DAG directory
    ├── spark_jobs/                    # Spark job scripts
    ├── data/                          # Data directory (gitignored)
    └── logs/                          # Application logs (gitignored)
```

## 🚀 Quick Start

### Prerequisites

- Docker Engine 20.10+
- Docker Compose 2.0+
- Python 3.8+ (for local development)
- 8GB RAM minimum (16GB recommended)

### Setting Up the ETL Pipeline

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-username/Mini_projet_Bigdata.git
   cd Mini_projet_Bigdata/Pipline_etl_spark_airflow
   ```

2. **Configure environment variables** (optional)
   ```bash
   cp .env.example .env
   # Edit .env with your configurations
   ```

3. **Launch the services**
   ```bash
   docker-compose up --build -d
   ```

4. **Access Airflow Web UI**
   - URL: http://localhost:8080
   - Default credentials: `airflow` / `airflow`

5. **Trigger your first DAG**
   - Navigate to the DAGs page
   - Enable and trigger your desired pipeline

### Running Benchmarks

1. **Navigate to the benchmark directory**
   ```bash
   cd étude_comparative_tez_spark_mr
   ```

2. **Run Spark benchmarks**
   ```bash
   spark-submit spark_bechmark.py
   ```

3. **Run Hive benchmarks**
   ```bash
   # With Tez
   bash hive_tez_benchmark.sh
   
   # With MapReduce
   bash hive_mapreduce_benchmark.sh
   ```

4. **Run Pig benchmarks**
   ```bash
   bash pig_mapreduce_benchmark.sh
   ```

## 📊 Benchmark Comparison

The comparative study evaluates:

- **Execution Time**: Query completion time across engines
- **Resource Utilization**: CPU, memory, and I/O metrics
- **Use Case Suitability**: Optimal engine for specific workloads

## 🔧 ETL Pipeline Features

- **Containerized Architecture**: Fully Dockerized for consistency
- **Workflow Orchestration**: Airflow DAGs for complex dependencies
- **Distributed Processing**: Apache Spark for large-scale data transformations
- **Monitoring**: Built-in logging and Airflow UI for pipeline visibility
- **Scalability**: Horizontal scaling support through Docker 

## 📦 Data Management

### Supported Formats
- CSV
- ORC (optimized columnar format)
- JSON

### Data Directories
```
data/
├── raw/              # Source data files
├── processed/        # Transformed data
└── archive/          # Historical data
```

**Note**: `data/` and `logs/` directories are excluded from Git. For large datasets, consider using:
- AWS S3
- Azure Blob Storage
- Google Cloud Storage
- Git LFS for version-controlled datasets

## 🛠️ Development

### Adding a New DAG

1. Create your DAG file in `dags/`
2. Define tasks and dependencies
3. Restart Airflow or wait for auto-detection
4. Verify in the Airflow UI

### Creating a New Spark Job

1. Add your script to `spark_jobs/`
2. Configure in `spark_jobs/config.yaml`
3. Reference in your Airflow DAG
4. Test locally before deployment

### Local Development Setup

```bash
# Install Python dependencies
pip install -r requirements.txt

# Run tests
pytest tests/

```

## 📈 Monitoring & Troubleshooting

### View Logs
```bash
# Airflow scheduler logs
docker-compose logs airflow-scheduler

# Spark job logs
docker-compose logs spark-worker

# All services
docker-compose logs -f
```

### Common Issues

**Port conflicts**: Modify ports in `docker-compose.yml`
**Memory errors**: Increase Docker memory allocation
**DAG not appearing**: Check file syntax and refresh interval

## 🧪 Testing

```bash
# Unit tests
pytest tests/unit/

# Integration tests
pytest tests/integration/

# Run all tests with coverage
pytest --cov=spark_jobs tests/
```

## 📚 Documentation

- [Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Spark Guide](https://spark.apache.org/docs/latest/)
- [Tez Documentation](https://tez.apache.org/)

## 🤝 Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 👥 Authors

- **AMERGA Younes**  - [GitHub Profile](https://github.com/Younessamg)
-  **SABROU Hafsa**
- **CHAJARI Salma** 
## 🙏 Acknowledgments

- Apache Software Foundation for open-source big data tools
- Docker community for containerization resources
- Contributors and maintainers


---

**⭐ Star this repository if you find it helpful!**
