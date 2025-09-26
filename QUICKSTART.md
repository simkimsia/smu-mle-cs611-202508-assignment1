# Quickstart Guide

## Running the ETL Pipeline with Docker

This project provides multiple ways to run the data processing pipeline using Docker.

### Prerequisites

- Docker and Docker Compose installed
- Raw data files in the `data/` directory

### Option 1: Using Docker Compose (Recommended)

#### Run Bronze Processing Only

Test your bronze layer implementation:

```bash
docker-compose --profile etl up etl-bronze
```

#### Run Full Pipeline

Execute the complete bronze → silver → gold pipeline:

```bash
docker-compose --profile etl up etl-full
```

#### Run JupyterLab

For interactive development and analysis:

```bash
docker-compose up jupyter
```

Access JupyterLab at: <http://localhost:8888>

### Option 2: Direct Docker Commands

#### Build the Image

```bash
docker build -t mle-assignment .
```

#### Run Bronze Processing Only

```bash
docker run --rm -v $(pwd):/app mle-assignment python main.py --bronze-only
```

#### Run Full Pipeline

```bash
docker run --rm -v $(pwd):/app mle-assignment python main.py
```

### Pipeline Stages

- **Bronze**: Raw data ingestion from CSV files
- **Silver**: Data cleaning and standardization
- **Gold**: ML-ready feature and label stores

### Output Structure

```
datamart/
├── bronze/          # Raw partitioned data
├── silver/          # Cleaned data
└── gold/            # ML-ready datasets
```

### Troubleshooting

- Ensure the `data/` directory contains all required CSV files
- Check Docker has sufficient memory allocated (recommended: 4GB+)
- Verify Java is properly configured in the container for PySpark
