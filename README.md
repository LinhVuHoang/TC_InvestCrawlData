# TCInvest Stock Data ETL Pipeline

An asynchronous stock market data crawling and ETL pipeline built with Python, Apache Airflow, Docker, Microsoft SQL Server, MinIO, Prometheus, and Grafana.

This project explores how to design, orchestrate, process, store, and monitor financial market data through a containerized ETL workflow.

> **Project Status:** Experimental / Learning Project  
> Developed for learning, testing, and exploring Data Engineering technologies. Not intended for production use.

## 1. Project Overview

The project collects intraday stock trading data from the TCInvest API, processes the retrieved JSON responses, and stores the resulting datasets in Microsoft SQL Server and MinIO.

Apache Airflow is used to orchestrate the ETL workflow, while asynchronous programming helps improve the efficiency of concurrent HTTP requests.

The project also integrates Prometheus and Grafana to explore system monitoring and performance tracking.

### Key Objectives

- Build an automated stock market data collection pipeline.
- Explore asynchronous HTTP requests using Python.
- Implement batch processing for multiple stock symbols.
- Orchestrate ETL workflows using Apache Airflow.
- Store structured data in Microsoft SQL Server.
- Export datasets as CSV files and upload them to MinIO.
- Monitor system resources and crawling performance using Prometheus and Grafana.
- Containerize the development environment using Docker Compose.

## 2. System Architecture

```text
                 TCInvest API
                       |
                       v
              Stock Code Retrieval
                       |
                       v
              Apache Airflow DAG
                       |
              +--------+--------+
              |                 |
              v                 v
       get_code_invest    XCom Transfer
              |                 |
              +--------+--------+
                       |
                       v
              run_my_async_task
                       |
                       v
              Async Data Crawling
               (asyncio + aiohttp)
                       |
                       v
                Pandas Processing
                       |
              +--------+--------+
              |                 |
              v                 v
         SQL Server          CSV Export
                                  |
                                  v
                              MinIO S3

              Monitoring Layer
                       |
              Prometheus + Grafana
```

## 3. Technology Stack

| Category | Technologies |
|---|---|
| Programming Language | Python |
| Data Processing | Pandas |
| HTTP Requests | Requests, aiohttp |
| Asynchronous Programming | asyncio |
| Workflow Orchestration | Apache Airflow 2.7.1 |
| Task Communication | Airflow XCom |
| Database | Microsoft SQL Server |
| Database Connectivity | PyODBC, SQLAlchemy |
| Object Storage | MinIO (S3-compatible) |
| Containerization | Docker, Docker Compose |
| Monitoring | Prometheus |
| Visualization | Grafana |
| System Monitoring | psutil |

## 4. ETL Workflow

### Step 1: Extract

- Retrieve stock symbols from a predefined text file.
- Request intraday historical trading data from the TCInvest API.
- Retrieve paginated API responses for each stock symbol.

### Step 2: Transform

- Process API responses using Pandas.
- Normalize JSON data into DataFrames.
- Organize stock data into individual datasets.

### Step 3: Load

Two storage destinations are implemented:

**Microsoft SQL Server**
- Create a database for each crawling execution.
- Store individual stock datasets in separate tables.

**MinIO Object Storage**
- Export processed DataFrames into CSV files.
- Upload CSV files to MinIO through Airflow's S3Hook.

## 5. Asynchronous Processing

The crawler uses Python's asyncio and aiohttp to perform concurrent HTTP requests.

Key configurations:

| Parameter | Configuration |
|---|---|
| Async semaphore | 48 |
| Batch size | 50 stock symbols |
| API pagination | 100 records per page |
| Processing library | Pandas |

The semaphore limits the number of concurrent operations, while batch processing divides the stock symbol list into smaller groups.

This approach provides an opportunity to explore asynchronous I/O and data ingestion performance.

## 6. Workflow Orchestration

Apache Airflow manages the ETL workflow through a DAG named `crawl_dag_v01`.

The DAG contains two PythonOperator tasks:

| Task | Description |
|---|---|
| `get_code_invest` | Reads stock symbols from a text file and pushes them to XCom. |
| `run_my_async_task` | Retrieves stock symbols from XCom and executes the asynchronous crawler. |

Task dependencies:

```text
get_code_invest
       |
       v
run_my_async_task
```

**Schedule:** `12 15 * * *` (15:12 daily, using the configured Airflow timezone).

## 7. Monitoring

The project explores monitoring through Prometheus and Grafana.

Metrics implemented in the Python crawler include:

- Crawling job execution duration.
- Number of failed link requests.
- Number of failed data requests.
- CPU utilization.
- RAM utilization.
- Number of crawled stock codes.
- Total crawling execution time.
- Crawling job state.

Prometheus collects monitoring metrics, while Grafana provides dashboards for visualization.

## 8. Docker Deployment

Docker Compose is used to orchestrate the development environment.

The configured services include:

- Apache Airflow Webserver
- Apache Airflow Scheduler
- Apache Airflow Worker
- Apache Airflow Triggerer
- PostgreSQL
- Redis
- Prometheus
- Grafana
- StatsD Exporter

SQL Server and MinIO are configured as external dependencies in the provided application code and must be available separately.

## 9. Project Structure

```text
project/
├── dags/
│   └── crawlinvest.py
├── data/
│   └── list_code_invest.txt
├── logs/
├── plugins/
├── config/
├── grafana/
│   └── volume/
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
├── crawlLinkinvest.py
└── test.py
```

*The structure above is illustrative and should be adjusted to match the actual repository.*

## 10. Installation and Setup

### Prerequisites

- Docker Desktop
- Docker Compose
- Python 3.10+
- Microsoft SQL Server
- MinIO

### Step 1: Clone the repository

```bash
git clone <repository-url>
cd <project-directory>
```

### Step 2: Configure dependencies

Install the required Python packages and Airflow providers.

```bash
pip install -r requirements.txt
```

Ensure that the Airflow version and provider packages are compatible with the project.

### Step 3: Configure environment variables

Configure database credentials, Airflow connections, API authentication, and storage settings.

Sensitive information should be stored in environment variables rather than hardcoded in Python files.

### Step 4: Start Docker services

```bash
docker compose up -d
```

### Step 5: Access monitoring interfaces

| Service | Default URL |
|---|---|
| Airflow | http://localhost:8081 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

## 11. Learning Outcomes

Through this project, I explored:

- Designing an ETL pipeline for financial market data.
- Implementing asynchronous HTTP data ingestion.
- Handling paginated API responses.
- Processing JSON data using Pandas.
- Integrating Python with Microsoft SQL Server.
- Building workflow orchestration using Apache Airflow.
- Passing data between Airflow tasks using XCom.
- Integrating S3-compatible object storage using MinIO.
- Containerizing multiple services with Docker Compose.
- Exploring system monitoring with Prometheus and Grafana.

## 12. Limitations and Future Improvements

This project was developed primarily for experimentation and learning.

Potential improvements include:

- Implement robust retry mechanisms and HTTP timeout handling.
- Implement API rate limiting and error recovery.
- Improve database schema management and data consistency.
- Separate configuration and credentials from source code.
- Implement structured logging and centralized error handling.
- Optimize database insertion performance.
- Improve Airflow DAG reliability and task-level monitoring.
- Add automated testing and CI/CD workflows.
- Introduce data validation and data quality checks.

## 13. Disclaimer

This project is an educational implementation intended to explore Data Engineering concepts and technologies.

It is not designed for production financial data processing, investment recommendations, or real-time trading decisions.
