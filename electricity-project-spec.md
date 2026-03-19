# Project 1: Real-Time Electricity Order Analytics Pipeline

## Overview

Build an end-to-end data engineering pipeline that ingests real-time electricity production events via Kafka, processes them with PySpark, and orchestrates the entire workflow using Apache Airflow. This project ties together all concepts from **Weeks 1–4** of the Data Engineering curriculum.

---

## Business Scenario

The Environmental Protection Agency (EPA) wants to:

1. **Stream** electicity production (fuel type, production respondent, production amount) in real time.
2. **Process** the raw events to compute hourly megawatt production, top-production respondents, and fuel-type breakdowns.
3. **Persist** both raw and transformed data to storage (local filesystem or S3).
4. **Orchestrate** the batch and streaming jobs on a daily schedule with retry and alerting.

---

## Architecture

```
┌──────────────┐       ┌─────────────┐       ┌─────────────────────┐
│  Order Event │       │             │       │  PySpark Streaming   │
│  Simulator   │──────▶│   Kafka     │──────▶│  Consumer / ETL      │
│  (Producer)  │       │  (Topic:    │       │  (Spark Structured   │
│              │       │  orders)    │       │   Streaming)          │
└──────────────┘       └─────────────┘       └──────────┬────────────┘
                                                        │
                                                        ▼
                                              ┌─────────────────────┐
                                              │  Raw Data Layer     │
                                              │  (Parquet / JSON)   │
                                              └──────────┬──────────┘
                                                         │
                                                         ▼
                                              ┌─────────────────────┐
                                              │  PySpark Batch ETL  │
                                              │  (Aggregations,     │
                                              │   Joins, Filters)   │
                                              └──────────┬──────────┘
                                                         │
                                                         ▼
                                              ┌─────────────────────┐
                                              │  Transformed Data   │
                                              │  (Parquet / CSV)    │
                                              └──────────┬──────────┘
                                                         │
                                                         ▼
                                              ┌─────────────────────┐
                                              │  Airflow DAG        │
                                              │  (Orchestration)    │
                                              └─────────────────────┘
```

---

## Tech Stack

| Technology     | Purpose                                      | Curriculum Week |
|----------------|----------------------------------------------|:---------------:|
| PySpark (RDDs) | Low-level data processing & custom transforms| Week 1          |
| PySpark (SQL)  | DataFrame operations, aggregations, joins    | Week 2          |
| Apache Kafka   | Real-time event ingestion (producer/consumer)| Week 3          |
| Apache Airflow | DAG-based job orchestration & scheduling     | Week 4          |

---

## Detailed Requirements

### Module 1 — Kafka Producer (Week 3)

**Goal:** Simulate a stream of order events.

- Create a Kafka topic named `electric_orders`.
- Write a Python Kafka producer (`electric_producer.py`) that generates JSON order events:
  ```json
  {
	  "period" : "2026-03-02T07",
	  "respondent" : "AVA",
	  "respondent_name" : "Avista Corporation",
	  "fueltype" : "WAT",
	  "type-name" : "Hydro", 
	  "value" : "729",
	  "value-units" : "megawatthours"
  }
  ```
- Use `respondent` values: 'AEC', 'AECI', 'AVA', 'AVRN', 'AZPS', 'BANC', 'BPAT', 'CAL', 'CAR', 'CENT', 'CHPD', 'CISO', 'CPLE', 'CPLW', 'DEAA', 'DOPD', 'DUK', 'EEI', 'EPE', 'ERCO', 'FLA', 'FMPP', 'FPC', 'FPL', 'GCPD', 'GLHB', 'GRID', 'GRIF', 'GVL', 'GWA',' HGMA', 'HST', 'IID', 'IPCO', 'ISNE', 'JEA', 'LDWP', 'LGEE', 'MIDA', 'MIDW', 'MISO', 'NE', 'NEVP', 'NSB', 'NW', 'NWMT', 'NY', 'NYIS', 'PACE', 'PACW', 'PGE', 'PJM', 'PNM', 'PSCO', 'PSEI', 'SC', 'SCEG', 'SCL', 'SE', 'SEC', 'SEPA', 'SIKE', 'SOCO', 'SPA', 'SRP', 'SW', 'SWPP', 'TAL', 'TEC', 'TEN', 'TEPC', 'TEX', 'TIDC', 'TPWR', 'TVA', 'US48', 'WACM', 'WALC', 'WAUW', 'WWA'.

- Use `respondent_name` values: 'PowerSouth Energy Cooperative', 'Associated Electric Cooperative, Inc.', 'Avista Corporation', 'Avangrid Renewables, LLC', 'Arizona Public Service Company', 'Balancing Authority of Northern California', 'Bonneville Power Administration', 'California', 'Carolinas', 'Central', 'Public Utility District No. 1 of Chelan County', 'California Independent System Operator', 'Duke Energy Progress East', 'Duke Energy Progress West', 'Arlington Valley, LLC', 'PUD No. 1 of Douglas County', 'Duke Energy Carolinas', 'Electric Energy, Inc.', 'El Paso Electric Company', 'Electric Reliability Council of Texas, Inc.', 'Florida', 'Florida Municipal Power Pool', 'Duke Energy Florida, Inc.', 'Florida Power & Light Co.', 'Public Utility District No. 2 of Grant County, Washington', 'GridLiance', 'Gridforce Energy Management, LLC', 'Griffith Energy, LLC', 'Gainesville Regional Utilities', 'NaturEner Power Watch, LLC', 'New Harquahala Generating Company, LLC', 'City of Homestead', 'Imperial Irrigation District', 'Idaho Power Company', 'ISO New England', 'JEA', 'Los Angeles Department of Water and Power', 'LG&E and KU Services Company as agent for Louisville Gas and Electric Company and Kentucky Utilities Company', 'Mid-Atlantic', 'Midwest', 'Midcontinent Independent System Operator, Inc.', 'New England', 'Nevada Power Company', 'Utilities Commission of New Smyrna Beach', 'Northwest', 'NorthWestern Corporation', 'New York', 'New York Independent System Operator', 'PacifiCorp East', 'PacifiCorp West', 'Portland General Electric Company', 'PJM Interconnection, LLC', 'Public Service Company of New Mexico', 'Public Service Company of Colorado', 'Puget Sound Energy, Inc.', 'South Carolina Public Service Authority', 'Dominion Energy South Carolina, Inc.', 'Seattle City Light', 'Southeast', 'Seminole Electric Cooperative', 'Southeastern Power Administration', 'Sikeston Board of Municipal Utilities', 'Southern Company Services, Inc. - Trans', 'Southwestern Power Administration', 'Salt River Project Agricultural Improvement and Power District', 'Southwest', 'Southwest Power Pool', 'City of Tallahassee', 'Tampa Electric Company', 'Tennessee', 'Tucson Electric Power', 'Texas', 'Turlock Irrigation District', 'City of Tacoma, Department of Public Utilities, Light Division' 'Tennessee Valley Authority', 'United States Lower 48', 'Western Area Power Administration - Rocky Mountain Region', 'Western Area Power Administration - Desert Southwest Region', 'Western Area Power Administration - Upper Great Plains West', 'NaturEner Wind Watch, LLC'.

- Use `fueltype` values: 'BAT', 'BAT', 'COL', 'GEO', 'NG', 'NUC', 'OES', 'OIL', 'OTH', 'PS', 'SNB', 'SNB', 'SUN', 'UES', 'UES', 'UNK', 'WAT', 'WNB', 'WND'.

- Use `type-name` values: 'Battery', 'Battery storage', 'Coal', 'Geothermal', 'Natural Gas', 'Nuclear', 'Other energy storage', 'Petroleum', 'Other',
'Pumped storage', 'Solar with integrated battery storage', 'Solar Battery', 'Solar', 'Unknown energy storage', 'Unknown Energy',
'Unknown', 'Hydro', 'Wind with integrated battery storage', 'Wind'.

- Produce at least **500 events** with randomized data using the `Faker` library.

---

### Module 2 — Kafka Consumer (Week 3)

**Goal:** Consume and persist the raw Kafka stream.

- Write a Python Kafka consumer (`electric_consumer.py`).
- Read from the `electric_orders` Kafka topic.
- Deserialize JSON messages into a Spark DataFrame.
- Write the raw data to a **Parquet** sink partitioned by `date` (derived from `timestamp`).
- Implement a 1-minute micro-batch trigger.

---

### Module 3 — Batch ETL with PySpark (Weeks 1 & 2)

**Goal:** Transform raw data into analytics-ready datasets.

#### 3A — RDD-Based Processing (Week 1)

- Load the raw JSON data as an RDD.
- Use RDD transformations (`map`, `filter`, `reduceByKey`) to:
  - Correct the `fuel-type` inconsistencies.
  - Filter out `INVALID` values.
  - Compute the total megawatthours per `respondent` using key-value pair RDDs.
- Save the result as a text file.

#### 3B — DataFrame / Spark SQL Processing (Week 2)

- Load the raw json data into a Spark DataFrame.
- Perform the following transformations:
  1. **Production By Respondent Name** — Group by respondent_name, compute `total_megawatthours`.
  2. **Energy Generated By Type** — Group by type-name, compute `total_megawatthours`.
  3. **Hourly Production By Fuel Type** — Group by fueltype and type-name, compute `average_hourly_production`.
- Write each output to CSV.
- Use **caching** on the base DataFrame to speed up multiple downstream transformations.

---

### Module 4 — Airflow Orchestration (Week 4)

**Goal:** Schedule and manage the full pipeline.

- Create an Airflow DAG named `ecommerce_pipeline` in a file called `ecommerce_dag.py`.
- Define the following tasks with proper dependencies:

  ```
  start >> check_kafka_topic >> run_streaming_job >> wait_for_raw_data
        >> run_rdd_etl >> run_df_etl >> validate_output >> end
  ```

- **Task details:**

  | Task                 | Operator Type       | Description                                      |
  |----------------------|---------------------|--------------------------------------------------|
  | `start`              | DummyOperator       | Pipeline entry point                             |
  | `check_kafka_topic`  | PythonOperator      | Verify the Kafka topic exists and has messages   |
  | `run_streaming_job`  | BashOperator        | Submit the Spark Streaming job via `spark-submit` |
  | `wait_for_raw_data`  | FileSensor          | Wait until raw Parquet files appear              |
  | `run_rdd_etl`        | BashOperator        | Submit the RDD batch job                         |
  | `run_df_etl`         | BashOperator        | Submit the DataFrame batch job                   |
  | `validate_output`    | PythonOperator      | Check row counts & schema of output files        |
  | `end`                | DummyOperator       | Pipeline exit point                              |

- Configure:
  - `schedule_interval`: `@daily`
  - `retries`: 2, `retry_delay`: 5 minutes
  - `email_on_failure`: `true`
  - Use **Connections** for Kafka broker and Spark cluster settings.
  - Create at least one **parameterized DAG** that accepts `execution_date` as a parameter.

---

## Deliverables

| #  | Deliverable                        | Format              |
|----|------------------------------------|----------------------|
| 1  | `electric_producer.py`             | Python script        |
| 2  | `electric_consumer.py`             | Python script        |
| 3  | `batch_df_etl.py`                  | PySpark script       |
| 4  | `batch_rdd_etl.py`                 | Setup & run guide    |
| 5  | `electric_producer_dag.py`         | AirFlow DAG          |
| 6  | `electric_consumer_dag.py`         | Airflow DAG          |
| 7  | `temp_api_data.json`               | Reference data       |
| 8  | `README.md`                        | Setup & run guide    |
| 9  | Sample output screenshots          | PNG / Markdown       |

---

## Folder Structure

```
README.md
code/
├── data/
│   ├── checkpoints/		  # Folder used to store spark streaming data
│   ├── raw/                  # Raw JSON output from streaming
│   └── transformed/          # Aggregated Parquet output from batch ETL
├── data_transformation/
│   ├── batch_df_etl.py		  
│   ├── batch_rdd_etl.py                  
│   └── electric_consumer.py          
├── kafka/
│   └── producer.py
├── data_collection/
│   ├── api_call.py
│   └── electric_producer.py
└── dags/
    ├── electric_consumer_dag.py
    └── electric_producer_dag.py

```

---

## Evaluation Criteria

| Area                     | Weight | What We Look For                                              |
|--------------------------|:------:|---------------------------------------------------------------|
| Kafka Integration        | 20%    | Proper topic setup, message schema, producer reliability      |
| Spark Streaming          | 15%    | Correct consumption, deserialization, partitioned Parquet sink |
| RDD Processing           | 15%    | Use of transformations, key-value RDDs, accumulators          |
| DataFrame / Spark SQL    | 20%    | Aggregations, joins, window functions, caching, bucketing     |
| Airflow DAG              | 20%    | Task dependencies, operator usage, parameterization, retries  |
| Code Quality & Docs      | 10%    | Clean code, README, inline comments, reproducibility          |

---

## Stretch Goals (Optional)

- Deploy the Spark jobs on an **AWS EMR** cluster (Week 1 - Friday).
- Use **Spark accumulators** to track bad/malformed records during RDD processing.
- Add a second Kafka topic (`order_updates`) for status changes and join both streams.
- Implement **dynamic DAGs** in Airflow that auto-generate tasks based on a config file.
- Add data quality checks using assertions in the `validate_output` task.
