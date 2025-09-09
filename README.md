# MSSQL → PostgreSQL Data Pipeline with Airflow

## 📌 About the Project
This project demonstrates a data pipeline that transfers data from an **MSSQL** database to a **PostgreSQL** database on a daily incremental basis.  
The workflow is orchestrated using **Apache Airflow** and runs inside **Docker Compose**.  

The pipeline follows an **ETL pattern**:
1. **Extract** → Pulls daily records from the `orders` table in MSSQL  
2. **Load** → Loads the extracted data into PostgreSQL using an **UPSERT (ON CONFLICT UPDATE)** strategy  
3. **Validate** → Compares row counts between source (MSSQL) and target (PostgreSQL)  

---

## Architecture
- **MSSQL** → Source database  
- **PostgreSQL** → Target database  
- **Airflow** → Orchestration and scheduling  
- **Docker Compose** → Containerized environment  
- **Pandas + Psycopg2** → Data transformation and PostgreSQL loading  

---

## Project Structure
```bash
.
├── dags/
│ └── mssql_to_postgresql_orders_daily.py # Airflow DAG
├── scripts/
│ └── csv_to_mssql.py # Script to load CSV into MSSQL
├── init/
│ ├── mssql_init.sql # MSSQL initialization script
│ └── postgres_init.sql # PostgreSQL initialization script
├── .env # Environment variables
├── docker-compose.yaml # Docker Compose configuration
└── README.md
```

---

## Getting Started
1. **Clone the repository**
   ```bash
   git clone https://github.com/erhanustun/ETL-mssql-to-postgres-dailySync
   ```
2. Start services with Docker Compose

   docker compose up -d

3. Access Airflow UI

   URL: http://localhost:8080
   
   Default user: airflow
   
   Password: airflow

4. Trigger the DAG

   From the Airflow UI, enable and trigger the orders_daily_sync DAG.

📊 Example Validation

   -- Check record count in PostgreSQL
   SELECT COUNT(*) FROM orders;
   
   -- Check record count in MSSQL
   SELECT COUNT(*) FROM orders;

   ✅ If counts match, synchronization was successful.

🔮 Future Improvements

   Store records with NULL OrderCreatedAt values in a separate table
   
   Add a dead_letter table for invalid/error rows
   
   Optimize for larger datasets with batch/chunk inserts
   
   Add CDC (Change Data Capture) support for near real-time sync
