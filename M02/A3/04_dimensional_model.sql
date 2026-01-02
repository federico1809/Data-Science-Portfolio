CREATE OR REPLACE DATABASE FLEETLOGIX_DW;
USE DATABASE FLEETLOGIX_DW;

CREATE OR REPLACE WAREHOUSE FLEETLOGIX_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    MIN_CLUSTER_COUNT = 1
    MAX_CLUSTER_COUNT = 1;
USE WAREHOUSE FLEETLOGIX_WH;

-- Crear schema ANALYTICS
CREATE SCHEMA IF NOT EXISTS ANALYTICS;

-- Usar el schema correcto para el ETL
USE DATABASE FLEETLOGIX_DW;
USE SCHEMA ANALYTICS;

ALTER DATABASE FLEETLOGIX_DW
SET DATA_RETENTION_TIME_IN_DAYS = 1;

-- ========== DIMENSIONS ==========

CREATE OR REPLACE TABLE dim_customer (
    customer_sk INTEGER AUTOINCREMENT PRIMARY KEY,
    customer_name VARCHAR,
    customer_type VARCHAR,
    city VARCHAR,
    first_delivery_date DATE,
    total_deliveries INTEGER,
    customer_category VARCHAR
);

CREATE OR REPLACE TABLE dim_driver (
    driver_sk INTEGER AUTOINCREMENT PRIMARY KEY,
    driver_id INTEGER,
    employee_code VARCHAR,
    full_name VARCHAR,
    license_number VARCHAR,
    license_expiry DATE,
    phone VARCHAR,
    hire_date DATE,
    status VARCHAR,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
);

CREATE OR REPLACE TABLE dim_vehicle (
    vehicle_sk INTEGER AUTOINCREMENT PRIMARY KEY,
    vehicle_id INTEGER,
    license_plate VARCHAR,
    vehicle_type VARCHAR,
    capacity_kg NUMBER(10,2),
    fuel_type VARCHAR,
    acquisition_date DATE,
    status VARCHAR,
    last_maintenance_date DATE,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN
);

CREATE OR REPLACE TABLE dim_route (
    route_sk INTEGER AUTOINCREMENT PRIMARY KEY,
    route_id INTEGER,
    origin_city VARCHAR,
    destination_city VARCHAR,
    distance_km NUMBER(10,2),
    estimated_duration_hours NUMBER(10,2),
    toll_cost NUMBER(10,2)
);

CREATE OR REPLACE TABLE dim_date (
    date_sk INTEGER PRIMARY KEY,
    full_date DATE,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    day INTEGER,
    day_of_week INTEGER,
    is_weekend BOOLEAN
);

CREATE OR REPLACE TABLE dim_time (
    time_sk INTEGER PRIMARY KEY,
    hour INTEGER,
    minute INTEGER,
    second INTEGER
);

-- ========== FACT TABLE ==========

CREATE OR REPLACE TABLE fact_deliveries (
    delivery_sk INTEGER AUTOINCREMENT PRIMARY KEY,

    date_sk INTEGER,
    time_sk INTEGER,
    driver_sk INTEGER,
    vehicle_sk INTEGER,
    route_sk INTEGER,
    customer_sk INTEGER,

    delivery_id INTEGER,
    trip_id INTEGER,
    scheduled_delivery_datetime TIMESTAMP,
    delivered_datetime TIMESTAMP,
    delivery_status VARCHAR,
    distance_km NUMBER(10,2),
    fuel_used_liters NUMBER(10,2),

    delivery_time_minutes NUMBER(10,2),
    delay_minutes NUMBER(10,2),
    deliveries_in_trip NUMBER(10,2),
    deliveries_per_hour NUMBER(10,2),
    fuel_efficiency_km_per_liter NUMBER(10,2),
    cost_per_delivery NUMBER(10,2),
    revenue_per_delivery NUMBER(10,2),

    etl_batch_id INTEGER,
    load_datetime TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

SHOW TABLES IN SCHEMA FLEETLOGIX_DW.ANALYTICS;