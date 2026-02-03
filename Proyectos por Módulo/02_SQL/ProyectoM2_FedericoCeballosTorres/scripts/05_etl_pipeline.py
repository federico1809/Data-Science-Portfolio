"""
FleetLogix - Pipeline ETL Automático
Extrae de PostgreSQL, Transforma y Carga en Snowflake
Ejecución diaria automatizada
"""

import psycopg2
import snowflake.connector
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import logging
import schedule
import time
import json
from typing import Dict, List, Tuple

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)

# Configuración de conexiones
POSTGRES_CONFIG = {
    'host': 'localhost',
    'database': 'fleetlogix',
    'user': 'postgres',
    'password': 'fede0309',
    'port': 5432
}

SNOWFLAKE_CONFIG = {
    'user': 'fededatabasing',
    'password': 'AtenaKanka1809',
    'account': 'ITFRMHR-JQ67302',
    'warehouse': 'FLEETLOGIX_WH',
    'database': 'FLEETLOGIX_DW',
    'schema': 'ANALYTICS'
}

class FleetLogixETL:
    def __init__(self):
        self.pg_conn = None
        self.sf_conn = None
        self.batch_id = int(datetime.now().timestamp())
        self.metrics = {
            'records_extracted': 0,
            'records_transformed': 0,
            'records_loaded': 0,
            'errors': 0
        }
    
    def connect_databases(self):
        """Establecer conexiones con PostgreSQL y Snowflake"""
        try:
            # PostgreSQL
            self.pg_conn = psycopg2.connect(**POSTGRES_CONFIG)
            logging.info(" Conectado a PostgreSQL")
            
            # Snowflake
            self.sf_conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
            logging.info(" Conectado a Snowflake")
            
            return True
        except Exception as e:
            logging.error(f" Error en conexión: {e}")
            return False
    
    def extract_daily_data(self) -> pd.DataFrame:
        """Extraer datos del día anterior de PostgreSQL"""
        logging.info(" Iniciando extracción de datos...")
        
        query = """
SELECT d.delivery_id,
       d.trip_id,
       d.tracking_number,
       d.customer_name,
       d.delivery_address,
       d.package_weight_kg,
       d.scheduled_datetime,
       d.delivered_datetime,
       d.delivery_status,
       d.recipient_signature,
       t.departure_datetime,
       t.arrival_datetime,
       t.fuel_consumed_liters,
       t.total_weight_kg,
       t.status AS trip_status,
       r.route_id,
       r.origin_city,
       r.destination_city,
       r.distance_km,
       r.estimated_duration_hours,
       r.toll_cost,
       v.vehicle_id,
       v.license_plate,
       v.vehicle_type,
       v.capacity_kg,
       v.fuel_type,
       v.acquisition_date,
       v.status AS vehicle_status,
       dr.driver_id,
       dr.employee_code,
       dr.first_name || ' ' || dr.last_name AS full_name,
       dr.license_number,
       dr.license_expiry,
       dr.phone,
       dr.hire_date,
       dr.status AS driver_status
FROM deliveries d
JOIN trips t ON d.trip_id = t.trip_id
JOIN routes r ON t.route_id = r.route_id
JOIN vehicles v ON t.vehicle_id = v.vehicle_id
JOIN drivers dr ON t.driver_id = dr.driver_id
WHERE d.delivered_datetime >= CURRENT_DATE - INTERVAL '1 day'
  AND d.delivered_datetime < CURRENT_DATE;
"""

        
        try:
            df = pd.read_sql(query, self.pg_conn)
            self.metrics['records_extracted'] = len(df)
            logging.info(f" Extraídos {len(df)} registros")
            return df
        except Exception as e:
            logging.error(f" Error en extracción: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()
    
    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Transformar datos para el modelo dimensional"""
        logging.info(" Iniciando transformación de datos...")
        
        try:
            # Calcular métricas
            df['delivery_time_minutes'] = (
                (pd.to_datetime(df['delivered_datetime']) - 
                 pd.to_datetime(df['scheduled_datetime'])).dt.total_seconds() / 60
            ).round(2)
            
            df['delay_minutes'] = df['delivery_time_minutes'].apply(
                lambda x: max(0, x) if x > 0 else 0
            )
            
            df['is_on_time'] = df['delay_minutes'] <= 30
            
            # Calcular entregas por hora
            df['trip_duration_hours'] = (
                (pd.to_datetime(df['arrival_datetime']) - 
                 pd.to_datetime(df['departure_datetime'])).dt.total_seconds() / 3600
            ).round(2)
            
            # Agrupar entregas por trip para calcular entregas/hora
            deliveries_per_trip = df.groupby('trip_id').size()
            df['deliveries_in_trip'] = df['trip_id'].map(deliveries_per_trip)
            df['deliveries_per_hour'] = (
                df['deliveries_in_trip'] / df['trip_duration_hours']
            ).round(2)
            
            # Eficiencia de combustible
            df['fuel_efficiency_km_per_liter'] = (
                df['distance_km'] / df['fuel_consumed_liters']
            ).round(2)
            
            # Costo estimado por entrega
            df['cost_per_delivery'] = (
                (df['fuel_consumed_liters'] * 5000 + df['toll_cost']) / 
                df['deliveries_in_trip']
            ).round(2)
            
            # Revenue estimado (ejemplo: $20,000 base + $500 por kg)
            df['revenue_per_delivery'] = (20000 + df['package_weight_kg'] * 500).round(2)
            
            # Validaciones de calidad
            # No permitir tiempos negativos
            df = df[df['delivery_time_minutes'] >= 0]
            
            # No permitir pesos fuera de rango
            df = df[(df['package_weight_kg'] > 0) & (df['package_weight_kg'] < 10000)]
            
            # Manejar cambios históricos (SCD Type 2 para conductor/vehículo)
            df['valid_from'] = pd.to_datetime(df['scheduled_datetime']).dt.date
            df['valid_to'] = pd.to_datetime('9999-12-31')
            df['is_current'] = True
            
            self.metrics['records_transformed'] = len(df)
            logging.info(f" Transformados {len(df)} registros")
            
            return df
            
        except Exception as e:
            logging.error(f" Error en transformación: {e}")
            self.metrics['errors'] += 1
            return pd.DataFrame()
    
    def load_dimensions(self, df: pd.DataFrame):
        """Cargar o actualizar dimensiones en Snowflake con SCD Type 2"""
        logging.info(" Cargando dimensiones...")

        cursor = self.sf_conn.cursor()

        try:
            # === DIM_CUSTOMER (ejemplo simple) ===
            customers = df[['customer_name', 'destination_city']].drop_duplicates()
            for _, row in customers.iterrows():
                cursor.execute("""
                    MERGE INTO dim_customer c
                    USING (SELECT %s AS customer_name, %s AS city) s
                    ON c.customer_name = s.customer_name
                    WHEN NOT MATCHED THEN
                        INSERT (customer_name, customer_type, city, first_delivery_date, 
                                total_deliveries, customer_category)
                        VALUES (%s, 'Individual', %s, CURRENT_DATE(), 0, 'Regular')
                """, (row['customer_name'], row['destination_city'],
                      row['customer_name'], row['destination_city']))

            # === DIM_DRIVER (SCD Type 2) ===
            drivers = df[['driver_id','employee_code','full_name','license_number',
                          'license_expiry','phone','hire_date','status']].drop_duplicates()
            for _, row in drivers.iterrows():
                cursor.execute("""
                    UPDATE dim_driver
                    SET valid_to = CURRENT_DATE - 1, is_current = FALSE
                    WHERE driver_id = %s AND is_current = TRUE
                      AND (full_name <> %s OR status <> %s OR phone <> %s)
                """, (row['driver_id'], row['full_name'], row['status'], row['phone']))

                cursor.execute("""
                    INSERT INTO dim_driver (
                        driver_id, employee_code, full_name, license_number,
                        license_expiry, phone, hire_date, status,
                        valid_from, valid_to, is_current
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_DATE,'9999-12-31',TRUE)
                """, (
                    row['driver_id'], row['employee_code'], row['full_name'], row['license_number'],
                    row['license_expiry'], row['phone'], row['hire_date'], row['status']
                ))

            # === DIM_VEHICLE (SCD Type 2) ===
            vehicles = df[['vehicle_id','license_plate','vehicle_type','capacity_kg',
                           'fuel_type','acquisition_date','status','last_maintenance_date']].drop_duplicates()
            for _, row in vehicles.iterrows():
                cursor.execute("""
                    UPDATE dim_vehicle
                    SET valid_to = CURRENT_DATE - 1, is_current = FALSE
                    WHERE vehicle_id = %s AND is_current = TRUE
                      AND (status <> %s OR capacity_kg <> %s OR fuel_type <> %s)
                """, (row['vehicle_id'], row['status'], row['capacity_kg'], row['fuel_type']))

                cursor.execute("""
                    INSERT INTO dim_vehicle (
                        vehicle_id, license_plate, vehicle_type, capacity_kg,
                        fuel_type, acquisition_date, status,
                        last_maintenance_date, valid_from, valid_to, is_current
                    )
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,CURRENT_DATE,'9999-12-31',TRUE)
                """, (
                    row['vehicle_id'], row['license_plate'], row['vehicle_type'], row['capacity_kg'],
                    row['fuel_type'], row['acquisition_date'], row['status'], row['last_maintenance_date']
                ))

            self.sf_conn.commit()
            logging.info(" Dimensiones actualizadas con SCD Type 2")

        except Exception as e:
            logging.error(f" Error cargando dimensiones: {e}")
            self.sf_conn.rollback()
            self.metrics['errors'] += 1

    
    def run_etl(self):
        """Ejecutar pipeline ETL completo"""
        start_time = datetime.now()
        logging.info(f" Iniciando ETL - Batch ID: {self.batch_id}")
        
        try:
            # Conectar
            if not self.connect_databases():
                return
            
            # ETL
            df = self.extract_daily_data()
            if not df.empty:
                df_transformed = self.transform_data(df)
                if not df_transformed.empty:
                    self.load_dimensions(df_transformed)
                    self.load_facts(df_transformed)
            
            # Calcular totales para reportes
            self._calculate_daily_totals()
            
            # Cerrar conexiones
            self.close_connections()
            
            # Log final
            duration = (datetime.now() - start_time).total_seconds()
            logging.info(f" ETL completado en {duration:.2f} segundos")
            logging.info(f" Métricas: {json.dumps(self.metrics, indent=2)}")
            
        except Exception as e:
            logging.error(f" Error fatal en ETL: {e}")
            self.metrics['errors'] += 1
            self.close_connections()
    
    def _calculate_daily_totals(self):
        """Pre-calcular totales para reportes rápidos"""
        cursor = self.sf_conn.cursor()
        
        try:
            # Crear tabla de totales si no existe
            cursor.execute("""
CREATE TABLE IF NOT EXISTS daily_totals (
    total_date DATE,
    total_deliveries INT,
    avg_delivery_time DECIMAL(10,2),
    avg_fuel_efficiency DECIMAL(10,2),
    total_revenue DECIMAL(10,2),
    etl_batch_id INT,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
""")
            
            # Insertar totales del día
            cursor.execute("""
INSERT INTO daily_totals (total_date, total_deliveries, avg_delivery_time,
                          avg_fuel_efficiency, total_revenue, etl_batch_id)
SELECT CURRENT_DATE,
       COUNT(*),
       AVG(delivery_time_minutes),
       AVG(fuel_efficiency_km_per_liter),
       SUM(revenue_per_delivery),
       %s
FROM fact_deliveries
WHERE etl_batch_id = %s;
""", (self.batch_id, self.batch_id))
            
            self.sf_conn.commit()
            logging.info(" Totales diarios calculados")
            
        except Exception as e:
            logging.error(f" Error calculando totales: {e}")
    
    def close_connections(self):
        """Cerrar conexiones a bases de datos"""
        if self.pg_conn:
            self.pg_conn.close()
        if self.sf_conn:
            self.sf_conn.close()
        logging.info(" Conexiones cerradas")

def job():
    """Función para programar con schedule"""
    etl = FleetLogixETL()
    etl.run_etl()

def main():
    """Función principal - Automatización diaria"""
    logging.info(" Pipeline ETL FleetLogix iniciado")
    
    # Programar ejecución diaria a las 2:00 AM
    schedule.every().day.at("02:00").do(job)
    
    logging.info(" ETL programado para ejecutarse diariamente a las 2:00 AM")
    logging.info("Presiona Ctrl+C para detener")
    
    # Ejecutar una vez al inicio (para pruebas)
    job()
    
    # Loop infinito esperando la hora programada
    while True:
        schedule.run_pending()
        time.sleep(60)  # Verificar cada minuto

if __name__ == "__main__":
    main()