"""
FleetLogix - Configuración AWS (Infraestructura como Código)
Script para aprovisionar servicios: RDS, S3 y DynamoDB
"""

import boto3
import json
import time

# Configuración Global
AWS_REGION = 'us-east-1'
RDS_INSTANCE_ID = 'fleetlogix-db-prod'
S3_BUCKET_NAME = 'fleetlogix-historical-data-v1'
DYNAMO_TABLE_DELIVERIES = 'FleetLogix_Deliveries_Status'
DYNAMO_TABLE_ALERTS = 'FleetLogix_Alerts'

# Clientes Boto3 (Simulación de credenciales configuradas en AWS CLI)
rds = boto3.client('rds', region_name=AWS_REGION)
s3 = boto3.client('s3', region_name=AWS_REGION)
dynamodb = boto3.client('dynamodb', region_name=AWS_REGION)

def crear_rds_postgresql():
    """
    Crea una instancia administrada de PostgreSQL en AWS RDS.
    Incluye configuración de backups automáticos.
    """
    print(f"🚀 Creando instancia RDS: {RDS_INSTANCE_ID}...")
    try:
        rds.create_db_instance(
            DBInstanceIdentifier=RDS_INSTANCE_ID,
            DBInstanceClass='db.t3.micro', # Capa gratuita elegible
            Engine='postgres',
            MasterUsername='fleet_admin',
            MasterUserPassword='SecurePassword123!', 
            AllocatedStorage=20,
            BackupRetentionPeriod=7,       # Backup automático de 7 días
            MultiAZ=False,                 # False para ahorrar costos en dev
            PubliclyAccessible=True,       # Para conectar desde DBeaver local
            StorageType='gp2'
        )
        print("✅ Solicitud de RDS enviada. La base de datos se está aprovisionando.")
    except Exception as e:
        print(f"⚠️ Nota sobre RDS: {e}")

def crear_s3_bucket():
    """
    Crea un bucket S3 para almacenamiento de logs históricos.
    """
    print(f"📦 Creando Bucket S3: {S3_BUCKET_NAME}...")
    try:
        s3.create_bucket(Bucket=S3_BUCKET_NAME)
        print("✅ Bucket S3 creado exitosamente.")
    except Exception as e:
        print(f"⚠️ Nota sobre S3: {e}")

def crear_tablas_dynamodb():
    """
    Crea tablas NoSQL en DynamoDB para estado en tiempo real.
    """
    print("⚡ Creando tablas DynamoDB...")
    
    # Tabla 1: Estado de Entregas (Búsqueda rápida por delivery_id)
    try:
        dynamodb.create_table(
            TableName=DYNAMO_TABLE_DELIVERIES,
            KeySchema=[{'AttributeName': 'delivery_id', 'KeyType': 'HASH'}],
            AttributeDefinitions=[{'AttributeName': 'delivery_id', 'AttributeType': 'N'}],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        print(f"✅ Tabla {DYNAMO_TABLE_DELIVERIES} creada.")
    except Exception as e:
        print(f"⚠️ Nota DynamoDB: {e}")

    # Tabla 2: Historial de Alertas (Búsqueda por vehicle_id)
    try:
        dynamodb.create_table(
            TableName=DYNAMO_TABLE_ALERTS,
            KeySchema=[
                {'AttributeName': 'vehicle_id', 'KeyType': 'HASH'},  # Partition Key
                {'AttributeName': 'timestamp', 'KeyType': 'RANGE'}   # Sort Key
            ],
            AttributeDefinitions=[
                {'AttributeName': 'vehicle_id', 'AttributeType': 'N'},
                {'AttributeName': 'timestamp', 'AttributeType': 'S'}
            ],
            ProvisionedThroughput={'ReadCapacityUnits': 5, 'WriteCapacityUnits': 5}
        )
        print(f"✅ Tabla {DYNAMO_TABLE_ALERTS} creada.")
    except Exception as e:
        print(f"⚠️ Nota DynamoDB: {e}")

def main():
    print("--- INICIANDO DESPLIEGUE DE INFRAESTRUCTURA FLEETLOGIX ---")
    crear_s3_bucket()
    crear_tablas_dynamodb()
    crear_rds_postgresql()
    print("\n--- DESPLIEGUE FINALIZADO ---")
    print("Recuerda: RDS tarda unos 5-10 minutos en estar disponible ('Available').")

if __name__ == '__main__':
    main()