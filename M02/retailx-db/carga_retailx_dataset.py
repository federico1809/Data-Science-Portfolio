import pandas as pd
from sqlalchemy import create_engine
import os

# --- CONFIGURACIÓN ---
# Ruta de tu archivo CSV
csv_file_path = r'C:\Users\feder\Documents\Data_Science\m02\retailx-db\dataset_retailx.csv'

# Parámetros de conexión a la base de datos PostgreSQL
# Ajusta estos valores según tu configuración local
db_user = 'postgres'       # Tu usuario de PostgreSQL
db_password = 'fede0309' # Tu contraseña de PostgreSQL
db_host = 'localhost'      # Host (generalmente localhost)
db_port = '5432'           # Puerto (por defecto 5432)
db_name = 'retailx_db'     # Nombre de la base de datos destino
table_name = 'retailx_dataset' # Nombre de la tabla donde se cargarán los datos

# --- PROCESO DE CARGA ---

def load_data():
    # 1. Verificar si el archivo existe
    if not os.path.exists(csv_file_path):
        print(f"Error: No se encontró el archivo en {csv_file_path}")
        return

    print("--- Iniciando proceso de carga ---")

    try:
        # 2. Leer el archivo CSV con Pandas
        print(f"Leyendo archivo CSV: {csv_file_path}...")
        df = pd.read_csv(csv_file_path)
        print(f"Archivo leído exitosamente. Registros encontrados: {len(df)}")

        # Opcional: Mostrar las primeras filas para verificar
        print("Vista previa de los datos:")
        print(df.head())

        # 3. Crear la conexión a la base de datos (Engine)
        # Formato: postgresql://user:password@host:port/database
        connection_string = f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        engine = create_engine(connection_string)

        # 4. Subir el DataFrame a PostgreSQL
        print(f"Cargando datos a la tabla '{table_name}' en la base de datos '{db_name}'...")
        
        # 'if_exists' puede ser:
        #   'fail': Falla si la tabla ya existe (por defecto).
        #   'replace': Elimina la tabla anterior y crea una nueva.
        #   'append': Agrega los datos a la tabla existente.
        df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)

        print("--- Carga completada exitosamente ---")

    except Exception as e:
        print(f"Ocurrió un error durante el proceso: {e}")

if __name__ == "__main__":
    load_data()