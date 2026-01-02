import os
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv

# Cargar variables desde el archivo .env
load_dotenv()

# Recuperar la URI de conexión
uri = os.getenv("MONGO_URI")

# Crear cliente y conectar
client = MongoClient(uri, server_api=ServerApi('1'))

# Validar conexión
try:
    client.admin.command("ping")
    print("✅ Conexión exitosa a MongoDB Atlas")
except Exception as e:
    print("❌ Error de conexión:", e)

# Selección de base de datos y colección
db = client["fede_taller01"]
coleccion = db["archivos locales"]

# Índice único en 'nombre' para evitar duplicados y acelerar búsquedas
coleccion.create_index([("nombre", 1)], unique=True)

# Índice en 'fecha' para optimizar consultas por rango temporal
coleccion.create_index([("fecha", 1)])

print("✅ Índices creados correctamente")


# Documento de prueba
doc = {
    "nombre": "informe_final.pdf",
    "fecha": "2025-12-12",
    "tipo": "PDF"
}

# Insertar y validar
coleccion.insert_one(doc)
print("📄 Documento insertado:", coleccion.find_one({"nombre": "informe_final.pdf"}))

