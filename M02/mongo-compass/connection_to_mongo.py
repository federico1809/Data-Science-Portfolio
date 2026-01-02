
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

uri = "mongodb+srv://parulito:AtenaKanka1809%2B@mi-taller.zdnbqzm.mongodb.net/?retryWrites=true&w=majority"

# Create a new client and connect to the server
client = MongoClient(uri, server_api=ServerApi('1'))

# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)


    db = client["fede_taller01"]
coleccion = db["archivos locales"]

# Índice ascendente sobre el campo 'nombre'
coleccion.create_index([("nombre", 1)], unique=True)