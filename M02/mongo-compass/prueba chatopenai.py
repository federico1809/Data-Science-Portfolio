import os
from langchain_openai import ChatOpenAI
from langchain_voyageai import VoyageAIEmbeddings

# Configurar claves desde entorno
os.environ["OPENAI_API_KEY"] = "<tu_openai_api_key>"
os.environ["VOYAGE_API_KEY"] = "<tu_voyage_api_key>"

# Probar ChatOpenAI
llm = ChatOpenAI(model="gpt-3.5-turbo")
print(llm.invoke("Hola Federico, probando conexión con OpenAI"))

# Probar VoyageAI embeddings
embeddings = VoyageAIEmbeddings(model="voyage-large")
vector = embeddings.embed_query("Probando embeddings con VoyageAI")
print(vector[:5])  # primeros 5 valores del embedding
