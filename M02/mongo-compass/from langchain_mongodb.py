import os, pymongo, pprint
from langchain_community.document_loaders import PyPDFLoader
from langchain_core.output_parsers import StrOutputParser
from langchain_core.runnables import RunnablePassthrough
from langchain_mongodb import MongoDBAtlasVectorSearch
from langchain_voyageai import VoyageAIEmbeddings
from langchain_openai import ChatOpenAI
from langchain.prompts import PromptTemplate
from langchain_text_splitters import RecursiveCharacterTextSplitter
from pymongo import MongoClient
from pymongo.operations import SearchIndexModel


os.environ["VOYAGE_API_KEY"] = "<voyage-api-key>"
os.environ["OPENAI_API_KEY"] = "<openai-api-key>"
MONGODB_URI = "mongodb+srv://parulito:AtenaKanka1809%2B@<clusterName>.<hostname>.mongodb.net"


# Load the PDF
loader = PyPDFLoader("https://investors.mongodb.com/node/13176/pdf")
data = loader.load()

# Split PDF into documents
text_splitter = RecursiveCharacterTextSplitter(chunk_size=200, chunk_overlap=20)
docs = text_splitter.split_documents(data)

# Print the first document
docs[0]