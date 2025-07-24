# ===================== pinecone_embedder.py =====================

import os
from sqlalchemy import create_engine, inspect
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from pinecone import Pinecone, ServerlessSpec
from dotenv import load_dotenv

load_dotenv()
os.environ["GOOGLE_API_KEY"] = os.getenv("GEMINI_API_KEY")

embedder = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
print(pc.list_indexes())
index_name='multi-db-index'
if index_name not in [idx.name for idx in pc.list_indexes()]:
    print(f"Creating index: {index_name}")
    pc.create_index(
        name=index_name,
        dimension=768,  
        metric="cosine",
        spec=ServerlessSpec(
            cloud="aws",
            region="us-east-1"
        )
    )
    print("Index created successfully")
else:
    print(f"Index {index_name} already exists")

index = pc.Index(index_name)

def get_table_schemas(db_url):
    engine = create_engine(db_url)
    inspector = inspect(engine)
    schemas = []
    for table_name in inspector.get_table_names():
        columns = inspector.get_columns(table_name)
        col_defs = [f"{col['name']} {col['type']}" for col in columns]
        schema_text = f"Table: {table_name}\nColumns:\n" + "\n".join(col_defs)
        schemas.append({"table": table_name, "schema": schema_text})
    return schemas



def embed_and_store_schema(db_name, schema):
    for schema in schema:
        content = f"Table: {schema['table']}\n , { schema['schema']}"
        embedding = embedder.embed_query(content)
        index.upsert([(f"{db_name}:{schema['table']}", embedding, {"db": db_name, "table": schema['table']})])

if __name__ == "__main__":
    # blinkit_db_url = os.getenv("blinkit_db_url")
    zepto_db_url = os.getenv("zepto_db_url")
    instamart_db_url = os.getenv("instamart_db_url")
    bigbasket_db_url = os.getenv("bigbasket_db_url")
    # schemas = get_table_schemas(blinkit_db_url)
    # db_name = "blinkit_db"
    # embed_and_store_schema(db_name, schemas)
    schemas = get_table_schemas(zepto_db_url)
    db_name = "zepto_db"
    embed_and_store_schema(db_name, schemas)
    schemas = get_table_schemas(instamart_db_url)
    db_name = "instamart_db"
    embed_and_store_schema(db_name, schemas)
    schemas = get_table_schemas(bigbasket_db_url)
    db_name = "bigbasket_db"
    embed_and_store_schema(db_name, schemas)
# load_dotenv()
# pinecone.init(api_key=os.getenv("PINECONE_API_KEY"), environment=os.getenv("PINECONE_ENV"))
# index = pinecone.Index("quick-commerce")
# embedder = GoogleGenerativeAIEmbeddings(model="models/embedding-001")









# DATABASE_URL = os.getenv("DATABASE_URL")
# engine = create_engine(DATABASE_URL)
# inspector = inspect(engine)

# # Configure Google API key - the library expects GOOGLE_API_KEY environment variable
# gemini_api_key = os.getenv("GEMINI_API_KEY")
# os.environ["GOOGLE_API_KEY"] = gemini_api_key

# index_name = "sql-agent"
# embedder = GoogleGenerativeAIEmbeddings(model="models/embedding-001")

# pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
# print("Existing indexes:", pc.list_indexes())



# def get_table_schemas():
#     schemas = []
#     for table_name in inspector.get_table_names():
#         columns = inspector.get_columns(table_name)
#         col_defs = [f"{col['name']} {col['type']}" for col in columns]
#         schema_text = f"Table: {table_name}\nColumns:\n" + "\n".join(col_defs)
#         schemas.append({"table": table_name, "schema": schema_text})
#     return schemas

# def embed_and_push_to_pinecone(schemas):
#     index = pc.Index(index_name)
#     print("Connected to index:", index_name)

#     schema_texts = [s["schema"] for s in schemas]
#     vectors = embedder.embed_documents(schema_texts)
#     print("Vectors embedded successfully")
#     print(vectors[0])

#     for schema, vec in zip(schemas, vectors):
#         index.upsert([{
#             "id": f"table-{schema['table']}",
#             "values": vec,
#             "metadata": {"table": schema["table"], "schema": schema["schema"]}
#         }])

# if __name__ == "__main__":
#     schemas = get_table_schemas()
#     embed_and_push_to_pinecone(schemas)