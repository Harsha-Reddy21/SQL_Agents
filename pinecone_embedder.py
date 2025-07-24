# ===================== pinecone_embedder.py =====================

import os
from sqlalchemy import create_engine, inspect
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from pinecone import Pinecone, ServerlessSpec
from dotenv import load_dotenv

load_dotenv()
os.environ["GOOGLE_API_KEY"] = os.getenv("GEMINI_API_KEY")

# Initialize
embedder = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
index_name = 'multi-db-index'

# Create index if needed
if index_name not in [idx.name for idx in pc.list_indexes()]:
    pc.create_index(
        name=index_name,
        dimension=768,
        metric="cosine",
        spec=ServerlessSpec(cloud="aws", region="us-east-1")
    )
    print(f"✅ Created index: {index_name}")
else:
    print(f"✅ Index {index_name} exists")

index = pc.Index(index_name)

def extract_and_embed_schemas(db_configs: dict):
    """Extract schemas from databases and embed them"""
    for db_name, db_url in db_configs.items():
        print(f"Processing {db_name}...")
        
        # Extract schema
        engine = create_engine(db_url)
        inspector = inspect(engine)
        
        # Embed and store each table
        for table_name in inspector.get_table_names():
            columns = inspector.get_columns(table_name)
            col_defs = [f"{col['name']} {col['type']}" for col in columns]
            schema_text = f"Table: {table_name}\nColumns: {', '.join(col_defs)}"
            
            embedding = embedder.embed_query(schema_text)
            index.upsert([(
                f"{db_name}:{table_name}", 
                embedding, 
                {"db": db_name, "table": table_name}
            )])
        
        print(f"✅ Embedded {len(inspector.get_table_names())} tables from {db_name}")

if __name__ == "__main__":
    db_configs = {
        "blinkit_db": os.getenv("blinkit_db_url"),
        "zepto_db": os.getenv("zepto_db_url"),
        "instamart_db": os.getenv("instamart_db_url"),
        "bigbasket_db": os.getenv("bigbasket_db_url")
    }
    
    extract_and_embed_schemas(db_configs)