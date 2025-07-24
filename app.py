# Directory structure:
# - requirements.txt
# - schema_extractor.py
# - pinecone_embedder.py
# - semantic_query_planner.py
# - multi_db_executor.py
# - streamlit_app.py


# ===================== schema_extractor.py =====================
from sqlalchemy import create_engine, inspect

def extract_schema(database_url):
    engine = create_engine(database_url)
    inspector = inspect(engine)
    schema = {}
    for table_name in inspector.get_table_names():
        columns = inspector.get_columns(table_name)
        schema[table_name] = [(col['name'], str(col['type'])) for col in columns]
    return schema

# ===================== pinecone_embedder.py =====================
import os
import pinecone
from langchain.embeddings import GoogleGenerativeAIEmbeddings
from dotenv import load_dotenv

load_dotenv()
pinecone.init(api_key=os.getenv("PINECONE_API_KEY"), environment=os.getenv("PINECONE_ENV"))
index = pinecone.Index("quick-commerce")
embedder = GoogleGenerativeAIEmbeddings(model="models/embedding-001")

def embed_and_store_schema(db_name, schema):
    for table, cols in schema.items():
        content = f"Table: {table}\n" + "\n".join([f"{c[0]}: {c[1]}" for c in cols])
        embedding = embedder.embed_query(content)
        index.upsert([(f"{db_name}:{table}", embedding, {"db": db_name, "table": table})])

# ===================== semantic_query_planner.py =====================
def get_relevant_tables(query, db_name, top_k=5):
    query_vector = embedder.embed_query(query)
    results = index.query(query_vector, top_k=top_k, include_metadata=True, filter={"db": {"$eq": db_name}})
    return list({item["metadata"]["table"] for item in results.matches})

# ===================== multi_db_executor.py =====================
from langchain.agents.agent_toolkits import SQLDatabaseToolkit
from langchain.agents import create_sql_agent
from langchain.sql_database import SQLDatabase
from langchain.llms import ChatGoogleGenerativeAI
import os

llm = ChatGoogleGenerativeAI(model="gemini-1.5-flash", temperature=0)

DB_URLS = {
    "blinkit": os.getenv("BLINKIT_DB_URL"),
    "zepto": os.getenv("ZEPTO_DB_URL"),
    "instamart": os.getenv("INSTAMART_DB_URL"),
    "bigbasket": os.getenv("BIGBASKET_DB_URL")
}

def get_executor(db_name, relevant_tables):
    class FilteredSQLDatabase(SQLDatabase):
        def get_table_info(self, table_names=None):
            return super().get_table_info(table_names=relevant_tables)

    db = FilteredSQLDatabase.from_uri(DB_URLS[db_name])
    toolkit = SQLDatabaseToolkit(db=db)
    return create_sql_agent(llm=llm, toolkit=toolkit, verbose=False)

def run_multi_db_query(query):
    responses = []
    for db_name in DB_URLS:
        relevant = get_relevant_tables(query, db_name)
        if not relevant:
            continue
        executor = get_executor(db_name, relevant)
        try:
            result = executor.run(query)
            responses.append((db_name, result))
        except Exception as e:
            responses.append((db_name, f"Error: {str(e)}"))
    return responses

# ===================== streamlit_app.py =====================
import streamlit as st
from multi_db_executor import run_multi_db_query

st.title("🛒 Quick Commerce Price Comparison (RAG + SQL Toolkit)")

query = st.text_input("Ask a question like: cheapest milk, discount on eggs, etc")

if query:
    st.write("Running intelligent DB agent...")
    responses = run_multi_db_query(query)
    for db, res in responses:
        st.subheader(db.capitalize())
        st.code(res, language="sql")
