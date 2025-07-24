import os
from functools import lru_cache
from sqlalchemy import create_engine
from sqlalchemy.pool import QueuePool
from dotenv import load_dotenv
from pinecone import Pinecone
from langchain_google_genai import GoogleGenerativeAIEmbeddings
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_community.utilities import SQLDatabase
from langchain_openai import ChatOpenAI

load_dotenv()

# Set environment variables
os.environ["GOOGLE_API_KEY"] = os.getenv("GEMINI_API_KEY")
os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")

# Initialize once
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
embedder = GoogleGenerativeAIEmbeddings(model="models/embedding-001")
index = pc.Index("multi-db-index")

# Database URLs with connection pooling
DB_ENGINES = {
    db: create_engine(
        url, 
        poolclass=QueuePool,
        pool_size=5,
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=3600
    )
    for db, url in {
        "blinkit_db": os.getenv("blinkit_db_url"),
        "zepto_db": os.getenv("zepto_db_url"), 
        "instamart_db": os.getenv("instamart_db_url"),
        "bigbasket_db": os.getenv("bigbasket_db_url")
    }.items()
}

# Cache SQL agents to avoid recreation
@lru_cache(maxsize=32)
def get_cached_agent(db_name: str, table_names_tuple: tuple):
    """Create and cache SQL agents with filtered tables"""
    class FilteredSQLDatabase(SQLDatabase):
        def __init__(self, engine):
            super().__init__(engine)
            self.filtered_tables = list(table_names_tuple)
            
        def get_table_info(self, table_names=None):
            return super().get_table_info(table_names=self.filtered_tables)

    db = FilteredSQLDatabase(DB_ENGINES[db_name])
    toolkit = SQLDatabaseToolkit(db=db, llm=llm)
    return create_sql_agent(
        llm=llm, 
        toolkit=toolkit, 
        verbose=True, 
        handle_parsing_errors=True,
        agent_executor_kwargs={"handle_parsing_errors": True}
    )

def get_relevant_tables(query: str, top_k: int = 5):
    """Get relevant tables using vector similarity search"""
    query_vec = embedder.embed_query(query)
    matches = index.query(vector=query_vec, top_k=top_k, include_metadata=True)
    return matches["matches"]

def run_multi_db_query(query: str, relevant_tables: list):
    """Execute query across relevant databases concurrently"""
    responses = []
    
    # Group tables by database
    db_tables = {}
    for match in relevant_tables:
        db_name = match['metadata']['db']
        table_name = match['metadata']['table']
        if db_name not in db_tables:
            db_tables[db_name] = []
        db_tables[db_name].append(table_name)
    
    print(f"Querying {len(db_tables)} databases with {len(relevant_tables)} relevant tables")
    
    # Process each database with relevant tables
    for db_name, table_names in db_tables.items():
        try:
            # Use cached agent with table names as tuple for hashing
            agent = get_cached_agent(db_name, tuple(table_names))
            result = agent.invoke({"input": query})
            output = result.get("output", result)
            responses.append((db_name, output))
            print(f"✓ {db_name}: Success")
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            responses.append((db_name, error_msg))
            print(f"✗ {db_name}: {error_msg}")
    
    return responses

# if __name__ == "__main__":
#     query = "what is the price of the product with id 1"
#     responses = run_multi_db_query(query)
#     print(responses)








