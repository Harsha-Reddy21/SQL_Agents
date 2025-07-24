from pinecone import Pinecone, ServerlessSpec
from dotenv import load_dotenv
import os
from langchain_google_genai import GoogleGenerativeAIEmbeddings
load_dotenv()
from langchain_community.agent_toolkits.sql.toolkit import SQLDatabaseToolkit
from langchain_community.agent_toolkits.sql.base import create_sql_agent
from langchain_community.utilities import SQLDatabase
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_openai import ChatOpenAI
os.environ["GOOGLE_API_KEY"] = os.getenv("GEMINI_API_KEY")
os.environ["OPENAI_API_KEY"] = os.getenv("OPENAI_API_KEY")

llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))

DB_URLS = {
    "blinkit_db": os.getenv("blinkit_db_url"),
    "zepto_db": os.getenv("zepto_db_url"),
    "instamart_db": os.getenv("instamart_db_url"),
    "bigbasket_db": os.getenv("bigbasket_db_url")
}
embedder = GoogleGenerativeAIEmbeddings(model="models/embedding-001")

index_name = "multi-db-index"
index = pc.Index(index_name)

def get_relevant_tables(query: str, top_k: int = 5):
    query_vec = embedder.embed_query(query)
    matches = index.query(vector=query_vec, top_k=top_k, include_metadata=True)
    return [match for match in matches["matches"]]

# relevant_tables = get_relevant_tables("show all tables", top_k=5)
# print(relevant_tables)


def get_executor(db_name, relevant_tables):
    filtered_table_names = [
        match['metadata']['table'] 
        for match in relevant_tables 
        if match['metadata']['db'] == db_name
    ]
    
    class FilteredSQLDatabase(SQLDatabase):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            self.filtered_tables = filtered_table_names
            
        def get_table_info(self, table_names=None):
            return super().get_table_info(table_names=self.filtered_tables)

    db = FilteredSQLDatabase.from_uri(DB_URLS[db_name])
    toolkit = SQLDatabaseToolkit(db=db, llm=llm)
    return create_sql_agent(
        llm=llm, 
        toolkit=toolkit, 
        verbose=False, 
        handle_parsing_errors=True,
        agent_executor_kwargs={"handle_parsing_errors": True}
    )


def run_multi_db_query(query,relevant_tables):
    responses = []
    # relevant = get_relevant_tables(query, top_k=5)
    
    print(f"Found {len(relevant_tables)} relevant tables:")
    for match in relevant_tables:
        print(f"  - {match['metadata']['db']}.{match['metadata']['table']} (score: {match['score']:.3f})")
    
    for db_name in DB_URLS:
        # Check if this database has any relevant tables
        db_relevant_tables = [
            match for match in relevant_tables 
            if match['metadata']['db'] == db_name
        ]
        
        if not db_relevant_tables:
            print(f"Skipping {db_name}: no relevant tables found")
            continue
            
        print(f"Processing {db_name} with {len(db_relevant_tables)} relevant tables...")
        
        try:
            executor = get_executor(db_name, relevant_tables    )
            result = executor.invoke({"input": query})
            
            output = result.get("output", result)
            responses.append((db_name, output))
            print(f"✓ {db_name}: {output}")
        except Exception as e:
            error_msg = f"Error: {str(e)}"
            responses.append((db_name, error_msg))
            print(f"✗ {db_name}: {error_msg}")
    
    return responses

# if __name__ == "__main__":
#     query = "what is the price of the product with id 1"
#     responses = run_multi_db_query(query)
#     print(responses)








