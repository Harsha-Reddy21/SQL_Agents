import streamlit as st
from multi_db_executor import get_relevant_tables, run_multi_db_query
from groq import Groq
import os
from dotenv import load_dotenv

load_dotenv()
groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def analyze_with_groq(query: str, responses: list) -> str:
    """Analyze multi-DB responses using Groq"""
    try:
        response_text = f"Query: {query}\n\nResults:\n"
        for db_name, output in responses:
            response_text += f"\n--- {db_name.upper()} ---\n{output}\n"

        prompt = f"""
        Analyze this quick commerce data query: "{query}"
        
        Database Results:
        {response_text}
        
        Provide:
        1. Clear summary of findings
        2. Key insights and comparisons 
        3. Actionable recommendations
        4. Most relevant information
        
        Keep it concise and user-friendly.
        The output should be in markdown format within 3 lines.
        """
        
        completion = groq_client.chat.completions.create(
            messages=[{"role": "user", "content": prompt}],
            model="llama-3.1-8b-instant",
            temperature=0.1
        )
        return completion.choices[0].message.content
        
    except Exception as e:
        return f"Analysis failed: {e}\n\nRaw Results:\n{responses}"

# Streamlit UI
st.set_page_config(page_title="Quick Commerce SQL Agent", page_icon="🛒")
st.title("🛒 Quick Commerce SQL Agent")

query = st.text_input("Ask about products across platforms:", 
                     placeholder="e.g., Cheapest onions available")

if query:
    with st.spinner("🔍 Finding relevant data..."):
        tables = get_relevant_tables(query)
        st.info(f"📊 Using {len(tables)} relevant tables")
        st.write(tables)
        
        raw_responses = run_multi_db_query(query, tables)
        analysis = analyze_with_groq(query, raw_responses)
        
        st.success("✅ Analysis Complete")
        st.write(analysis)
        
        # Raw results in expander
        with st.expander("🔍 View Raw Database Results"):
            for db_name, output in raw_responses:
                st.subheader(f"{db_name.replace('_', ' ').title()}")
                st.text(output)