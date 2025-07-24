import streamlit as st
from multi_db_executor import get_relevant_tables, run_multi_db_query
from groq import Groq
import os
from dotenv import load_dotenv

load_dotenv()

# Initialize Groq client
groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))

def analyze_response_with_groq(user_query, multi_db_response):
    """
    Analyze the multi-database response using Groq API to provide insights and summary
    """
    try:
        # Format the response data for analysis
        response_text = f"User Query: {user_query}\n\nDatabase Responses:\n"
        
        for db_name, output in multi_db_response:
            response_text += f"\n--- {db_name.upper()} ---\n{output}\n"
        
        # Create analysis prompt
        analysis_prompt = f"""
        You are an expert data analyst for quick commerce platforms. 
        
        A user asked: "{user_query}"
        
        Here are the responses from multiple quick commerce databases:
        {response_text}
        
        Please provide:
        1. A clear, concise summary of the findings
        2. Key insights and comparisons across platforms
        3. Actionable recommendations if applicable
        4. Highlight the most relevant information for the user's query
        
        Make your response user-friendly and focus on the most important insights.
        """
        
        # Call Groq API
        chat_completion = groq_client.chat.completions.create(
            messages=[
                {
                    "role": "user",
                    "content": analysis_prompt
                }
            ],
            model="llama-3.1-8b-instant",
            temperature=0.1
        )
        
        return chat_completion.choices[0].message.content
        
    except Exception as e:
        return f"Analysis failed: {str(e)}\n\nRaw Results:\n{multi_db_response}"

st.set_page_config(page_title="Quick Commerce SQL Agent", page_icon="🛒")

st.title("🛒 Quick Commerce SQL Agent")
user_query = st.text_input("Ask a natural language query (e.g. Cheapest onions in Blinkit):")

if user_query:
    with st.spinner("🔍 Finding relevant tables..."):
        tables = get_relevant_tables(user_query)
        st.write("📦 Relevant Tables Used:", tables)

        with st.spinner("🧠 Thinking..."):
            try:
                raw_response = run_multi_db_query(user_query, tables)
                
                with st.spinner("🔍 Analyzing results..."):
                    analyzed_response = analyze_response_with_groq(user_query, raw_response)
                
                st.success("✅ Analysis:")
                st.write(analyzed_response)
                
                # Show raw results in an expander for reference
                with st.expander("📊 View Raw Database Results"):
                    for db_name, output in raw_response:
                        st.subheader(f"{db_name.title()}")
                        st.write(output)
                        
            except Exception as e:
                st.error(f"❌ Error: {e}")