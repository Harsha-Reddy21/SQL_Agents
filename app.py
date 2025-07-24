import streamlit as st
from multi_db_executor import get_relevant_tables, run_multi_db_query

st.set_page_config(page_title="Quick Commerce SQL Agent", page_icon="🛒")

st.title("🛒 Quick Commerce SQL Agent")
user_query = st.text_input("Ask a natural language query (e.g. Cheapest onions in Blinkit):")

if user_query:
    with st.spinner("🔍 Finding relevant tables..."):
        tables = get_relevant_tables(user_query)
        st.write("📦 Relevant Tables Used:", tables)

        with st.spinner("🧠 Thinking..."):
            try:
                answer = run_multi_db_query(user_query,tables)
                st.success("✅ Answer:")
                st.write(answer)
            except Exception as e:
                st.error(f"❌ Error: {e}")