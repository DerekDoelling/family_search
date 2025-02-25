import streamlit as st
import pandas as pd
from mitosheet.streamlit.v1 import spreadsheet

st.set_page_config(
    page_title="Family History Library - Metadata Cleanup",
    page_icon="assets/Family Search Logo.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Format Analysis")

st.markdown("Upload your file and leverage Mito Spreadsheet to transform your data seamlessly. Once you're done, you can easily download the generated Python code as a .py file.")

uploaded_file = st.file_uploader(
    "Upload your MARC records file",
    type=["xlsx", "csv"],
    accept_multiple_files=False,
    key="file_uploader"
)

if uploaded_file:
    file_name = uploaded_file.name.lower()

    try:
        if file_name.endswith(".csv"):
            df = pd.read_csv(uploaded_file, encoding="utf-8")  
        elif file_name.endswith(".xlsx"):
            df = pd.read_excel(uploaded_file)
        else:
            st.error("Unsupported file format!")
            st.stop()
    # Ensures pandas can read the csv file through different encodings        
    except UnicodeDecodeError:
        try:
            df = pd.read_csv(uploaded_file, encoding="ISO-8859-1")  
        except UnicodeDecodeError:
            df = pd.read_csv(uploaded_file, encoding="windows-1252")  


    # Ensure the DataFrame is in the correct format
    df = df.copy()  # Ensure it's not a view
    df.dropna(axis=1, how='all', inplace=True) # Remove columns containing all NAs
    df.dropna(axis=0, how='all', inplace=True) # Remove rows containing all NAs
    df.columns = df.columns.astype(str)

    # Store DataFrame in session state so it persists
    st.session_state["df"] = df  

# Ensure df is in session state
if "df" in st.session_state and st.session_state["df"] is not None:
    df = st.session_state["df"]

    # Pass DataFrame to Mito
    new_df, code = spreadsheet(df)  

    # Store new_df back in session state so changes persist
    st.session_state["df"] = new_df

    # Display modified DataFrame and generated code
    st.write(new_df)
    st.code(code)

    # Provide a download button for the generated code
    if code:
        st.download_button(
            label="Download Python Script",
            data=code,
            file_name="mito_spreadsheet_code.py",
            mime="text/plain"
        )
