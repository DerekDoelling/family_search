# %%
import streamlit as st
import polars as pl
from lets_plot import *
LetsPlot.setup_html()
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sys

from marc_bibliography_mapping import marc_field_mapping_bibliographic_flat

def drop_columns_that_are_all_null(_df: pl.DataFrame) -> pl.DataFrame:
    return _df[[s.name for s in _df if not (s.null_count() == _df.height)]]

# Sets initial page configuration settings
st.set_page_config(
    page_title="Family History Library - Metadata Cleanup",
    page_icon="assets/Family Search Logo.png",
    layout="wide",
    initial_sidebar_state="expanded"
    )

# Create file uploader
uploaded_file = st.file_uploader("Upload your MARC records file", type=["csv", "xlsx"], accept_multiple_files=False, key="heatmap")

if uploaded_file is not None:
    raw = pl.read_excel(uploaded_file)

    df = raw.rename({tag: marc_field_mapping_bibliographic_flat.get(tag, tag) for tag in raw.columns})
    df = drop_columns_that_are_all_null(df)

    # Step 3: Filter columns with specific prefixes
    prefixes = [
        '000', '001', '008', '245$6', '245$a', '245$b', '245$f', '245$n', '245$p',
        '260$a', '260$b', '260$c',
        '264$a', '264$b', '264$c', '773$w'
    ]
    df_filtered = df_cleaned[[col for col in df_cleaned.columns if any(col.startswith(prefix) for prefix in prefixes)]]

    # Step 4: Split '008-Fixed-Length Data Elements-General Information' into separate columns
    if '008-Fixed-Length Data Elements-General Information' in df_filtered.columns:
        df_split = df_filtered['008-Fixed-Length Data Elements-General Information'].apply(
            lambda x: pd.Series({
                'Record Creation Date': x[:6] if len(x) >= 6 else np.nan,
                'Publication Status': x[6] if len(x) >= 7 else np.nan,
                'Date 1': x[7:11] if len(x) >= 11 else np.nan,
                'Date 2': x[11:15] if len(x) >= 15 else np.nan,
                'Place of Publication': x[15:18] if len(x) >= 18 else np.nan,
                'Language': x[35:38] if len(x) >= 38 else np.nan,
                'Modified Record': x[38] if len(x) >= 39 else np.nan
            })
        )
        df_combined = pd.concat([df_filtered, df_split], axis=1)
    else:
        df_combined = df_filtered

    # Step 5: Add 'Bibliography' column from '000-Leader'
    if '000-Leader' in df_combined.columns:
        df_combined['Bibliography'] = df_combined['000-Leader'].str[6]  # 7th character is at index 6
        df_combined['6th'] = df_combined['000-Leader'].str[5]  # 6th character (index starts from 0)
        df_combined['8th'] = df_combined['000-Leader'].str[7]  # 8th character
    else:
        st.error(st.error("'000-Leader' column is missing. 'Bibliography' column cannot be created."))

    st.write("df_combined['Bibliography']")
    # Step 6: Filter rows based on 'Bibliography' values
    exclude_chars = ['a', 'm', 's', 'b']
    filtered_df = df_combined[~df_combined['Bibliography'].isin(exclude_chars)]

    # Step 7: Select specific columns and add new columns from '000-Leader'
    result_df = filtered_df[['000-Leader', 'Bibliography', '6th', '8th']].copy()

    # Step 11.3: Count distinct values for 'Publication Status' and 'Language'
    st.header("Step 11.3: Count Distinct Values for 'Publication Status' and 'Language'")
    value_counts_publication_status = df_combined['Publication Status'].value_counts()
    st.write("Count of each distinct value in the 'Publication Status' column:")
    st.table(value_counts_publication_status.reset_index().head(10).rename(columns={'index': 'Publication Status', 'Publication Status': 'Count'}))

    value_counts_language = df_combined['Language'].value_counts()
    st.write("Count of each distinct value in the 'Language' column:")
    st.table(value_counts_language.reset_index().head(10).rename(columns={'index': 'Language', 'Language': 'Count'}))

    # Step 12.5: Create a new column 'Parent Control Number' in df_combined to store the matched '001-Control Number' values
    if '773$w' in df_combined.columns and '001-Control Number' in df_combined.columns:
        values_in_773w = df_combined['773$w'].dropna()
        matching_rows = df_combined[df_combined['001-Control Number'].isin(values_in_773w)]
        df_combined['Parent Control Number'] = np.where(df_combined['773$w'].isin(matching_rows['001-Control Number']), df_combined['773$w'], np.nan)

    # Step 12.6: Child Records with Existing Parent Records
    st.header("Step 12.6: Child Records with Existing Parent Records")
    if 'Parent Control Number' in df_combined.columns:
        matched_df = df_combined[df_combined['Parent Control Number'].notna()][['000-Leader', '001-Control Number', '773$w', 'Parent Control Number', '245$a-Title']]
        st.write("This table shows child records that have existing parent records:")
        st.table(matched_df.head(10))

    # Step 12.8: Child Records without Existing Parent Records
    st.header("Step 12.8: Child Records without Existing Parent Records")
    if 'Parent Control Number' in df_combined.columns:
        unmatched_df = df_combined[df_combined['773$w'].isin(values_in_773w) & ~df_combined['773$w'].isin(df_combined['001-Control Number'])]
        unmatched_df_filtered = unmatched_df[['000-Leader', '001-Control Number', '773$w', 'Parent Control Number', '245$a-Title']]
        st.write("This table shows child records that do not have existing parent records in the data:")
        st.table(unmatched_df_filtered.head(10))

    # Step 13.2: Filter rows where '336$2' is not null
    st.header("Step 13.2: Filter Rows where '336$2' is Not Null")
    filtered_df_336 = df_cleaned[df_cleaned['336$2'].notna()]
    st.write("This table shows rows where '336$2' is not null:")
    st.dataframe(filtered_df_336.head(10))

    # Step 13.3: Display Distinct Values in Specified Columns
    st.header("Step 13.3: Distinct Values in Specified Columns")
    distinct_values = {}
    columns_to_check = ['336$2', '336$a', '336$b', '337$2', '337$a', '337$b', '338$2', '338$a', '338$b', '362$a-Start date of publication']

    for col in columns_to_check:
        if col in filtered_df_336.columns:
            distinct_values[col] = filtered_df_336[col].dropna().unique()

    distinct_values_df = pd.DataFrame.from_dict(distinct_values, orient='index').transpose()
    st.write("This table shows distinct values in the specified columns:")
    st.table(distinct_values_df.head(10))

    # Step 14: Final DataFrame with Specified Columns
    st.header("Step 14: Final DataFrame with Specified Columns")
    final_columns = [
        '000-Leader', '336$2', '336$a', '336$b', '337$2', '337$a', '337$b',
        '338$2', '338$a', '338$b', '362$a-Start date of publication',
        '337$a_count', '337$b_count', '338$2_count', '338$a_count', '338$b_count'
    ]

    if '001-Control Number' in filtered_df_336.columns:
        final_columns.insert(1, '001-Control Number')

    available_final_columns = [col for col in final_columns if col in filtered_df_336.columns]
    final_df = filtered_df_336[available_final_columns].copy()
    st.write("This table shows the final DataFrame with the specified columns:")
    st.table(final_df.head(10))

    # Step 13.5: Create new columns to count the occurrences of ';' in each of the specified columns
    st.header("Step 13.5: Count Occurrences of ';' in Specified Columns")
    columns_to_analyze = ['001-Control Number', '337$a', '337$b', '338$2', '338$a', '338$b']

    # Initialize count columns in filtered_df_336 for each analyzed column
    for col in columns_to_analyze:
        if col in filtered_df_336.columns:
            count_col_name = f"{col}_count"
            filtered_df_336[count_col_name] = filtered_df_336[col].apply(lambda x: str(x).count(';') if pd.notna(x) else 0)

    # Add 1 to each cell in the specified count columns
    count_columns = ['337$a_count', '337$b_count', '338$2_count', '338$a_count', '338$b_count']

    # Increment the values in each of the specified columns by 1
    for col in count_columns:
        if col in filtered_df_336.columns:
            filtered_df_336[col] += 1  # Add 1 to each cell in the count columns

    # Ensure that '001-Control Number' is included in filtered_df if it exists in df_cleaned
    if '001-Control Number' in df_cleaned.columns:
        filtered_df_336['001-Control Number'] = df_cleaned['001-Control Number']

    # Define columns to display, including '001-Control Number' and incremented count columns
    columns_to_display = ['001-Control Number'] + count_columns

    # Create a condition to filter rows where values are not equal across the count columns (excluding '001-Control Number')
    unequal_condition = filtered_df_336[count_columns].nunique(axis=1) > 1

    # Filter rows based on the condition and include '001-Control Number' in the result
    unequal_rows_df = filtered_df_336[unequal_condition]

    # Select only the columns that are available in the DataFrame
    available_columns = [col for col in columns_to_display if col in unequal_rows_df.columns]
    unequal_rows_df_filtered = unequal_rows_df[available_columns]

    # Display the resulting DataFrame with unequal count values, including '001-Control Number'
    st.write("This table shows rows with unequal count values across the specified columns:")
    st.table(unequal_rows_df_filtered.head(10))