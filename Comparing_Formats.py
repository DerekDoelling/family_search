#%% Imports and functions
import streamlit as st
import polars as pl
import pandas as pd
import altair as alt
import re
from collections import Counter
import numpy as np
import matplotlib.pyplot as plt
import plotly.express as px
#from marc_bibliography_mapping import marc_field_mapping_bibliographic_flat

if "df" not in st.session_state:
    st.session_state["df"] = None

def remove_non_special_chars(series: pl.Series) -> pl.Series:
    # Define the regex pattern to keep only the specified special characters
    pattern = r"[^@_!#$%^&*()<>?/\|}{~:.]"
    
    # Apply the regex pattern to the series, replacing everything except special characters
    return series.str.replace_all(pattern, "")

def remove_digits(series: pl.Series) -> pl.Series:
    # Define the regex pattern for numbers (digits 0-9)
    pattern = r"\d"
    
    # Apply the regex pattern to the series, replacing numbers with an empty string
    return series.str.replace_all(pattern, "")

def drop_columns_that_are_all_null(_df: pl.DataFrame) -> pl.DataFrame:
    return _df[[s.name for s in _df if not (s.null_count() == _df.height)]]

def process_and_combine_files(file_names: list) -> pl.DataFrame:

    # Read and cast all uploaded files to String type
    dataframes = [pl.read_csv(file_name).cast(pl.String) for file_name in file_names]
    
    # Combine all DataFrames vertically
    combined = pl.concat(dataframes, how="vertical")
    
    # Cast '001' column to Int64 type
    combined = combined.with_columns(pl.col("001.1.").cast(pl.Int64))
    
    # Rename columns using the mapping
    #combined = combined.rename({tag: marc_field_mapping_bibliographic_flat.get(tag, tag) for tag in combined.columns})
    
    # Drop columns that are all null
    combined_new = drop_columns_that_are_all_null(combined)

    return combined_new

# def validate_column_format(column_data):
#     """
#     Validate whether the column matches the expected format.
#     Criteria:
#     - Missing values allowed.
#     - Patterns such as 'YYYY', 'YYYY-YYYY', or 'Other' formats only.

#     Returns:
#     - True if valid, False otherwise.
#     """

#     pattern_valid = column_data.drop_nulls().map_elements(
#     lambda x: bool(re.match(r'^\d{4}$|^\d{4}-\d{4}$', str(x)))
#     ).all()

#     # pattern_valid = column_data.drop_nulls().map_elements(
#     #     lambda x: bool(re.match(r'^\d{4}$|^\d{4}-\d{4}$', x))
#     # ).all()
#     return pattern_valid

# def categorize_date_pattern(date: str) -> str:
#     if not date or date == "":
#         return "Missing"
#     elif re.match(r'^\d{4}-\d{4}$', date):
#         return "Date Range"
#     elif re.match(r'^\d{4}$', date):
#         return "Single Year"
#     else:
#         return "Other"

def identify_format(date_value):
    for format_type, pattern in date_pattern_types.items():
        if re.match(pattern, date_value):
            return format_type
    return "Unknown"

def count_special_characters(series: pl.Series) -> pl.DataFrame:
    char_counts = Counter()
    special_char_pattern = re.compile(r"[@_!#$%^&*()<>?/\|}{~:]+")
    
    for entry in series.drop_nulls():
        matches = special_char_pattern.findall(entry)
        char_counts.update(matches)
    
    char_df = (
        pl.DataFrame(list(char_counts.items()), schema=["Character Sequence", "Count"])
        .with_columns((pl.col("Count") / pl.col("Count").sum() * 100).round(2).alias("Percentage"))
    )
    return char_df

# @st.cache_data
# def convert_df(_df):
#     # IMPORTANT: Cache the conversion to prevent computation on every rerun
#     return df.write_csv().encode("utf-8")

@st.cache_data
def convert_df(_df: pl.DataFrame) -> bytes:
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return _df.write_csv().encode("utf-8")


################################## End of Imports and Function Declarations ##################################

st.set_page_config(
    page_title="Family History Library - Metadata Cleanup",
    page_icon="assets/Family Search Logo.png",
    layout="wide",
    initial_sidebar_state="expanded"
    )

tab1, tab2 = st.tabs(["Comparing Formats", "Comparing Dates"])

uploaded_file = st.file_uploader(
    "Upload your MARC records file",
    type=["xlsx", "csv"],
    accept_multiple_files=False,
    key="file_uploader"
)

if uploaded_file:
    # Load data into Polars DataFrame
    try:
        raw = pl.read_excel(uploaded_file)
    except Exception as e:
        st.error(f"Error reading file: {e}")
        st.stop()

    raw = pl.read_excel(uploaded_file)
    
    # Rename columns using your mapping logic
    #df = raw.rename({tag: marc_field_mapping_bibliographic_flat.get(tag, tag) for tag in raw.columns})
    raw_cleaned = drop_columns_that_are_all_null(raw)
    df = raw_cleaned.rename({col: col.strip() for col in raw_cleaned.columns}) # Remove white space in the column names
    st.session_state["df"] = df # ensures that the uploaded file's DataFrame persists without needing to re-upload after each interaction


if "df" in st.session_state and st.session_state["df"] is not None:
    df = st.session_state["df"]

    with tab1:
        st.title("Identify Formatting Patterns")

        st.markdown("""### Instructions
        This page helps identify relationships between categorical columns and continuous columns.
        1. Upload your MARC data file.
        2. Use the sidebar to configure x/y axes and formatting options.
        3. Generate a heatmap to explore relationships.
        """)

        with st.sidebar:
            # Define possible x-axis columns
            possible_x = df.columns  # Ensure it's a list for easier indexing
            
            # Set the default x-axis value
            default_x_value = "LDR.1"  # Replace with your desired default column name
            default_x_index = possible_x.index(default_x_value) if default_x_value in possible_x else 0
            
            # Create the selectbox with the default x-axis value
            selected_x = st.selectbox(
                "Select an x-axis:", 
                possible_x, 
                index=default_x_index
            )
            
            # Define possible y-axis columns, excluding the selected x-axis column
            possible_y = [col for col in possible_x if col != selected_x]
            
            # Set the default y-axis value
            default_y_value = "001.1."  # Replace with your desired default column name
            default_y_index = possible_y.index(default_y_value) if default_y_value in possible_y else 0  # Fallback to the first item
            
            # Create the selectbox with the default y-axis value
            selected_y = st.selectbox(
                "Select a y-axis:", 
                possible_y, 
                index=default_y_index
            )


            y_option = st.radio(
                "Choose a transformation:",
                options=["Remove Non-special Characters", "Remove Digits"],
                key="y_action"
            )
            mapping = {"": np.nan, None: np.nan}
            if y_option == "Remove Non-special Characters":
                df_y = remove_non_special_chars(df[selected_y]).alias(selected_y)
                df_y = df_y.replace(mapping)
                df_x = remove_non_special_chars(df[selected_x]).alias(selected_x)
                df_x = df_x.replace(mapping)
                df_x_y = pl.DataFrame([df_x, df_y])
            else:
                df_y = remove_digits(df[selected_y]).alias(selected_y)
                df_y = df_y.replace(mapping)
                df_x = remove_digits(df[selected_x]).alias(selected_x)
                df_x = df_x.replace(mapping)
                df_x_y = pl.DataFrame([df_x, df_y])

        # df_transformed = (
        #     df_x_y
        #     .group_by(
        #         [selected_x,
        #         selected_y,]
        #     )
        #     .agg(pl.len().alias("count"))  # Ensure to name the count column
        #     .pivot(
        #         on=selected_y, 
        #         index=selected_x,
        #         values='count'
        #     )
        # ).to_pandas()

        # df_plot = df_transformed.melt(id_vars=selected_x, var_name="Format", value_name="Count")

        df_transformed = (
        df_x_y
        .group_by([selected_x, selected_y])
        .agg(pl.len().alias("Count"))
        .pivot(selected_x, index=selected_y, values='Count', aggregate_function="sum")).fill_null(0).to_pandas()

        df_plot = df_transformed.set_index(selected_y)

        # heatmap = alt.Chart(df_plot).mark_rect().encode(
        #     x=alt.X(f'{selected_x}:O', axis=alt.Axis(labelAngle=-60)),
        #     y=alt.Y('Format:O', axis=alt.Axis(title=f"{selected_y.split('-')[0].strip()} Formats")),
        #     color='Count:Q'
        #     #tooltip=[selected_y, selected_x, 'Count']  # Tooltip with Student, Subject, and Score
        # ).properties(
        #     title=f"{selected_x} VS {selected_y} Heatmap"
        # ).configure_view(
        #     strokeWidth=0  # Removes border around the plot
        # )

        # # Displaying the Altair heatmap in Streamlit
        # st.altair_chart(heatmap, use_container_width=True)

        heatmap = px.imshow(df_plot, 
                labels={'x': selected_x,
                        'y': selected_y,
                        'color': 'Count'},
                title=f"Heatmap Between X Column: {selected_x} & Y Column: {selected_y}",
                color_continuous_scale="blues")

        heatmap.update_layout(
            xaxis=dict(
                title=f"{selected_x} Format",
                tickfont=dict(size=14, color="red", weight="bold"),
                ticks=""
            ),
            yaxis=dict(
                title=f"{selected_y} Format",
                tickfont=dict(size=14, color="red", weight="bold"),
                ticks=""
            )
        )

        heatmap.update_traces(
            hoverlabel=dict(
                font=dict(size=17)
            )
        )

        # Displaying the Plotly heatmap in Streamlit
        st.plotly_chart(heatmap, use_container_width=True)
        
    #     csv = convert_df(df_plot)

    #     st.download_button(   
    #     label="Download heatmap data as CSV",
    #     data=csv,
    #     file_name="heatmap_data.csv",
    #     mime="text/csv",
    # )

    with tab2:
        st.markdown("""### Instructions
        This page analyzes date patterns in your data.
        1. Select a column to analyze.
        2. View a distribution of date formats (e.g., 'YYYY', 'YYYY-YYYY').
        """)

        if uploaded_file:
            st.title("Analyze Date Patterns")

            date_columns_names = [
                "005.1.", "100.1.d", "110.1.d", "245.1.f", "245.1.g", "260.1.c", 
                "264.1.c", "600.1.d", "610.1.9", "700.1.d", "362.1.a", "610.1.d", 
                "046.1.a", "046.1.b", "046.1.j", "240.1.d", "240.1.f", "362.1.b"
            ]
            existing_columns = [col for col in date_columns_names if col in df.columns]

            df_date = df.select(existing_columns)

            # Allow user to select a column for analysis
            selected = st.selectbox("Select a column for analysis:", df_date.columns)
            if selected:
                st.write(f"Analyzing column: **{selected}**") 
                selected_column = selected
                date_pattern_types = {
                    "YYYYMMDDHHMMSS.MS": r"^\d{4}\d{2}\d{2}\d{2}\d{2}\d{2}\.\d+$",
                    "YYYY-": r"^\d{4}-$",
                    "YY": r"^\d{2}$",
                    "YYYY-YYYY": r"^\d{4}-\d{4}$",
                    "YYYY": r"^\d{4}$",
                    "YYYYMMDD": r"^\d{8}$",
                    "YYYY/MM/DD": r"^\d{4}/\d{2}/\d{2}$",
                    "YYYY-MM-DD": r"^\d{4}-\d{2}-\d{2}$",
                    "YYYY-MM":r"^\d{4}-\d{2}$",
                    "YYYY/MM": r"^\d{4}/\d{2}$",
                    "YYYYMM": r"^\d{6}$",
                    "letter. YYYY": r"^a\.\s\d{4}$",
                    "letter. YYYY-": r"^a\.\s\d{4}-$",
                    "letter. YYYY-YYYY": r"^a\.\s\d{4}-\d{4}$",
                    "letter. YYYYMMDD": r"^a\.\s\d{8}$",
                    "letter. YYYYMM": r"^a\.\s\d{6}$",
                    "letter. YYYY-MM-DD": r"^a\.\s\d{4}-\d{2}-\d{2}$",
                    "letter. YYYY/MM/DD": r"^a\.\s\d{4}/\d{2}/\d{2}$",
                    "MMDDYYYY": r"^\d{8}$",
                    "MM/DD/YYYY": r"^\d{2}/\d{2}/\d{4}$",
                }

                # Function to match patterns and replace with format type
                def identify_format(date_value):
                    if date_value == "":  # Check for blank values
                        return "Empty"
                    for format_type, pattern in date_pattern_types.items():
                        if re.match(pattern, date_value):
                            return format_type
                    return "Other"  # If no match is found

               
                df_date_format = df_date.select(
                    pl.col(selected_column).map_elements(identify_format).alias("format"))

               
                df_date_format = df_date_format.select(pl.col("format").value_counts())
                df_date_formats = df_date_format.unnest("format").rename({"format": "Format", "count": "Count"})
                total_date_patterns = df_date_formats.select(pl.sum("Count")).item()

                df_date_formats = df_date_formats.with_columns(
                    pl.col("Format").fill_null("Empty").alias("Format")
                )

                date_pattern_df = df_date_formats.with_columns(
                    (pl.col("Count") / total_date_patterns * 100).round(2).alias("Percentage")
                )

                date_pattern_df = date_pattern_df.sort("Percentage", descending=False)

                date_bar = px.bar(date_pattern_df, x="Percentage", y="Format", title=f"Distribution of {total_date_patterns:,d} Date Patterns for {selected}", orientation="h")

                date_bar.update_layout(
                    plot_bgcolor='rgba(0,0,0,0)'
                )

                date_bar.update_layout(
                    yaxis=dict(
                        tickfont=dict(size=14, color="red", weight="bold"),  
                        ticks=""  
                    )
                )
                date_bar.update_traces(
                    hoverlabel=dict(
                        font=dict(size=17)
                    )
                )

                date_bar.update_traces(
                    hovertemplate="Percentage: %{x}%<br>Format: %{y}<br>Count: %{customdata:,d}",  
                    customdata=date_pattern_df[['Count']]
                )

                st.plotly_chart(date_bar, use_container_width=True)


                # # Special Character Analysis
                # st.header("Step 3: Special Character Analysis")
                # if st.checkbox("Analyze Special Characters"):
                #     df_cleaned = df.with_columns(remove_non_special_chars(pl.col(selected)).alias("Cleaned"))
                #     char_df = count_special_characters(df_cleaned.get_column("Cleaned"))

                #     st.subheader("Special Character Analysis")
                #     if not char_df.is_empty():
                #         st.dataframe(char_df.to_pandas())
                #     else:
                #         st.warning("No special characters found in the selected column.")

                # # Download Updated Data
                # st.header("Step 4: Download Updated Data")
                # new_csv = df.to_csv()
                # st.download_button(
                #     label="Download Updated Dataset",
                #     data=new_csv,
                #     file_name="updated_data.csv",
                #     mime="text/csv"
                # )

                # # Special Character Analysis
                # st.header("Step 3: Special Character Analysis")
                # if st.checkbox("Analyze Special Characters"):
                #     df_cleaned = df.with_columns(remove_non_special_chars(pl.col(selected_column)).alias("Cleaned"))
                #     char_df = count_special_characters(df_cleaned.get_column("Cleaned"))
                #     char_df = char_df.to_pandas()
                    
                #     st.subheader("Special Character Analysis")
                #     st.dataframe(char_df.to_pandas())

                # # Download Updated Data
                # st.header("Step 4: Download Updated Data")
                # new_csv = char_df.to_csv()
                # st.download_button(
                #     label="Download Updated Dataset",
                #     data=new_csv,
                #     file_name="updated_data.csv",
                #     mime="text/csv"
                # )

            else:
                st.error(f"The selected column '{selected}' does not match the expected format. Please select a column with patterns like 'YYYY' or 'YYYY-YYYY'.")















# with tab2:
#         st.markdown("""### Instructions
#         This page analyzes date patterns in your data.
#         1. Select a column to analyze.
#         2. View a distribution of date formats (e.g., 'YYYY', 'YYYY-YYYY').
#         """)

#         if uploaded_file:
#             st.title("Analyze Date Patterns")

#             # date_columns_names = ["005.1.", "100.1.d", "110.1.d", "245.1.f", "245.1.g", "260.1.c", "264.1.c", "600.1.d", "610.1.9", "700.1.d", "362.1.a", "610.1.d", "046.1.a", "046.1.b", "046.1.j", "240.1.d", "240.1.f", "362.1.b"]
#             # existing_columns = [col for col in date_columns_names if col in df.columns]

#             # # df_date = df.select([col for col in df.columns if "date" in col.lower()])
#             # # date_columns = [col for col in df.columns if "date" in col.lower()]
#             # # if not date_columns:
#             # #     st.error("No date-related columns found.")
#             # #     st.stop()

#             # if not existing_columns:
#             #     st.error("No date-related columns found.")
#             #     st.stop()

#             # df_date = df.select(existing_columns)


#             # # Allow user to select a column for analysis
#             # selected_column = st.selectbox("Select a column for analysis:", df_date.columns)

#             # if selected_column:
#             #     st.write(f"Analyzing column: **{selected_column}**")
                
#             #     if validate_column_format(df[selected_column]):
#             #         st.header("Step 2: Categorize Date Patterns")

#             #         df = df.with_columns(
#             #             pl.col(selected_column).apply(categorize_date_pattern).alias("date_pattern")
#             #         )
                    
#             #         date_pattern_counts = df.select("date_pattern").group_by("date_pattern").agg(pl.count()).sort("date_pattern")
#             #         total_date_patterns = date_pattern_counts.select(pl.col("count").sum()).item()
                    
#             #         date_pattern_df = date_pattern_counts.with_columns(
#             #             (pl.col("count") / total_date_patterns * 100).round(2).alias("Percentage")
#             #         )

#             #         st.subheader("Date Pattern Distribution")
#             #         st.dataframe(date_pattern_df.to_pandas())

#             #         # Visualization
#             #         fig, ax = plt.subplots(figsize=(10, 5))
#             #         ax.bar(date_pattern_df["date_pattern"], date_pattern_df["count"], color=['blue', 'green', 'orange', 'red'])
#             #         ax.set_xlabel("Date Pattern Category")
#             #         ax.set_ylabel("Count of Records")
#             #         ax.set_title(f"Distribution of Date Patterns in '{selected_column}'")
#             #         st.pyplot(fig)

#             date_columns_names = ["005.1.", "100.1.d", "110.1.d", "245.1.f", "245.1.g", "260.1.c", "264.1.c", "600.1.d", "610.1.9", "700.1.d", "362.1.a", "610.1.d", "046.1.a", "046.1.b", "046.1.j", "240.1.d", "240.1.f", "362.1.b"]
#             existing_columns = [col for col in date_columns_names if col in df.columns]

#                         # df_date = df.select([col for col in df.columns if "date" in col.lower()])
#                         # date_columns = [col for col in df.columns if "date" in col.lower()]
#                         # if not date_columns:
#                         #     st.error("No date-related columns found.")
#                         #     st.stop()

#             df_date = df.select(existing_columns)

#             #             # Allow user to select a column for analysis
#             selected = st.selectbox("Select a column for analysis:", df_date.columns)
#             if selected:
#                 st.write(f"Analyzing column: **{selected}**") 
#                 selected_column = df_date.get_column(selected)
#                 date_pattern_types = {
#                     "YYYYMMDDHHMMSS.MS": r"^\d{4}\d{2}\d{2}\d{2}\d{2}\d{2}\.\d+$",
#                     "YYYY-": r"^\d{4}-$",
#                     "YY": r"^\d{2}$",
#                     "YYYY-YYYY": r"^\d{4}-\d{4}$",
#                     "YYYY": r"^\d{4}$",
#                     "YYYYMMDD": r"^\d{8}$",
#                     "YYYY/MM/DD": r"^\d{4}/\d{2}/\d{2}$",
#                     "YYYY-MM-DD": r"^\d{4}-\d{2}-\d{2}$",
#                     "YYYY-MM":r"^\d{4}-\d{2}$",
#                     "YYYY/MM": r"^\d{4}/\d{2}$",
#                     "YYYYMM": r"^\d{6}$",
#                     "letter. YYYY": r"^a\.\s\d{4}$",
#                     "letter. YYYY-": r"^a\.\s\d{4}-$",
#                     "letter. YYYY-YYYY": r"^a\.\s\d{4}-\d{4}$",
#                     "letter. YYYYMMDD": r"^a\.\s\d{8}$",
#                     "letter. YYYYMM": r"^a\.\s\d{6}$",
#                     "letter. YYYY-MM-DD": r"^a\.\s\d{4}-\d{2}-\d{2}$",
#                     "letter. YYYY/MM/DD": r"^a\.\s\d{4}/\d{2}/\d{2}$",
#                     "MMDDYYYY": r"^\d{8}$",
#                     "MM/DD/YYYY": r"^\d{2}/\d{2}/\d{4}$",
#                 }

#                 # Function to match patterns and replace with format type
#                 def identify_format(date_value):
#                     if date_value == "":  # Check for blank values
#                         return "Empty"
#                     for format_type, pattern in date_pattern_types.items():
#                         if re.match(pattern, date_value):
#                             return format_type
#                     return "Other"  # If no match is found

#                 # Apply the function to the column
#                 df_date_format = df_date.with_columns(
#                     pl.col(selected).map_elements(identify_format).alias("format")
#                 )

#                 # Count occurrences of each format type, including NaNs
#                 df_date_format = df_date_format.select(pl.col("format").value_counts())
#                 df_date_formats = df_date_format.unnest("format").rename({"format": "Format", "count": "Count"})
#                 total_date_patterns = df_date_formats.select(pl.sum("Count")).item()
#                 # (name="count")).rename({"format": "Format", "count": "Count"})

#                 # df_date_formats
#                 df_date_formats = df_date_formats.with_columns(
#                     pl.col("Format").fill_null("Empty").alias("Format")
#                 )

#                 date_pattern_df = df_date_formats.with_columns(
#                     (pl.col("Count") / total_date_patterns * 100).round(2).alias("Percentage")
#                                     )

#                 date_pattern_df = date_pattern_df.sort("Percentage", descending=False)

#                 date_bar = px.bar(date_pattern_df, x="Percentage", y="Format", title=f"Distribution of {total_date_patterns:,d} Date Patterns for {selected}", orientation="h")

#                 date_bar.update_layout(
#                     plot_bgcolor='rgba(0,0,0,0)'
#                 )

#                 date_bar.update_layout(
#                     yaxis=dict(
#                         tickfont=dict(size=14, color="red", weight="bold"),  # Y-axis font
#                         ticks=""  # Correct placement: outside tickfont
#                     )
#                 )
#                 date_bar.update_traces(
#                     hoverlabel=dict(
#                         font=dict(size=17)
#                     )
#                 )

#                 date_bar.update_traces(
#                     hovertemplate="Percentage: %{x}%<br>Format: %{y}<br>Count: %{customdata:,d}",  # Corrected this line
#                     customdata=date_pattern_df[['Count']]
#                 )

#                 st.plotly_chart(date_bar, use_container_width=True)

#                 # Special Character Analysis
#                 st.header("Step 3: Special Character Analysis")
#                 if st.checkbox("Analyze Special Characters"):
#                     df_cleaned = df.with_columns(remove_non_special_chars(pl.col(selected_column)).alias("Cleaned"))
#                     char_df = count_special_characters(df_cleaned.get_column("Cleaned"))
#                             # char_df = count_special_characters(df_cleaned["Cleaned"])

#                     st.subheader("Special Character Analysis")
#                     st.dataframe(char_df.to_pandas())

#                         # Download Updated Data
#                 st.header("Step 4: Download Updated Data")
#                     new_csv = df.to_csv()
#                     st.download_button(
#                         label="Download Updated Dataset",
#                         data=new_csv,
#                         file_name="updated_data.csv",
#                         mime="text/csv"
#                     )
                    
#                 else:
#                     st.error(f"The selected column '{selected_column}' does not match the expected format. Please select a column with patterns like 'YYYY' or 'YYYY-YYYY'.")
