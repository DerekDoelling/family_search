# %%
import streamlit as st
import polars as pl
from lets_plot import *
LetsPlot.setup_html()
from marc_bibliography_mapping import marc_field_mapping_bibliographic_flat
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

## Functions
def remove_non_special_chars(df: pl.DataFrame, column_names: list) -> pl.DataFrame:
    # Define the regex pattern to keep only the specified special characters
    pattern = r"[^@_!#$%^&*()<>?/\|}{~:]"
    
    # Loop through each column name in the provided list
    for column_name in column_names:
        # Apply the regex pattern to the column, replacing everything except special characters with an empty string
        df = df.with_columns(
            pl.col(column_name).str.replace_all(pattern, "").alias(column_name)
        )
    
    return df

def remove_numbers(df: pl.DataFrame, column_names:list) -> pl.DataFrame:
    # Define the regex pattern for numbers (digits 0-9)
    pattern = r"\d"
    
    # Apply the regex pattern to the column, replacing numbers with an empty string
    for column_name in column_names:

        df = df.with_columns(
            pl.col(column_name).str.replace_all(pattern, "").alias(column_name)
        )
    
    return df

def drop_columns_that_are_all_null(_df: pl.DataFrame) -> pl.DataFrame:
    return _df[[s.name for s in _df if not (s.null_count() == _df.height)]]

def process_and_combine_files(file_names: list) -> pl.DataFrame:
    from marc_bibliography_mapping import marc_field_mapping_bibliographic_flat

    # Read and cast all uploaded files to String type
    dataframes = [pl.read_csv(file_name).cast(pl.String) for file_name in file_names]
    
    # Combine all DataFrames vertically
    combined = pl.concat(dataframes, how="vertical")
    
    # Cast '001' column to Int64 type
    combined = combined.with_columns(pl.col("001").cast(pl.Int64))
    
    # Rename columns using the mapping
    combined = combined.rename({tag: marc_field_mapping_bibliographic_flat.get(tag, tag) for tag in combined.columns})
    
    # Function to drop columns that are entirely null
    def drop_columns_that_are_all_null(_df: pl.DataFrame) -> pl.DataFrame:
        return _df[[s.name for s in _df if not (s.null_count() == _df.height)]]
    
    # Drop columns that are all null
    combined_new = drop_columns_that_are_all_null(combined)

    return combined_new

@st.cache_data
def convert_df(_df):
    # IMPORTANT: Cache the conversion to prevent computation on every rerun
    return df.write_csv().encode("utf-8")
#%% Streamlit start

def split_language(df, col_name, delimiter):

    df[col_name] = df[col_name].str.strip()
    split_df = df[col_name].str.split(delimiter, expand=True)
    split_df.columns = [f"{col_name}_part{i+1}" for i in range(split_df.shape[1])]
    df = pd.concat([df, split_df], axis=1)
    
    return df 

# Language code to language name mapping
language_mapping = {
    'en': 'eng',  # English
    'de': 'ger',  # German
    'es': 'spa',  # Spanish
    'fr': 'fre',  # French
    'sv': 'swe',  # Swedish
    'da': 'dan',  # Danish
    'nl': 'dut',  # Dutch
    'no': 'nor',  # Norwegian
    'pt': 'por',  # Portuguese
    'it': 'ita',  # Italian
    'fi': 'fin',  # Finnish
    'cs': 'cze',  # Czech
    'gl': 'glg',  # Galician
    'hu': 'hun',  # Hungarian
    'la': 'lat',  # Latin
    'id': 'ind',  # Indonesian
    'pl': 'pol',  # Polish
    'ms': 'may',  # Malay
    'nn': 'nno',  # Norwegian (Nynorsk)
    'sk': 'slo',  # Slovak
    'is': 'ice',  # Icelandic
    'af': 'afr',  # Afrikaans
    'cy': 'wel',  # Welsh
    'vo': 'vol',  # Volapük
    'ca': 'cat',  # Catalan
    'ro': 'rum',  # Romanian
    'lt': 'lit',  # Lithuanian
    'nb': 'nob',  # Norwegian (Bokmål)
    'eu': 'baq',  # Basque
    'sw': 'swa',  # Swahili
    'hr': 'hrv',  # Croatian
    'fo': 'fao',  # Faroese
    'et': 'est',  # Estonian
    'sl': 'slv',  # Slovenian
    'mg': 'mlg',  # Malagasy
    'lv': 'lav',  # Latvian
    'ga': 'gle',  # Irish
    'tr': 'tur',  # Turkish
    'qu': 'que',  # Quechua
    'tl': 'tgl',  # Tagalog
    'jv': 'jav',  # Javanese
    'ja': 'jpn',  # Japanese
    'lb': 'ltz',  # Luxembourgish
    'eo': 'epo',  # Esperanto
    'xh': 'xho',  # Xhosa
    'rw': 'kin',  # Kinyarwanda
    'mt': 'mlt',  # Maltese
    'an': 'arg',  # Aragonese
    'ru': 'rus',  # Russian
    'hy': 'arm',  # Armenian
    'oc': 'oci',  # Occitan (post-1500)
    'bg': 'bul',  # Bulgarian
    'se': 'sme',  # Northern Sami
    'ht': 'hat',  # Haitian French Creole
    'wa': 'wln',  # Walloon
    'zh': 'chi',   # Chinese
    'sr': 'srp'   # Serbian
}

############################################################################################################################################################

# Sets initial page configuration settings
st.set_page_config(
    page_title="Family History Library - Metadata Cleanup",
    page_icon="assets/Family Search Logo.png",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Create file uploader
uploaded_file = st.file_uploader("Upload your MARC records file", type=["csv", "xlsx"], accept_multiple_files=False)

# %%
st.title('Title and Language Analysis')
st.markdown("""
To effectively utilize the bibliographic data from MARC 21, we aim to clean the dataset concerning titles and languages. This dataset includes fields such as: 

* 008 - Fixed-Length Data Elements - General Information
* 040$b - Language of Cataloging
* 041$a - Language Code of Text
* 546$a - Language Note
* 245\$a - Title
* 245\$b - Remainder of Title
* 245\$c - Statement of Responsibility
* 245\$f - Inclusive Dates
* 245\$n
* 45\$p - Name of Part/Section of Work

Currently, the information is dispersed across different columns, making it challenging to identify the correct data. The primary goal of this project is to organize the title and language columns to facilitate analysis by Family Search.

Upload data to get started!
""")

if uploaded_file is not None:
    # Creates dataframe for uploaded file
    raw = pl.read_excel(uploaded_file)

    # Renames all columns according to the MARC bibliographic standards
    df = raw.rename({tag: marc_field_mapping_bibliographic_flat.get(tag, tag) for tag in raw.columns})
    df = drop_columns_that_are_all_null(df)

    # Prints the head of the renamed df
    st.write(df.head())

    st.header('Language columns Clean')
    st.write("The '008 - Fixed-Length Data Elements - General Information' field provides language information in positions 35 to 37. When multiple languages are indicated in the '008' field, only the '041\$a - Language Code of Text' field is used to represent these languages. The combined '008' and '041' fields are used when multiple languages are present in '008,' as these languages are relevant for family search purposes.")

    # %%
    df1 = df[['008-Fixed-Length Data Elements-General Information','040$b-Language of cataloging', '041$a-Language code of text','546$a-Language note', '245$a-Title', '245$b-Remainder of title']]
    df1 = df1.to_pandas()
    # st.dataframe(df1.head())
    # %%
    df1['008-language'] = df1['008-Fixed-Length Data Elements-General Information'].str.slice(35, 38)
    df1['008+041'] = np.where(pd.isna(df1['041$a-Language code of text']), 
                                df1['008-language'],  
                                df1['041$a-Language code of text'])

    df1 = split_language(df1, '008+041', r';')
    # %%
    result1 = df1.groupby('008+041').size().reset_index(name='count').sort_values(by='count', ascending=False)

    st.write("Resulting DataFrame:")
    st.write ("The table shows that the top five languages used are English, French, German, Spanish, and Dutch.")
    st.dataframe(result1.head())

    # %%
    st.header('Title columns Clean')
    st.write("The 245\$a - Title and 245\$b - Remainder of Title fields display the title and subtitle. These fields were combined and then split by the delimiter '=' to create separate columns for each value, organizing the information effectively. The langid library was used to determine the language used in the title. This library use different way to detect the lanague with MARC21.")

    # %% combined 245a and 245b (title and subtitle)
    df1['245$ab'] = df1['245$a-Title'] + ' ' + df1['245$b-Remainder of title'].fillna('')
    # %% 
    def split_title(df, col_name, delimiter):
        df[col_name] = df[col_name].str.strip()
        
        def should_split(value):
            if '= :' in value:
                return False  
            return delimiter in value
        
        split_df = df[col_name].apply(lambda x: x.split(delimiter) if should_split(x) else [x])
        split_df = pd.DataFrame(split_df.tolist(), index=df.index)
        split_df.columns = [f"{col_name}_part{i+1}" for i in range(split_df.shape[1])]
        df = pd.concat([df, split_df], axis=1)
        
        return df

    df1 = split_title(df1, '245$ab', r'=')
    st.subheader("DataFrame with title split parts:")
    st.dataframe(df1.head())
    # %%
    import langid
    # %%
    # Function to detect the language
    def detect_language(text):
        try:
            lang, _ = langid.classify(text)
            return lang
        except:
            return np.nan
    # %%
    
    #%% function to apply the lanague detection
    def apply_language(df, columns):
        for i, col in enumerate(columns, start=1):
            new_col = f"{col}_lan{i}"
            df[new_col] = df[col].apply(detect_language)
            df[new_col] = df[new_col].replace(language_mapping)
        
        return df

    columns_to_detect = [col for col in df1.columns if '245$ab_part' in col]
    df1 = apply_language(df1, columns_to_detect)

    # %%

    def get_language_counts(df, columns):
        value_counts_list = [df[col].value_counts() for col in columns] 
        title_lan = pd.concat(value_counts_list, axis=1) 
        title_lan.columns = columns
        title_lan = title_lan.fillna(0)  
        title_lan['Total'] = title_lan.sum(axis=1)  
        
        return title_lan

    # Define the columns to calculate value counts for
    columns_to_count = [f"{col}_lan{i}" for i, col in enumerate(columns_to_detect, start=1)]
    title_lan = get_language_counts(df1, columns_to_count)

    st.subheader("Language count table for title:")
    st.dataframe(title_lan.head())
    # %% ????
    # page_size = 20
    # num_pages = len(title_lan) // page_size + 1

    # page = st.slider("Select Page", 1, num_pages, 1)

    # start_row = (page - 1) * page_size
    # end_row = start_row + page_size

    # st.dataframe(title_lan.iloc[start_row:end_row])

    # %%
    st.header('Analysis of Language and title')
    st.markdown("""We identified four cases: 
        <ul>
        <li>1. Language and title match. </li>
        <li>2. Cases with multiple languages in the language column but not in the title. </li>
        <li>3. Cases with multiple languages in the title but not in the language column. </li>
        <li>4. Cases where languages differ between the language and title columns.</li>
        </ul>""", unsafe_allow_html=True)

    # %%
    # List of the columns you're interested in
    title_cols = [col for col in df1.columns if '245$ab_part' in col and '_lan' in col]
    lan_cols = [col for col in df1.columns if '008+041_part' in col]

    with st.expander("Title Language columns:", expanded=False):
        st.write(title_cols)

    with st.expander("Language columns:", expanded=False):
        st.write(lan_cols)
    # %%
    df1 = df1.fillna('None')
    def compare_columns(row):
        matching_values = []
        lan_not_matching = []
        title_not_matching = []
        
        # Iterate over corresponding column pairs
        for lan_col, title_col in zip(lan_cols, title_cols):
            lan_value = row.get(lan_col, '')
            title_value = row.get(title_col, '')
            
            # If both lan_value and part_value match and are non-empty ('none' excluded)
            if lan_value == title_value and lan_value != np.nan and title_value != 'None':  # Non-empty, matching values
                matching_values.append(lan_value)
            else:
                # If lan_value is not 'none' and doesn't match part_value, add it to lan_not_matching
                if lan_value != 'None' and lan_value != title_value:
                    lan_not_matching.append(lan_value)
                
                # If part_value is not 'none' and doesn't match lan_value, add it to part_not_matching
                if title_value != 'None' and title_value != lan_value:
                    title_not_matching.append(title_value)
        
        # Prepare the results
        matching_value_result = ', '.join(matching_values) if matching_values else 'None'
        lan_not_matching_result = ', '.join(lan_not_matching) if lan_not_matching else 'None'
        title_not_matching_result = ', '.join(title_not_matching) if title_not_matching else 'None'
        
        return matching_value_result, lan_not_matching_result, title_not_matching_result

    # Apply the function across the DataFrame and expand results into new columns
    df1[['matching_value', 'mul-Language', 'mul-title']] = df1.apply(compare_columns, axis=1, result_type='expand')

    # %%
    def update_language_columns(row):
        # Split the 'matching_value' into individual language values
        matching_values = [value.strip() for value in row['matching_value'].split(',') if value.strip() != 'None']
        
        if row['mul-title'] in matching_values:
            row['mul-title'] = 'None'
        
        if row['mul-Language'] in matching_values:
            row['mul-Language'] = 'None'
        
        return row

    df1 = df1.apply(update_language_columns, axis=1)

    #%%

    # Clean the 'matching_value' column and remove 'None' entries
    df1['matching_value'] = df1['matching_value'].apply(lambda x: ', '.join(value.strip() for value in x.split(',') if value.strip() != 'None'))
    # %%
    def clean_none(value):
        return ', '.join([lang for lang in value.split(', ') if lang.strip() != 'None']) if value else 'None'

    df1['mul-Language'] = df1['mul-Language'].apply(clean_none).fillna('None').replace('', 'None')
    df1['mul-title'] = df1['mul-title'].apply(clean_none).fillna('None').replace('', 'None')

    # %%
    # Create a column to check if both 'language_245' and 'language_008+041' are matching
    df1['both_matching'] = (df1['mul-Language'] == 'None') & (df1['mul-title'] == 'None')

    # %% Find the cases
    filtered = df1[['245$a-Title', 'both_matching','matching_value', 'mul-Language', 'mul-title']]

    case1 = filtered[filtered['both_matching'] == True]
    case2 = filtered[(filtered['mul-title']== "None") & (filtered['both_matching'] == False)] 
    case3 = filtered[(filtered['mul-Language']== "None") & (filtered['both_matching'] == False)] 
    case4 = filtered[(filtered['mul-title'] != 'None') & (filtered['mul-Language'] != 'None') ] 

    # %%
    st.subheader("Result Table:")
    tab1, tab2, tab3, tab4 = st.tabs(['Case1' , 'Case2', 'Case3','Case4'])
    tab1.dataframe(case1)
    tab2.dataframe(case2)
    tab3.dataframe(case3)
    tab4.dataframe(case4)

    # %% final result
    col1, col2 = st.columns(2)
    # %%
    # Count rows in each case
    counts = {
        "Case 1": case1.shape[0],
        "Case 2": case2.shape[0],
        "Case 3": case3.shape[0],
        "Case 4": case4.shape[0]
    }

    # Convert to DataFrame for plotting
    counts_df = pd.DataFrame(list(counts.items()), columns=['Case', 'Count'])

    col1.subheader("Result Case Counts:")
    col1.dataframe(counts_df.head())

    # Plot
    plt.figure(figsize=(6, 4))
    bars = plt.bar(counts_df['Case'], counts_df['Count'], color='skyblue')

    # Add counts on top of each bar
    for bar in bars:
        yval = bar.get_height()
        plt.text(bar.get_x() + bar.get_width()/2, yval, int(yval), ha='center', va='bottom')

    plt.xlabel("Case")
    plt.ylabel("Count")
    plt.title("Number of Rows per Case")
    plt.show()
    plt.savefig("plot.png", bbox_inches="tight")
    col2.image("plot.png", width=600)

    # Create a dictionary of DataFrames to write
    df_dict = {
        'All': filtered,
        'Case1': case1,
        'Case2': case2,
        'Case3': case3,
        'Case4': case4
    }

    # Generate Excel file in memory
    excel_file = save_to_excel(df_dict)

    csv = convert_df(df)

    st.subheader("Results can be download!")
    # Provide download button in Streamlit
    st.download_button(
        label="Download CSV",
        data=csv,
        file_name="output_cases.csv",
        mime="text/csv"
    )