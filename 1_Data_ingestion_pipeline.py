# Databricks notebook source
# MAGIC %pip install --quiet databricks-sdk==0.24.0 mlflow==2.14.1 unstructured==0.13.7 sentence-transformers==3.0.1 torch==2.3.0 transformers==4.40.1 accelerate==0.27.2
# MAGIC %pip install "unstructured[pdf]"
# MAGIC %pip install pymupdf
# MAGIC %pip install pypdf2

# COMMAND ----------

import os
import re
import pandas as pd
from pyspark.sql.functions import col, lit
from unstructured.partition.pdf import partition_pdf
from PyPDF2 import PdfReader

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

catalog_name = "kyra_wulffert"
schema_name = "poc_ala"
volume_name = "raw_docs"
processed_table_name = "bronze_docs"
volume_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}"

# COMMAND ----------

import os
from pypdf import PdfReader
from unstructured.partition.pdf import partition_pdf

def process_file(file_path):
    """
    Processes a PDF file, extracts text, metadata, and basic info.
    """
    file_name = os.path.basename(file_path)
    file_extension = os.path.splitext(file_name)[1].lower()
    try:
        if file_extension == ".pdf":
            # Extract text using unstructured
            pdf_elements = partition_pdf(file_path)
            text = "\n".join([element.text for element in pdf_elements if element.text])
        else:
            text = f"Unsupported file type: {file_extension}"

        length = len(text) if text else 0
        print(file_name)
        return {
            "file_name": file_name,
            "file_extension": file_extension,
            "length": length,
            "text": text,
        }

    except Exception as e:
        return {
            "file_name": file_name,
            "file_extension": file_extension,
            "length": 0,
            "text": f"Error processing file: {str(e)}",
        }


# COMMAND ----------

# Walk through files in volume and parse only those related to ALA

import os
import pandas as pd

data = []
search_terms = ["ALA", "Asset Life Assessment"]

for root, dirs, files in os.walk(volume_path):
    for file in files:
        # Convert filename to lowercase for case-insensitive matching
        file_lower = file.lower()

        # Check if any of the search terms appear in the file name
        if any(term.lower() in file_lower for term in search_terms):
            file_path = os.path.join(root, file)
            file_data = process_file(file_path)
            data.append(file_data)

# Convert to a Pandas DataFrame
df = pd.DataFrame(data)


# COMMAND ----------

df

# COMMAND ----------

import re

def chunk_text_at_contents(text):
    # This regex will look for a line that just says "contents" or "Contents" etc.
    # (?i) makes it case-insensitive
    # The pattern looks for a line that starts with optional whitespace, then the word "contents" alone, then optional whitespace, and end of the line.
    pattern = r'(?i)^[ \t]*contents[ \t]*$'

    # Use re.split with the multiline flag to split on this line
    # If 'Contents' is guaranteed to appear, this will give you two parts:
    # everything before 'Contents', and everything after (including 'Contents').
    # If 'Contents' might not appear, you'll want to handle that case.
    parts = re.split(pattern, text, flags=re.MULTILINE)

    if len(parts) == 1:
        # 'Contents' was not found, so we return the entire text as one chunk
        return [text]
    else:
        # parts[0] is everything before 'Contents'
        # parts[1] is everything after 'Contents'
        # If you want to include the line 'Contents' in the second chunk:
        # Just prepend 'Contents\n' or handle accordingly.
        # Here, we assume the split removes the match line, so we can manually add it back if desired.
        chunks = []
        before_contents = parts[0].strip()
        after_contents = parts[1].strip()

        # Add the chunks
        if before_contents:
            chunks.append(before_contents)
        if after_contents:
            chunks.append("Contents\n" + after_contents)

        return chunks


# COMMAND ----------

df['chunks'] = df['text'].apply(chunk_text_at_contents)

# COMMAND ----------

df['chunk_1'] = df['chunks'].apply(lambda x: x[0] if len(x) > 0 else "")
df['chunk_2'] = df['chunks'].apply(lambda x: x[1] if len(x) > 1 else "")

# COMMAND ----------

df.head()

# COMMAND ----------

spark.createDataFrame(df) \
    .write.format("delta") \
    .option("mergeSchema", "true") \
    .mode("overwrite") \
    .saveAsTable(f"{catalog_name}.{schema_name}.bronze_docs")

# COMMAND ----------

# MAGIC %md
# MAGIC Metadata using pdfreader didn't provide good results

# COMMAND ----------

import os
from pypdf import PdfReader
from unstructured.partition.pdf import partition_pdf

def process_file(file_path):
    """
    Processes a PDF file, extracts text, metadata, and basic info.
    """
    file_name = os.path.basename(file_path)
    file_extension = os.path.splitext(file_name)[1].lower()
    try:
        if file_extension == ".pdf":
            # Extract text using unstructured
            pdf_elements = partition_pdf(file_path)
            text = "\n".join([element.text for element in pdf_elements if element.text])

            # Initialize PdfReader to access metadata and pages
            pdf_reader = PdfReader(file_path)
            meta = pdf_reader.metadata

            author = meta.author if meta.author else ''
            creation_date = meta.creation_date if meta.creation_date else ''
            creator = meta.creator if meta.creator else ''
            mod_date = meta.modification_date if meta.modification_date else ''
            producer = meta.producer if meta.producer else ''
            title = meta.title if meta.title else ''

            num_pages = len(pdf_reader.pages)
        else:
            text = f"Unsupported file type: {file_extension}"
            num_pages = None
            author = ''
            creation_date = ''
            creator = ''
            mod_date = ''
            producer = ''
            title = ''

        length = len(text) if text else 0

        return {
            "file_name": file_name,
            "file_extension": file_extension,
            "num_pages": num_pages,
            "length": length,
            "text": text,
            "Author": author,
            "CreationDate": creation_date,
            "Creator": creator,
            "ModDate": mod_date,
            "Producer": producer,
            "Title": title,
        }

    except Exception as e:
        return {
            "file_name": file_name,
            "file_extension": file_extension,
            "num_pages": None,
            "length": 0,
            "text": f"Error processing file: {str(e)}",
            "Author": '',
            "CreationDate": '',
            "Creator": '',
            "ModDate": '',
            "Producer": '',
            "Title": '',
        }


# COMMAND ----------

import os
import pandas as pd

data = []
search_terms = ["ALA", "Asset Life Assessment"]

for root, dirs, files in os.walk(volume_path):
    for file in files:
        # Convert filename to lowercase for case-insensitive matching
        file_lower = file.lower()

        # Check if any of the search terms appear in the file name
        if any(term.lower() in file_lower for term in search_terms):
            file_path = os.path.join(root, file)
            file_data = process_file(file_path)
            data.append(file_data)

# Convert to a Pandas DataFrame
df = pd.DataFrame(data)


# COMMAND ----------

df

# COMMAND ----------

