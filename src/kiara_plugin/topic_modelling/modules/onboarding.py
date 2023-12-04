# -*- coding: utf-8 -*-

from kiara.api import KiaraModule
import urllib.request
import zipfile
import io
import os
import polars as pl
import pyarrow as pa

# class CreateTableFromUrl(KiaraModule):
#     """
#     This module retrieves text files from a specified sub-path within a zip file hosted at a given URL.
#     It outputs a table with two columns: one for the file names and the other for the content of these files.

#     Dependencies:
#     - urllib.request: https://docs.python.org/3/library/urllib.request.html
#     - zipfile: https://docs.python.org/3/library/zipfile.html
#     - polars: https://www.pola.rs/
#     """

#     _module_type_name = "topic_modelling.create_table_from_url"

#     def create_inputs_schema(self):
#         return {
#             "url": {
#                 "type": "string",
#                 "doc": "URL of the zip file to retrieve text files from."
#             },
#             "sub_path": {
#                 "type": "string",
#                 "doc": "Sub-path within the zip file to look for text files."
#             }
#         }

#     def create_outputs_schema(self):
#         return {
#             "corpus_table": {
#                 "type": "table",
#                 "doc": "Table with file names and contents."
#             }
#         }

#     def process(self, inputs, outputs):
        
#         url = inputs.get_value_data("url")
#         sub_path = inputs.get_value_data("sub_path")

#         try:
#             with urllib.request.urlopen(url) as response:
#                 zip_file = zipfile.ZipFile(io.BytesIO(response.read()))

#             file_contents = []
            
#             for file in zip_file.namelist():
#                 if file.startswith(sub_path) and file.endswith('.txt'):
#                     with zip_file.open(file) as f:
#                         content = f.read().decode('utf-8')
#                     file_name = os.path.basename(file)
#                     file_contents.append({'file_name': file_name, 'content': content})
    
#             pl_df = pl.DataFrame(file_contents)
#             pa_table = pl_df.to_arrow()
            
#             outputs['corpus_table'].set_value(pa_table)

#         except Exception as e:
#             # Print exception details
#             print(f"Error retrieving files: {e}")

        

class CreateTableFromZenodo(KiaraModule):
    """
    This module retrieves text files from a specified folder hosted on Zenodo.
    It takes the DOI and the name of the file as inputs.
    It outputs a table with two columns: one for the file names and the other for the content of these files.

    Dependencies:
    - urllib.request: https://docs.python.org/3/library/urllib.request.html
    - zipfile: https://docs.python.org/3/library/zipfile.html
    - polars: https://www.pola.rs/
    """

    _module_type_name = "topic_modelling.create_table_from_zenodo"

    def create_inputs_schema(self):
        return {
            "doi": {
                "type": "string",
                "doc": "The Digital Object Identifier for the resource."
            },
            "file_name": {
                "type": "string",
                "doc": "The name of the file to be processed."
            }
        }

    def create_outputs_schema(self):
        return {
            "corpus_table": {
                "type": "table",
                "doc": "A table with two columns: file names and their contents."
            }
        }

    def process(self, inputs, outputs):
        doi = inputs.get_value_data("doi")
        file_name = inputs.get_value_data("file_name")
        url = f"https://zenodo.org/record/{doi}/files/{file_name}"

        # Download and extract the zip file
        urllib.request.urlretrieve(url, "temp.zip")
        with zipfile.ZipFile("temp.zip", 'r') as zip_ref:
            zip_ref.extractall("temp_folder")

        # Process text files and create the table
        file_names = []
        file_contents = []
        for root, dirs, files in os.walk("temp_folder"):
            for file in files:
                if file.endswith(".txt"):
                    file_path = os.path.join(root, file)
                    with open(file_path, 'r') as f:
                        content = f.read()
                        file_names.append(file)
                        file_contents.append(content)

        # Create a table with polars
        pl_df = pl.DataFrame({"file_name": file_names, "content": file_contents})
        pa_table = pl_df.to_arrow()

        outputs.set_value("corpus_table", pa_table)
