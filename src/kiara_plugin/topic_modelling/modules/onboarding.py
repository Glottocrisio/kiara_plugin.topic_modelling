# -*- coding: utf-8 -*-

from kiara.api import KiaraModule
import urllib.request
import zipfile
import io
import os
import polars as pl

class GetUrlTextFilesModule(KiaraModule):
    """
    This module retrieves text files from a specified sub-path within a zip file hosted at a given URL.
    It outputs a table with two columns: one for the file names and the other for the content of these files.

    Dependencies:
    - urllib.request: https://docs.python.org/3/library/urllib.request.html
    - zipfile: https://docs.python.org/3/library/zipfile.html
    - polars: https://www.pola.rs/
    """

    _module_type_name = "topic_modelling.create.table.from.url"

    def create_inputs_schema(self):
        return {
            "url": {
                "type": "string",
                "doc": "URL of the zip file to retrieve text files from."
            },
            "sub_path": {
                "type": "string",
                "doc": "Sub-path within the zip file to look for text files."
            }
        }

    def create_outputs_schema(self):
        return {
            "file_data": {
                "type": "table",
                "doc": "Table with file names and contents."
            }
        }

    def process(self, inputs, outputs):
        url = inputs['url'].get_value()
        sub_path = inputs['sub_path'].get_value()
        file_data = self._retrieve_text_files(url, sub_path)
        outputs['file_data'].set_value(file_data)

    def _retrieve_text_files(self, url, sub_path):
        try:
            with urllib.request.urlopen(url) as response:
                zip_file = zipfile.ZipFile(io.BytesIO(response.read()))

            file_contents = []
            for file in zip_file.namelist():
                if file.startswith(sub_path) and file.endswith('.txt'):
                    with zip_file.open(file) as f:
                        content = f.read().decode('utf-8')
                    file_name = os.path.basename(file)
                    file_contents.append({'file_name': file_name, 'content': content})

            return pl.DataFrame(file_contents)
        except Exception as e:
            # Handle exceptions
            raise e