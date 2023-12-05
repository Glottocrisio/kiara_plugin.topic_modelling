# -*- coding: utf-8 -*-

from kiara.api import KiaraModule
import polars as pl
import pyarrow as pa

class GetTextMetrics(KiaraModule):
    """
    This module retrieves text files from a specified sub-path within a zip file hosted at a given URL.
    It outputs a table with two columns: one for the file names and the other for the content of these files.

    Dependencies:
    - polars: https://www.pola.rs/
    """

    _module_type_name = "topic_modelling.get_text_metrics"

    def create_inputs_schema(self):
        
        return {
            "corpus_table": {
                "type": "table",
                "doc": "The corpus for which we want to add words and characters count.",
            },
            "column_name": {
                "type": "string",
                "doc": "The column containing the text for which we want the count."
            }
        }

    def create_outputs_schema(self):
        return {
            "augmented_table": {
                "type": "table",
                "doc": "Augmented table containing words and characters count."
            }
        }

    def process(self, inputs, outputs) -> None:

        table_obj = inputs.get_value_obj("corpus_table")
        column_name = inputs.get_value_obj("column_name").data

        sources = table_obj.data
        
        sources = sources.with_columns([
            pl.col(column_name).str.lengths().alias('chars_count'),
            pl.col(column_name).str.split(' ').arr.lengths().alias('words_count')
        ])

        pa_table = sources.to_arrow()
        outputs.set_value("augmented_table", pa_table)