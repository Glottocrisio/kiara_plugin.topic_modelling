# -*- coding: utf-8 -*-

from kiara.api import KiaraModule
import polars as pl
import pyarrow as pa
import re


class GetLCCNMetadata(KiaraModule):
    """
    This module will get metadata from strings that comply with LCCN pattern: '/sn86069873/1900-01-05/' to get the publication references and the dates and add those informations as two new columns.
    In addition, if a mapping scheme is provided between publication references and publication names, it will add a column with the publication names.
    Such a map is provided in the form of a list of lists with publication references and publication names in the same order.
    Here is an example of how it should look:
    [["2012271201","sn85054967","sn93053873"],["Cronaca_Sovversiva","Il_Patriota","L'Indipendente"]]

    Dependencies:
    - polars: https://www.pola.rs/
    - pyarrow: https://arrow.apache.org/docs/python/
    - re: https://docs.python.org/3/library/re.html
    """

    _module_type_name = "topic_modelling.get_lccn_metadata"

    def create_inputs_schema(self):
        return {
            "corpus_table": {
                "type": "table",
                "doc": "The corpus for which we want to get metadata from file names.",
            },
            "file_name_col": {
                "type": "string",
                "doc": "The column containing file names with metadata. In order to work, file names need to comply with LCCN pattern '/sn86069873/1900-01-05/' containing publication reference and date."
            },
            "map": {
                "type": "list",
                "doc": "List of lists of unique publications references and publication names in the collection provided in the same order.",
                "optional": True,
            }
        }

    def create_outputs_schema(self):
        return {
            "corpus_table": {
                "type": "table",
                "doc": "Augmented table containing extracted metadata."
            }
        }

    def process(self, inputs, outputs) -> None:
        
        table_obj = inputs.get_value_obj("corpus_table")
        column_name = inputs.get_value_obj("file_name_col").data
        pub_refs, pub_names = None, None

        try:
            pub_refs = inputs.get_value_obj("map").data[0]
            pub_names = inputs.get_value_obj("map").data[1]

        except Exception as e:
            pass;


        sources = pl.from_arrow(table_obj.data.arrow_table)

        def get_ref(file):
            try:
                ref_match = re.findall(r'(\w+\d+)_\d{4}-\d{2}-\d{2}_',file)
                if not ref_match:
                    return None
                return ref_match[0]
            
            except Exception as e:
                print(f"Error in get_ref: {e}")
                return None

        def get_date(file):
            try:
                date_match = re.findall(r'_(\d{4}-\d{2}-\d{2})_',file)
                if not date_match:
                    return None
                return date_match[0]
            except Exception as e:
                print(f"Error in get_date: {e}")
                return None

        try:
            sources = sources.with_columns([
                sources[column_name].apply(get_date).alias('date'),
                sources[column_name].apply(get_ref).alias('publication_ref')
            ])
        except Exception as e:

            print(f"An error occurred while augmenting the dataframe: {e}")

        try:
            if pub_refs and pub_names:
                pub_ref_to_name = dict(zip(pub_refs, pub_names))
                sources = sources.with_columns(
                    sources['publication_ref'].apply(lambda x: pub_ref_to_name.get(x, None)).alias('publication_name')
                )
        except Exception as e:
            print(e)
            pass

        try:
            output_table = sources.to_arrow()

        except Exception as e:
            print(f"An error occurred: {e}")

        outputs.set_value("corpus_table", output_table)