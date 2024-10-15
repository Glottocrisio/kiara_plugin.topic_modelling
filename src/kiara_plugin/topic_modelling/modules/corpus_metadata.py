# -*- coding: utf-8 -*-

from kiara.api import KiaraModule
from kiara.exceptions import KiaraProcessingException


class GetLccnMetadata(KiaraModule):
    """
    This module will get metadata from strings that comply with LCCN pattern: '/sn86069873/1900-01-05/' to get the publication references and the dates and add those informations as two new columns.
    In addition, if a mapping scheme is provided between publication references and publication names, it will add a column with the publication names.
    Such a map is provided in the form of a list of lists with publication references and publication names in the same order.
    Here is an example of how it should look:
    [["2012271201","sn85054967","sn93053873"],["Cronaca_Sovversiva","Il_Patriota","L'Indipendente"]]
    """

    _module_type_name = "topic_modelling.lccn_metadata"

    def create_inputs_schema(self):
        return {
            "corpus_table": {
                "type": "table",
                "doc": "Table that contains a column with the file names.",
            },
            "column_name": {
                "type": "string",
                "doc": "Name of the column that contains the file names.",
                "optional": False,
            },
            "map": {
                "type": "list",
                "doc": "List of lists of unique publications references and publication names in the collection provided in the same order.",
                "optional": True,
            },
        }

    def create_outputs_schema(self):
        return {
            "corpus_table": {
                "type": "table",
                "doc": "The augmented table with extracted metadata."
            }
        }

    def process(self, inputs, outputs):

        import re
        import polars as pl # type: ignore
        import pyarrow as pa # type: ignore

        table_obj = inputs.get_value_obj("corpus_table")
        column_name = inputs.get_value_obj("file_name_col").data

        
        if table_obj.is_set:

            sources = table_obj.data
            
            assert sources is not None

            sources_col_names = sources.column_names

            if column_name not in sources_col_names:

                raise KiaraProcessingException(
                    f"Could not find file names column '{column_name}' in the table. Please specify a valid column name manually, using one of: {', '.join(sources_col_names)}"
                )
            
            sources_data: pa.Table = table_obj.data.arrow_table
            
            sources_tb: pl.DataFrame = pl.from_arrow(sources_data) # type: ignore


        def get_ref(file):
            try:
                ref_match = re.findall(r"(\w+\d+)_\d{4}-\d{2}-\d{2}_", file)
                if not ref_match:
                    return None
                return ref_match[0]

            except Exception as e:
                raise KiaraProcessingException(e)

        def get_date(file):
            try:
                date_match = re.findall(r"_(\d{4}-\d{2}-\d{2})_", file)
                if not date_match:
                    return None
                return date_match[0]
            except Exception as e:
                msg = f"Error in get_date: {e}"
                raise KiaraProcessingException(msg)

        
        try:
            
            pub_refs: list[str] = inputs.get_value_obj("map").data[0]
            pub_names: list[str] = inputs.get_value_obj("map").data[1]

            pub_ref_to_name = dict(zip(pub_refs, pub_names))
            
            augm_sources = sources_tb.with_columns(
                    sources_tb["publication_ref"]
                    .map_elements(lambda x: pub_ref_to_name.get(x))
                    .alias("publication_name")
                )

        except:
            try:
                augm_sources = sources_tb.with_columns(
                    [
                        sources_tb[column_name].map_elements(get_date).alias("date"),
                        sources_tb[column_name].map_elements(get_ref).alias("publication_ref"),
                    ]
                )

            except Exception as e:
                msg = f"An error occurred while augmenting the dataframe: {e}"
                raise KiaraProcessingException(msg)

        try:
            output_table = augm_sources.to_arrow()

        except Exception as e:
            raise KiaraProcessingException(e)

        outputs.set_value("corpus_table", output_table)
        