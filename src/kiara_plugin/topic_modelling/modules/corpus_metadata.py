# -*- coding: utf-8 -*-

from kiara.api import KiaraModule
from kiara.exceptions import KiaraProcessingException
from typing import Union


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
                "optional": False,
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
        column_name = inputs.get_value_obj("column_name").data

        sources = table_obj.data
            
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
                msg = f"There was a problem in the publication reference pattern: {e}"
                raise KiaraProcessingException(e)

        def get_date(file):
            try:
                date_match = re.findall(r"_(\d{4}-\d{2}-\d{2})_", file)
                if not date_match:
                    return None
                return date_match[0]
            except Exception as e:
                msg = f"There was a problem in the date pattern: {e}"
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
                augm_sources = sources_tb.with_columns([
                sources_tb[column_name].map_elements(get_date, return_dtype=pl.Utf8).alias("date"),
                sources_tb[column_name].map_elements(get_ref, return_dtype=pl.Utf8).alias("publication_ref"),
            ])

            except Exception as e:
                msg = f"An error occurred while augmenting the dataframe: {e}"
                raise KiaraProcessingException(msg)

        try:
            output_table = augm_sources.to_arrow()

        except Exception as e:
            raise KiaraProcessingException(e)

        outputs.set_value("corpus_table", output_table)


class CorpusDistTime(KiaraModule):
    """
    This module aggregates a table by day, month or year from a corpus table that contains a date column. It returns the distribution over time, which can be used for display purposes, such as visualization.
    
    """

    _module_type_name = "topic_modelling.corpus_distribution"

    def create_inputs_schema(self):

        return {
            "periodicity": {
                "type": "string",
                "type_config": {"allowed_strings": ["day", "month", "year"]},
                "doc": "The desired data periodicity to aggregate the data. Values can be either 'day','month' or 'year'.",
                "optional": False,
            },
            "date_col": {
                "type": "string",
                "doc": "Column name of the column that contains the date. Values in this column need to comply with date format: https://docs.rs/chrono/latest/chrono/format/strftime/index.html.",
                "optional": False,
            },
            "publication_ref_col": {
                "type": "string",
                "doc": "Column name of the values containing publication names or ref/id. This column will be used in the output.",
                "optional": False,
            },
            "corpus_table": {
                "type": "table",
                "doc": "The corpus table for which the distribution over time is needed.",
                "optional": False,
            },
        }

    def create_outputs_schema(self):
        return {"dist_table": {"type": "table", "doc": "The aggregated data table."}}

    def process(self, inputs, outputs) -> None:
        
        import duckdb # type: ignore
        import polars as pl # type: ignore
        import pyarrow as pa   # type: ignore

        agg = inputs.get_value_obj("periodicity").data
        title_col = inputs.get_value_obj("publication_ref_col").data
        time_col = inputs.get_value_obj("date_col").data
        table_obj = inputs.get_value_obj("corpus_table")


        sources: Union[pa.Table, None] = table_obj.data
            
        sources_col_names = sources.column_names

        if title_col not in sources_col_names:

            raise KiaraProcessingException(
                f"Could not find title name/id column '{title_col}' in the table. Please specify a valid column name manually, using one of: {', '.join(sources_col_names)}"
            )
            
        if time_col not in sources_col_names:

            raise KiaraProcessingException(
                f"Could not find date column '{time_col}' in the table. Please specify a valid column name manually, using one of: {', '.join(sources_col_names)}"
            )
            
        sources_data: pa.Table = table_obj.data.arrow_table
            
        sources_tb: pl.DataFrame = pl.from_arrow(sources_data) # type: ignore

        try:
            sources_tb = sources_tb.with_columns(
                pl.col(time_col).str.strptime(pl.Date, "%Y-%m-%d")
            )

        except:
            raise KiaraProcessingException(
                f"Could not convert time column to a valid date format. Please check the pattern of source values."
            )

        if agg == "month":
            query = f"SELECT EXTRACT(MONTH FROM date) AS month, EXTRACT(YEAR FROM date) AS year, {title_col}, COUNT(*) as count FROM sources_tb GROUP BY {title_col}, EXTRACT(YEAR FROM date), EXTRACT(MONTH FROM date)"  # noqa
        elif agg == "year":
            query = f"SELECT EXTRACT(YEAR FROM date) AS year, {title_col}, COUNT(*) as count FROM sources_tb GROUP BY {title_col}, EXTRACT(YEAR FROM date)"  # noqa
        elif agg == "day":
            query = f"SELECT date, {title_col}, COUNT(*) as count FROM sources_tb GROUP BY {title_col}, date"  # noqa
        
        else:
            raise KiaraProcessingException(
                f'Please check the value chosen for periodicity. Supported values are "day", "month" or "year"'
            )

        queried_df = duckdb.query(query)

        pa_out_table = queried_df.arrow()

        outputs.set_value("dist_table", pa_out_table)