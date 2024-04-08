# -*- coding: utf-8 -*-
from kiara.api import KiaraModule
from kiara.exceptions import KiaraProcessingException

#TODO add type hints
#TODO add tokenization by character

class TokenizeCorpus(KiaraModule):
    """
    This module creates tokens from an array or from a table.
    It returns a table containing the initial array or table, and the tokens as a new column.
    It is possible to tokenize by word or by character. If not specified, tokenization is done by word.

    Dependencies:
    - NLTK: https://www.nltk.org/
    """

    _module_type_name = "topic_modelling.tokenize_corpus"

    def create_inputs_schema(self):
        return {
            "corpus_table": {
                "type": "table",
                "doc": "Corpus table.",
                "optional": True,
            },
            "column_name": {
                "type": "string",
                "doc": "Name of the column that contains the texts to tokenize.",
                "optional": True,
            },
            "corpus_array": {
                "type": "array",
                "doc": "Array that contains the text to tokenize.",
                "optional": True,
            },
            "tokenize_by_character": {
                "type": "boolean",
                "doc": "Tokenization",
                "optional": True,
                "default": False
            }
        }

    def create_outputs_schema(self):
        return {
            "tokens_table": {
                "type": "table",
                "doc": "Table with the tokenized texts as a new column."
            }
        }

    def process(self, inputs, outputs):

        import nltk  # type: ignore
        import pyarrow as pa  # type: ignore
        nltk.download("punkt")

        tokenized_list = None
        table_pa = None
        
        # check that both inputs table and array are not set simultaneously
        if inputs.get_value_obj("corpus_table").is_set and inputs.get_value_obj("corpus_array").is_set:
            raise KiaraProcessingException("Both 'corpus_table' and 'array' inputs cannot be set simultaneously.")

        if inputs.get_value_obj("corpus_table").is_set:
            
            table_data = inputs.get_value_data("corpus_table")
            table_pa = table_data.arrow_table
            
            if not inputs.get_value_obj("column_name").is_set:
                raise KiaraProcessingException("The 'column_name' input must be set when 'corpus_table' is set.")
            
            column_name: str = inputs.get_value_obj("column_name").data
            table_cols: list = table_pa.column_names

            # check that the column name provided exists in the table
            if column_name not in table_cols:
                raise KiaraProcessingException(
                    f"""Could not find title name/id column '{column_name}' in the table.
                    Please specify a valid column name manually, using one of: {', '.join(table_cols)}"""
                )
            
            corpus_array_pa = table_pa.column(column_name)
        
        elif inputs.get_value_obj("corpus_array").is_set:     
            corpus_array = inputs.get_value_data("corpus_array")
            corpus_array_pa = corpus_array.arrow_array
        
        if not inputs.get_value_data("tokenize_by_character"):
            corpus_list = corpus_array_pa.to_pylist()

            def tokenize(text):
                try:
                    return nltk.word_tokenize(str(text))
                except Exception:
                    return None
            try:
                tokenized_list = [tokenize(str(x)) for x in corpus_list]
                tokens_array = pa.array(tokenized_list)

            except Exception as e:
                raise KiaraProcessingException(
                    f"An error occurred while tokenizing the corpus: {e}."
                )
            
        if table_pa is not None:
            output_table = table_pa.add_column(len(table_cols),"tokens", [tokens_array])

        elif tokenized_list is not None:
            output_table = pa.Table.from_arrays([corpus_array_pa, tokens_array], names=["content", "tokens"])
            
        else:
            raise KiaraProcessingException("The operation could not complete.")
        
        outputs.set_value("tokens_table", output_table)