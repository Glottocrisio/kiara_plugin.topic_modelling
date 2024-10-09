# -*- coding: utf-8 -*-
from kiara.api import KiaraModule
from kiara.exceptions import KiaraProcessingException


class TokenizeArray(KiaraModule):
    """
    This module creates tokens from an array or from a table.
    It returns a table containing the initial array or table, and the tokens as a new column.
    It is possible to tokenize by word or by character. If not specified, tokenization is done by word.

    Dependencies:
    - NLTK: https://www.nltk.org/
    """

    _module_type_name = "topic_modelling.tokenize_array"

    def create_inputs_schema(self):
        return {
            "corpus_array": {
                "type": "array",
                "doc": "Array that contains the text to tokenize.",
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
            "tokens_array": {
                "type": "array",
                "doc": "The tokenized array."
            }
        }

    def process(self, inputs, outputs):

        import nltk  # type: ignore
        import pyarrow as pa  # type: ignore
        from nltk.tokenize.simple import CharTokenizer  # type: ignore

        nltk.download("punkt")

        corpus_array = inputs.get_value_data("corpus_array")
        corpus_array_pa = corpus_array.arrow_array
        corpus_list = corpus_array_pa.to_pylist()

        def tokenize(text: str, tokenize_by_character:bool = False):
            if not tokenize_by_character:
                try:
                    return nltk.word_tokenize(str(text))
                except Exception:
                    return None
            else:
                try:
                    tokenizer = CharTokenizer()
                    return tokenizer.tokenize(text)
                except Exception:
                    return None

        if not inputs.get_value_data("tokenize_by_character"):
            try:
                tokenized_list = [tokenize(str(x)) for x in corpus_list]
                tokens_array = pa.array(tokenized_list)

            except Exception as e:
                raise KiaraProcessingException(
                    f"An error occurred while tokenizing the corpus by word: {e}."
                )
        else:
            try:
                tokenized_list = [tokenize(str(x), tokenize_by_character=True) for x in corpus_list]
                tokens_array = pa.array(tokenized_list)

            except Exception as e:
                raise KiaraProcessingException(
                    f"An error occurred while tokenizing the corpus by character: {e}."
                )
        
        outputs.set_value("tokens_array", tokens_array)

class PreprocessTokens(KiaraModule):
    """
    This module offers pre-processing options for an array of tokens.

    """

    _module_type_name = "topic_modelling.preprocess_tokens"

    def create_inputs_schema(self):
        return {
            "tokens_array": {
                "type": "array",
                "doc": "Array that contains the tokens to pre-process.",
            },
            "lowercase": {
                "type": "boolean",
                "doc": "Whether to lowercase the tokens.",
                "optional": True,
                "default": False
            },
            "isalpha": {
                "type": "boolean",
                "doc": "Whether to remove tokens that contain other characters than letters.",
                "optional": True,
                "default": False
            },
            "min_length": {
                "type": "integer",
                "doc": "Whether to remove tokens that contain less than min_length characters.",
                "optional": True,
                "default": False
            }
        }

    def create_outputs_schema(self):
        return {
            "tokens_array": {
                "type": "array",
                "doc": "The array that contains the pre-processed tokens."
            }
        }

    def process(self, inputs, outputs):
        import polars as pl # type: ignore
        import pyarrow as pa # type: ignore

        tokens_array = inputs.get_value_data("tokens_array")
        tokens_array_pa = tokens_array.arrow_array
        
        tokens_list = tokens_array_pa.to_pylist()

        do_lowercase = inputs.get_value_data("lowercase")
        do_isalpha = inputs.get_value_data("isalpha")
        min_length = inputs.get_value_data("min_length")

        def preprocess_token(token):
            if not isinstance(token, str):
                token = str(token)
            
            if do_lowercase:
                token = token.lower()
            
            if do_isalpha and not token.isalpha():
                return None
            
            if min_length and len(token) < min_length:
                return None
            
            return token

        def process_nested(item):
            if isinstance(item, list):
                return [y for y in (process_nested(subitem) for subitem in item) if y is not None]
            else:
                return preprocess_token(item)


        processed_tokens = process_nested(tokens_list)
        processed_array = pa.array(processed_tokens)

        outputs.set_value("tokens_array", processed_array)