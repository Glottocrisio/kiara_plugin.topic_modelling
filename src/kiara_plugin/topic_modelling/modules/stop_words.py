# -*- coding: utf-8 -*-
from kiara.api import KiaraModule
from kiara.exceptions import KiaraProcessingException
from typing import List, Optional


class CreateSwList(KiaraModule):
    """
    This module creates a stop words list and enables to combine predefined stop words lists from nltk and/or a custom additional stop words list.

    Dependencies:
    - NLTK: https://www.nltk.org/
    """

    _module_type_name = "topic_modelling.stopwords_list"

    def create_inputs_schema(self):
        return {
            "languages": {
                "type": "list",
                "doc": "List of languages supported by NLTK, e.g. ['english', 'dutch']. Please consult NLTK documentation for an exhaustive list of supported languages.",
                "optional": True,
                "default": []
            },
            "stopwords_list": {
                "type": "list",
                "doc": "A python list of stopwords.",
                "optional": True,
                "default": False
            }
        }

    def create_outputs_schema(self):
        return {
            "stopwords_list": {
                "type": "list",
                "doc": "The combined stop words list."
            }
        }

    def process(self, inputs, outputs):

        import nltk  # type: ignore
        from nltk.corpus import stopwords  # type: ignore
        nltk.download('stopwords', quiet=True)

        nltk_languages = set(stopwords.fileids())
        languages: List[str] = inputs.get_value_data("languages")
        custom_stopwords: List[str] = inputs.get_value_data("stopwords_list")

        if not languages and not custom_stopwords:
            raise KiaraProcessingException("At least one language or custom stopwords list must be provided.")

        sw_list: List[str] = []

        for lang in languages:
            if lang not in nltk_languages:
                raise KiaraProcessingException(f"Language '{lang}' not supported by NLTK.")
            try:
                sw_list.extend(stopwords.words(lang))
            except LookupError as e:
                raise KiaraProcessingException(f"Failed to create stopwords list for language '{lang}': {e}")

        sw_list.extend(custom_stopwords)
        sw_list = list(dict.fromkeys(sw_list))
        outputs.set_value("stopwords_list", sw_list)


class RemoveSw(KiaraModule):
    """
    
    This module removes stop words from an array of tokens.
    
    """

    _module_type_name = "topic_modelling.remove_stopwords"

    def create_inputs_schema(self):
        return {
            "stopwords_list": {
                "type": "list",
                "doc": "A list of stop words to be removed from the tokens.",
                "optional": False
            },
            "tokens_array": {
                "type": "array",
                "doc": "An array of tokens.",
                "optional": False,
            }
        }

    def create_outputs_schema(self):
        return {
            "tokens_array": {
                "type": "array",
                "doc": "The array of tokens without stop words."
            }
        }

    def process(self, inputs, outputs):
        import pyarrow as pa # type: ignore
        from pyarrow import compute as pc # type: ignore
        
        tokens_array = inputs.get_value_data("tokens_array")
        sw_list = inputs.get_value_data("stopwords_list")
        
        stopwords_set = set(sw_list)
        tokens_array_pa = tokens_array.arrow_array
        
        def remove_stopwords(words, stopwords_set):
            return [word for word in words if word not in stopwords_set]

        try:
            tokens_nostop = pa.array([remove_stopwords(words, stopwords_set) for words in tokens_array_pa.to_pylist()])
        except Exception as e:
            raise KiaraProcessingException(f"An error occurred while removing stop words: {e}")

        outputs.set_value("tokens_array", tokens_nostop)