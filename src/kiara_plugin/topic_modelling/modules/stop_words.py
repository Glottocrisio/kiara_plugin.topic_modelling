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