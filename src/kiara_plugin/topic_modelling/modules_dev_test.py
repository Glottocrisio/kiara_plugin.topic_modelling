from kiara.api import KiaraAPI
kiara = KiaraAPI.instance()

# this file is only a helper for debugging while developing the modules

create_table_from_zenodo_inputs = {
    "doi": "4596345",
    "file_name": "ChroniclItaly_3.0_original.zip"
}

create_table_from_zenodo_results = kiara.run_job('topic_modelling.create_table_from_zenodo', inputs=create_table_from_zenodo_inputs)

corpus_table_zenodo = create_table_from_zenodo_results['corpus_table']

# create_table_from_url_inputs = {
#     "url": "https://github.com/DHARPA-Project/kiara.examples/archive/refs/heads/main.zip",
#     "sub_path": "kiara.examples-main/examples/workshops/dh_benelux_2023/data"
# }

# create_table_from_url_results = kiara.run_job('topic_modelling.create_table_from_url', inputs=create_table_from_url_inputs)

# corpus_table_url = create_table_from_url_results['corpus_table']

# test_table_inputs = {
#     "test_table": corpus_table_zenodo,
# }

# test_table_results = kiara.run_job('topic_modelling.test_table_conversion', inputs=test_table_inputs)

get_lccn_metadata_inputs = {
    "corpus_table": corpus_table_zenodo,
    "file_name_col": "file_name",
    "map": [['2012271201','sn85054967','sn93053873','sn85066408','sn85055164','sn84037024','sn84037025','sn84020351','sn86092310','sn92051386'],['Cronaca_Sovversiva','Il_Patriota','L\'Indipendente','L\'Italia','La_Libera_Parola','La_Ragione','La_Rassegna','La_Sentinella','La_Sentinella_del_West','La_Tribuna_del_Connecticut']],
}

get_lccn_metadata_results = kiara.run_job('topic_modelling.get_lccn_metadata', inputs=get_lccn_metadata_inputs)

data = get_lccn_metadata_results['corpus_table'].data
print(data.arrow_table.to_pandas())

# time_dist_inputs = {
#     "periodicity": 'month',
#     "time_column": "date",
#     "title_column": "pub_ref",
#     "corpus_table": get_lccn_metadata_results['corpus_table'],
# }

# time_dist_inputs_results = kiara.run_job('topic_modelling.time_dist', inputs=time_dist_inputs)