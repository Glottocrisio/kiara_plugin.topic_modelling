# [**kiara**](https://dharpa.org/kiara.documentation) plugin: topic_modelling

This package contains a set of commonly used/useful modules, pipelines, types and metadata schemas for [*Kiara*](https://github.com/DHARPA-project/kiara).

## Description

A Kiara plugin toho create a Topic Modelling workflow.

## Package content

{% for item_type, item_group in get_context_info().get_all_info().items() %}

### {{ item_type }}
{% for item, details in item_group.item_infos.items() %}
- [`{{ item }}`][kiara_info.{{ item_type }}.{{ item }}]: {{ details.documentation.description }}
{% endfor %}
{% endfor %}

## Links

 - Documentation: [https://DHARPA-Project.github.io/kiara_plugin.topic_modelling](https://DHARPA-Project.github.io/kiara_plugin.topic_modelling)
 - Code: [https://github.com/DHARPA-Project/kiara_plugin.topic_modelling](https://github.com/DHARPA-Project/kiara_plugin.topic_modelling)
