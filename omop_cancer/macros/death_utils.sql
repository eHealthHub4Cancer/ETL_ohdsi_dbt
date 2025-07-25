{% macro generate_cause_date(cause_col) %}
    {% set cause_mapping = {
        'treatment-related death in remission': 135766,
        'unrelated to disease or treatment': 4134000,
        'blood disorder': 434008,
        'transplant': 4139964
    } 
    %}
    
    -- This macro generates a cause concept id based on the code using dictionary mapping
    case 
        {% for cause_text, concept_id in cause_mapping.items() %}
        when lower(trim({{ cause_col }})) = '{{ cause_text }}' then {{ concept_id }}
        {% endfor %}
        else 0
    end as cause_concept_id
{% endmacro %}

{% macro generate_default_death_values() %}
    -- This macro generates default values for death information
    null::integer as cause_source_concept_id,
    null::varchar as cause_source_value
{% endmacro %}


{% macro get_cdm_values() %}
  {{
    return({
      "cdm_source_name": "Ireland Cancer",
      "cdm_source_abbreviation": "Icancer",
      "cdm_holder": "eHealthHub",
      "source_description": "Ireland Cancer project for mapping series of cancer data",
      "source_documentation_reference": "###",
      "cdm_etl_reference": "https://github.com/eHealthHub4Cancer/ETL_ohdsi_dbt.git",
      "source_release_date": "2025-05-30",
      "cdm_version": "v5.4"
    })
  }}
{% endmacro %}
