{{

  config(
    materialized='table',
    )

}}

{% set cdm_values = get_cdm_values() %}
with get_cdm_source as (
  select
    '{{ cdm_values.cdm_source_name }}' as cdm_source_name,
    '{{ cdm_values.cdm_source_abbreviation }}' as cdm_source_abbreviation,
    '{{ cdm_values.cdm_holder }}' as cdm_holder,
    '{{ cdm_values.source_description }}' as source_description,
    '{{ cdm_values.source_documentation_reference }}' as source_documentation_reference,
    '{{ cdm_values.cdm_etl_reference }}' as cdm_etl_reference,
    '{{ cdm_values.source_release_date }}' as source_release_date,
    '{{ cdm_values.cdm_version }}' as cdm_version,
    vocabulary_concept_id as cdm_version_concept_id,
    vocabulary_version as vocabulary_version
    FROM {{ source('omop_data', 'vocabulary') }} p
    WHERE lower(p.vocabulary_id) = 'language'
)

select * from get_cdm_source