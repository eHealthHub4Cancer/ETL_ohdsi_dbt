{% macro encrypt_id(ehealth_id_col) %}
    -- This macro is used to generate a person_id based on the ehealth_id
    -- It uses the encrypt_id macro to hash the ehealth_id
    md5(
        {{ ehealth_id_col }}
    ) 

{% endmacro %}

{% macro generate_stable_person_id(ehealth_id_col) %}
    -- This macro generates a stable person_id based on the ehealth_id
    -- It uses the encrypt_id macro to hash the ehealth_id
    abs(hashtext(
        {{ ehealth_id_col }}
    ))::bigint
{% endmacro %}

{% macro generate_gender_concept_id(gender) %}
    -- This macro generates a gender concept id for the gender passed
    case
        when lower(trim({{ gender }})) = 'm' then 8507
        when lower(trim({{ gender }})) = 'f' then 8532
        when lower(trim({{ gender }})) = 'other' then 8551
        else 0
    end
{% endmacro %}

{% macro generate_date(age) %}
    -- This macro calculates year of birth from age
   extract(year from current_date) - {{ age }} as year_of_birth,
   NULL as month_of_birth,
   NULL as day_of_birth,
   make_timestamp(extract(year from current_date)::int - {{ age }}::int, 1, 1, 0, 0, 0) as birth_datetime
{% endmacro %}

{% macro generate_default_values() %}
    0 as race_concept_id,
    0 as ethnicity_concept_id,
    null as location_id,
    null as provider_id,
    null as care_site_id,
    null as gender_source_concept_id,
    null as race_source_value,
    null as race_source_concept_id,
    null as ethnicity_source_value,
    null as ethnicity_source_concept_id
{% endmacro %}



