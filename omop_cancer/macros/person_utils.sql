{% macro encrypt_id(ehealth_id_col) %}
    -- This macro is used to generate a person_id based on the ehealth_id
    -- It uses the encrypt_id macro to hash the ehealth_id
    encode(digest(
        {{ ehealth_id_col }},
        'sha256'
    ), 'hex')

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
        when lower(trim({{ gender }})) = 'male' then 8507
        when lower(trim({{ gender }})) = 'female' then 8532
        when lower(trim({{ gender }})) = 'other' then 8551
        else 0
    end
{% endmacro %}

{% macro generate_date(age) %}
    -- This macro calculates year of birth from age
(
   extract(year from current_date) - {{ age }} as year_of_birth,
   NULL as month_of_birth,
   NULL as day_of_birth
   NULL as birth_datetime
)
{% endmacro %}



