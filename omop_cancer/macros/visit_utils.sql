{% macro encounter_class(visit_type) %}
    -- This macro returns the encounter class for the visit
    -- It can be used to categorize encounters based on their type
    case 
        when lower(trim({{ visit_type }})) = 'inpatient' then 9201
        when lower(trim({{ visit_type }})) = 'outpatient' then 9202
        when lower(trim({{ visit_type }})) = 'emergency' then 9203
        when lower(trim({{ visit_type }})) = 'urgent care' then 8782
        when lower(trim({{ visit_type }})) = 'telehealth' then 9205
        when lower(trim({{ visit_type }})) = 'home health' then 9206
        else 0 
    end as visit_concept_id
{% endmacro %}