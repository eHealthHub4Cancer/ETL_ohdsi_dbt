with person_death as (
    select 
        ehealth_id,
        to_date(date_death, 'DD/MM/YYYY') as date_death,
        relation_death
    from {{ source('source_data', 'aml_death_information') }}
),

final as (
    {% set death_expr = generate_plausible_date('date_death') %}
    select
        {{ generate_stable_person_id('ehealth_id') }} as person_id,
        {{ death_expr }}::date as death_date,
        {{ death_expr }}::timestamp as death_datetime,
        32817 as death_type_concept_id,
        {{ generate_cause_date('relation_death') }}
    from person_death
)

select * from final

