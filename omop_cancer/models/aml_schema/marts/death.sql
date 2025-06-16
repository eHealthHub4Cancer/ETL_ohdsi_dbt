with source_data as (
    select * from {{ ref('stg__death') }}
),

final as (
    {% set death_expr = validate_dates('c.death_date', 'p.birth_datetime::date') %}
    select
        c.person_id,
        c.death_type_concept_id,
        c.cause_concept_id,
        {{ generate_default_death_values() }},
        {{ death_expr }}::date as death_date,
        {{ death_expr }}::timestamp as death_datetime 
    from source_data c 
    inner join {{ ref('stg__person') }} p
        on c.person_id = p.person_id
)

select * from final