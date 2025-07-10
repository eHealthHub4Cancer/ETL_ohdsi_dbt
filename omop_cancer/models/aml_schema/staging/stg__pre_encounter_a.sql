with encounter_a as (
    select 
        sym_date_dx, 
        ehealth_id,
        date_dx,
        'unknown' as visit_type
    from {{ source('source_data', 'aml_diagnosis') }}
    WHERE NOT (sym_date_dx IS NULL AND date_dx IS NULL)
),

final as (
    select 
        {{ generate_stable_person_id('ehealth_id') }} as person_id,
        sym_date_dx as encounter_start_date,
        date_dx as encounter_end_date,
        visit_type
    from encounter_a
    where ehealth_id is not null
)

select * from final