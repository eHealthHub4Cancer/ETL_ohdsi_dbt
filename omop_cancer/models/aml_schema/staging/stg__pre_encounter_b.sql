with encounter_b as (
    select 
        date_hosp_contact,
        visit_end_date,
        ehealth_id,
        visit_type
    from {{ source('source_data', 'aml_encounters') }}
    WHERE NOT (date_hosp_contact IS NULL AND visit_end_date IS NULL)

),

final as (
    select
        {{ generate_stable_person_id('ehealth_id') }} as person_id,
        date_hosp_contact as encounter_start_date,
        visit_end_date as encounter_end_date,
        visit_type
    from encounter_b
    where ehealth_id is not null
)

select * from final