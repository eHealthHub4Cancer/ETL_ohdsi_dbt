with src as (
    select ehealth_id, sex, age 
    from {{ source('source_data', 'aml_personal_information') }}
),

final as (
    select 
        {{ generate_stable_person_id('ehealth_id') }} as person_id,
        {{ generate_gender_concept_id('sex') }} as gender_concept_id,
        {{ encrypt_id('ehealth_id') }} as person_source_value,
        lower(trim(sex)) as gender_source_value,
        {{ generate_date('age') }}
    from src
)

select * from final