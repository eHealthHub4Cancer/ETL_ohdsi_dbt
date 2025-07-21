{% macro generate_plausible_date(date_col) %}
    -- This macro generates a death date based on the provided date column
    -- ensures the date is not in the future and is not null
    -- It also assumes the date_col is in a format that can be cast to date
    least(coalesce({{ date_col }}::date, current_date), current_date)
{% endmacro %}

-- using this to ensure clinical events occurs after or on birthdate
{% macro validate_dates(date_col_1, date_col_2) %}
    -- This macro validates that the first date is not after the second date
    -- date_col_1 is expected to be same or above date_col_2
    least(
        coalesce(greatest({{ date_col_1 }}::date , {{ date_col_2 }}::date), current_date), current_date
        )
                                              
{% endmacro %}


{% macro validate_death_date(initial_date, death_date) %}
    -- This ensure that the event occur before or on the death date
    -- It returns the initial date if it is before or on the death date, otherwise it
    -- returns the death date

    least(
        coalesce({{ initial_date }}::date, current_date), 
        coalesce({{ death_date }}::date, current_date)
    )

{% endmacro %}


-- defining all constants
{% macro etl_constants() %}
    -- This macro defines all constants used in the ETL process
    -- It returns a dictionary of constants
    {% set constants = {
        'obser_window': 30,
        'drug_gap': 30
    } %}
    {{ return(constants) }}
{% endmacro %}

{% macro generate_stable_id(id_column) %}
    -- This macro generates a stable person_id based on the ehealth_id
    -- It uses the encrypt_id macro to hash the ehealth_id
    abs(hashtext(
        {{ id_column }}
    ))::bigint
{% endmacro %}