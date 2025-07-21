{% macro least_date(date_col_1, date_col_2) %}
    -- This macro returns the least of two date columns
    -- It ensures that the dates are not null and returns the earliest date
    least(coalesce({{ date_col_1 }}::date, CURRENT_DATE), coalesce({{ date_col_2 }}::date, CURRENT_DATE))
{% endmacro %}

{% macro greatest_date(date_col_1, date_col_2) %}
    -- This macro returns the greatest of two date columns
    -- It ensures that the dates are not null and returns the latest date
    greatest(coalesce({{ date_col_1 }}::date, current_date), 
            coalesce({{ date_col_2 }}::date, current_date))
{% endmacro %}

{% macro generate_observation_period_type_id() %}
    -- This macro generates a default observation period type id
    -- It returns 0 as the default value
    32817 as period_type_concept_id
{% endmacro %}