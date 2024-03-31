{# 
  This macro retrieves a specific condition code based on the provided parameters.
  It dynamically generates and executes a SQL query to fetch the value from a specified column in a given table.

  Parameters:
  - table: Name of the table to query (string).
  - param: Name of the parameter to filter on, typically the condition name (string).
  - option: Name of the column from which to retrieve the value, e.g., the column storing BigQuery condition codes (string).

  Usage Example:
  {{ macro_get_param_value(
      table='ghseet_marketing_channels_mapping_conditions', 
      param='search_paid_condition',
      option='condition_code_bigquery'
  ) }}
#}

{% macro macro_get_param_value(table, param, option) %}

{% set query %}
    SELECT {{ option }}
    FROM {{ table }}
    WHERE condition_name = '{{ param }}'
{% endset %}

{{ return(run_query(query).first().first()) }}

{% endmacro %}