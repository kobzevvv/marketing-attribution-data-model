/*
    Extracts a value from an array of structs where each struct contains a 'key' and a 'value' field.

    This macro unnests the array, filters the structs by a specified key, and returns the first non-null 
    value for a specified field within the 'value' struct. It is useful for extracting specific pieces of 
    information from complex nested data structures commonly found in event tracking or log data.

    Parameters:
    - array_name: The name of the array of structs variable.
    - key_name: The key to filter the structs by.
    - value_field: The field within the struct's 'value' from which to extract the value.

    Example usage:
    {{ get_key_from_array_tuple('event_params', 'hashed_email', 'string_value') }}

    This example extracts the 'string_value' from the first struct in 'event_params' array where the key 
    equals 'hashed_email'.
*/



{%  macro macro_get_key_from_array_struct(
        array_name, 
        key_name, 
        value_field
    ) %}

    array(
        select event_param.value.{{ value_field }}                                      as value

        from unnest ({{ array_name }})                                                  as event_param

        where 
            event_param.key = '{{ key_name }}'
        and
            event_param.value.{{ value_field }} IS NOT NULL

    )[safe_offset(0)]

{% endmacro %}