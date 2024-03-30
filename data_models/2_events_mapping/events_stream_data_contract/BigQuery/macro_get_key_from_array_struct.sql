{%  macro macro_get_key_from_array_struct(
        array_name, 
        key_name, 
        value_field
    ) %}

    ARRAY(
        select event_param.value.{{ value_field }}                                      as value

        from unnest ({{ array_name }})                                                  as event_param

        where 
            event_param.key = '{{ key_name }}'
        and
            event_param.value.{{ value_field }} IS NOT NULL

    )[SAFE_OFFSET(0)]

{% endmacro %}