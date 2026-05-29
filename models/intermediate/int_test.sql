{{
    config(
        enabled=var('dbt_observability:marts_enabled', true)
    )
}}
select
    command_invocation_id,
    node_id,
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=1) }} as resource_type,
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=2) }} as project,
    {{ dbt.split_part(string_text='node_id', delimiter_text="'.'", part_number=3) }} as resource_name,
    run_started_at,
    name,
    description,
    depends_on_nodes,
    package_name,
    test_path,
    {{ dbt.split_part(string_text='test_path', delimiter_text="'/'", part_number=2) }} as test_layer,
    tags,
    test_metadata,
    -- parsed test_metadata attributes (see macros/json_extract_scalar.sql)
    {{ json_extract_scalar('test_metadata', ['namespace']) }} as test_package,
    {{ json_extract_scalar('test_metadata', ['name']) }} as test_name,
    {{ json_extract_scalar('test_metadata', ['kwargs', 'model']) }} as test_model,
    -- prefer the manifest's first-class column_name (set for every column-attached
    -- test regardless of kwargs); fall back to kwargs for data predating that capture
    coalesce(
        nullif(column_name, ''),
        {{ json_extract_scalar('test_metadata', ['kwargs', 'column_name']) }}
    ) as test_column,
    -- combination_of_columns is a JSON array; extract it then strip [ ] " to a plain list
    {{ dbt.replace(
        dbt.replace(
            dbt.replace(
                json_extract_array('test_metadata', ['kwargs', 'combination_of_columns']),
                "'\"'", "''"
            ),
            "'['", "''"
        ),
        "']'", "''"
    ) }} as test_combination_of_columns,
    {{ json_extract_scalar('test_metadata', ['kwargs', 'value']) }} as test_column_value,
    {{ dbt.safe_cast(json_extract_scalar('test_metadata', ['kwargs', 'min_value']), dbt.type_int()) }}
        as test_column_min_value,
    {{ dbt.safe_cast(json_extract_scalar('test_metadata', ['kwargs', 'max_value']), dbt.type_int()) }}
        as test_column_max_value,
    {{ json_extract_scalar('test_metadata', ['kwargs', 'from_condition']) }}
        as test_relationship_from_model_condition,
    coalesce(
        {{ json_extract_scalar('test_metadata', ['kwargs', 'to']) }},
        {{ json_extract_scalar('test_metadata', ['kwargs', 'compare_model']) }}
    ) as test_to_model,
    {{ json_extract_scalar('test_metadata', ['kwargs', 'field']) }} as test_relationship_to_field,
    {{ json_extract_scalar('test_metadata', ['kwargs', 'to_condition']) }}
        as test_relationship_to_model_condition,
    {{ json_extract_scalar('test_metadata', ['kwargs', 'expression']) }} as test_column_expression
from {{ ref('dbt_observability_marts', 'stg_test') }}
