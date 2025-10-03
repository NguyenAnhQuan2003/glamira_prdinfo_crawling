SELECT
    user_key,
    device_id,
    email_address,
    user_agent
FROM
    {{ref('stg_raw_user')}}