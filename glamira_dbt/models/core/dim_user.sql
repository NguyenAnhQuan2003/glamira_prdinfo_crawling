SELECT
    user_key,
    email_address,
FROM
    {{ref('stg_raw_user')}}