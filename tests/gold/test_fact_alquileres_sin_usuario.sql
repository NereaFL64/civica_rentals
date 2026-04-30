SELECT *
FROM {{ ref('fact_alquileres') }}
WHERE sk_usuario IS NULL