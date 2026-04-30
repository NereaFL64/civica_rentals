SELECT *
FROM {{ ref('fact_alquileres') }}
WHERE sk_producto IS NULL