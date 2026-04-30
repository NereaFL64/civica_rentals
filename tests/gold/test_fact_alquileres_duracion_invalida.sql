SELECT *
FROM {{ ref('fact_alquileres') }}
WHERE duracion_dias <= 0