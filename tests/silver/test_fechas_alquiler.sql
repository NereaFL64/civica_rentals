SELECT *
FROM {{ ref('int_alquiler') }}
WHERE fecha_fin < fecha_inicio