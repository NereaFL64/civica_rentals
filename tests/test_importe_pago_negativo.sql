SELECT *
FROM {{ ref('int_pago') }}
WHERE importe < 0