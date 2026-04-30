SELECT *
FROM {{ ref('fact_pagos') }}
WHERE importe < 0