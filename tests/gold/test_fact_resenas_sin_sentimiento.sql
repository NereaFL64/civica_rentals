SELECT *
FROM {{ ref('fact_resenas') }}
WHERE sentimiento IS NULL