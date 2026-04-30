SELECT *
FROM {{ ref('int_resena') }}
WHERE rating NOT BETWEEN 1 AND 5