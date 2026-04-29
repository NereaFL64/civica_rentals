WITH metodos AS (

    SELECT DISTINCT
          COALESCE(NULLIF(TRIM(metodo_pago), ''), 'DESCONOCIDO') AS nombre
    FROM {{ ref('stg_pagos') }}

),

final AS (

    SELECT
          ROW_NUMBER() OVER (ORDER BY nombre) AS id_metodo_pago
        , nombre
    FROM metodos

)

SELECT *
FROM final