WITH src_pagos AS (

    SELECT *
    FROM {{ source('bronze', 'PAGOS') }}

),

renamed_casted AS (

    SELECT
          TRY_TO_NUMBER(ID_PAGO) AS id_pago
        , TRY_TO_NUMBER(ID_ALQUILER) AS id_alquiler
        , COALESCE(
              TRY_TO_DATE(NULLIF(TRIM(FECHA_PAGO), ''), 'YYYY-MM-DD'),
              TRY_TO_DATE(NULLIF(TRIM(FECHA_PAGO), ''), 'YYYY/MM/DD'),
              TRY_TO_DATE(NULLIF(TRIM(FECHA_PAGO), ''), 'DD-MM-YYYY')
          ) AS fecha_pago
        , COALESCE(
              TRY_TO_DECIMAL(
                  REPLACE(REPLACE(NULLIF(TRIM(IMPORTE), ''), '€', ''), ',', '.'),
                  10, 2
              ),
              0
          ) AS importe
        , COALESCE(UPPER(NULLIF(TRIM(METODO_PAGO), '')), 'DESCONOCIDO') AS metodo_pago
        , COALESCE(UPPER(NULLIF(TRIM(ESTADO_PAGO), '')), 'DESCONOCIDO') AS estado_pago

    FROM src_pagos

),

deduplicated AS (

    SELECT *
    FROM renamed_casted
    WHERE id_pago IS NOT NULL
      AND id_alquiler IS NOT NULL
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY id_pago
        ORDER BY fecha_pago DESC NULLS LAST
    ) = 1

)

SELECT *
FROM deduplicated