WITH pagos AS (

    SELECT *
    FROM {{ ref('stg_pagos') }}

),

metodos AS (

    SELECT DISTINCT
        TRIM(UPPER(metodo_pago)) AS nombre
    FROM pagos
    WHERE metodo_pago IS NOT NULL

),

final AS (

    SELECT
          {{ dbt_utils.generate_surrogate_key(['nombre']) }} AS id_metodo_pago
        , nombre

    FROM metodos

)

SELECT *
FROM final