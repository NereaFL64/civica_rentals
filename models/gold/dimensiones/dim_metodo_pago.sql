WITH metodos AS (

    SELECT *
    FROM {{ ref('int_metodo_pago') }}

),

final AS (

    SELECT
          id_metodo_pago
        , nombre AS metodo_pago

    FROM metodos

)

SELECT *
FROM final