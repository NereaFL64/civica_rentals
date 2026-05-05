{{
  config(
    materialized='incremental',
    unique_key='id_resena',
    incremental_strategy='merge'
  )
}}

WITH resenas AS (

    SELECT *
    FROM {{ ref('int_resena') }}

),

final AS (

    SELECT
          id_resena
        , id_alquiler
        , id_producto
        , rating
        , comentario
        , fecha_resena
        , AI_SENTIMENT(comentario) AS sentimiento

    FROM resenas

    {% if is_incremental() %}
    WHERE fecha_resena > (
        SELECT COALESCE(MAX(fecha_resena), '1900-01-01')
        FROM {{ this }}
    )
    {% endif %}

)

SELECT *
FROM final