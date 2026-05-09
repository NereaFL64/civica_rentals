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

sentimiento AS (

    SELECT
          id_resena
        , id_alquiler
        , id_producto
        , rating
        , comentario
        , fecha_resena
        , AI_SENTIMENT(comentario) AS sentimiento_raw

    FROM resenas

),

final AS (

    SELECT
          id_resena
        , id_alquiler
        , id_producto
        , rating
        , comentario
        , fecha_resena
        , sentimiento_raw:categories[0]:sentiment::STRING AS sentimiento

    FROM sentimiento

)

SELECT *
FROM final