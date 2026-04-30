{% snapshot snap_producto %}

{{
    config(
        target_schema='GOLD',
        unique_key='id_producto',
        strategy='check',
        check_cols=[
            'nombre',
            'descripcion',
            'precio_dia',
            'id_usuario',
            'id_categoria',
            'estado_producto'
        ],
        invalidate_hard_deletes=True
    )
}}

SELECT
      id_producto
    , nombre
    , descripcion
    , precio_dia
    , id_usuario
    , id_categoria
    , estado_producto

FROM {{ ref('int_producto') }}

{% endsnapshot %}