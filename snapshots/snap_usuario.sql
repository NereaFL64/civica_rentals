{% snapshot snap_usuario %}

{{
    config(
        target_schema='GOLD',
        unique_key='id_usuario',
        strategy='check',
        check_cols=[
            'nombre',
            'email',
            'fecha_registro',
            'id_ubicacion',
            'telefono',
            'origen_alta'
        ],
        invalidate_hard_deletes=True
    )
}}

SELECT
      id_usuario
    , nombre
    , email
    , fecha_registro
    , id_ubicacion
    , telefono
    , origen_alta

FROM {{ ref('int_usuario') }}

{% endsnapshot %}