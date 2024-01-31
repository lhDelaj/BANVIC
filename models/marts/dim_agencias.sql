with 
    agencias as (
        select * 
        from {{ ref('erp_banvic_agencias') }}
    )

    , dim_agencias as (
        select 
            id_agencia
            ,nome_agencia
            ,endereco
            ,cidade
            ,estado_agencia
            ,data_abertura
            ,tipo_agencia
        from agencias
    )
select * 
from dim_agencias