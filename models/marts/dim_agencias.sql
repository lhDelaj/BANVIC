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
    
    
    , generate_key as (
    select  
        row_number() over(order by id_agencia) as sk_agencia
        , *
    from dim_agencias
    
    )


    select *
    from generate_key