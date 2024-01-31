with 
    transacoes as(
        select *
        from {{ ref('erp_banvic_transacoes') }}
    )
    
    
    , dim_clientes as (
    select * 
    from {{ ref('dim_clientes') }}

    )
    , fat_transacoes as (
        select id_transacao
            , id_conta
            , data_transacao
            , tipo_transacao
            , valor_transacao
            , dim_clientes.id_agencia
        from transacoes
        left join dim_clientes on dim_clientes.id_cliente = transacoes.id_conta
    )

    select * from fat_transacoes