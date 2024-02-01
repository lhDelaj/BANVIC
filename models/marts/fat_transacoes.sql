with 
    transacoes as(
        select *
        from {{ ref('erp_banvic_transacoes') }}
    )
    
    
    , dim_clientes as (
    select * 
    from {{ ref('dim_clientes') }}

    )
    , join_tabelas as (
        select id_transacao
             , id_conta
             , data_transacao
             , tipo_transacao
             , valor_transacao
             , dim_clientes.id_agencia
        from transacoes
        left join dim_clientes on dim_clientes.id_cliente = transacoes.id_conta
    )

    
    , generate_key as (
    select  
        row_number() over(order by id_transacao) as sk_transacao
        , *
    from join_tabelas
    
    )


    select *
    from generate_key