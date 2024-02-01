with 
    propostas_credito as(
        select *
        from {{ ref('erp_banvic_propostas_credito') }}
    )
    

    , dim_colaboradores as (
    select * 
    from {{ ref('dim_colaboradores') }}

    )

    , join_tabelas as (
        select propostas_credito.id_proposta
             , propostas_credito.id_cliente
             , propostas_credito.id_colaborador
             , propostas_credito.data_entrada_proposta
             , propostas_credito.taxa_juros_mensal
             , propostas_credito.valor_proposta
             , propostas_credito.valor_entrada
             , propostas_credito.valor_prestacao
             , propostas_credito.quantidade_parcelas
             , propostas_credito.carencia
             , propostas_credito.status
             , dim_colaboradores.id_agencia
             ,(propostas_credito.valor_proposta-propostas_credito.valor_entrada) as valor_financiado
             ,(quantidade_parcelas*valor_prestacao) AS MontanteFinal
             ,((quantidade_parcelas*valor_prestacao)-(propostas_credito.valor_proposta-propostas_credito.valor_entrada)) as lucro
             ,(((quantidade_parcelas*valor_prestacao)-(propostas_credito.valor_proposta-propostas_credito.valor_entrada))/(propostas_credito.valor_proposta-propostas_credito.valor_entrada))*100 as margem_lucro
        from propostas_credito
        left join dim_colaboradores on dim_colaboradores.id_colaborador = propostas_credito.id_colaborador
    )

    
    , generate_key as (
    select  
        row_number() over(order by id_proposta) as sk_proposta
        , *
    from join_tabelas
    
    )


    select *
    from generate_key


