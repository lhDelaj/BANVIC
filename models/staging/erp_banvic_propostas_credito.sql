with 
    propostas_credito as (
        select 
            cast(cod_proposta as int) as id_proposta
            ,cast(cod_cliente as int) as id_cliente
            ,cast(cod_colaborador as int) as id_colaborador
            ,DATE(data_entrada_proposta) as data_entrada_proposta
            ,cast(taxa_juros_mensal as numeric) as taxa_juros_mensal
            ,cast(valor_proposta as numeric) as valor_proposta
            ,cast(valor_entrada as numeric) as valor_entrada
            ,cast(valor_prestacao as numeric) as valor_prestacao
            ,cast(quantidade_parcelas as int) as quantidade_parcelas
            ,cast(carencia as int) as carencia
            ,cast(status_proposta as string) as status
        from {{ ref('propostas_credito') }}
    )

    select *
     from propostas_credito