with
    propostas_credito as (
        select *
        from {{ ref('erp_banvic_propostas_credito') }}
    )

    ,proposta_status as (
        select 
            status
            ,((COUNT(status)/2000)*100 ) as porcentagem_status
            ,AVG(anos_corridos) as media_permanencia_anos
            ,SUM(valor_proposta) as soma_valor_retido
            ,AVG(taxa_juros_mensal) as taxa_juros_mensal_media
            ,AVG(quantidade_parcelas) as quantidade_parcelas_media
        from propostas_credito
        group by status
    )

    ,generate_key as (
    select  
        row_number() over(order by status) as sk_status_proposta
        , *
    from proposta_status
    
    )


    select *
    from generate_key