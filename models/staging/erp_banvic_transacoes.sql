with 
    transacoes as (
        select
            cast(cod_transacao as int) as id_transacao
            ,cast(num_conta as int) as id_conta
            ,DATE(data_transacao) as data_transacao
            ,cast(nome_transacao as string) as tipo_transacao
            ,cast(valor_transacao as numeric) as valor_transacao

        from {{ ref('transacoes') }}
    )

    select * from transacoes