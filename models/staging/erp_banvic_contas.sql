with 
    contas as (
        select 
            cast(cod_cliente as int) as id_conta
            ,cast (cod_agencia as int) as id_agencia
            ,cast(cod_colaborador as int) as id_colaborador
            ,DATE(data_abertura ) as data_abertura
            ,cast(saldo_disponivel as numeric) as saldo
            ,DATE(data_ultimo_lancamento) as data_ultimo_lancamento
        from {{ ref('contas') }}
    )

    select * from contas