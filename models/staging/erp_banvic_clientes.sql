with 
    clientes as (
        select 
            cast(cod_cliente as int) as id_cliente
            ,cast(primeiro_nome || ' ' || ultimo_nome as string) as nome_completo
            ,cast(tipo_cliente as string) as tipo_cliente
            ,DATE(data_inclusao) as data_inclusao
            ,extract(year from data_nascimento) as ano_nascimento
            ,RIGHT(endereco, 2) as estado_cliente
        from {{ ref('clientes') }}
    )

   select *
    , (2024-ano_nascimento) as idade
    from clientes