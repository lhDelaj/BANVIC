with 
    agencias as (
        select 
            cast(cod_agencia as int) as id_agencia
            ,cast(nome as string) as nome_agencia
            ,cast(endereco as string) as endereco
            ,cast(cidade as string) as cidade
            ,cast(uf as string) as estado_agencia
            ,cast(data_abertura as date) as data_abertura
            ,cast(tipo_agencia as string) as tipo_agencia
            
        from {{ ref('agencias') }}
    )

    select * from agencias