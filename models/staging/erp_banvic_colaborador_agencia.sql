with 
    colaborador_agencia as (
        select 
            cast(cod_colaborador as int) as id_colaborador
            ,cast(cod_agencia as int) as id_agencia
        from {{ ref('colaborador_agencia') }}
    )

    select * from colaborador_agencia