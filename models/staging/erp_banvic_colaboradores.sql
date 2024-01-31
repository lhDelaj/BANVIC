with 
    colaboradores as (
        select 
            cast(cod_colaborador as int) as id_colaborador
            ,cast(primeiro_nome || ' ' || ultimo_nome as string) as nome_completo
            ,extract(year from data_nascimento) as ano_nascimento
            ,RIGHT(endereco, 2) as estado_colaborador

        from {{ ref('colaboradores') }}
    )

    select 
        id_colaborador
        , nome_completo
        , (2024-ano_nascimento) as idade
        , estado_colaborador

    from colaboradores