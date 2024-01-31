with 
    colaboradores as (
        select * 
        from {{ ref('erp_banvic_colaboradores') }}
    )

    , agencias as (
        select *
        from {{ ref('erp_banvic_agencias') }}
    )
    , colaborador_agencia as (
        select * 
        from {{ ref('erp_banvic_colaborador_agencia') }}
    )
    , dim_colaboradores as (
        select colaboradores.id_colaborador
            , colaboradores.nome_completo
            , colaboradores.idade
            , colaboradores.estado_colaborador
            , agencias.id_agencia
            , agencias.nome_agencia
            , agencias.cidade
            , agencias.tipo_agencia
        from colaboradores 
        left join colaborador_agencia on colaborador_agencia.id_colaborador = colaboradores.id_colaborador
        left join agencias on agencias.id_agencia = colaborador_agencia.id_agencia
    )

    select * 
    from dim_colaboradores