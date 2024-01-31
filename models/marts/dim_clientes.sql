with 
    clientes as (
        select * 
        from {{ ref('erp_banvic_clientes') }}
    )

    , contas as (
        select *
        from {{ ref('erp_banvic_contas') }}
    )
    , propostas_credito as (
        select * 
        from {{ ref('erp_banvic_propostas_credito') }}
    )
    , dim_clientes as (
        select clientes.id_cliente
             ,clientes.nome_completo
             ,tipo_cliente
             ,data_inclusao
             ,ano_nascimento
             ,estado_cliente
             ,idade
             ,id_agencia
             ,contas.id_colaborador as colaborador_responsavel_conta
             ,saldo
             ,data_ultimo_lancamento
             ,COUNT(DISTINCT propostas_credito.id_proposta) as numero_propostas_credito
             ,COUNT(DISTINCT CASE WHEN propostas_credito.status = 'Aprovada' THEN id_proposta END) as numero_propostas_credito_aprovadas
        from clientes
        left join contas on clientes.id_cliente = contas.id_conta
        left join propostas_credito on propostas_credito.id_cliente = clientes.id_cliente
        group by clientes.id_cliente,
            nome_completo,
            tipo_cliente,
            data_inclusao,
            ano_nascimento,
            estado_cliente,
            idade,
            id_agencia,
            contas.id_colaborador,
            saldo,
            data_ultimo_lancamento
        order by clientes.id_cliente
    )

    select * 
        , CASE 
            when numero_propostas_credito_aprovadas <> 0 THEN (numero_propostas_credito_aprovadas/numero_propostas_credito)*100
            else null
            end as Taxa_aprovacao_credito
        from dim_clientes
    
   
