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

    , transacoes as (
        select *
        from {{ ref('erp_banvic_transacoes') }}
    )

    , join_tabelas as (
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
             ,AVG(transacoes.valor_transacao) as valor_transacao_medio
             ,extract(year from data_ultimo_lancamento) as ano_ultimo_lancamento
        from clientes
        left join contas on clientes.id_cliente = contas.id_conta
        left join propostas_credito on propostas_credito.id_cliente = clientes.id_cliente
        left join transacoes on transacoes.id_conta = contas.id_conta
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
    
     , generate_key as (
        select  
        row_number() over(order by id_cliente) as sk_cliente
        , *
        from join_tabelas
    
    )


    select * 
        , (2024 -ano_ultimo_lancamento)  as anos_inativos
        , CASE 
            when numero_propostas_credito_aprovadas <> 0 THEN (numero_propostas_credito_aprovadas/numero_propostas_credito)*100
            else null
            end as Taxa_aprovacao_credito
        , CASE 
            when (2024 -ano_ultimo_lancamento) > 2 THEN 'Inativa'
            else 'Ativa' 
            end as status_conta     
              from generate_key

    
   
