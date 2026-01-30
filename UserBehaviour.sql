WITH 

tb_transactions AS (

    SELECT IdTransacao,
           IdCliente,
           QtdePontos,
           datetime(substr(dtcriacao,1,19)) AS CriationDate,
           julianday('now') - julianday(substr(dtCriacao,1,10)) AS difDate,
           strftime('%H',substr(dtcriacao,1,19)) AS DtHour
    FROM transacoes
),

tb_transact_consolidation AS

(
SELECT 
    IdCliente,
-- Amount of transactions per client (life D7, D14, D28, D56)
    COUNT(IdTransacao) AS TransactLife,
    COUNT(CASE WHEN difDate <= 7 THEN IdTransacao END) AS D7,
    COUNT(CASE WHEN difDate <= 14 THEN IdTransacao END) AS D14,
    COUNT(CASE WHEN difDate <= 28 THEN IdTransacao END) AS D28,
    COUNT(CASE WHEN difDate <= 56 THEN IdTransacao END) AS D56,
-- Amount of days since last transaction  
    MIN (difDate) AS LastTransactionDay,
-- Total points of each cliente
    SUM(QtdePontos) AS TotalPoints,
-- Total Positive Points
    SUM (CASE WHEN qtdePontos > 0 THEN qtdePontos ELSE 0 END) AS GainedPoints,
-- Total Positive Points per day
    sum (CASE WHEN qtdePontos > 0 AND difdate <=  7 THEN qtdePontos ELSE  0 END) AS QtGainedPointsD7,
    sum (CASE WHEN qtdePontos > 0 AND difdate <= 14 THEN qtdePontos ELSE 0 END) AS QtGainedPointsD14,
    sum (CASE WHEN qtdePontos > 0 AND difdate <= 28 THEN qtdePontos ELSE 0 END) AS QtGainedPointsD28,
    sum (CASE WHEN qtdePontos > 0 AND difdate <= 56 THEN qtdePontos ELSE  0 END) AS QtGainedPointsD56,
-- Total Negative Points per day
    SUM (CASE WHEN qtdePontos < 0 THEN qtdePontos ELSE 0 END) AS LostPoints,
-- Total Negative Points Per day
    sum (CASE WHEN qtdePontos < 0 AND difdate <=  7 THEN qtdePontos else 0 END) AS QtLostPointsD7,
    sum (CASE WHEN qtdePontos < 0 AND difdate <= 14 THEN qtdePontos else 0 END) AS QtLostPointsD14,
    sum (CASE WHEN qtdePontos < 0 AND difdate <= 28 THEN qtdePontos else 0 END) AS QtLostPointsD28,
    sum (CASE WHEN qtdePontos < 0 AND difdate <= 56 THEN qtdePontos else 0 END) AS QtLostPointsD56
FROM tb_transactions
GROUP BY IdCliente
),

-- Client age

tb_cliente AS (
    SELECT IdCliente,
           datetime(substr(dtcriacao,1,19)) AS cDate,
           julianday('now') - julianday(substr(dtcriacao,1,10)) AS clientBaseAge
    FROM clientes
),

-- # Most used product per person

-- JOINING TABLES: TRANSACTION, TRANSACTION PRODUCT, PRODUCT

tb_transacaoProduto AS 

(
SELECT t1.*,
       t3.DescNomeProduto,
       t3.DescCategoriaProduto
FROM tb_transactions AS t1
LEFT JOIN transacao_produto AS t2
ON t1.IdTransacao = t2.IdTransacao
LEFT JOIN produtos AS t3
ON t2.IdProduto = t3.IdProduto
),

-- counting of type products per day
tb_cliente_produto 

AS
(
SELECT IdCliente,
        DescNomeProduto,
        COUNT(*) qtdLife,
        COUNT (CASE WHEN difDate >= 7 THEN IdTransacao END) AS Qt_7,
        COUNT (CASE WHEN difDate >= 14 THEN IdTransacao END) AS Qt_14,
        COUNT (CASE WHEN difDate >= 28 THEN IdTransacao END) AS Qt_28,
        COUNT (CASE WHEN difDate >= 56 THEN IdTransacao END) AS Qt_56
FROM tb_transacaoProduto
GROUP BY IdCliente, DescNomeProduto

),

-- Ranking products per day

tb_cliente_productRn
AS
(
SELECT *,
-- Favorite product per client for life
    row_number() OVER (PARTITION BY IdCliente ORDER BY qtdLife DESC) AS rn_Life,
-- Favorite product per flient D7, d14, d28, d56
    row_number() OVER (PARTITION BY IdCliente ORDER BY Qt_7 DESC) AS rn_Life7,
    row_number() OVER (PARTITION BY IdCliente ORDER BY Qt_14 DESC) AS rn_Life14,
    row_number() OVER (PARTITION BY IdCliente ORDER BY Qt_28 DESC) AS rn_Life28,
    row_number() OVER (PARTITION BY IdCliente ORDER BY Qt_56 DESC) AS rn_Life56

FROM tb_cliente_produto 
),

tb_clienteDia
AS (

SELECT IdCliente,
       strftime('%w',CriationDate) AS dtDia,
       COUNT (*) AS trans_qtt
FROM tb_transactions 
GROUP BY IdCliente,dtDia
),

ranking_cliente AS
(
SELECT *,
    row_number () OVER (PARTITION BY idCliente ORDER BY trans_qtt) AS ranking
FROM tb_clienteDia),

tb_client_period 

AS

(

SELECT 
    IdCliente,
    CASE 
    WHEN DtHour BETWEEN '7' AND '12' THEN 'MORNING'
    WHEN DtHour BETWEEN '13' AND '18' THEN 'AFTERNOON'
    WHEN DtHour BETWEEN '19' AND '24' THEN 'NIGHT'
    WHEN DtHour BETWEEN '24' AND '6' THEN 'DAWN'
    ELSE 'No information'
    END AS houroftheday,
    COUNT(*) AS qtd_transc

FROM tb_transactions
GROUP BY 1,2
),

row_number_period

AS

(
SELECT *,
    ROW_NUMBER () OVER (PARTITION BY Idcliente ORDER BY qtd_transc DESC) as fav_period 
FROM tb_client_period 

),


tb_join AS

(
    SELECT t1.*,
           clientBaseAge,
           t3.DescNomeProduto AS Fav_prodct_Life,
           t4.DescNomeProduto AS Fav_prodct_D7,
           t5.DescNomeProduto AS Fav_prodct_D14,
           t6.DescNomeProduto AS Fav_prodct_D28,
           t7.DescNomeProduto AS Fav_prodct_D56,
           t8.dtDia,
           t9.houroftheday,
    --- how many transactions from a client total happened in the last 28 days? 
           1. * t10.Qt_28 / t10.qtdLife AS trans_last_28
           
    FROM tb_transact_consolidation AS t1

    LEFT JOIN tb_cliente AS t2
    ON t1.IdCliente = t2.IdCliente

    LEFT JOIN tb_cliente_productRn AS t3
    ON t1.IdCliente = t3.IdCliente
    AND t3.rn_Life = 1

    LEFT JOIN tb_cliente_productRn AS t4
    ON t1.IdCliente = t4.IdCliente
    AND t4.rn_Life7 = 1

    LEFT JOIN tb_cliente_productRn AS t5
    ON t1.IdCliente = t5.IdCliente
    AND t5.rn_Life14 = 1

    LEFT JOIN tb_cliente_productRn AS t6
    ON t1.IdCliente = t6.IdCliente
    AND t6.rn_Life28 = 1

    LEFT JOIN tb_cliente_productRn AS t7
    ON t1.IdCliente = t7.IdCliente
    AND t7.rn_Life56 = 1

    LEFT JOIN ranking_cliente AS t8
    ON t1.IdCliente = t8.IdCliente
    AND ranking = 1

    LEFT JOIN row_number_period AS t9
    ON t1.IdCliente = t9.IdCliente
    AND fav_period = 1

    LEFT JOIN tb_cliente_produto AS t10
    ON t1.IdCliente = t10.IdCliente
)

SELECT *
FROM tb_join