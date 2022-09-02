## Bibliotecas necessárias

library(DBI)
library(dplyr)


## conexão com banco replica 

con2 <- dbConnect(odbc::odbc(), "reproreplica")



#sql que extrai os dados dos acordos de tratamentos 

acordos_trat <- dbGetQuery(con2,"
WITH TABPROMO AS(SELECT TBPCODIGO
                         FROM TABPRECO
                          WHERE TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO%'),
                       
     CLI AS (SELECT CLICODIGO, 
                     GCLCODIGO GRUPO
                      FROM CLIEN
                       WHERE CLICLIENTE='S'),
         
      CLITB AS (SELECT CTB.TBPCODIGO,
                        GRUPO,
                         CTB.CLICODIGO 
                          FROM CLITBP CTB
                           INNER JOIN TABPROMO TPM ON CTB.TBPCODIGO=TPM.TBPCODIGO
                            LEFT JOIN CLI C ON C.CLICODIGO=CTB.CLICODIGO),
                            
         TOTACORDO AS (SELECT DISTINCT T.TBPCODIGO,
                               TBPPCOVENDA
                                 FROM TBPPRODU T
                                  INNER JOIN CLITB C ON T.TBPCODIGO=C.TBPCODIGO
                                   WHERE PROCODIGO='CONTTB')
                                   
           SELECT CLICODIGO, 
                   GRUPO,
                    T.TBPCODIGO COD_TABELA, 
                     RIGHT(TBPDESCRICAO,1) TIPO,
                      TBPDTINICIO INICIO,
                       TBPDTVALIDADE VALIDADE, 
                        TBPSITUACAO SITUACAO,
                         TBPPCOVENDA TOTAL_ACORDO
                          FROM TABPRECO T
                           INNER JOIN CLITB CT ON T.TBPCODIGO=CT.TBPCODIGO 
                            LEFT JOIN TOTACORDO TT ON T.TBPCODIGO=TT.TBPCODIGO
                             ORDER BY  COD_TABELA DESC
                              ") 

## sql que gera a consulta de contagem dos acordo de tratamentos com data de incio e termino

trat_t_qtd <- dbGetQuery(con2,"
  EXECUTE BLOCK RETURNS (CLICODIGO INT,
                          NOME VARCHAR(50),
                           GRUPO INT,
                            ID_PEDIDO VARCHAR(30),
                             FISCODIGO VARCHAR(30), 
                              EMISSAO DATE,
                               PROCODIGO VARCHAR(30),
                                QTD DECIMAL(15,2), 
                                 VRVENDA DECIMAL(15,2))
  
  AS DECLARE VARIABLE CLIENTE INT;
   DECLARE VARIABLE TBCODIGO INT;
  
  BEGIN
  FOR
  
  WITH TB AS (SELECT TBPCODIGO
  FROM TABPRECO 
  WHERE TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO T%' AND TBPDTVALIDADE>='TODAY')
  
  SELECT DISTINCT CLICODIGO,C.TBPCODIGO 
  FROM CLITBP C
  INNER JOIN TB T ON C.TBPCODIGO=T.TBPCODIGO
  INTO :CLIENTE , :TBCODIGO 
  DO
  BEGIN
  FOR
  
  WITH FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','SR','R')),
  
  CLI AS (SELECT CLICODIGO,
                  CLINOMEFANT,
                   GCLCODIGO 
                    FROM CLIEN 
                     WHERE CLICODIGO= :CLIENTE),
  
  PED AS (SELECT ID_PEDIDO,
                    P.CLICODIGO,
                     CLINOMEFANT,
                      GCLCODIGO,
                       PEDDTEMIS
                        FROM PEDID P
                         INNER JOIN FIS ON P.FISCODIGO1=FIS.FISCODIGO
                          INNER JOIN CLI ON P.CLICODIGO=CLI.CLICODIGO
                           WHERE PEDDTEMIS BETWEEN (SELECT T.TBPDTINICIO FROM TABPRECO T
                            INNER JOIN CLITBP C ON C.TBPCODIGO=T.TBPCODIGO 
                             WHERE T.TBPCODIGO=:TBCODIGO AND CLICODIGO=:CLIENTE) AND 'TODAY' AND PEDSITPED<>'C'),
  
  TAB AS (SELECT C.TBPCODIGO 
                  FROM CLITBP C
                   INNER JOIN CLI ON C.CLICODIGO=CLI.CLICODIGO
                    INNER JOIN (SELECT TBPCODIGO FROM TABPRECO WHERE  TBPCODIGO=:TBCODIGO)A 
                     ON C.TBPCODIGO=A.TBPCODIGO),
  
  PROD AS (SELECT DISTINCT PROCODIGO,
                   TBPPRODU.TBPCODIGO 
                    FROM TBPPRODU
                     INNER JOIN TAB ON TAB.TBPCODIGO=TBPPRODU.TBPCODIGO),
  
  PED_PROMO_PAP AS (SELECT P1.ID_PEDIDO ID_PEDIDO_PROMO FROM PDPRD P1
                            INNER JOIN PED ON P1.ID_PEDIDO=PED.ID_PEDIDO
                              WHERE PROCODIGO='PAP'),
  
 PED_PROMO_PLUGIN AS (SELECT ID_PEDIDPROMOCAO ID_PEDIDO_PROMO 
                              FROM PEDIDPROMO P2
                               INNER JOIN PED ON P2.ID_PEDIDPROMOCAO=PED.ID_PEDIDO),
  
 PED_PROMO_CONECTA AS (SELECT P3.ID_PEDIDO ID_PEDIDO_PROMO 
                               FROM PDINFOPROMO P3
                                INNER JOIN PED ON P3.ID_PEDIDO=PED.ID_PEDIDO
                                 WHERE PIPPAR=2),
                            
 PED_PROMO_UNION AS (SELECT ID_PEDIDO_PROMO FROM PED_PROMO_PAP UNION
                       SELECT ID_PEDIDO_PROMO FROM PED_PROMO_PLUGIN UNION
                         SELECT ID_PEDIDO_PROMO FROM PED_PROMO_CONECTA),
                    
  PED_PROMO_DISTINCT AS (SELECT DISTINCT ID_PEDIDO_PROMO FROM PED_PROMO_UNION) 
  
  SELECT CLICODIGO,
          CLINOMEFANT,
           GCLCODIGO GRUPO,
            P3.ID_PEDIDO,
             PEDDTEMIS,
              P3.PROCODIGO,
               PDPQTDADE QTD,
                PDPUNITLIQUIDO VRVENDA
                 FROM PDPRD P3
                  INNER JOIN PED ON P3.ID_PEDIDO=PED.ID_PEDIDO
                   INNER JOIN PROD ON P3.PROCODIGO=PROD.PROCODIGO
                    LEFT OUTER JOIN PED_PROMO_DISTINCT ON P3.ID_PEDIDO=PED_PROMO_DISTINCT.ID_PEDIDO_PROMO
                     WHERE ID_PEDIDO_PROMO IS NULL 
                      GROUP BY 1,2,3,4,5,6,7,8 HAVING PDPUNITLIQUIDO<=1 ORDER BY ID_PEDIDO DESC
  
  INTO :CLICODIGO,
        :NOME,
         :GRUPO,
          :ID_PEDIDO,
           :EMISSAO,
            :PROCODIGO,
             :QTD,
              :VRVENDA
  
  DO BEGIN
  
  SUSPEND;
  
  END
  END
  END 
  
  ")

## sql que gera a consulta de contagem dos acordo de tratamentos recorrentes


trat_r_qtd1 <- dbGetQuery(con2,"
  EXECUTE BLOCK RETURNS (CLICODIGO INT,
                           GRUPO INT,
                            ID_PEDIDO VARCHAR(30),
                               PROCODIGO VARCHAR(30),
                                QTD DECIMAL(15,2), 
                                 VRVENDA DECIMAL(15,2))
AS DECLARE VARIABLE CLIENTE INT;
BEGIN
FOR
WITH CLI AS (SELECT DISTINCT CLICODIGO,GCLCODIGO FROM CLIEN WHERE GCLCODIGO IS NULL
             AND CLICLIENTE ='S')
SELECT DISTINCT C.CLICODIGO FROM CLITBP C
INNER JOIN CLI ON C.CLICODIGO=CLI.CLICODIGO
INNER JOIN (SELECT TBPCODIGO FROM TABPRECO WHERE TBPDTVALIDADE>='TODAY' 
            AND TBPDESCRICAO='TRATAMENTO BONIFICADO R')A 
ON C.TBPCODIGO=A.TBPCODIGO
INTO : CLIENTE
DO
BEGIN
FOR
 
  WITH FIS AS 
  (SELECT FISCODIGO 
    FROM TBFIS WHERE FISTPNATOP IN ('V','SR','R')),
  
  CLI AS 
  (SELECT CLICODIGO,GCLCODIGO FROM CLIEN WHERE CLICODIGO=:CLIENTE),
  
  PED AS 
  (SELECT ID_PEDIDO,P.CLICODIGO,PEDDTEMIS,GCLCODIGO
    FROM PEDID P
    INNER JOIN FIS ON P.FISCODIGO1=FIS.FISCODIGO
    INNER JOIN CLI ON P.CLICODIGO=CLI.CLICODIGO
    WHERE PEDDTEMIS 
    BETWEEN DATEADD (-EXTRACT(DAY FROM CURRENT_DATE)+1 DAY TO CURRENT_DATE) AND 'TODAY'
    AND PEDSITPED<>'C'),
  
  TAB AS 
  (SELECT C.TBPCODIGO 
    FROM CLITBP C
    INNER JOIN CLI ON C.CLICODIGO=CLI.CLICODIGO
    INNER JOIN (SELECT TBPCODIGO FROM TABPRECO 
                WHERE TBPDESCRICAO='TRATAMENTO BONIFICADO R')A ON C.TBPCODIGO=A.TBPCODIGO),
  
  PROD AS 
  (SELECT DISTINCT PROCODIGO,TBPPRODU.TBPCODIGO 
    FROM TBPPRODU
    INNER JOIN TAB ON TAB.TBPCODIGO=TBPPRODU.TBPCODIGO),
  
  PED_PROMO_PAP AS 
  (SELECT P1.ID_PEDIDO ID_PEDIDO_PROMO 
    FROM PDPRD P1
    INNER JOIN PED ON P1.ID_PEDIDO=PED.ID_PEDIDO
    WHERE PROCODIGO='PAP'),
  
  PED_PROMO_PLUGIN AS 
  (SELECT ID_PEDIDPROMOCAO ID_PEDIDO_PROMO 
    FROM PEDIDPROMO P2
    INNER JOIN PED ON P2.ID_PEDIDPROMOCAO=PED.ID_PEDIDO),
  
  PED_PROMO_CONECTA AS (SELECT P3.ID_PEDIDO ID_PEDIDO_PROMO FROM PDINFOPROMO P3
                          INNER JOIN PED ON P3.ID_PEDIDO=PED.ID_PEDIDO
                            WHERE PIPPAR=2),
                            
      PED_PROMO_UNION AS (SELECT ID_PEDIDO_PROMO FROM PED_PROMO_PAP UNION
                           SELECT ID_PEDIDO_PROMO FROM PED_PROMO_PLUGIN UNION
                             SELECT ID_PEDIDO_PROMO FROM PED_PROMO_CONECTA),
                    
        PED_PROMO_DISTINCT AS (SELECT DISTINCT ID_PEDIDO_PROMO FROM PED_PROMO_UNION) 
  
SELECT CLICODIGO,
        GCLCODIGO GRUPO,
          P3.ID_PEDIDO,
            P3.PROCODIGO,
             PDPQTDADE QTD,
              PDPUNITLIQUIDO VRVENDA
                  FROM PDPRD P3
                  INNER JOIN PED ON P3.ID_PEDIDO=PED.ID_PEDIDO
                  INNER JOIN PROD ON P3.PROCODIGO=PROD.PROCODIGO
                  LEFT OUTER JOIN PED_PROMO_DISTINCT ON P3.ID_PEDIDO=PED_PROMO_DISTINCT.ID_PEDIDO_PROMO
                  WHERE 
                  ID_PEDIDO_PROMO IS NULL 
                  GROUP BY 1,2,3,4,5,6 HAVING PDPUNITLIQUIDO<=1
                  
                  
                INTO :CLICODIGO,:GRUPO, :ID_PEDIDO, :PROCODIGO,:QTD,:VRVENDA
  
  DO BEGIN
  
  SUSPEND;
  
  END
  END
  END")



trat_r_qtd2 <- dbGetQuery(con2,"
  EXECUTE BLOCK RETURNS (CLICODIGO INT,
                           GRUPO INT,
                            ID_PEDIDO VARCHAR(30),
                               PROCODIGO VARCHAR(30),
                                QTD DECIMAL(15,2), 
                                 VRVENDA DECIMAL(15,2))
AS DECLARE VARIABLE GRUP INT;
BEGIN
FOR
WITH CLI AS (SELECT CLICODIGO,GCLCODIGO FROM CLIEN WHERE GCLCODIGO IS NOT NULL
             AND CLICLIENTE ='S')
SELECT DISTINCT GCLCODIGO FROM CLITBP C
INNER JOIN CLI ON C.CLICODIGO=CLI.CLICODIGO
INNER JOIN (SELECT TBPCODIGO FROM TABPRECO WHERE TBPDTVALIDADE>='TODAY' 
            AND TBPDESCRICAO='TRATAMENTO BONIFICADO R')A 
ON C.TBPCODIGO=A.TBPCODIGO
INTO : GRUP
DO
BEGIN
FOR
 
  WITH FIS AS 
  (SELECT FISCODIGO 
    FROM TBFIS WHERE FISTPNATOP IN ('V','SR','R')),
  
  CLI AS 
  (SELECT CLICODIGO,GCLCODIGO FROM CLIEN WHERE CLICODIGO IN (SELECT CLICODIGO FROM CLIEN WHERE GCLCODIGO=:GRUP)),
  
  PED AS 
  (SELECT ID_PEDIDO,P.CLICODIGO,GCLCODIGO
    FROM PEDID P
    INNER JOIN FIS ON P.FISCODIGO1=FIS.FISCODIGO
    INNER JOIN CLI ON P.CLICODIGO=CLI.CLICODIGO
    WHERE PEDDTEMIS 
    BETWEEN DATEADD (-EXTRACT(DAY FROM CURRENT_DATE)+1 DAY TO CURRENT_DATE) AND 'TODAY'
    AND PEDSITPED<>'C'),
  
  TAB AS 
  (SELECT C.TBPCODIGO 
    FROM CLITBP C
    INNER JOIN CLI ON C.CLICODIGO=CLI.CLICODIGO
    INNER JOIN (SELECT TBPCODIGO FROM TABPRECO 
                WHERE TBPDESCRICAO='TRATAMENTO BONIFICADO R')A ON C.TBPCODIGO=A.TBPCODIGO),
  
  PROD AS 
  (SELECT DISTINCT PROCODIGO,TBPPRODU.TBPCODIGO 
    FROM TBPPRODU
    INNER JOIN TAB ON TAB.TBPCODIGO=TBPPRODU.TBPCODIGO),
  
  PED_PROMO_PAP AS 
  (SELECT P1.ID_PEDIDO ID_PEDIDO_PROMO 
    FROM PDPRD P1
    INNER JOIN PED ON P1.ID_PEDIDO=PED.ID_PEDIDO
    WHERE PROCODIGO='PAP'),
  
  PED_PROMO_PLUGIN AS 
  (SELECT ID_PEDIDPROMOCAO ID_PEDIDO_PROMO 
    FROM PEDIDPROMO P2
    INNER JOIN PED ON P2.ID_PEDIDPROMOCAO=PED.ID_PEDIDO),
  
     PED_PROMO_CONECTA AS (SELECT P3.ID_PEDIDO ID_PEDIDO_PROMO FROM PDINFOPROMO P3
                          INNER JOIN PED ON P3.ID_PEDIDO=PED.ID_PEDIDO
                            WHERE PIPPAR=2),
                            
       PED_PROMO_UNION AS (SELECT ID_PEDIDO_PROMO FROM PED_PROMO_PAP UNION
                           SELECT ID_PEDIDO_PROMO FROM PED_PROMO_PLUGIN UNION
                             SELECT ID_PEDIDO_PROMO FROM PED_PROMO_CONECTA),
                    
         PED_PROMO_DISTINCT AS (SELECT DISTINCT ID_PEDIDO_PROMO FROM PED_PROMO_UNION) 
   
SELECT CLICODIGO,
        GCLCODIGO GRUPO,
          P3.ID_PEDIDO,
            P3.PROCODIGO,
             PDPQTDADE QTD,
              PDPUNITLIQUIDO VRVENDA
                  FROM PDPRD P3
                  INNER JOIN PED ON P3.ID_PEDIDO=PED.ID_PEDIDO
                   INNER JOIN PROD ON P3.PROCODIGO=PROD.PROCODIGO
                    LEFT OUTER JOIN PED_PROMO_DISTINCT ON P3.ID_PEDIDO=PED_PROMO_DISTINCT.ID_PEDIDO_PROMO
                     WHERE 
                      ID_PEDIDO_PROMO IS NULL 
                       GROUP BY 1,2,3,4,5,6 HAVING PDPUNITLIQUIDO<=1
                  
                  
                INTO :CLICODIGO,:GRUPO, :ID_PEDIDO,:PROCODIGO,:QTD,:VRVENDA
  
  DO BEGIN
  
  SUSPEND;
  
  END
  END
  END") 

## Desconecta do banco de réplica

dbDisconnect(con2)


## Junta os objetos das consultas SQL

trat_r_qtd3 <- union(trat_r_qtd1,trat_r_qtd2) 

## resumo dos tratamentos ===================================================


## Clientes com data de termino

trat_t_total <- trat_t_qtd %>% group_by(CLICODIGO) %>%
  filter(is.na(GRUPO)) %>% 
  summarize(Q=sum(QTD))

trat_t_g_total <- trat_t_qtd %>% group_by(GRUPO) %>%
  filter(!is.na(GRUPO)) %>% 
  summarize(Q=sum(QTD))

## Clientes recorrentes

trat_r_total <- trat_r_qtd3 %>% group_by(CLICODIGO) %>%
  filter(is.na(GRUPO)) %>% 
  summarize(Q=sum(QTD))

trat_r_g_total <- trat_r_qtd3 %>% group_by(GRUPO) %>%
  filter(!is.na(GRUPO)) %>% 
  summarize(Q=sum(QTD))


### Calculo de inativação====================================================


## Clientes com data de termino

inactivate_t_cli <- acordos_trat %>%  filter(TIPO=='T' & is.na(GRUPO)) %>% 
  inner_join(.,trat_t_total,by="CLICODIGO") %>% mutate(SALDO=TOTAL_ACORDO-Q) %>% 
  as.data.frame() %>% 
  filter(SALDO<=0) %>% select(COD_TABELA)

inactivate_t_grupo <- acordos_trat %>%  filter(TIPO=='T' & !is.na(GRUPO)) %>% 
  group_by(GRUPO,COD_TABELA) %>% summarize(TOTAL_ACORDO=max(TOTAL_ACORDO)) %>%
  inner_join(.,trat_t_g_total,by="GRUPO") %>% mutate(SALDO=TOTAL_ACORDO-Q) %>% 
  as.data.frame() %>% 
  filter(SALDO<=0) %>% select(COD_TABELA)

## Clientes recorrentes

inactivate_r_cli <- acordos_trat %>%  filter(TIPO=='R' & is.na(GRUPO)) %>% 
  left_join(.,trat_r_total,by="CLICODIGO") %>% mutate(SALDO=TOTAL_ACORDO-Q) %>% 
  as.data.frame() %>% 
  filter(SALDO<=0) %>% select(COD_TABELA)


inactivate_r_grupo <- acordos_trat %>%  filter(TIPO=='R' & !is.na(GRUPO)) %>% 
  group_by(GRUPO,COD_TABELA) %>% summarize(TOTAL_ACORDO=max(TOTAL_ACORDO)) %>%
  left_join(.,trat_r_g_total,by="GRUPO") %>% mutate(SALDO=TOTAL_ACORDO-Q) %>% 
  as.data.frame() %>% 
  filter(SALDO<=0) %>% select(COD_TABELA)


inactivate_all <- rbind(inactivate_t_cli,inactivate_t_grupo,inactivate_r_cli,inactivate_r_grupo) 

inactivate_all

### Cálculo de Ativação====================================================

## Clientes com data de termino

activate_t_cli <- acordos_trat %>%  filter(TIPO=='T' & VALIDADE>=Sys.Date() & is.na(GRUPO)) %>% 
  left_join(.,trat_t_total,by="CLICODIGO") %>% mutate(SALDO=TOTAL_ACORDO-Q) %>% 
  filter(SALDO>0 | is.na(SALDO)) %>% select(COD_TABELA)

activate_t_grupo <- acordos_trat %>%  filter(TIPO=='T' & VALIDADE>=Sys.Date() & !is.na(GRUPO)) %>% 
  group_by(GRUPO,COD_TABELA) %>% summarize(TOTAL_ACORDO=max(TOTAL_ACORDO)) %>%
  left_join(.,trat_t_g_total,by="GRUPO") %>% mutate(SALDO=TOTAL_ACORDO-Q) %>% 
  as.data.frame() %>% 
  filter(SALDO>0 | is.na(SALDO)) %>% select(COD_TABELA)

## Clientes recorrentes

activate_r_cli <- acordos_trat %>%  filter(TIPO=='R' & VALIDADE>=Sys.Date() & is.na(GRUPO)) %>% 
  left_join(.,trat_r_total,by="CLICODIGO") %>% mutate(SALDO=TOTAL_ACORDO-Q) %>% 
  filter(SALDO>0 | is.na(SALDO)) %>% select(COD_TABELA)

activate_r_grupo <- acordos_trat %>%  filter(TIPO=='R' & VALIDADE>=Sys.Date() & !is.na(GRUPO) & TOTAL_ACORDO!=0) %>% 
  group_by(GRUPO,COD_TABELA) %>% summarize(TOTAL_ACORDO=max(TOTAL_ACORDO)) %>%
  left_join(.,trat_r_g_total,by="GRUPO") %>% mutate(SALDO=TOTAL_ACORDO-Q) %>% 
  as.data.frame() %>% 
  filter(SALDO>0 | is.na(SALDO)) %>% select(COD_TABELA)


activate_all <- rbind(activate_t_cli,activate_t_grupo,activate_r_cli,activate_r_grupo) 

activate_all



## conexão com banco producao como admin

con3 <- dbConnect(odbc::odbc(), "repro_prod", timeout = 10)



##  inicia o loop da sql de update =========================================================


x <- data.frame(TBPCODIGO=NA)

for (i in 1:nrow(inactivate_all)) {
  x[i,] <- inactivate_all[i,]
  queryX <- paste("UPDATE TABPRECO SET TBPSITUACAO='I'
                 WHERE TBPCODIGO= ",x[i,"TBPCODIGO"],"",sep = "")
  dbSendQuery(con3,queryX)
}


y <- data.frame(TBPCODIGO=NA)

for (i in 1:nrow(activate_all)) {
  y[i,] <- activate_all[i,]
  queryY <- paste("UPDATE TABPRECO SET TBPSITUACAO='A'
                 WHERE TBPCODIGO= ",y[i,"TBPCODIGO"],"",sep = "")
  dbSendQuery(con3,queryY)
}




##  verifica tabelas que expiraram e atualiza o status para inativo ==========

exp_tb <- acordos_trat %>%  filter(VALIDADE<Sys.Date() & SITUACAO=='A' | TOTAL_ACORDO==0) %>% 
  distinct(COD_TABELA)


z <- data.frame(TBPCODIGO=NA)

for (i in 1:nrow(exp_tb)) {
  z[i,] <- exp_tb[i,]
  queryz <- paste("UPDATE TABPRECO SET TBPSITUACAO='I'
                 WHERE TBPCODIGO= ",z[i,"TBPCODIGO"],"",sep = "")
  dbSendQuery(con3,queryz)
}

## desconecta do banco de produção

dbDisconnect(con3)