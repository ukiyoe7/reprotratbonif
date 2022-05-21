## CONTROLE DE ACORDOS DE TRATAMENTOS BONIFICADOS
## SANDRO JAKOSKA 20.05.2022
# BE HAPPY, LIFE WILL END ONE DAY


## LIBRARIES

library(DBI)
library(dplyr)
library(lubridate)
library(googlesheets4)


## DB CONNECTION

con2 <- dbConnect(odbc::odbc(), "reproreplica")


## LISTA TRATAMENTOS BONIFICADOS ================================================

acordo_trat <- dbGetQuery(con2,"
WITH CLI AS (SELECT C.CLICODIGO, CLINOMEFANT,GCLCODIGO GRUPO,SETOR FROM CLIEN C
               LEFT JOIN (SELECT CLICODIGO,ZODESCRICAO SETOR FROM ENDCLI ED
               INNER JOIN (SELECT ZOCODIGO,ZODESCRICAO FROM ZONA)Z ON ED.ZOCODIGO=Z.ZOCODIGO
               WHERE ENDFAT='S')E 
               ON C.CLICODIGO=E.CLICODIGO
               WHERE CLICLIENTE='S'),

CLITB AS (SELECT TBPCODIGO,
                   CLINOMEFANT,
                    GRUPO,SETOR,
                     CLITBP.CLICODIGO FROM CLITBP 
                      INNER JOIN CLI ON CLI.CLICODIGO=CLITBP.CLICODIGO),

TOTACORDO AS (SELECT DISTINCT T.TBPCODIGO,TBPPCOVENDA
FROM TBPPRODU T
INNER JOIN CLITB C ON T.TBPCODIGO=C.TBPCODIGO
WHERE PROCODIGO='CONTTB')

SELECT CLICODIGO,
         CLINOMEFANT CLIENTE, 
            GRUPO,T.TBPCODIGO COD_TABELA, 
             SETOR,
              TBPDESCRICAO DESCRICAO,
               TBPDTINICIO INICIO,TBPDTVALIDADE VALIDADE, TBPSITUACAO SITUACAO ,TBPPCOVENDA TOTAL_ACORDO
FROM TABPRECO T
INNER JOIN CLITB ON T.TBPCODIGO=CLITB.TBPCODIGO 
LEFT JOIN TOTACORDO TT ON T.TBPCODIGO=TT.TBPCODIGO
WHERE TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO%'
AND TBPDTVALIDADE >='YESTERDAY'
ORDER BY  COD_TABELA DESC
") 


## EXTRATO TRATAMENTOS BONIFICADOS ================================================

extrat <- dbGetQuery(con2,"
  EXECUTE BLOCK RETURNS (CLICODIGO INT,
                          NOME VARCHAR(50),
                           GRUPO INT,
                            SETOR VARCHAR(35),
                             ID_PEDIDO VARCHAR(35),
                              EMISSAO DATE,
                               MES INT,
                                PROCODIGO VARCHAR(35),
                                DESCRICAO VARCHAR(50),
                                 QTD DECIMAL(15,2), 
                                  VRVENDA DECIMAL(15,2))
AS DECLARE VARIABLE CLI INT;
BEGIN
FOR

SELECT DISTINCT CL.CLICODIGO FROM CLITBP CL
INNER JOIN (SELECT DISTINCT TBPCODIGO FROM TABPRECO WHERE TBPDTVALIDADE>='TODAY' 
            AND TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO%')T ON T.TBPCODIGO=CL.TBPCODIGO

INTO : CLI

DO
BEGIN
FOR
 
  WITH  CL AS 
  (SELECT C.CLICODIGO,CLINOMEFANT,GCLCODIGO,ZODESCRICAO FROM CLIEN C
   LEFT JOIN (SELECT CLICODIGO,E.ZOCODIGO,ZODESCRICAO FROM ENDCLI E
INNER JOIN (SELECT ZOCODIGO,ZODESCRICAO FROM ZONA)Z ON E.ZOCODIGO=Z.ZOCODIGO
WHERE ENDFAT='S') ED ON C.CLICODIGO=ED.CLICODIGO
  WHERE CLICLIENTE='S' AND C.CLICODIGO=:CLI),
  
   TAB AS 
  (SELECT DISTINCT C.TBPCODIGO
    FROM CLITBP C
    INNER JOIN (SELECT TBPCODIGO,TBPDTINICIO,TBPDTVALIDADE FROM TABPRECO WHERE TBPDTVALIDADE>='TODAY' 
            AND TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO%')A ON C.TBPCODIGO=A.TBPCODIGO
            WHERE CLICODIGO=:CLI),
  
  FIS AS 
  (SELECT FISCODIGO 
    FROM TBFIS WHERE FISTPNATOP IN ('V','SR','R')),
  
  PED AS 
  (SELECT ID_PEDIDO,P.CLICODIGO,CLINOMEFANT,GCLCODIGO,PEDDTEMIS,ZODESCRICAO
    FROM PEDID P
    INNER JOIN CL ON P.CLICODIGO=CL.CLICODIGO
    INNER JOIN FIS ON P.FISCODIGO1=FIS.FISCODIGO
    WHERE 
    PEDDTEMIS >= DATEADD(-120 DAY TO CURRENT_DATE)
    AND PEDSITPED<>'C'),
  
  PROD AS 
  (SELECT DISTINCT PROCODIGO
    FROM TBPPRODU T
    INNER JOIN TAB ON TAB.TBPCODIGO=T.TBPCODIGO),
  
  PED_PROMO_PAP AS 
  (SELECT P1.ID_PEDIDO ID_PEDIDO_PROMO 
    FROM PDPRD P1
    INNER JOIN PED ON P1.ID_PEDIDO=PED.ID_PEDIDO
    WHERE PROCODIGO='PAP'),
  
  PED_PROMO_PLUGIN AS 
  (SELECT ID_PEDIDPROMOCAO ID_PEDIDO_PROMO 
    FROM PEDIDPROMO P2
    INNER JOIN PED ON P2.ID_PEDIDPROMOCAO=PED.ID_PEDIDO),
  
  PED_PROMO_UNION AS 
  (SELECT ID_PEDIDO_PROMO 
    FROM PED_PROMO_PAP UNION
    SELECT ID_PEDIDO_PROMO 
    FROM PED_PROMO_PLUGIN)
  
SELECT CLICODIGO,
        CLINOMEFANT,
         GCLCODIGO,
          ZODESCRICAO,
           PD.ID_PEDIDO,
             PEDDTEMIS,
              EXTRACT(MONTH FROM PEDDTEMIS) MES,
               PD.PROCODIGO,
               PDPDESCRICAO,
                PDPQTDADE QTD,
                 PDPUNITLIQUIDO VRVENDA
                  FROM PDPRD PD
                  INNER JOIN PED P ON PD.ID_PEDIDO=P.ID_PEDIDO
                  INNER JOIN PROD ON PD.PROCODIGO=PROD.PROCODIGO
                  LEFT OUTER JOIN PED_PROMO_UNION PMU ON PD.ID_PEDIDO=PMU.ID_PEDIDO_PROMO
                  WHERE 
                  ID_PEDIDO_PROMO IS NULL 
                  GROUP BY 1,2,3,4,5,6,7,8,9,10,11 HAVING PDPUNITLIQUIDO<=1
                  
                  
                INTO :CLICODIGO,
                      :NOME,
                       :GRUPO,
                        :SETOR,
                         :ID_PEDIDO,
                          :EMISSAO,
                           :MES,
                            :PROCODIGO,
                             :DESCRICAO,
                              :QTD,
                               :VRVENDA
  
  DO BEGIN
  
  SUSPEND;
  
  END
  END
  END")



## === ACORDOS RECORRENTES =======================================================


## LOJAS =========


acordo_recorr_loja <- acordo_trat %>% filter(DESCRICAO=="TRATAMENTO BONIFICADO R") %>% 
  filter(is.na(GRUPO))


extrat_recorr_loja <- extrat %>% group_by(CLICODIGO) %>% 
                          filter(floor_date(EMISSAO,"month")==floor_date(Sys.Date(),"month")) %>%
                            summarize(USO=sum(QTD))


acordo_recorr_loja_2 <- inner_join(acordo_recorr_loja,extrat_cli,by="CLICODIGO") %>% mutate(SALDO=TOTAL_ACORDO-USO) 



## GRUPOS ===========

## NOME DOS GRUPOS

grupo <- dbGetQuery(con2,"SELECT GCLCODIGO GRUPO,GCLNOME NOME FROM GRUPOCLI")


acordo_recorr_grupo <- acordo_trat %>% filter(DESCRICAO=="TRATAMENTO BONIFICADO R") %>% 
                          filter(!is.na(GRUPO))  %>% 
                            group_by(.[,3:9]) %>% summarize(TOTAL_ACORDO=max(TOTAL_ACORDO)) 


extrat_recorr_grupo <- extrat %>% group_by(GRUPO) %>% filter(!is.na(GRUPO)) %>% 
  filter(floor_date(EMISSAO,"month")==floor_date(Sys.Date(),"month")) %>%
  summarize(USO=sum(QTD)) 


acordo_recorr_grupo_2 <- inner_join(acordo_recorr_grupo,extrat_recorr_grupo,by="GRUPO") %>% 
                         mutate(SALDO=TOTAL_ACORDO-USO) %>% 
                             left_join(.,grupo,by="GRUPO") %>% .[,c(1,11,2:10)] 

## === ACORDOS COM TERMINO =======================================================


## LOJAS ========

acordo_termino_loja <- acordo_trat %>% filter(DESCRICAO=="TRATAMENTO BONIFICADO T") %>%  filter(is.na(GRUPO)) 


extrat_termino_loja <- extrat %>% 
                          group_by(CLICODIGO,EMISSAO,ID_PEDIDO) %>% 
                           summarize(QTD=sum(QTD)) %>% 
                            inner_join(.,acordo_termino_loja %>% 
                              select(CLICODIGO,INICIO,VALIDADE),by="CLICODIGO") %>% 
                                group_by(CLICODIGO) %>% summarize(USO=sum(QTD[EMISSAO>=INICIO & EMISSAO<=VALIDADE]))



acordo_termino_loja_2 <- inner_join(acordo_termino_loja,extrat_termino_loja) %>%  
                             mutate(SALDO=TOTAL_ACORDO-USO) 


## GRUPOS =======

acordo_termino_grupo <-  acordo_trat %>% filter(DESCRICAO=="TRATAMENTO BONIFICADO T") %>% filter(!is.na(GRUPO))  %>% 
  group_by(.[,3:9]) %>% summarize(TOTAL_ACORDO=max(TOTAL_ACORDO))  

extrat_termino_grupo <- extrat %>% group_by(GRUPO,EMISSAO) %>% summarize(QTD=sum(QTD)) %>% 
  inner_join(.,acordo_termino_grupo %>% select(GRUPO,INICIO,VALIDADE),by="GRUPO") %>% 
  group_by(GRUPO) %>% summarize(USO=sum(QTD[EMISSAO>=INICIO & EMISSAO<=VALIDADE]))


acordo_termino_grupo_2 <- inner_join(acordo_termino_grupo,extrat_termino_grupo) %>%  
                               mutate(SALDO=TOTAL_ACORDO-USO) %>% 
                                  left_join(.,grupo,by="GRUPO") %>% .[,c(1,11,2:10)] 


## === WRITE ON GOOGLE ==============================================================


lojas <- union_all(acordo_recorr_loja_2,acordos_termino_loja_2) %>% arrange(VALIDADE,SALDO)


grupos <- union_all(acordo_recorr_grupo_2,acordos_termino_grupo_2)  %>% arrange(VALIDADE,SALDO)


range_write("1FnrTEE_RZyu0qMGB8xYpOlQvg2EFIJdNBbgUG3h4NuY",data=lojas,sheet = "ACORDOS LOJAS",
            range = "A1",reformat = FALSE) 


range_write("1FnrTEE_RZyu0qMGB8xYpOlQvg2EFIJdNBbgUG3h4NuY",data=grupos,sheet = "ACORDOS GRUPOS",
            range = "A1",reformat = FALSE) 









