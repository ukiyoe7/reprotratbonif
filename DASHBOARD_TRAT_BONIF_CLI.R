##  BASE DASHBOARD TRAT BONIF
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

acordo_trat_cli <- dbGetQuery(con2,"
WITH CLI AS (SELECT C.CLICODIGO, CLINOMEFANT,GCLCODIGO GRUPO,SETOR FROM CLIEN C
               LEFT JOIN (SELECT CLICODIGO,ZODESCRICAO SETOR FROM ENDCLI ED
               INNER JOIN (SELECT ZOCODIGO,ZODESCRICAO FROM ZONA)Z ON ED.ZOCODIGO=Z.ZOCODIGO
               WHERE ENDFAT='S')E 
               ON C.CLICODIGO=E.CLICODIGO
               WHERE CLICLIENTE='S' AND (C.CLICODIGO IN (400,46,241,709) OR 
               GCLCODIGO=77)),

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

View(acordo_trat_cli)


## EXTRATO TRATAMENTOS BONIFICADOS ================================================

extrat_cli <- dbGetQuery(con2,"
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
            WHERE (CLICODIGO IN (400,46,241,709) OR CLICODIGO IN (SELECT CLICODIGO FROM CLIEN WHERE GCLCODIGO=77))

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



## === EXTRATO =======================================================

extrat_dashboard <- extrat_cli %>% 
  select(CLICODIGO,GRUPO,EMISSAO,ID_PEDIDO,DESCRICAO,QTD) %>% 
  `colnames<-`(c("CLICODIGO","GRUPO","EMISSAO","ID_PEDIDO","TRATAMENTO","USO"))


## === WRITE ON GOOGLE ==============================================================


range_write("1UMTvehWT18ciPdNJh2jfoZGpe5z_oW1IyBrAuSFFdgA",data=extrat_dashboard,sheet = "DADOS2",
            range = "A:F",reformat = FALSE) 


## === ACORDOS RECORRENTES =======================================================


## LOJAS =========


acordo_recorr_loja_dashboard <- acordo_trat_cli %>% 
  filter(DESCRICAO=="TRATAMENTO BONIFICADO R") %>% 
   filter(is.na(GRUPO))


extrat_recorr_loja_dashboard <- extrat_cli %>% group_by(CLICODIGO) %>% 
  filter(floor_date(EMISSAO,"month")==floor_date(Sys.Date(),"month")) %>%
  summarize(USO=sum(QTD))


acordo_recorr_loja_2_dashboard  <- inner_join(acordo_recorr_loja_dashboard,extrat_recorr_loja_dashboard,by="CLICODIGO") %>% 
  mutate(SALDO=TOTAL_ACORDO-USO)


## GRUPOS =========


acordo_recorr_grupo_dashboard <- acordo_trat_cli  %>% filter(DESCRICAO=="TRATAMENTO BONIFICADO R") %>% 
  filter(!is.na(GRUPO))  %>% 
  group_by(.[,3:9]) %>% summarize(TOTAL_ACORDO=max(TOTAL_ACORDO)) %>% as.data.frame() 



extrat_recorr_grupo_dashboard <- extrat_cli %>% group_by(GRUPO) %>% 
  filter(floor_date(EMISSAO,"month")==floor_date(Sys.Date(),"month")) %>%
  summarize(USO=sum(QTD)) 

acordo_recorr_grupo_2_dashboard  <- inner_join(acordo_recorr_grupo_dashboard,extrat_recorr_grupo_dashboard,by="GRUPO") %>% 
  mutate(SALDO=TOTAL_ACORDO-USO) %>%  mutate(CLICODIGO='') %>% mutate(CLIENTE='') %>%
  .[,c(11,12,1:10)]


acordo_trat_bind <- rbind(acordo_recorr_loja_2_dashboard,acordo_recorr_grupo_2_dashboard)

## === WRITE ON GOOGLE ==============================================================

range_write("1UMTvehWT18ciPdNJh2jfoZGpe5z_oW1IyBrAuSFFdgA",data=acordo_trat_bind,
            sheet = "DADOS",
            range = "A:L",reformat = FALSE) 




