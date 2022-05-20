## CONTROLE DE ACORDOS DE TRATAMENTOS BONIFICADOS

## bibliotecas

library(DBI)
library(dplyr)
library(lubridate)
library(googlesheets4)


## conexão com banco replica 

con2 <- dbConnect(odbc::odbc(), "reproreplica")


## sql lista acordos

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

View(acordo_trat)


grupo <- dbGetQuery(con2,"SELECT GCLCODIGO GRUPO,GCLNOME NOME FROM GRUPOCLI")


## === ACORDOS RECORRENTES =======================================================


## LOJAS ===============================


acordo_recorr_loja <- acordo_trat %>% filter(DESCRICAO=="TRATAMENTO BONIFICADO R") %>% 
  filter(is.na(GRUPO))


extrat_recorr_loja <- extrat %>% group_by(CLICODIGO) %>% 
                          filter(floor_date(EMISSAO,"month")==floor_date(Sys.Date(),"month")) %>%
                            summarize(USO=sum(QTD))


acordo_recorr_loja_2 <- inner_join(acordo_recorr_loja,extrat_cli,by="CLICODIGO") %>% mutate(SALDO=TOTAL_ACORDO-USO) 

View(acordo_recorr_loja_2)



## GRUPOS ===============================

acordo_recorr_grupo <- acordo_trat %>% filter(DESCRICAO=="TRATAMENTO BONIFICADO R") %>% 
                          filter(!is.na(GRUPO))  %>% 
                            group_by(.[,3:9]) %>% summarize(TOTAL_ACORDO=max(TOTAL_ACORDO)) 


View(acordo_recorr_grupo)


extrat_recorr_grupo <- extrat %>% group_by(GRUPO) %>% filter(!is.na(GRUPO)) %>% 
  filter(floor_date(EMISSAO,"month")==floor_date(Sys.Date(),"month")) %>%
  summarize(USO=sum(QTD)) 



View(extrat_recorr_grupo)

acordo_recorr_grupo_2 <- inner_join(acordo_recorr_grupo,extrat_recorr_grupo,by="GRUPO") %>% 
                         mutate(SALDO=TOTAL_ACORDO-USO) %>% 
                             left_join(.,grupo,by="GRUPO") %>% .[,c(1,11,2:10)] 

View(acordo_recorr_grupo_2)


## === ACORDOS COM TERMINO =======================================================


## LOJAS ===============================

acordo_termino_loja <- acordo_trat %>% filter(DESCRICAO=="TRATAMENTO BONIFICADO T") %>%  filter(is.na(GRUPO)) 


View(acordo_termino_loja)


extrat_termino_loja <- extrat %>% 
                          group_by(CLICODIGO,EMISSAO,ID_PEDIDO) %>% 
                           summarize(QTD=sum(QTD)) %>% 
                            inner_join(.,acordo_termino_loja %>% 
                              select(CLICODIGO,INICIO,VALIDADE),by="CLICODIGO") %>% 
                                group_by(CLICODIGO) %>% summarize(USO=sum(QTD[EMISSAO>=INICIO & EMISSAO<=VALIDADE]))


View(extrat_termnino_loja)

acordo_termino_loja_2 <- inner_join(acordo_termino_loja,extrat_termino_loja) %>%  
                             mutate(SALDO=TOTAL_ACORDO-USO) 


View(acordo_termino_loja_2)

## GRUPOS ===============================

acordo_termino_grupo <-  acordo_trat %>% filter(DESCRICAO=="TRATAMENTO BONIFICADO T") %>% filter(!is.na(GRUPO))  %>% 
  group_by(.[,3:9]) %>% summarize(TOTAL_ACORDO=max(TOTAL_ACORDO))  

View(acordo_termino_grupo)

extrat_termino_grupo <- extrat %>% group_by(GRUPO,EMISSAO) %>% summarize(QTD=sum(QTD)) %>% 
  inner_join(.,acordo_termino_grupo %>% select(GRUPO,INICIO,VALIDADE),by="GRUPO") %>% 
  group_by(GRUPO) %>% summarize(USO=sum(QTD[EMISSAO>=INICIO & EMISSAO<=VALIDADE]))


acordo_termino_grupo_2 <- inner_join(acordo_termino_grupo,extrat_termino_grupo) %>%  
                               mutate(SALDO=TOTAL_ACORDO-USO) %>% 
                                  left_join(.,grupo,by="GRUPO") %>% .[,c(1,11,2:10)] 

View(acordo_termino_grupo_2)


## === WRITE ON GOOGLE ==============================================================


lojas <- union_all(acordo_recorr_loja_2,acordos_termino_loja_2)

View(lojas)


grupos <- union_all(acordo_recorr_grupo_2,acordos_termino_grupo_2)

View(grupos)


range_write("1FnrTEE_RZyu0qMGB8xYpOlQvg2EFIJdNBbgUG3h4NuY",data=lojas,sheet = "ACORDOS LOJAS",
            range = "A1",reformat = FALSE) 


range_write("1FnrTEE_RZyu0qMGB8xYpOlQvg2EFIJdNBbgUG3h4NuY",data=grupos,sheet = "ACORDOS GRUPOS",
            range = "A1",reformat = FALSE) 









