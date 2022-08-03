## CONTRACT 709
## OTICA BRUST
## 

library(DBI)
library(dplyr)
library(googlesheets4)

con2 <- dbConnect(odbc::odbc(), "reproreplica", timeout = 10)
con3 <- dbConnect(odbc::odbc(), "repro_prod", timeout = 10)


#CURRENT MONTH

result_709 <- dbGetQuery(con2,"
  
WITH FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','R','SR')),

PED AS (SELECT ID_PEDIDO,PEDID.CLICODIGO,PEDDTBAIXA FROM PEDID 
INNER JOIN FIS ON PEDID.FISCODIGO1=FIS.FISCODIGO
   WHERE PEDDTBAIXA
     BETWEEN DATEADD(MONTH, -1, CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) + 1)
      AND CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) AND 
     PEDSITPED<>'C' AND 
      PEDLCFINANC IN ('S','L','N') AND
        CLICODIGO IN (709))

SELECT CLICODIGO,PEDDTBAIXA,SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA 
 FROM PDPRD PD
  INNER JOIN PED P ON P.ID_PEDIDO=PD.ID_PEDIDO
  GROUP BY 1,2

")

## GET RULES
rule_709 <- range_read("1yyUvQkTZPMvo_qijdF_OMUp-j5-YHlC9KPkyMieR1hU",sheet = "PARAM1")

## SET RULES
trat_qtd_709 <- rule_709 %>% mutate(A=result_709 %>% summarize(v=sum(VRVENDA))) %>% 
  mutate(B=A-VALOR) %>%  filter(B>0) %>% 
  filter(B==min(B)) %>% .[,c(1,3)] %>% as.data.frame() %>%  rename(TBPPCOVENDA=QTD) 

## WRITE DB  
z <- data.frame(TBPCODIGO=NA,TBPPCOVENDA=NA)

for (i in 1:nrow(trat_qtd_709)) {
  z[i,] <- trat_qtd_709[i,]
  query709 <- paste("UPDATE TBPPRODU SET TBPPCOVENDA=",z[i,"TBPPCOVENDA"]," WHERE TBPCODIGO=",z[i,"TBPCODIGO"]," AND PROCODIGO='CONTTB'; ",sep = "")
  dbSendQuery(con3,query709)
}











