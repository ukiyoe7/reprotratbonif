## CONTRACT 1876
## OTICA NANY 
## 

library(DBI)
library(dplyr)
library(googlesheets4)

con2 <- dbConnect(odbc::odbc(), "reproreplica", timeout = 10)
con3 <- dbConnect(odbc::odbc(), "repro_prod", timeout = 10)


#CURRENT MONTH

result_G163 <- dbGetQuery(con2,"
  
WITH FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','R','SR')),

PED AS (SELECT ID_PEDIDO,PEDID.CLICODIGO,PEDDTBAIXA FROM PEDID 
INNER JOIN FIS ON PEDID.FISCODIGO1=FIS.FISCODIGO
   WHERE PEDDTBAIXA
     BETWEEN DATEADD(MONTH, -1, CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) + 1)
      AND CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) AND 
     PEDSITPED<>'C' AND 
      PEDLCFINANC IN ('S','L','N') AND
        CLICODIGO IN (SELECT CLICODIGO FROM CLIEN WHERE GCLCODIGO=163))

SELECT CLICODIGO,PEDDTBAIXA,SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA 
 FROM PDPRD PD
  INNER JOIN PED P ON P.ID_PEDIDO=PD.ID_PEDIDO
  GROUP BY 1,2

")

## GET RULES
rule_G163 <- range_read("1euoU5tdODLmD1p4lWXeOe0lnEHDqKKrrZ4sAe1rPmko",sheet = "PARAM1")

## SET RULES
trat_qtd_G163 <- rule_G163 %>% mutate(A=result_G163 %>% summarize(v=sum(VRVENDA))) %>% 
  mutate(B=A-VALOR) %>%  filter(B>0) %>% 
  filter(B==min(B)) %>% .[,c(1,3)] %>% as.data.frame() %>%  rename(TBPPCOVENDA=QTD) 

## WRITE DB  
z <- data.frame(TBPCODIGO=NA,TBPPCOVENDA=NA)

for (i in 1:nrow(trat_qtd_G163)) {
  z[i,] <- trat_qtd_G163[i,]
  queryG163 <- paste("UPDATE TBPPRODU SET TBPPCOVENDA=",z[i,"TBPPCOVENDA"]," WHERE TBPCODIGO=",z[i,"TBPCODIGO"]," AND PROCODIGO='CONTTB'; ",sep = "")
  dbSendQuery(con3,queryG163)
}

