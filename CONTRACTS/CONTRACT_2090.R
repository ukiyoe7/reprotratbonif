library(DBI)
library(tidyverse)
library(lubridate)
library(googlesheets4)

con2 <- dbConnect(odbc::odbc(), "reproreplica", timeout = 10)


#CURRENT MONTH

result_2090 <- dbGetQuery(con2,"
  
WITH FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','R','SR')),

PED AS (SELECT ID_PEDIDO,PEDID.CLICODIGO,PEDDTBAIXA FROM PEDID 
INNER JOIN FIS ON PEDID.FISCODIGO1=FIS.FISCODIGO
   WHERE PEDDTBAIXA
     BETWEEN DATEADD(MONTH, -1, CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) + 1)
      AND CURRENT_DATE - EXTRACT(DAY FROM CURRENT_DATE) AND 
     PEDSITPED<>'C' AND 
      PEDLCFINANC IN ('S','L','N') AND
        CLICODIGO IN (2090))

SELECT CLICODIGO,PEDDTBAIXA,SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA 
 FROM PDPRD PD
  INNER JOIN PED P ON P.ID_PEDIDO=PD.ID_PEDIDO
  GROUP BY 1,2

")

result_2090 %>% summarize(v=sum(VRVENDA))

rule_2090 <- range_read("1fweQEqi1cS6Itw_feYmaR5frYjlUo57wnhWppcUq_V4",sheet = "PARAM1")



trat_qtd_2090 <- rule_2090 %>% mutate(A=result_2090 %>% summarize(v=sum(VRVENDA))) %>% 
               mutate(B=A-VALOR) %>%  filter(B>0) %>% 
                  filter(B==min(B)) %>% .[,c(1,3)] %>% as.data.frame() %>%  rename(TBPPCOVENDA=QTD) 


if(is.na(trat_qtd_2090))

z <- data.frame(TBPCODIGO=NA,TBPPCOVENDA=NA)

for (i in 1:nrow(trat_qtd_2090)) {
  z[i,] <- trat_qtd_2090[i,]
queryz <- paste("UPDATE TBPPRODU SET TBPPCOVENDA=",z[i,"TBPPCOVENDA"]," WHERE TBPCODIGO=",z[i,"TBPCODIGO"]," AND PROCODIGO='CONTTB'; ",sep = "")
print(queryz)
}

dbSendQuery(con3,queryz)








