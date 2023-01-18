
## COMMERCIAL AUTOMATION
## TABLE CREATION
## 18.01.2023

library(tidyverse)
library(DBI)

con3 <- dbConnect(odbc::odbc(), "repro_prod", timeout = 10)
con2 <- dbConnect(odbc::odbc(), "reproreplica")


## ---------------- CHECK CLIENTS ---------------------------------------------------------------


clitrat <- dbGetQuery(con2,"SELECT CLICODIGO FROM CLIEN WHERE (GCLCODIGO=71 OR CLICODIGO IN (4571,4483,4496,4528,4536)) AND CLICLIENTE='S'")


inativos <- dbGetQuery(con2,"
SELECT DISTINCT SITCLI.CLICODIGO,SITCODIGO FROM SITCLI
INNER JOIN (SELECT DISTINCT SITCLI.CLICODIGO,MAX(SITDATA)ULTIMA FROM SITCLI
GROUP BY 1)A ON SITCLI.CLICODIGO=A.CLICODIGO AND A.ULTIMA=SITCLI.SITDATA 
INNER JOIN (SELECT DISTINCT SITCLI.CLICODIGO,SITDATA,MAX(SITSEQ)USEQ FROM SITCLI
GROUP BY 1,2)MSEQ ON A.CLICODIGO=MSEQ.CLICODIGO AND MSEQ.SITDATA=A.ULTIMA 
AND MSEQ.USEQ=SITCLI.SITSEQ WHERE SITCODIGO=4
")


C1 <- anti_join(clitrat,inativos,by="CLICODIGO") 


## ---------------- INSERT TABLES ---------------------------------------------------------------


## get max number 

tbpcodigo_max <- dbGetQuery(con2,"SELECT MAX(TBPCODIGO) FROM TABPRECO") %>% as.numeric(.)

## number of tables

ntables <- count(C1) %>% as.numeric(.)


## select missing table numbers

T1 <- anti_join(data.frame(TBPCODIGO=seq(1,tbpcodigo_max)), 
          dbGetQuery(con2,"SELECT TBPCODIGO FROM TABPRECO") %>% arrange(TBPCODIGO),
          by="TBPCODIGO"
          ) %>% head(.,ntables)

## create data frame

T2 <- data.frame(TBPDESCRICAO="TRATAMENTO BONIFICADO R",TBPTIPO="N",TBPFECH="N",TBPDTVALIDADE="31.12.2023",TBPSITUACAO="A",TBPTABCOMB="N",TBPDTINICIO="18.01.2023")

T3 <- merge(T1,T2)


## loop

x <- data.frame(TBPCODIGO=NA,TBPDESCRICAO=NA,TBPTIPO=NA,TBPFECH=NA,TBPDTVALIDADE=NA,TBPSITUACAO=NA,TBPTABCOMB=NA,TBPDTINICIO=NA)

for (i in 1:nrow(T3)) {
  x[i,] <- T3[i,]
  query1 <- paste("INSERT INTO TABPRECO (TBPCODIGO,TBPDESCRICAO,TBPTIPO,TBPFECH,TBPDTVALIDADE,TBPSITUACAO,TBPTABCOMB,TBPDTINICIO) VALUES (",x[i,"TBPCODIGO"],",'",x[i,"TBPDESCRICAO"],"','",x[i,"TBPTIPO"],"','",x[i,"TBPFECH"],"','",x[i,"TBPDTVALIDADE"],"','",x[i,"TBPSITUACAO"],"','",x[i,"TBPTABCOMB"],"','",x[i,"TBPDTINICIO"],"');", sep = "")
  dbSendQuery(con3,query1)
}



## ---------------- INSERT PRODUCTS ---------------------------------------------------------------


P1 <- dbGetQuery(con2,"SELECT PROCODIGO FROM PRODU WHERE PROTIPO='T' AND PROSITUACAO='A'AND PRODESCRICAO LIKE '%ROCK%'") %>% mutate(PROCODIGO=trimws(PROCODIGO))

P2 <- data.frame(TBPPCOVENDA=1,TBPPCDESCTO2=0,TBPPCOVENDA2=1,TBPPCDESCTO=0) 

P3 <- merge(P1,P2) 

P4 <- merge(T1,P3)

## loop


y <- data.frame(TBPCODIGO=NA,PROCODIGO=NA,TBPPCOVENDA=NA,TBPPCDESCTO2=NA,TBPPCOVENDA2=NA,TBPPCDESCTO=NA)

for (i in 1:nrow(P4)) {
  y[i,] <- P4[i,]
  query2 <- paste("INSERT INTO TBPPRODU (TBPCODIGO,PROCODIGO,TBPPCOVENDA,TBPPCDESCTO2,TBPPCOVENDA2,TBPPCDESCTO) VALUES (",y[i,"TBPCODIGO"],",'",y[i,"PROCODIGO"],"',",y[i,"TBPPCOVENDA"],",",y[i,"TBPPCDESCTO2"],",",y[i,"TBPPCOVENDA2"],",",y[i,"TBPPCDESCTO"],");", sep = "")
  dbSendQuery(con3,query2)
}


## create data frame

P5 <- data.frame(PROCODIGO='CONTTB',TBPPCOVENDA=10,TBPPCDESCTO2="null",TBPPCOVENDA2="null",TBPPCDESCTO="null") 

P6 <- merge(T1,P5)


## loop


z <- data.frame(TBPCODIGO=NA,PROCODIGO=NA,TBPPCOVENDA=NA,TBPPCDESCTO2=NA,TBPPCOVENDA2=NA,TBPPCDESCTO=NA)

for (i in 1:nrow(P6)) {
  z[i,] <- P6[i,]
  query3 <- paste("INSERT INTO TBPPRODU (TBPCODIGO,PROCODIGO,TBPPCOVENDA,TBPPCDESCTO2,TBPPCOVENDA2,TBPPCDESCTO) VALUES (",z[i,"TBPCODIGO"],",'",z[i,"PROCODIGO"],"',",z[i,"TBPPCOVENDA"],",",z[i,"TBPPCDESCTO2"],",",z[i,"TBPPCOVENDA2"],",",z[i,"TBPPCDESCTO"],");", sep = "")
  dbSendQuery(con3,query3)
}



## ---------------- INSERT CLIENTS ---------------------------------------------------------------


C2 <- data.frame(TBPDESCFECH='S',TBPDESC="null",TBPDESC2="null",TBPDTCADASTRO="18.01.2023") 


corder <- c("CLICODIGO","TBPCODIGO","TBPDESCFECH","TBPDESC","TBPDESC2","TBPDTCADASTRO")


C3 <- merge(T1,C2) %>% cbind(.,C1) %>% .[,corder]


## loop

z <- data.frame(CLICODIGO=NA,TBPCODIGO=NA,TBPDESCFECH=NA,TBPDESC=NA,TBPDESC2=NA,TBPDTCADASTRO=NA)


for (i in 1:nrow(C3)) {
  z[i,] <- C3[i,]
  query4 <- paste("INSERT INTO CLITBP (CLICODIGO,TBPCODIGO,TBPDESCFECH,TBPDESC,TBPDESC2,TBPDTCADASTRO) VALUES (",z[i,"CLICODIGO"],",",z[i,"TBPCODIGO"],",'",z[i,"TBPDESCFECH"],"',",z[i,"TBPDESC"],",",z[i,"TBPDESC2"],",'",z[i,"TBPDTCADASTRO"],"');", sep = "")
  dbSendQuery(con3,query4)
}







