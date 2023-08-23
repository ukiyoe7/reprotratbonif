
## ATUALIZADOR DAS TABELAS DE COMBINADOS ==============
## 23.08.2023


## BIBLIOTECAS ========================================

library(DBI)
library(dplyr)
library(stringr)
library(lubridate)


## CONEXAO DB =========================================================

con2 <- dbConnect(odbc::odbc(), "reproreplica",encoding = "latin1")
con3 <- dbConnect(odbc::odbc(), "repro_prod", timeout = 10)

## BUSCA TABELAS ATIVAS ===============================================

tab_trat_ativos <- dbGetQuery(con2,"
   SELECT CLICODIGO,
           PROCODIGO
            FROM CLITBP C
             INNER JOIN (SELECT TBPCODIGO FROM TABPRECO 
               WHERE TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO%' AND TBPSITUACAO='A')A ON C.TBPCODIGO=A.TBPCODIGO
                LEFT JOIN (SELECT TBPCODIGO,PROCODIGO FROM TBPPRODU)B ON C.TBPCODIGO=B.TBPCODIGO")

View(tab_trat_ativos)


## INNER JOIN TABELAS INATIVAS TABELAS COMBINADOS ====================

tab_trat_comb <- dbGetQuery(con2,"
  SELECT T.TBPCODIGO,
           PROCODIGOB
            FROM TBPCOMBPROPRO T
             INNER JOIN (SELECT TBPCODIGO FROM TABPRECO 
              WHERE TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO CO %' AND TBPSITUACAO='A')A ON T.TBPCODIGO=A.TBPCODIGO") %>%
               rename(PROCODIGO=PROCODIGOB)  


inner_trat_tab_comb <-
inner_join(tab_trat_ativos,tab_trat_comb,by="PROCODIGO") %>% distinct(CLICODIGO,TBPCODIGO) 


trat_comb_cli <- dbGetQuery(con2,"
SELECT CLICODIGO,
        C.TBPCODIGO
          FROM CLITBPCOMB C
           INNER JOIN (SELECT TBPCODIGO FROM TABPRECO WHERE 
            TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO CO%')A ON C.TBPCODIGO=A.TBPCODIGO") 

View(trat_comb_cli)


anti_trat_tab_comb <-
anti_join(inner_trat_tab_comb,trat_comb_cli,by=c("CLICODIGO","TBPCODIGO"))

View(anti_trat_tab_comb)


## INCLUI TABELA COMBINADO PARA O CLIENTE ==============================================


tw <- data.frame(CLICODIGO=NA,TBPCODIGO=NA)

for (i in 1:nrow(anti_trat_tab_comb)) {
  tw[i,] <- anti_trat_tab_comb[i,]
  querycw <- paste("INSERT INTO CLITBPCOMB (CLICODIGO,TBPCODIGO,TBPDESCFECH,TBPDESC,TBPDESC2,CLITBPCOMBDTCADASTRO) VALUES(",tw[i,"CLICODIGO"],",",tw[i,"TBPCODIGO"],",'S',null,null,'",format(Sys.Date(),"%d.%m.%Y"),"') ;",sep = "")
  print(querycw) 
}


## BUSCA TABELAS INATIVAS ===============================================

acordos_trat_cli <- dbGetQuery(con2,"
 SELECT CLICODIGO,PROCODIGO
                    FROM CLITBP C
                    INNER JOIN (SELECT TBPCODIGO FROM TABPRECO 
                     WHERE TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO%' AND TBPSITUACAO='I')A ON C.TBPCODIGO=A.TBPCODIGO
                      LEFT JOIN (SELECT TBPCODIGO,PROCODIGO FROM TBPPRODU)B ON C.TBPCODIGO=B.TBPCODIGO")

View(acordos_trat_cli)


## BUSCA TABELAS COMBINADOS ===============================================


acordos_trat_comb_cli <- dbGetQuery(con2,"
SELECT CLICODIGO,
        C.TBPCODIGO,
         PROCODIGOB
          FROM CLITBPCOMB C
           INNER JOIN (SELECT TBPCODIGO FROM TABPRECO WHERE 
            TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO CO%')A ON C.TBPCODIGO=A.TBPCODIGO
              LEFT JOIN (SELECT TBPCODIGO,PROCODIGOB FROM TBPCOMBPROPRO)B ON C.TBPCODIGO=B.TBPCODIGO") %>% 
               rename(PROCODIGO=PROCODIGOB)

View(acordos_trat_comb_cli)


## CROSS JOIN TABELAS INATIVAS TABELAS COMBINADOS ===================================


tab_inativas_tab_comb <- 
inner_join(acordos_trat_cli,acordos_trat_comb_cli,by=c("CLICODIGO","PROCODIGO")) %>% 
   distinct(CLICODIGO,TBPCODIGO) 



## EXCLUI TABELA DO CLIENTE ==============================================


tx <- data.frame(CLICODIGO=NA,TBPCODIGO=NA)

for (i in 1:nrow(tab_inativas_tab_comb)) {
  tx[i,] <- tab_inativas_tab_comb[i,]
  querycb <- paste("DELETE FROM CLITBPCOMB WHERE CLICODIGO= ",tx[i,"CLICODIGO"]," AND TBPCODIGO= ",tx[i,"TBPCODIGO"],";",sep = "")
 print(querycb) 
}

dbSendQuery(con3,queryX)


