## ATUALIZADOR DAS TABELAS DE COMBINADOS ==============
## 23.08.2023


## BIBLIOTECAS =================================================================================================

library(DBI)
library(dplyr)


## CONEXAO DB ===============================================================================================

con2 <- dbConnect(odbc::odbc(), "reproreplica",encoding = "latin1")
con3 <- dbConnect(odbc::odbc(), "repro_prod", timeout = 10)


## BUSCA CLIENTES COM TABELAS ATIVAS ==================================================================================

tab_trat_ativos <- dbGetQuery(con2,"
   SELECT CLICODIGO,
           PROCODIGO
            FROM CLITBP C
             INNER JOIN (SELECT TBPCODIGO FROM TABPRECO 
               WHERE TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO%' AND TBPSITUACAO='A')A ON C.TBPCODIGO=A.TBPCODIGO
                LEFT JOIN (SELECT TBPCODIGO,PROCODIGO FROM TBPPRODU)B ON C.TBPCODIGO=B.TBPCODIGO")

View(tab_trat_ativos)


## BUSCA TABELAS COMBINADOS ===================================================================

tab_trat_comb <- dbGetQuery(con2,"
  SELECT T.TBPCODIGO,
           PROCODIGOB
            FROM TBPCOMBPROPRO T
             INNER JOIN (SELECT TBPCODIGO FROM TABPRECO 
              WHERE TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO CO%')A ON T.TBPCODIGO=A.TBPCODIGO") %>%
               rename(PROCODIGO=PROCODIGOB) %>% distinct(TBPCODIGO,PROCODIGO) 

View(tab_trat_comb)


## BUSCA CLIENTES TABELAS COMBINADOS ========================================================================================


trat_comb_cli <- dbGetQuery(con2,"
 SELECT CLICODIGO,
         C.TBPCODIGO,
          PROCODIGOB
           FROM CLITBPCOMB C
            INNER JOIN (SELECT TBPCODIGO FROM TABPRECO WHERE 
             TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO CO%')A ON C.TBPCODIGO=A.TBPCODIGO
              LEFT JOIN (SELECT TBPCODIGO,PROCODIGOB FROM TBPCOMBPROPRO)B ON C.TBPCODIGO=B.TBPCODIGO") %>% 
               rename(PROCODIGO=PROCODIGOB)

View(trat_comb_cli)


## BUSCA TABELAS INATIVAS ============================================================================================

cli_trat_inativos <- dbGetQuery(con2,"
 SELECT CLICODIGO,
         PROCODIGO
           FROM CLITBP C
                    INNER JOIN (SELECT TBPCODIGO FROM TABPRECO 
                     WHERE TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO%' AND TBPSITUACAO='I')A ON C.TBPCODIGO=A.TBPCODIGO
                      LEFT JOIN (SELECT TBPCODIGO,PROCODIGO FROM TBPPRODU)B ON C.TBPCODIGO=B.TBPCODIGO")

View(acordos_trat_cli)


## INNER JOIN TABELAS ATIVAS TABELAS COMBINADOS ===================================================================


inner_trat_tab_comb <-
  inner_join(tab_trat_ativos,tab_trat_comb,by="PROCODIGO") %>% distinct(CLICODIGO,TBPCODIGO) 

View(inner_trat_tab_comb)


## ANTI JOIN TABELAS COMBINADOS TABELAS INATIVAS ====================================================================


anti_trat_tab_comb <-
  anti_join(inner_trat_tab_comb,trat_comb_cli,by=c("CLICODIGO","TBPCODIGO"))

View(anti_trat_tab_comb)


## INCLUI TABELA COMBINADO PARA O CLIENTE ==============================================================================


tw <- data.frame(CLICODIGO=NA,TBPCODIGO=NA)

for (i in 1:nrow(anti_trat_tab_comb)) {
  tw[i,] <- anti_trat_tab_comb[i,]
  querytw <- paste("INSERT INTO CLITBPCOMB (CLICODIGO,TBPCODIGO,TBPDESCFECH,TBPDESC,TBPDESC2,CLITBPCOMBDTCADASTRO) VALUES(",tw[i,"CLICODIGO"],",",tw[i,"TBPCODIGO"],",'S',null,null,'",format(Sys.Date(),"%d.%m.%Y"),"') ;",sep = "")
  print(querytw) 
}


## INNER JOIN TABELAS INATIVAS TABELAS COMBINADOS ===============================================================


cli_comb_inativas <- 
  inner_join(cli_trat_inativos,trat_comb_cli,by=c("CLICODIGO","PROCODIGO")) %>% 
  distinct(CLICODIGO,TBPCODIGO) 

View(cli_comb_inativas)


## EXCLUI TABELA DO CLIENTE ======================================================================================


tx <- data.frame(CLICODIGO=NA,TBPCODIGO=NA)

for (i in 1:nrow(cli_comb_inativas)) {
  tx[i,] <- cli_comb_inativas[i,]
  querycb <- paste("DELETE FROM CLITBPCOMB WHERE CLICODIGO= ",tx[i,"CLICODIGO"]," AND TBPCODIGO= ",tx[i,"TBPCODIGO"],";",sep = "")
  print(querycb) 
}










