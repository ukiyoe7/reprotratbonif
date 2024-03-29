
library(DBI)
library(dplyr)
library(tidyverse)


## conexão com banco replica 

con2 <- dbConnect(odbc::odbc(), "reproreplica")

dbGetQuery(con2,"
  WITH CLITB AS (SELECT TBPCODIGO,CLICODIGO FROM CLITBP),
  
  
  TBPRECO AS (SELECT T.TBPCODIGO,CLICODIGO FROM TABPRECO T
  INNER JOIN CLITB C ON T.TBPCODIGO=C.TBPCODIGO
  WHERE TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO%'),
  
  PROD AS (SELECT PROCODIGO,PRODESCRICAO FROM PRODU WHERE PROSITUACAO='A')
  
  
  SELECT TP.TBPCODIGO,CLICODIGO,P.PROCODIGO,PRODESCRICAO,TBPPCOVENDA,TBPPCDESCTO2,TBPPCOVENDA2,TBPPCDESCTO
  FROM TBPPRODU TP
  INNER JOIN TBPRECO TB ON TP.TBPCODIGO=TB.TBPCODIGO
  LEFT JOIN PROD P ON TP.PROCODIGO=P.PROCODIGO
  ") %>% View()

## TRATAMENTOS POR CLIENTE

list_trat <- dbGetQuery(con2,"
  WITH CLITB AS (SELECT TBPCODIGO,CLICODIGO FROM CLITBP WHERE TBPCODIGO=493
),
  
  
  TBPRECO AS (SELECT T.TBPCODIGO,CLICODIGO FROM TABPRECO T
  INNER JOIN CLITB C ON T.TBPCODIGO=C.TBPCODIGO
  WHERE TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO%'),
  
  PROD AS (SELECT PROCODIGO,PRODESCRICAO FROM PRODU WHERE PROSITUACAO='A')
  
  
  SELECT TP.TBPCODIGO,CLICODIGO,P.PROCODIGO,PRODESCRICAO,TBPPCOVENDA,TBPPCDESCTO2,TBPPCOVENDA2,TBPPCDESCTO
  FROM TBPPRODU TP
  INNER JOIN TBPRECO TB ON TP.TBPCODIGO=TB.TBPCODIGO
  LEFT JOIN PROD P ON TP.PROCODIGO=P.PROCODIGO
  ") 

View(list_trat)

## TRATAMENTOS POR GRUPO

list_trat <- dbGetQuery(con2,"
  WITH CLITB AS (SELECT TBPCODIGO,CLICODIGO FROM CLITBP WHERE 
  CLICODIGO IN (SELECT CLICODIGO FROM CLIEN WHERE GCLCODIGO=25)
),
  
  
  TBPRECO AS (SELECT T.TBPCODIGO,CLICODIGO FROM TABPRECO T
  INNER JOIN CLITB C ON T.TBPCODIGO=C.TBPCODIGO
  WHERE TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO%'),
  
  PROD AS (SELECT PROCODIGO,PRODESCRICAO FROM PRODU WHERE PROSITUACAO='A')
  
  
  SELECT TP.TBPCODIGO,CLICODIGO,P.PROCODIGO,PRODESCRICAO,TBPPCOVENDA,TBPPCDESCTO2,TBPPCOVENDA2,TBPPCDESCTO
  FROM TBPPRODU TP
  INNER JOIN TBPRECO TB ON TP.TBPCODIGO=TB.TBPCODIGO
  LEFT JOIN PROD P ON TP.PROCODIGO=P.PROCODIGO
  ") 

View(list_trat)
