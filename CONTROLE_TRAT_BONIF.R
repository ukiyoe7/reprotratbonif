## CONTROLE TRATAMENTOS BONIFICADOS 
## ULTIMA ATUALIZACAOO 23.04.2024

## LOAD ===================================

library(DBI)
library(dplyr)
library(tidyverse)
library(openxlsx)

con2 <- dbConnect(odbc::odbc(), "repro",encoding="Latin1")

## ACORDOS ATIVOS ================================================

acordo_trat <- dbGetQuery(con2,"
WITH CLI AS (SELECT C.CLICODIGO, CLINOMEFANT,C.GCLCODIGO COD_GRUPO,GCLNOME NOME_GRUPO, SETOR FROM CLIEN C
               LEFT JOIN (SELECT CLICODIGO,ZODESCRICAO SETOR FROM ENDCLI ED
               INNER JOIN (SELECT ZOCODIGO,ZODESCRICAO FROM ZONA)Z ON ED.ZOCODIGO=Z.ZOCODIGO
               WHERE ENDFAT='S')E 
               ON C.CLICODIGO=E.CLICODIGO
                LEFT JOIN GRUPOCLI GC ON GC.GCLCODIGO=C.GCLCODIGO 
               WHERE CLICLIENTE='S'),

CLITB AS (SELECT TBPCODIGO,
                   CLINOMEFANT,
                    COD_GRUPO,
                     NOME_GRUPO,
                      SETOR,
                       CLITBP.CLICODIGO FROM CLITBP 
                        INNER JOIN CLI ON CLI.CLICODIGO=CLITBP.CLICODIGO)

SELECT CLICODIGO,
         CLINOMEFANT CLIENTE, 
          COD_GRUPO,
            NOME_GRUPO,
             T.TBPCODIGO, 
             SETOR,
              TBPDESCRICAO DESCRICAO,
               TBPDTINICIO INICIO,TBPDTVALIDADE VALIDADE, TBPSITUACAO SITUACAO
               
               
FROM TABPRECO T
INNER JOIN CLITB ON T.TBPCODIGO=CLITB.TBPCODIGO 
WHERE TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO%'
AND TBPDTVALIDADE >='TODAY'
") 



acordo_trat_conttb <- dbGetQuery(con2,"
SELECT DISTINCT TBPCODIGO,TBPPCOVENDA TOTAL
FROM TBPPRODU 
WHERE PROCODIGO='CONTTB'
") 


## LOJAS ATIVOS ==================

acordo_trat_lojas <- 
  left_join(acordo_trat,acordo_trat_conttb,by="TBPCODIGO") %>% filter(is.na(COD_GRUPO)) %>% 
   select(-COD_GRUPO,-NOME_GRUPO) %>% 
    arrange(desc(VALIDADE))


## GRUPOS ATIVOS ==================

acordo_trat_grupos <-
 acordo_trat %>% select(-CLICODIGO,-CLIENTE,-SETOR) %>% 
  filter(!is.na(COD_GRUPO))  %>% group_by_all() %>% tally() %>% select(-n) %>% 
    left_join(.,acordo_trat_conttb,by="TBPCODIGO") 


## PRODUTOS ========================

tbp_prod <- dbGetQuery(con2,"SELECT TBPCODIGO,PROCODIGO FROM TBPPRODU WHERE LEFT(PROCODIGO,2)='TR'") 

acordo_trat_prod <-
inner_join(acordo_trat,tbp_prod,by="TBPCODIGO") 


## ACORDOS VENCIDOS ================================================

acordo_trat_venc <- dbGetQuery(con2,"
WITH CLI AS (SELECT C.CLICODIGO, CLINOMEFANT,C.GCLCODIGO COD_GRUPO,GCLNOME NOME_GRUPO, SETOR FROM CLIEN C
               LEFT JOIN (SELECT CLICODIGO,ZODESCRICAO SETOR FROM ENDCLI ED
               INNER JOIN (SELECT ZOCODIGO,ZODESCRICAO FROM ZONA)Z ON ED.ZOCODIGO=Z.ZOCODIGO
               WHERE ENDFAT='S')E 
               ON C.CLICODIGO=E.CLICODIGO
                LEFT JOIN GRUPOCLI GC ON GC.GCLCODIGO=C.GCLCODIGO 
               WHERE CLICLIENTE='S' AND C.CLICODIGO<>124),

CLITB AS (SELECT TBPCODIGO,
                   CLINOMEFANT,
                    COD_GRUPO,
                     NOME_GRUPO,
                      SETOR,
                       CLITBP.CLICODIGO FROM CLITBP 
                        INNER JOIN CLI ON CLI.CLICODIGO=CLITBP.CLICODIGO)

SELECT CLICODIGO,
         CLINOMEFANT CLIENTE, 
           COD_GRUPO,
            NOME_GRUPO,
             T.TBPCODIGO, 
             SETOR,
              TBPDESCRICAO DESCRICAO,
               TBPDTINICIO INICIO,TBPDTVALIDADE VALIDADE, TBPSITUACAO SITUACAO
               
               
FROM TABPRECO T
INNER JOIN CLITB ON T.TBPCODIGO=CLITB.TBPCODIGO 
WHERE TBPDESCRICAO LIKE '%TRATAMENTO BONIFICADO%'
AND TBPDTVALIDADE BETWEEN DATEADD(-120 DAY TO CURRENT_DATE) AND 'YESTERDAY'") 



acordo_trat_venc_conttb <- dbGetQuery(con2,"
SELECT DISTINCT TBPCODIGO,TBPPCOVENDA TOTAL
FROM TBPPRODU 
WHERE PROCODIGO='CONTTB'
") 



## LOJAS =================

acordo_trat_venc_lojas <- 
  left_join(acordo_trat_venc,acordo_trat_venc_conttb,by="TBPCODIGO") %>% 
    arrange(desc(VALIDADE)) %>% filter(is.na(COD_GRUPO))  %>% 
     select(-COD_GRUPO,-NOME_GRUPO) %>% 
      arrange(desc(VALIDADE))



## GRUPOS ===================

acordo_trat_venc_grupos <-
  acordo_trat_venc %>% select(-CLICODIGO,-CLIENTE,-SETOR) %>% 
   filter(!is.na(COD_GRUPO))  %>% group_by_all() %>% tally() %>% select(-n) %>% 
    left_join(.,acordo_trat_conttb,by="TBPCODIGO") 


## PEDIDOS ====================================

## sql traz todos os tratamentos

pedidos_trat <- dbGetQuery(con2,"
    
    WITH FIS AS (SELECT FISCODIGO FROM TBFIS WHERE FISTPNATOP IN ('V','R','SR')),
    
    CLI AS (SELECT C.CLICODIGO,
                     CLINOMEFANT, GCLCODIGO COD_GRUPO, SETOR FROM CLIEN C
    LEFT JOIN (SELECT CLICODIGO, ZODESCRICAO SETOR FROM ENDCLI E
    INNER JOIN (SELECT ZOCODIGO,ZODESCRICAO FROM ZONA)Z ON E.ZOCODIGO=Z.ZOCODIGO
    WHERE ENDFAT='S'
    ) ED ON C.CLICODIGO=ED.CLICODIGO
    WHERE CLICLIENTE='S'),
    
    PED AS (SELECT ID_PEDIDO,PEDDTEMIS,P.CLICODIGO,COD_GRUPO,SETOR FROM PEDID P
     INNER JOIN CLI C ON P.CLICODIGO=C.CLICODIGO
      WHERE PEDDTEMIS BETWEEN DATEADD(-120 DAY TO CURRENT_DATE) AND 'TODAY' AND PEDSITPED<>'C' AND PEDLCFINANC IN ('S', 'L','N')),
      
      PROD AS (SELECT PROCODIGO FROM PRODU WHERE PROTIPO='T')
    
    SELECT PD.ID_PEDIDO,
     PEDDTEMIS,
      EXTRACT(MONTH FROM PEDDTEMIS) MES,
       CLICODIGO,
        COD_GRUPO,
         SETOR,
          PD.PROCODIGO,
           PDPDESCRICAO,
     SUM(PDPQTDADE)QTD,
      SUM(PDPUNITLIQUIDO*PDPQTDADE)VRVENDA 
       FROM PDPRD PD
        INNER JOIN PED P ON PD.ID_PEDIDO=P.ID_PEDIDO
         INNER JOIN FIS F ON PD.FISCODIGO=F.FISCODIGO
          INNER JOIN PROD PRD ON PRD.PROCODIGO=PD.PROCODIGO
    GROUP BY 1,2,3,4,5,6,7,8
    ")


## sql pedidos promo

promo <- dbGetQuery(con2,"
    WITH FIS AS 
    (SELECT FISCODIGO 
      FROM TBFIS WHERE FISTPNATOP IN ('V','SR','R')),
    
    PED AS 
    (SELECT ID_PEDIDO
      FROM PEDID P
      INNER JOIN FIS ON P.FISCODIGO1=FIS.FISCODIGO
      WHERE 
      PEDDTEMIS BETWEEN DATEADD(-120 DAY TO CURRENT_DATE) AND 'TODAY'
      AND PEDSITPED<>'C'),
    
    PED_PROMO_PAP AS 
    (SELECT P1.ID_PEDIDO ID_PEDIDO_PROMO 
      FROM PDPRD P1
      INNER JOIN PED ON P1.ID_PEDIDO=PED.ID_PEDIDO
      WHERE PROCODIGO='PAP'),
    
    PED_PROMO_PLUGIN AS 
    (SELECT ID_PEDIDPROMOCAO ID_PEDIDO_PROMO 
      FROM PEDIDPROMO P2
      INNER JOIN PED ON P2.ID_PEDIDPROMOCAO=PED.ID_PEDIDO),
      
      PED_PROMO_CONECTA AS (SELECT P3.ID_PEDIDO ID_PEDIDO_PROMO 
                                 FROM PDINFOPROMO P3
                                  INNER JOIN PED ON P3.ID_PEDIDO=PED.ID_PEDIDO
                                   WHERE PIPPAR=2)
  
  SELECT ID_PEDIDO_PROMO ID_PEDIDO, '1' PROMO
      FROM PED_PROMO_PAP UNION
      SELECT ID_PEDIDO_PROMO ID_PEDIDO, '1' PROMO 
      FROM PED_PROMO_PLUGIN UNION
      SELECT ID_PEDIDO_PROMO ID_PEDIDO, '2' PROMO
      FROM PED_PROMO_CONECTA")  

## filtra tratamentos com valor 1

pedido_trat_bonif <- 
anti_join(pedidos_trat,promo,by="ID_PEDIDO") %>% 
  filter(VRVENDA<=1 & VRVENDA>0.01) %>%  
  arrange(desc(PEDDTEMIS))

## associa clientes e tratamento

pedido_trat_bonif_acordo <- 
inner_join(pedido_trat_bonif,acordo_trat_prod,c("CLICODIGO","PROCODIGO")) 

 

## CONTAGEM DE TRATAMENTOS =================================

## LOJAS

trat_bonif_uso_R <-
pedido_trat_bonif_acordo %>% 
   
  mutate(TIPO_ACORDO=substr(DESCRICAO,nchar(DESCRICAO), nchar(DESCRICAO))) %>% 
  
   group_by(CLICODIGO,COD_GRUPO.x,TBPCODIGO) %>% 
  
    filter(TIPO_ACORDO=='R') %>% 
  
     summarize(USO=sum(QTD[floor_date(PEDDTEMIS,"day")>=floor_date(Sys.Date(), "month") & floor_date(PEDDTEMIS,"day")<=ceiling_date(Sys.Date(),'month') %m-% days(1)],na.rm = TRUE)) %>% 
     
      rename(COD_GRUPO=COD_GRUPO.x)




trat_bonif_uso_T <- 
pedido_trat_bonif_acordo %>% 
  
  mutate(TIPO_ACORDO=substr(DESCRICAO,nchar(DESCRICAO), nchar(DESCRICAO))) %>% 
  
   group_by(CLICODIGO,COD_GRUPO.x,TBPCODIGO) %>% 
  
    filter(TIPO_ACORDO=='T') %>% 
  
     summarize(USO=sum(QTD[floor_date(PEDDTEMIS,"day")>=INICIO & floor_date(PEDDTEMIS,"day")<=VALIDADE],na.rm = TRUE)) %>% 
  
       rename(COD_GRUPO=COD_GRUPO.x)




## GRUPOS

trat_bonif_uso_grupos_R <-
trat_bonif_uso_R %>% group_by(COD_GRUPO,TBPCODIGO) %>% summarize(USO=sum(USO)) %>% filter(!is.na(COD_GRUPO))


trat_bonif_uso_grupos_T <-
trat_bonif_uso_T %>% group_by(COD_GRUPO,TBPCODIGO) %>% summarize(USO=sum(USO)) %>% filter(!is.na(COD_GRUPO)) 


## RESUMOS ======

## acordos ativos lojas

acordo_trat_ativo_lojas <- 
left_join(acordo_trat_lojas,union_all(trat_bonif_uso_T,trat_bonif_uso_R),by=c("CLICODIGO","TBPCODIGO")) %>% 
  mutate(USO=if_else(is.na(USO),0,USO)) %>% 
  mutate(SALDO=TOTAL-USO) %>% select(-COD_GRUPO) %>%  arrange(CLICODIGO) 

## acordos ativos grupos

acordo_trat_ativo_grupos <- 
left_join(acordo_trat_grupos,union_all(trat_bonif_uso_grupos_T,trat_bonif_uso_grupos_R),by=c("COD_GRUPO","TBPCODIGO")) %>%
  mutate(USO=if_else(is.na(USO),0,USO)) %>% 
  mutate(SALDO=TOTAL-USO) %>% arrange(COD_GRUPO) 



## EXTRATOS ======

corder <- c("ID_PEDIDO","PEDDTEMIS","MES","CLICODIGO","COD_GRUPO.x","SETOR.x","PROCODIGO","PDPDESCRICAO","QTD","VRVENDA")

trat_bonif_extrato <- 
pedido_trat_bonif_acordo %>% .[,corder] %>% rename(COD_GRUPO=COD_GRUPO.x) %>% rename(SETOR=SETOR.x)



## CREATE EXCEL ===============================

## GERAL

wbtrat <- createWorkbook()

addWorksheet(wbtrat, "ACORDOS ATIVOS LOJAS")

## FORMAT

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS LOJAS", cols = 1, widths = 12)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS LOJAS", cols = 2, widths = 30)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS LOJAS", cols = 3, widths = 12)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS LOJAS", cols = 4, widths = 30)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS LOJAS", cols = 5, widths = 30)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS LOJAS", cols = 6, widths = 13)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS LOJAS", cols = 7, widths = 13)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS LOJAS", cols = 8, widths = 10)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS LOJAS", cols = 9, widths = 10)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS LOJAS", cols = 10, widths = 10)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS LOJAS", cols = 11, widths = 10)

crescimento <- createStyle(fontColour = "#02862a", bgFill = "#ccf2d8")

queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat, "ACORDOS ATIVOS LOJAS", acordo_trat_ativo_lojas, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat, sheet = "ACORDOS ATIVOS LOJAS", style = datesty, cols = c(6:7), rows = 1:nrow(acordo_trat_ativo_lojas)+1, gridExpand = TRUE)

conditionalFormatting(wbtrat, sheet = "ACORDOS ATIVOS LOJAS", cols = 11, rows = 1:nrow(acordo_trat_ativo_lojas), rule = 'K1 <= 0', style = queda)

conditionalFormatting(wbtrat, sheet = "ACORDOS ATIVOS LOJAS", cols = 8, rows = 1:nrow(acordo_trat_ativo_lojas), rule = 'H1== "I"', style = queda)


## GRUPOS

addWorksheet(wbtrat, "ACORDOS ATIVOS GRUPO")

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS GRUPO", cols = 1, widths = 12)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS GRUPO", cols = 2, widths = 30)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS GRUPO", cols = 3, widths = 12)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS GRUPO", cols = 4, widths = 30)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS GRUPO", cols = 5, widths = 13)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS GRUPO", cols = 6, widths = 13)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS GRUPO", cols = 7, widths = 13)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS GRUPO", cols = 8, widths = 10)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS GRUPO", cols = 9, widths = 10)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS GRUPO", cols = 10, widths = 10)

setColWidths(wbtrat, sheet = "ACORDOS ATIVOS GRUPO", cols = 11, widths = 10)

writeDataTable(wbtrat, "ACORDOS ATIVOS GRUPO", acordo_trat_ativo_grupos, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat, sheet = "ACORDOS ATIVOS GRUPO", style = datesty, cols = c(5:6), rows = 1:nrow(acordo_trat_ativo_grupos)+1, gridExpand = TRUE)

conditionalFormatting(wbtrat, sheet = "ACORDOS ATIVOS GRUPO", cols = 10, rows = 1:nrow(acordo_trat_ativo_grupos), rule = 'J1 <= 0', style = queda)

conditionalFormatting(wbtrat, sheet = "ACORDOS ATIVOS GRUPO", cols = 7, rows = 1:nrow(acordo_trat_ativo_grupos), rule = 'G1== "I"', style = queda)


## EXTRATOS

addWorksheet(wbtrat, "EXTRATO")

setColWidths(wbtrat, sheet = "EXTRATO", cols = 1, widths = 12)

setColWidths(wbtrat, sheet = "EXTRATO", cols = 2, widths = 12)

setColWidths(wbtrat, sheet = "EXTRATO", cols = 3, widths = 12)

setColWidths(wbtrat, sheet = "EXTRATO", cols = 4, widths = 12)

setColWidths(wbtrat, sheet = "EXTRATO", cols = 5, widths = 12)

setColWidths(wbtrat, sheet = "EXTRATO", cols = 6, widths = 30)

setColWidths(wbtrat, sheet = "EXTRATO", cols = 7, widths = 10)

setColWidths(wbtrat, sheet = "EXTRATO", cols = 8, widths = 30)

setColWidths(wbtrat, sheet = "EXTRATO", cols = 9, widths = 12)

setColWidths(wbtrat, sheet = "EXTRATO", cols = 10, widths = 12)


writeDataTable(wbtrat, "EXTRATO", trat_bonif_extrato, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat, sheet = "EXTRATO", style = datesty, cols = 2, rows = 1:nrow(trat_bonif_extrato)+1, gridExpand = TRUE)

## ACORDOS VENCIDOS LOJAS ==================

addWorksheet(wbtrat, "ACORDOS VENCIDOS LOJAS")

## FORMAT

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS LOJAS", cols = 1, widths = 12)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS LOJAS", cols = 2, widths = 30)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS LOJAS", cols = 3, widths = 12)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS LOJAS", cols = 4, widths = 30)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS LOJAS", cols = 5, widths = 30)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS LOJAS", cols = 6, widths = 13)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS LOJAS", cols = 7, widths = 13)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS LOJAS", cols = 8, widths = 10)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS LOJAS", cols = 9, widths = 10)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS LOJAS", cols = 10, widths = 10)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS LOJAS", cols = 11, widths = 10)


queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat, "ACORDOS VENCIDOS LOJAS", acordo_trat_venc_lojas, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat, sheet = "ACORDOS VENCIDOS LOJAS", style = datesty, cols = c(6:7), rows = 1:nrow(acordo_trat_venc_lojas)+1, gridExpand = TRUE)

## ACORDOS VENCIDOS GRUPOS ==================

addWorksheet(wbtrat, "ACORDOS VENCIDOS GRUPOS")

## FORMAT

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 1, widths = 12)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 2, widths = 30)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 3, widths = 12)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 4, widths = 30)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 5, widths = 13)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 6, widths = 13)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 7, widths = 13)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 8, widths = 10)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 9, widths = 10)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 10, widths = 10)

setColWidths(wbtrat, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 11, widths = 10)


queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat, "ACORDOS VENCIDOS GRUPOS", acordo_trat_venc_grupos, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat, sheet = "ACORDOS VENCIDOS GRUPOS", style = datesty, cols = c(5:6), rows = 1:nrow(acordo_trat_venc_grupos)+1, gridExpand = TRUE)

## WORKBOOK ====

saveWorkbook(wbtrat, file = "TRATAMENTOS_BONIFICADOS.xlsx", overwrite = TRUE)


## SETOR 1 ===============================

## GERAL

wbtrat_st1 <- createWorkbook()

addWorksheet(wbtrat_st1, "ACORDOS ATIVOS LOJAS")

## FORMAT

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS LOJAS", cols = 1, widths = 12)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS LOJAS", cols = 2, widths = 30)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS LOJAS", cols = 3, widths = 12)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS LOJAS", cols = 4, widths = 30)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS LOJAS", cols = 5, widths = 30)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS LOJAS", cols = 6, widths = 13)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS LOJAS", cols = 7, widths = 13)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS LOJAS", cols = 8, widths = 10)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS LOJAS", cols = 9, widths = 10)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS LOJAS", cols = 10, widths = 10)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS LOJAS", cols = 11, widths = 10)

crescimento <- createStyle(fontColour = "#02862a", bgFill = "#ccf2d8")

queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st1, "ACORDOS ATIVOS LOJAS", acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 1 - FLORIANOPOLIS REDES'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st1, sheet = "ACORDOS ATIVOS LOJAS", style = datesty, cols = c(6:7), rows = 1:nrow(acordo_trat_ativo_lojas)+1, gridExpand = TRUE)

conditionalFormatting(wbtrat_st1, sheet = "ACORDOS ATIVOS LOJAS", cols = 11, rows = 1:nrow(acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 1 - FLORIANOPOLIS REDES')), rule = 'K1 <= 0', style = queda)

conditionalFormatting(wbtrat_st1, sheet = "ACORDOS ATIVOS LOJAS", cols = 8, rows = 1:nrow(acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 1 - FLORIANOPOLIS REDES')), rule = 'H1== "I"', style = queda)


## GRUPOS

addWorksheet(wbtrat_st1, "ACORDOS ATIVOS GRUPO")

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS GRUPO", cols = 1, widths = 12)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS GRUPO", cols = 2, widths = 30)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS GRUPO", cols = 3, widths = 12)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS GRUPO", cols = 4, widths = 30)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS GRUPO", cols = 5, widths = 13)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS GRUPO", cols = 6, widths = 13)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS GRUPO", cols = 7, widths = 13)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS GRUPO", cols = 8, widths = 10)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS GRUPO", cols = 9, widths = 10)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS GRUPO", cols = 10, widths = 10)

setColWidths(wbtrat_st1, sheet = "ACORDOS ATIVOS GRUPO", cols = 11, widths = 10)

writeDataTable(wbtrat_st1, "ACORDOS ATIVOS GRUPO", acordo_trat_ativo_grupos, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st1, sheet = "ACORDOS ATIVOS GRUPO", style = datesty, cols = c(5:6), rows = 1:nrow(acordo_trat_ativo_grupos)+1, gridExpand = TRUE)

conditionalFormatting(wbtrat_st1, sheet = "ACORDOS ATIVOS GRUPO", cols = 10, rows = 1:nrow(acordo_trat_ativo_grupos), rule = 'J1 <= 0', style = queda)

conditionalFormatting(wbtrat_st1, sheet = "ACORDOS ATIVOS GRUPO", cols = 7, rows = 1:nrow(acordo_trat_ativo_grupos), rule = 'G1== "I"', style = queda)


## EXTRATOS

addWorksheet(wbtrat_st1, "EXTRATO")

setColWidths(wbtrat_st1, sheet = "EXTRATO", cols = 1, widths = 12)

setColWidths(wbtrat_st1, sheet = "EXTRATO", cols = 2, widths = 12)

setColWidths(wbtrat_st1, sheet = "EXTRATO", cols = 3, widths = 12)

setColWidths(wbtrat_st1, sheet = "EXTRATO", cols = 4, widths = 12)

setColWidths(wbtrat_st1, sheet = "EXTRATO", cols = 5, widths = 12)

setColWidths(wbtrat_st1, sheet = "EXTRATO", cols = 6, widths = 30)

setColWidths(wbtrat_st1, sheet = "EXTRATO", cols = 7, widths = 10)

setColWidths(wbtrat_st1, sheet = "EXTRATO", cols = 8, widths = 30)

setColWidths(wbtrat_st1, sheet = "EXTRATO", cols = 9, widths = 12)

setColWidths(wbtrat_st1, sheet = "EXTRATO", cols = 10, widths = 12)


writeDataTable(wbtrat_st1, "EXTRATO", trat_bonif_extrato %>%  filter(SETOR=='SETOR 1 - FLORIANOPOLIS REDES'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st1, sheet = "EXTRATO", style = datesty, cols = 2, rows = 1:nrow(trat_bonif_extrato %>%  filter(SETOR=='SETOR 1 - FLORIANOPOLIS REDES'))+1, gridExpand = TRUE)

## ACORDOS VENCIDOS LOJAS ==================

addWorksheet(wbtrat_st1, "ACORDOS VENCIDOS LOJAS")

## FORMAT

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS LOJAS", cols = 1, widths = 12)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS LOJAS", cols = 2, widths = 30)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS LOJAS", cols = 3, widths = 12)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS LOJAS", cols = 4, widths = 30)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS LOJAS", cols = 5, widths = 30)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS LOJAS", cols = 6, widths = 13)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS LOJAS", cols = 7, widths = 13)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS LOJAS", cols = 8, widths = 10)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS LOJAS", cols = 9, widths = 10)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS LOJAS", cols = 10, widths = 10)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS LOJAS", cols = 11, widths = 10)


queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st1, "ACORDOS VENCIDOS LOJAS", acordo_trat_venc_lojas %>%  filter(SETOR=='SETOR 1 - FLORIANOPOLIS REDES'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st1, sheet = "ACORDOS VENCIDOS LOJAS", style = datesty, cols = c(6:7), rows = 1:nrow(acordo_trat_venc_lojas %>%  filter(SETOR=='SETOR 1 - FLORIANOPOLIS REDES'))+1, gridExpand = TRUE)

## ACORDOS VENCIDOS GRUPOS ==================

addWorksheet(wbtrat_st1, "ACORDOS VENCIDOS GRUPOS")

## FORMAT

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 1, widths = 12)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 2, widths = 30)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 3, widths = 12)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 4, widths = 30)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 5, widths = 13)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 6, widths = 13)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 7, widths = 13)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 8, widths = 10)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 9, widths = 10)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 10, widths = 10)

setColWidths(wbtrat_st1, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 11, widths = 10)


queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st1, "ACORDOS VENCIDOS GRUPOS", acordo_trat_venc_grupos, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st1, sheet = "ACORDOS VENCIDOS GRUPOS", style = datesty, cols = c(5:6), rows = 1:nrow(acordo_trat_venc_grupos)+1, gridExpand = TRUE)

## WORKBOOK ====

saveWorkbook(wbtrat_st1, file = "TRATAMENTOS_BONIFICADOS_SETOR1.xlsx", overwrite = TRUE)

## SETOR 2 ===============================

## GERAL

wbtrat_st2 <- createWorkbook()

addWorksheet(wbtrat_st2, "ACORDOS ATIVOS LOJAS")

## FORMAT

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS LOJAS", cols = 1, widths = 12)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS LOJAS", cols = 2, widths = 30)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS LOJAS", cols = 3, widths = 12)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS LOJAS", cols = 4, widths = 30)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS LOJAS", cols = 5, widths = 30)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS LOJAS", cols = 6, widths = 13)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS LOJAS", cols = 7, widths = 13)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS LOJAS", cols = 8, widths = 10)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS LOJAS", cols = 9, widths = 10)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS LOJAS", cols = 10, widths = 10)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS LOJAS", cols = 11, widths = 10)

crescimento <- createStyle(fontColour = "#02862a", bgFill = "#ccf2d8")

queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st2, "ACORDOS ATIVOS LOJAS", acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 2 - CRICIUMA - SUL'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st2, sheet = "ACORDOS ATIVOS LOJAS", style = datesty, cols = c(6:7), rows = 1:nrow(acordo_trat_ativo_lojas)+1, gridExpand = TRUE)

conditionalFormatting(wbtrat_st2, sheet = "ACORDOS ATIVOS LOJAS", cols = 11, rows = 1:nrow(acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 2 - CRICIUMA - SUL')), rule = 'K1 <= 0', style = queda)

conditionalFormatting(wbtrat_st2, sheet = "ACORDOS ATIVOS LOJAS", cols = 8, rows = 1:nrow(acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 2 - CRICIUMA - SUL')), rule = 'H1== "I"', style = queda)


## GRUPOS

addWorksheet(wbtrat_st2, "ACORDOS ATIVOS GRUPO")

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS GRUPO", cols = 1, widths = 12)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS GRUPO", cols = 2, widths = 30)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS GRUPO", cols = 3, widths = 12)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS GRUPO", cols = 4, widths = 30)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS GRUPO", cols = 5, widths = 13)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS GRUPO", cols = 6, widths = 13)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS GRUPO", cols = 7, widths = 13)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS GRUPO", cols = 8, widths = 10)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS GRUPO", cols = 9, widths = 10)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS GRUPO", cols = 10, widths = 10)

setColWidths(wbtrat_st2, sheet = "ACORDOS ATIVOS GRUPO", cols = 11, widths = 10)

writeDataTable(wbtrat_st2, "ACORDOS ATIVOS GRUPO", acordo_trat_ativo_grupos, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st2, sheet = "ACORDOS ATIVOS GRUPO", style = datesty, cols = c(5:6), rows = 1:nrow(acordo_trat_ativo_grupos)+1, gridExpand = TRUE)

conditionalFormatting(wbtrat_st2, sheet = "ACORDOS ATIVOS GRUPO", cols = 10, rows = 1:nrow(acordo_trat_ativo_grupos), rule = 'J1 <= 0', style = queda)

conditionalFormatting(wbtrat_st2, sheet = "ACORDOS ATIVOS GRUPO", cols = 7, rows = 1:nrow(acordo_trat_ativo_grupos), rule = 'G1== "I"', style = queda)


## EXTRATOS

addWorksheet(wbtrat_st2, "EXTRATO")

setColWidths(wbtrat_st2, sheet = "EXTRATO", cols = 1, widths = 12)

setColWidths(wbtrat_st2, sheet = "EXTRATO", cols = 2, widths = 12)

setColWidths(wbtrat_st2, sheet = "EXTRATO", cols = 3, widths = 12)

setColWidths(wbtrat_st2, sheet = "EXTRATO", cols = 4, widths = 12)

setColWidths(wbtrat_st2, sheet = "EXTRATO", cols = 5, widths = 12)

setColWidths(wbtrat_st2, sheet = "EXTRATO", cols = 6, widths = 30)

setColWidths(wbtrat_st2, sheet = "EXTRATO", cols = 7, widths = 10)

setColWidths(wbtrat_st2, sheet = "EXTRATO", cols = 8, widths = 30)

setColWidths(wbtrat_st2, sheet = "EXTRATO", cols = 9, widths = 12)

setColWidths(wbtrat_st2, sheet = "EXTRATO", cols = 10, widths = 12)


writeDataTable(wbtrat_st2, "EXTRATO", trat_bonif_extrato %>%  filter(SETOR=='SETOR 2 - CRICIUMA - SUL'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st2, sheet = "EXTRATO", style = datesty, cols = 2, rows = 1:nrow(trat_bonif_extrato %>%  filter(SETOR=='SETOR 2 - CRICIUMA - SUL'))+1, gridExpand = TRUE)

## ACORDOS VENCIDOS LOJAS ==================

addWorksheet(wbtrat_st2, "ACORDOS VENCIDOS LOJAS")

## FORMAT

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS LOJAS", cols = 1, widths = 12)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS LOJAS", cols = 2, widths = 30)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS LOJAS", cols = 3, widths = 12)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS LOJAS", cols = 4, widths = 30)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS LOJAS", cols = 5, widths = 30)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS LOJAS", cols = 6, widths = 13)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS LOJAS", cols = 7, widths = 13)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS LOJAS", cols = 8, widths = 10)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS LOJAS", cols = 9, widths = 10)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS LOJAS", cols = 10, widths = 10)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS LOJAS", cols = 11, widths = 10)


queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st2, "ACORDOS VENCIDOS LOJAS", acordo_trat_venc_lojas %>%  filter(SETOR=='SETOR 2 - CRICIUMA - SUL'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st2, sheet = "ACORDOS VENCIDOS LOJAS", style = datesty, cols = c(6:7), rows = 1:nrow(acordo_trat_venc_lojas %>%  filter(SETOR=='SETOR 2 - CRICIUMA - SUL'))+1, gridExpand = TRUE)

## ACORDOS VENCIDOS GRUPOS ==================

addWorksheet(wbtrat_st2, "ACORDOS VENCIDOS GRUPOS")

## FORMAT

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 1, widths = 12)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 2, widths = 30)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 3, widths = 12)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 4, widths = 30)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 5, widths = 13)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 6, widths = 13)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 7, widths = 13)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 8, widths = 10)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 9, widths = 10)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 10, widths = 10)

setColWidths(wbtrat_st2, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 11, widths = 10)


queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st2, "ACORDOS VENCIDOS GRUPOS", acordo_trat_venc_grupos, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st2, sheet = "ACORDOS VENCIDOS GRUPOS", style = datesty, cols = c(5:6), rows = 1:nrow(acordo_trat_venc_grupos)+1, gridExpand = TRUE)

## WORKBOOK ====

saveWorkbook(wbtrat_st2, file = "TRATAMENTOS_BONIFICADOS_SETOR2.xlsx", overwrite = TRUE)

## SETOR 3 ===============================

## GERAL

wbtrat_st3 <- createWorkbook()

addWorksheet(wbtrat_st3, "ACORDOS ATIVOS LOJAS")

## FORMAT

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS LOJAS", cols = 1, widths = 12)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS LOJAS", cols = 2, widths = 30)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS LOJAS", cols = 3, widths = 12)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS LOJAS", cols = 4, widths = 30)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS LOJAS", cols = 5, widths = 30)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS LOJAS", cols = 6, widths = 13)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS LOJAS", cols = 7, widths = 13)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS LOJAS", cols = 8, widths = 10)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS LOJAS", cols = 9, widths = 10)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS LOJAS", cols = 10, widths = 10)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS LOJAS", cols = 11, widths = 10)

crescimento <- createStyle(fontColour = "#02862a", bgFill = "#ccf2d8")

queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st3, "ACORDOS ATIVOS LOJAS", acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 3 - CHAPECO - OESTE - RS'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st3, sheet = "ACORDOS ATIVOS LOJAS", style = datesty, cols = c(6:7), rows = 1:nrow(acordo_trat_ativo_lojas)+1, gridExpand = TRUE)

conditionalFormatting(wbtrat_st3, sheet = "ACORDOS ATIVOS LOJAS", cols = 11, rows = 1:nrow(acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 3 - CHAPECO - OESTE - RS')), rule = 'K1 <= 0', style = queda)

conditionalFormatting(wbtrat_st3, sheet = "ACORDOS ATIVOS LOJAS", cols = 8, rows = 1:nrow(acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 3 - CHAPECO - OESTE - RS')), rule = 'H1== "I"', style = queda)


## GRUPOS

addWorksheet(wbtrat_st3, "ACORDOS ATIVOS GRUPO")

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS GRUPO", cols = 1, widths = 12)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS GRUPO", cols = 2, widths = 30)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS GRUPO", cols = 3, widths = 12)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS GRUPO", cols = 4, widths = 30)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS GRUPO", cols = 5, widths = 13)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS GRUPO", cols = 6, widths = 13)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS GRUPO", cols = 7, widths = 13)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS GRUPO", cols = 8, widths = 10)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS GRUPO", cols = 9, widths = 10)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS GRUPO", cols = 10, widths = 10)

setColWidths(wbtrat_st3, sheet = "ACORDOS ATIVOS GRUPO", cols = 11, widths = 10)

writeDataTable(wbtrat_st3, "ACORDOS ATIVOS GRUPO", acordo_trat_ativo_grupos, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st3, sheet = "ACORDOS ATIVOS GRUPO", style = datesty, cols = c(5:6), rows = 1:nrow(acordo_trat_ativo_grupos)+1, gridExpand = TRUE)

conditionalFormatting(wbtrat_st3, sheet = "ACORDOS ATIVOS GRUPO", cols = 10, rows = 1:nrow(acordo_trat_ativo_grupos), rule = 'J1 <= 0', style = queda)

conditionalFormatting(wbtrat_st3, sheet = "ACORDOS ATIVOS GRUPO", cols = 7, rows = 1:nrow(acordo_trat_ativo_grupos), rule = 'G1== "I"', style = queda)


## EXTRATOS

addWorksheet(wbtrat_st3, "EXTRATO")

setColWidths(wbtrat_st3, sheet = "EXTRATO", cols = 1, widths = 12)

setColWidths(wbtrat_st3, sheet = "EXTRATO", cols = 2, widths = 12)

setColWidths(wbtrat_st3, sheet = "EXTRATO", cols = 3, widths = 12)

setColWidths(wbtrat_st3, sheet = "EXTRATO", cols = 4, widths = 12)

setColWidths(wbtrat_st3, sheet = "EXTRATO", cols = 5, widths = 12)

setColWidths(wbtrat_st3, sheet = "EXTRATO", cols = 6, widths = 30)

setColWidths(wbtrat_st3, sheet = "EXTRATO", cols = 7, widths = 10)

setColWidths(wbtrat_st3, sheet = "EXTRATO", cols = 8, widths = 30)

setColWidths(wbtrat_st3, sheet = "EXTRATO", cols = 9, widths = 12)

setColWidths(wbtrat_st3, sheet = "EXTRATO", cols = 10, widths = 12)


writeDataTable(wbtrat_st3, "EXTRATO", trat_bonif_extrato %>%  filter(SETOR=='SETOR 3 - CHAPECO - OESTE - RS'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st3, sheet = "EXTRATO", style = datesty, cols = 2, rows = 1:nrow(trat_bonif_extrato %>%  filter(SETOR=='SETOR 3 - CHAPECO - OESTE - RS'))+1, gridExpand = TRUE)

## ACORDOS VENCIDOS LOJAS ==================

addWorksheet(wbtrat_st3, "ACORDOS VENCIDOS LOJAS")

## FORMAT

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS LOJAS", cols = 1, widths = 12)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS LOJAS", cols = 2, widths = 30)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS LOJAS", cols = 3, widths = 12)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS LOJAS", cols = 4, widths = 30)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS LOJAS", cols = 5, widths = 30)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS LOJAS", cols = 6, widths = 13)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS LOJAS", cols = 7, widths = 13)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS LOJAS", cols = 8, widths = 10)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS LOJAS", cols = 9, widths = 10)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS LOJAS", cols = 10, widths = 10)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS LOJAS", cols = 11, widths = 10)


queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st3, "ACORDOS VENCIDOS LOJAS", acordo_trat_venc_lojas %>%  filter(SETOR=='SETOR 3 - CHAPECO - OESTE - RS'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st3, sheet = "ACORDOS VENCIDOS LOJAS", style = datesty, cols = c(6:7), rows = 1:nrow(acordo_trat_venc_lojas %>%  filter(SETOR=='SETOR 3 - CHAPECO - OESTE - RS'))+1, gridExpand = TRUE)

## ACORDOS VENCIDOS GRUPOS ==================

addWorksheet(wbtrat_st3, "ACORDOS VENCIDOS GRUPOS")

## FORMAT

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 1, widths = 12)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 2, widths = 30)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 3, widths = 12)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 4, widths = 30)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 5, widths = 13)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 6, widths = 13)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 7, widths = 13)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 8, widths = 10)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 9, widths = 10)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 10, widths = 10)

setColWidths(wbtrat_st3, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 11, widths = 10)


queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st3, "ACORDOS VENCIDOS GRUPOS", acordo_trat_venc_grupos, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st3, sheet = "ACORDOS VENCIDOS GRUPOS", style = datesty, cols = c(5:6), rows = 1:nrow(acordo_trat_venc_grupos)+1, gridExpand = TRUE)

## WORKBOOK ====

saveWorkbook(wbtrat_st3, file = "TRATAMENTOS_BONIFICADOS_SETOR3.xlsx", overwrite = TRUE)


## SETOR 4 ===============================

## GERAL

wbtrat_st4 <- createWorkbook()

addWorksheet(wbtrat_st4, "ACORDOS ATIVOS LOJAS")

## FORMAT

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS LOJAS", cols = 1, widths = 12)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS LOJAS", cols = 2, widths = 30)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS LOJAS", cols = 3, widths = 12)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS LOJAS", cols = 4, widths = 30)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS LOJAS", cols = 5, widths = 30)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS LOJAS", cols = 6, widths = 13)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS LOJAS", cols = 7, widths = 13)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS LOJAS", cols = 8, widths = 10)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS LOJAS", cols = 9, widths = 10)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS LOJAS", cols = 10, widths = 10)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS LOJAS", cols = 11, widths = 10)

crescimento <- createStyle(fontColour = "#02862a", bgFill = "#ccf2d8")

queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st4, "ACORDOS ATIVOS LOJAS", acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 4 - JOINVILLE - NORTE'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st4, sheet = "ACORDOS ATIVOS LOJAS", style = datesty, cols = c(6:7), rows = 1:nrow(acordo_trat_ativo_lojas)+1, gridExpand = TRUE)

conditionalFormatting(wbtrat_st4, sheet = "ACORDOS ATIVOS LOJAS", cols = 11, rows = 1:nrow(acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 4 - JOINVILLE - NORTE')), rule = 'K1 <= 0', style = queda)

conditionalFormatting(wbtrat_st4, sheet = "ACORDOS ATIVOS LOJAS", cols = 8, rows = 1:nrow(acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 4 - JOINVILLE - NORTE')), rule = 'H1== "I"', style = queda)


## GRUPOS

addWorksheet(wbtrat_st4, "ACORDOS ATIVOS GRUPO")

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS GRUPO", cols = 1, widths = 12)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS GRUPO", cols = 2, widths = 30)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS GRUPO", cols = 3, widths = 12)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS GRUPO", cols = 4, widths = 30)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS GRUPO", cols = 5, widths = 13)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS GRUPO", cols = 6, widths = 13)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS GRUPO", cols = 7, widths = 13)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS GRUPO", cols = 8, widths = 10)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS GRUPO", cols = 9, widths = 10)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS GRUPO", cols = 10, widths = 10)

setColWidths(wbtrat_st4, sheet = "ACORDOS ATIVOS GRUPO", cols = 11, widths = 10)

writeDataTable(wbtrat_st4, "ACORDOS ATIVOS GRUPO", acordo_trat_ativo_grupos, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st4, sheet = "ACORDOS ATIVOS GRUPO", style = datesty, cols = c(5:6), rows = 1:nrow(acordo_trat_ativo_grupos)+1, gridExpand = TRUE)

conditionalFormatting(wbtrat_st4, sheet = "ACORDOS ATIVOS GRUPO", cols = 10, rows = 1:nrow(acordo_trat_ativo_grupos), rule = 'J1 <= 0', style = queda)

conditionalFormatting(wbtrat_st4, sheet = "ACORDOS ATIVOS GRUPO", cols = 7, rows = 1:nrow(acordo_trat_ativo_grupos), rule = 'G1== "I"', style = queda)


## EXTRATOS

addWorksheet(wbtrat_st4, "EXTRATO")

setColWidths(wbtrat_st4, sheet = "EXTRATO", cols = 1, widths = 12)

setColWidths(wbtrat_st4, sheet = "EXTRATO", cols = 2, widths = 12)

setColWidths(wbtrat_st4, sheet = "EXTRATO", cols = 3, widths = 12)

setColWidths(wbtrat_st4, sheet = "EXTRATO", cols = 4, widths = 12)

setColWidths(wbtrat_st4, sheet = "EXTRATO", cols = 5, widths = 12)

setColWidths(wbtrat_st4, sheet = "EXTRATO", cols = 6, widths = 30)

setColWidths(wbtrat_st4, sheet = "EXTRATO", cols = 7, widths = 10)

setColWidths(wbtrat_st4, sheet = "EXTRATO", cols = 8, widths = 30)

setColWidths(wbtrat_st4, sheet = "EXTRATO", cols = 9, widths = 12)

setColWidths(wbtrat_st4, sheet = "EXTRATO", cols = 10, widths = 12)


writeDataTable(wbtrat_st4, "EXTRATO", trat_bonif_extrato %>%  filter(SETOR=='SETOR 4 - JOINVILLE - NORTE'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st4, sheet = "EXTRATO", style = datesty, cols = 2, rows = 1:nrow(trat_bonif_extrato %>%  filter(SETOR=='SETOR 4 - JOINVILLE - NORTE'))+1, gridExpand = TRUE)

## ACORDOS VENCIDOS LOJAS ==================

addWorksheet(wbtrat_st4, "ACORDOS VENCIDOS LOJAS")

## FORMAT

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS LOJAS", cols = 1, widths = 12)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS LOJAS", cols = 2, widths = 30)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS LOJAS", cols = 3, widths = 12)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS LOJAS", cols = 4, widths = 30)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS LOJAS", cols = 5, widths = 30)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS LOJAS", cols = 6, widths = 13)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS LOJAS", cols = 7, widths = 13)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS LOJAS", cols = 8, widths = 10)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS LOJAS", cols = 9, widths = 10)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS LOJAS", cols = 10, widths = 10)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS LOJAS", cols = 11, widths = 10)


queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st4, "ACORDOS VENCIDOS LOJAS", acordo_trat_venc_lojas %>%  filter(SETOR=='SETOR 4 - JOINVILLE - NORTE'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st4, sheet = "ACORDOS VENCIDOS LOJAS", style = datesty, cols = c(6:7), rows = 1:nrow(acordo_trat_venc_lojas %>%  filter(SETOR=='SETOR 4 - JOINVILLE - NORTE'))+1, gridExpand = TRUE)

## ACORDOS VENCIDOS GRUPOS ==================

addWorksheet(wbtrat_st4, "ACORDOS VENCIDOS GRUPOS")

## FORMAT

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 1, widths = 12)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 2, widths = 30)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 3, widths = 12)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 4, widths = 30)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 5, widths = 13)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 6, widths = 13)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 7, widths = 13)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 8, widths = 10)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 9, widths = 10)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 10, widths = 10)

setColWidths(wbtrat_st4, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 11, widths = 10)


queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st4, "ACORDOS VENCIDOS GRUPOS", acordo_trat_venc_grupos, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st4, sheet = "ACORDOS VENCIDOS GRUPOS", style = datesty, cols = c(5:6), rows = 1:nrow(acordo_trat_venc_grupos)+1, gridExpand = TRUE)

## WORKBOOK ====

saveWorkbook(wbtrat_st4, file = "TRATAMENTOS_BONIFICADOS_SETOR4.xlsx", overwrite = TRUE)


## SETOR 5 ===============================

## GERAL

wbtrat_st5 <- createWorkbook()

addWorksheet(wbtrat_st5, "ACORDOS ATIVOS LOJAS")

## FORMAT

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS LOJAS", cols = 1, widths = 12)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS LOJAS", cols = 2, widths = 30)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS LOJAS", cols = 3, widths = 12)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS LOJAS", cols = 4, widths = 30)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS LOJAS", cols = 5, widths = 30)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS LOJAS", cols = 6, widths = 13)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS LOJAS", cols = 7, widths = 13)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS LOJAS", cols = 8, widths = 10)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS LOJAS", cols = 9, widths = 10)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS LOJAS", cols = 10, widths = 10)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS LOJAS", cols = 11, widths = 10)

crescimento <- createStyle(fontColour = "#02862a", bgFill = "#ccf2d8")

queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st5, "ACORDOS ATIVOS LOJAS", acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 5 - BLUMENAU - VALE'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st5, sheet = "ACORDOS ATIVOS LOJAS", style = datesty, cols = c(6:7), rows = 1:nrow(acordo_trat_ativo_lojas)+1, gridExpand = TRUE)

conditionalFormatting(wbtrat_st5, sheet = "ACORDOS ATIVOS LOJAS", cols = 11, rows = 1:nrow(acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 5 - BLUMENAU - VALE')), rule = 'K1 <= 0', style = queda)

conditionalFormatting(wbtrat_st5, sheet = "ACORDOS ATIVOS LOJAS", cols = 8, rows = 1:nrow(acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 5 - BLUMENAU - VALE')), rule = 'H1== "I"', style = queda)


## GRUPOS

addWorksheet(wbtrat_st5, "ACORDOS ATIVOS GRUPO")

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS GRUPO", cols = 1, widths = 12)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS GRUPO", cols = 2, widths = 30)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS GRUPO", cols = 3, widths = 12)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS GRUPO", cols = 4, widths = 30)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS GRUPO", cols = 5, widths = 13)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS GRUPO", cols = 6, widths = 13)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS GRUPO", cols = 7, widths = 13)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS GRUPO", cols = 8, widths = 10)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS GRUPO", cols = 9, widths = 10)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS GRUPO", cols = 10, widths = 10)

setColWidths(wbtrat_st5, sheet = "ACORDOS ATIVOS GRUPO", cols = 11, widths = 10)

writeDataTable(wbtrat_st5, "ACORDOS ATIVOS GRUPO", acordo_trat_ativo_grupos, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st5, sheet = "ACORDOS ATIVOS GRUPO", style = datesty, cols = c(5:6), rows = 1:nrow(acordo_trat_ativo_grupos)+1, gridExpand = TRUE)

conditionalFormatting(wbtrat_st5, sheet = "ACORDOS ATIVOS GRUPO", cols = 10, rows = 1:nrow(acordo_trat_ativo_grupos), rule = 'J1 <= 0', style = queda)

conditionalFormatting(wbtrat_st5, sheet = "ACORDOS ATIVOS GRUPO", cols = 7, rows = 1:nrow(acordo_trat_ativo_grupos), rule = 'G1== "I"', style = queda)


## EXTRATOS

addWorksheet(wbtrat_st5, "EXTRATO")

setColWidths(wbtrat_st5, sheet = "EXTRATO", cols = 1, widths = 12)

setColWidths(wbtrat_st5, sheet = "EXTRATO", cols = 2, widths = 12)

setColWidths(wbtrat_st5, sheet = "EXTRATO", cols = 3, widths = 12)

setColWidths(wbtrat_st5, sheet = "EXTRATO", cols = 4, widths = 12)

setColWidths(wbtrat_st5, sheet = "EXTRATO", cols = 5, widths = 12)

setColWidths(wbtrat_st5, sheet = "EXTRATO", cols = 6, widths = 30)

setColWidths(wbtrat_st5, sheet = "EXTRATO", cols = 7, widths = 10)

setColWidths(wbtrat_st5, sheet = "EXTRATO", cols = 8, widths = 30)

setColWidths(wbtrat_st5, sheet = "EXTRATO", cols = 9, widths = 12)

setColWidths(wbtrat_st5, sheet = "EXTRATO", cols = 10, widths = 12)


writeDataTable(wbtrat_st5, "EXTRATO", trat_bonif_extrato %>%  filter(SETOR=='SETOR 5 - BLUMENAU - VALE'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st5, sheet = "EXTRATO", style = datesty, cols = 2, rows = 1:nrow(trat_bonif_extrato %>%  filter(SETOR=='SETOR 5 - BLUMENAU - VALE'))+1, gridExpand = TRUE)

## ACORDOS VENCIDOS LOJAS ==================

addWorksheet(wbtrat_st5, "ACORDOS VENCIDOS LOJAS")

## FORMAT

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS LOJAS", cols = 1, widths = 12)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS LOJAS", cols = 2, widths = 30)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS LOJAS", cols = 3, widths = 12)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS LOJAS", cols = 4, widths = 30)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS LOJAS", cols = 5, widths = 30)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS LOJAS", cols = 6, widths = 13)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS LOJAS", cols = 7, widths = 13)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS LOJAS", cols = 8, widths = 10)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS LOJAS", cols = 9, widths = 10)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS LOJAS", cols = 10, widths = 10)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS LOJAS", cols = 11, widths = 10)


queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st5, "ACORDOS VENCIDOS LOJAS", acordo_trat_venc_lojas %>%  filter(SETOR=='SETOR 5 - BLUMENAU - VALE'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st5, sheet = "ACORDOS VENCIDOS LOJAS", style = datesty, cols = c(6:7), rows = 1:nrow(acordo_trat_venc_lojas %>%  filter(SETOR=='SETOR 5 - BLUMENAU - VALE'))+1, gridExpand = TRUE)

## ACORDOS VENCIDOS GRUPOS ==================

addWorksheet(wbtrat_st5, "ACORDOS VENCIDOS GRUPOS")

## FORMAT

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 1, widths = 12)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 2, widths = 30)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 3, widths = 12)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 4, widths = 30)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 5, widths = 13)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 6, widths = 13)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 7, widths = 13)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 8, widths = 10)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 9, widths = 10)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 10, widths = 10)

setColWidths(wbtrat_st5, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 11, widths = 10)


queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st5, "ACORDOS VENCIDOS GRUPOS", acordo_trat_venc_grupos, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st5, sheet = "ACORDOS VENCIDOS GRUPOS", style = datesty, cols = c(5:6), rows = 1:nrow(acordo_trat_venc_grupos)+1, gridExpand = TRUE)

## WORKBOOK ====

saveWorkbook(wbtrat_st5, file = "TRATAMENTOS_BONIFICADOS_SETOR5.xlsx", overwrite = TRUE)

## SETOR 6 ===============================

## GERAL

wbtrat_st6 <- createWorkbook()

addWorksheet(wbtrat_st6, "ACORDOS ATIVOS LOJAS")

## FORMAT

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS LOJAS", cols = 1, widths = 12)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS LOJAS", cols = 2, widths = 30)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS LOJAS", cols = 3, widths = 12)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS LOJAS", cols = 4, widths = 30)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS LOJAS", cols = 5, widths = 30)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS LOJAS", cols = 6, widths = 13)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS LOJAS", cols = 7, widths = 13)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS LOJAS", cols = 8, widths = 10)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS LOJAS", cols = 9, widths = 10)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS LOJAS", cols = 10, widths = 10)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS LOJAS", cols = 11, widths = 10)

crescimento <- createStyle(fontColour = "#02862a", bgFill = "#ccf2d8")

queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st6, "ACORDOS ATIVOS LOJAS", acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 6 - B CAMBORIU - LITORAL'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st6, sheet = "ACORDOS ATIVOS LOJAS", style = datesty, cols = c(6:7), rows = 1:nrow(acordo_trat_ativo_lojas)+1, gridExpand = TRUE)

conditionalFormatting(wbtrat_st6, sheet = "ACORDOS ATIVOS LOJAS", cols = 11, rows = 1:nrow(acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 6 - B CAMBORIU - LITORAL')), rule = 'K1 <= 0', style = queda)

conditionalFormatting(wbtrat_st6, sheet = "ACORDOS ATIVOS LOJAS", cols = 8, rows = 1:nrow(acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 6 - B CAMBORIU - LITORAL')), rule = 'H1== "I"', style = queda)


## GRUPOS

addWorksheet(wbtrat_st6, "ACORDOS ATIVOS GRUPO")

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS GRUPO", cols = 1, widths = 12)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS GRUPO", cols = 2, widths = 30)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS GRUPO", cols = 3, widths = 12)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS GRUPO", cols = 4, widths = 30)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS GRUPO", cols = 5, widths = 13)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS GRUPO", cols = 6, widths = 13)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS GRUPO", cols = 7, widths = 13)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS GRUPO", cols = 8, widths = 10)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS GRUPO", cols = 9, widths = 10)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS GRUPO", cols = 10, widths = 10)

setColWidths(wbtrat_st6, sheet = "ACORDOS ATIVOS GRUPO", cols = 11, widths = 10)

writeDataTable(wbtrat_st6, "ACORDOS ATIVOS GRUPO", acordo_trat_ativo_grupos, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st6, sheet = "ACORDOS ATIVOS GRUPO", style = datesty, cols = c(5:6), rows = 1:nrow(acordo_trat_ativo_grupos)+1, gridExpand = TRUE)

conditionalFormatting(wbtrat_st6, sheet = "ACORDOS ATIVOS GRUPO", cols = 10, rows = 1:nrow(acordo_trat_ativo_grupos), rule = 'J1 <= 0', style = queda)

conditionalFormatting(wbtrat_st6, sheet = "ACORDOS ATIVOS GRUPO", cols = 7, rows = 1:nrow(acordo_trat_ativo_grupos), rule = 'G1== "I"', style = queda)


## EXTRATOS

addWorksheet(wbtrat_st6, "EXTRATO")

setColWidths(wbtrat_st6, sheet = "EXTRATO", cols = 1, widths = 12)

setColWidths(wbtrat_st6, sheet = "EXTRATO", cols = 2, widths = 12)

setColWidths(wbtrat_st6, sheet = "EXTRATO", cols = 3, widths = 12)

setColWidths(wbtrat_st6, sheet = "EXTRATO", cols = 4, widths = 12)

setColWidths(wbtrat_st6, sheet = "EXTRATO", cols = 5, widths = 12)

setColWidths(wbtrat_st6, sheet = "EXTRATO", cols = 6, widths = 30)

setColWidths(wbtrat_st6, sheet = "EXTRATO", cols = 7, widths = 10)

setColWidths(wbtrat_st6, sheet = "EXTRATO", cols = 8, widths = 30)

setColWidths(wbtrat_st6, sheet = "EXTRATO", cols = 9, widths = 12)

setColWidths(wbtrat_st6, sheet = "EXTRATO", cols = 10, widths = 12)


writeDataTable(wbtrat_st6, "EXTRATO", trat_bonif_extrato %>%  filter(SETOR=='SETOR 6 - B CAMBORIU - LITORAL'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st6, sheet = "EXTRATO", style = datesty, cols = 2, rows = 1:nrow(trat_bonif_extrato %>%  filter(SETOR=='SETOR 6 - B CAMBORIU - LITORAL'))+1, gridExpand = TRUE)

## ACORDOS VENCIDOS LOJAS ==================

addWorksheet(wbtrat_st6, "ACORDOS VENCIDOS LOJAS")

## FORMAT

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS LOJAS", cols = 1, widths = 12)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS LOJAS", cols = 2, widths = 30)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS LOJAS", cols = 3, widths = 12)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS LOJAS", cols = 4, widths = 30)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS LOJAS", cols = 5, widths = 30)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS LOJAS", cols = 6, widths = 13)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS LOJAS", cols = 7, widths = 13)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS LOJAS", cols = 8, widths = 10)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS LOJAS", cols = 9, widths = 10)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS LOJAS", cols = 10, widths = 10)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS LOJAS", cols = 11, widths = 10)


queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st6, "ACORDOS VENCIDOS LOJAS", acordo_trat_venc_lojas %>%  filter(SETOR=='SETOR 6 - B CAMBORIU - LITORAL'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st6, sheet = "ACORDOS VENCIDOS LOJAS", style = datesty, cols = c(6:7), rows = 1:nrow(acordo_trat_venc_lojas %>%  filter(SETOR=='SETOR 6 - B CAMBORIU - LITORAL'))+1, gridExpand = TRUE)

## ACORDOS VENCIDOS GRUPOS ==================

addWorksheet(wbtrat_st6, "ACORDOS VENCIDOS GRUPOS")

## FORMAT

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 1, widths = 12)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 2, widths = 30)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 3, widths = 12)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 4, widths = 30)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 5, widths = 13)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 6, widths = 13)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 7, widths = 13)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 8, widths = 10)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 9, widths = 10)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 10, widths = 10)

setColWidths(wbtrat_st6, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 11, widths = 10)


queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st6, "ACORDOS VENCIDOS GRUPOS", acordo_trat_venc_grupos, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st6, sheet = "ACORDOS VENCIDOS GRUPOS", style = datesty, cols = c(5:6), rows = 1:nrow(acordo_trat_venc_grupos)+1, gridExpand = TRUE)

## WORKBOOK ====

saveWorkbook(wbtrat_st6, file = "TRATAMENTOS_BONIFICADOS_SETOR6.xlsx", overwrite = TRUE)

## SETOR 7 ===============================

## GERAL

wbtrat_st7 <- createWorkbook()

addWorksheet(wbtrat_st7, "ACORDOS ATIVOS LOJAS")

## FORMAT

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS LOJAS", cols = 1, widths = 12)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS LOJAS", cols = 2, widths = 30)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS LOJAS", cols = 3, widths = 12)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS LOJAS", cols = 4, widths = 30)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS LOJAS", cols = 5, widths = 30)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS LOJAS", cols = 6, widths = 13)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS LOJAS", cols = 7, widths = 13)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS LOJAS", cols = 8, widths = 10)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS LOJAS", cols = 9, widths = 10)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS LOJAS", cols = 10, widths = 10)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS LOJAS", cols = 11, widths = 10)

crescimento <- createStyle(fontColour = "#02862a", bgFill = "#ccf2d8")

queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st7, "ACORDOS ATIVOS LOJAS", acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 7 - FLORIANOPOLIS  LOJAS'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st7, sheet = "ACORDOS ATIVOS LOJAS", style = datesty, cols = c(6:7), rows = 1:nrow(acordo_trat_ativo_lojas)+1, gridExpand = TRUE)

conditionalFormatting(wbtrat_st7, sheet = "ACORDOS ATIVOS LOJAS", cols = 11, rows = 1:nrow(acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 7 - FLORIANOPOLIS  LOJAS')), rule = 'K1 <= 0', style = queda)

conditionalFormatting(wbtrat_st7, sheet = "ACORDOS ATIVOS LOJAS", cols = 8, rows = 1:nrow(acordo_trat_ativo_lojas %>% filter(SETOR=='SETOR 7 - FLORIANOPOLIS  LOJAS')), rule = 'H1== "I"', style = queda)


## GRUPOS

addWorksheet(wbtrat_st7, "ACORDOS ATIVOS GRUPO")

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS GRUPO", cols = 1, widths = 12)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS GRUPO", cols = 2, widths = 30)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS GRUPO", cols = 3, widths = 12)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS GRUPO", cols = 4, widths = 30)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS GRUPO", cols = 5, widths = 13)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS GRUPO", cols = 6, widths = 13)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS GRUPO", cols = 7, widths = 13)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS GRUPO", cols = 8, widths = 10)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS GRUPO", cols = 9, widths = 10)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS GRUPO", cols = 10, widths = 10)

setColWidths(wbtrat_st7, sheet = "ACORDOS ATIVOS GRUPO", cols = 11, widths = 10)

writeDataTable(wbtrat_st7, "ACORDOS ATIVOS GRUPO", acordo_trat_ativo_grupos, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st7, sheet = "ACORDOS ATIVOS GRUPO", style = datesty, cols = c(5:6), rows = 1:nrow(acordo_trat_ativo_grupos)+1, gridExpand = TRUE)

conditionalFormatting(wbtrat_st7, sheet = "ACORDOS ATIVOS GRUPO", cols = 10, rows = 1:nrow(acordo_trat_ativo_grupos), rule = 'J1 <= 0', style = queda)

conditionalFormatting(wbtrat_st7, sheet = "ACORDOS ATIVOS GRUPO", cols = 7, rows = 1:nrow(acordo_trat_ativo_grupos), rule = 'G1== "I"', style = queda)


## EXTRATOS

addWorksheet(wbtrat_st7, "EXTRATO")

setColWidths(wbtrat_st7, sheet = "EXTRATO", cols = 1, widths = 12)

setColWidths(wbtrat_st7, sheet = "EXTRATO", cols = 2, widths = 12)

setColWidths(wbtrat_st7, sheet = "EXTRATO", cols = 3, widths = 12)

setColWidths(wbtrat_st7, sheet = "EXTRATO", cols = 4, widths = 12)

setColWidths(wbtrat_st7, sheet = "EXTRATO", cols = 5, widths = 12)

setColWidths(wbtrat_st7, sheet = "EXTRATO", cols = 6, widths = 30)

setColWidths(wbtrat_st7, sheet = "EXTRATO", cols = 7, widths = 10)

setColWidths(wbtrat_st7, sheet = "EXTRATO", cols = 8, widths = 30)

setColWidths(wbtrat_st7, sheet = "EXTRATO", cols = 9, widths = 12)

setColWidths(wbtrat_st7, sheet = "EXTRATO", cols = 10, widths = 12)


writeDataTable(wbtrat_st7, "EXTRATO", trat_bonif_extrato %>%  filter(SETOR=='SETOR 7 - FLORIANOPOLIS  LOJAS'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st7, sheet = "EXTRATO", style = datesty, cols = 2, rows = 1:nrow(trat_bonif_extrato %>%  filter(SETOR=='SETOR 7 - FLORIANOPOLIS  LOJAS'))+1, gridExpand = TRUE)

## ACORDOS VENCIDOS LOJAS ==================

addWorksheet(wbtrat_st7, "ACORDOS VENCIDOS LOJAS")

## FORMAT

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS LOJAS", cols = 1, widths = 12)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS LOJAS", cols = 2, widths = 30)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS LOJAS", cols = 3, widths = 12)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS LOJAS", cols = 4, widths = 30)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS LOJAS", cols = 5, widths = 30)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS LOJAS", cols = 6, widths = 13)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS LOJAS", cols = 7, widths = 13)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS LOJAS", cols = 8, widths = 10)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS LOJAS", cols = 9, widths = 10)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS LOJAS", cols = 10, widths = 10)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS LOJAS", cols = 11, widths = 10)


queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st7, "ACORDOS VENCIDOS LOJAS", acordo_trat_venc_lojas %>%  filter(SETOR=='SETOR 7 - FLORIANOPOLIS  LOJAS'), startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st7, sheet = "ACORDOS VENCIDOS LOJAS", style = datesty, cols = c(6:7), rows = 1:nrow(acordo_trat_venc_lojas %>%  filter(SETOR=='SETOR 6 - B CAMBORIU - LITORAL'))+1, gridExpand = TRUE)

## ACORDOS VENCIDOS GRUPOS ==================

addWorksheet(wbtrat_st7, "ACORDOS VENCIDOS GRUPOS")

## FORMAT

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 1, widths = 12)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 2, widths = 30)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 3, widths = 12)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 4, widths = 30)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 5, widths = 13)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 6, widths = 13)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 7, widths = 13)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 8, widths = 10)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 9, widths = 10)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 10, widths = 10)

setColWidths(wbtrat_st7, sheet = "ACORDOS VENCIDOS GRUPOS", cols = 11, widths = 10)


queda <- createStyle(fontColour = "#7b1e1e", bgFill = "#e69999")

datesty <- createStyle(numFmt = "dd/MM/yyyy")

writeDataTable(wbtrat_st7, "ACORDOS VENCIDOS GRUPOS", acordo_trat_venc_grupos, startCol = 1, startRow = 1, xy = NULL, colNames = TRUE, rowNames = FALSE, tableStyle = "TableStyleMedium2", tableName = NULL, headerStyle = NULL, withFilter = FALSE, keepNA = FALSE, na.string = NULL, sep = ", ", stack = FALSE, firstColumn = FALSE, lastColumn = FALSE, bandedRows = TRUE, bandedCols = FALSE)

addStyle(wbtrat_st7, sheet = "ACORDOS VENCIDOS GRUPOS", style = datesty, cols = c(5:6), rows = 1:nrow(acordo_trat_venc_grupos)+1, gridExpand = TRUE)

## WORKBOOK ====

saveWorkbook(wbtrat_st7, file = "TRATAMENTOS_BONIFICADOS_SETOR7.xlsx", overwrite = TRUE)




