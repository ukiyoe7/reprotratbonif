## RESUMO MENSAL TRAT BONIF


library(DBI)
library(tidyverse)
library(googlesheets4)
library(reshape2)
library(lubridate)
library(gmailr)

con2 <- dbConnect(odbc::odbc(), "reproreplica")


## GET DATA

trat_dt <- dbGetQuery(con2,statement = read_file(file = "C:\\Users\\Repro\\Documents\\R\\ADM\\TRAT BONIF\\SQL\\TRAT_BONIF_MONTHLY.sql"))

promo_dt <- dbGetQuery(con2,statement = read_file(file = "C:\\Users\\Repro\\Documents\\R\\ADM\\TRAT BONIF\\SQL\\PROMO.sql"))


## TRANSFORM DATA

trat_dt2 <- anti_join(trat_dt,promo_dt,by="ID_PEDIDO") 

trat_dt3 <- 
trat_dt2 %>% 
  group_by(MES=format(floor_date(PEDDTEMIS,"month"),"%b/%y"),PROCODIGO) %>% 
   summarize(VALOR=sum(QTD)) %>% 
    dcast(PROCODIGO ~ 
     factor(MES, levels = format(seq(floor_date(Sys.Date(), "year"),by="month",length.out = 12),"%b/%y")),value.var = "VALOR",fun.aggregate=sum) %>% 
      rowwise() %>%
        mutate(TOTAL= rowSums(across(where(is.numeric)),na.rm = TRUE)) %>% 
         arrange(desc(TOTAL)) %>% 
          as.data.frame() %>% 
           bind_rows(summarise(., across(where(is.numeric), sum),
             across(where(is.character), ~'TOTAL'))) 


range_write("1RFpNrOVCa29dL0H3KQBGrDorCE8X_UgYYmGKKzouBx4",
            sheet = "RESUMO",data = trat_dt3, range = "A:M",reformat = FALSE)


## SEND MAIL ====================================================================


gm_auth_configure(path = "C:\\Users\\Repro\\Documents\\R\\ADM\\TRAT BONIF\\REPORTS\\sendmail.json")

mymail <- gm_mime() %>% 
  gm_to("sandro.jakoska@repro.com.br,cristiano.regis@repro.com.br") %>% 
  gm_from ("comunicacao@repro.com.br") %>%
  gm_subject("RELATÓRIO - RESUMO TRATAMENTOS BONIFICADOS") %>%
  gm_text_body("RESUMO TRATAMENTOS BONIFICADOS.
ACESSE O LINK https://docs.google.com/spreadsheets/d/1RFpNrOVCa29dL0H3KQBGrDorCE8X_UgYYmGKKzouBx4/edit?usp=sharing.

ESSE É UM EMAIL AUTOMÁTICO.") 
gm_send_message(mymail)


