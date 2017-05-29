library(dplyr)
library(lubridate)
library(stringr)
library(gsubfn)
data<-"C:\\Users\\HariKrishna.Majety\\Documents\\TeamInd"
setwd(data)

gap<-read.csv("gaps1.csv")
eval<-read.csv("situations1.csv")
keyques<-read.csv("keyquestions1.csv")
View(eval)
View(gap)

eval2<-eval
eval2$future_state<-gsub("(^Outcome:)|(^outcome:)|(<[^>]*>)"," ",str_trim(eval$future_state))
eval2$future_state<-str_trim(gsub("(&[a-zA-Z]*;)"," ",eval2$future_state))
eval2$future_state<-str_trim(gsub("[\\n]"," ",eval2$future_state))
eval2$future_state<-gsub("([a-zA-Z]*:)|([a-zA-Z]* :)","",str_trim(eval2$future_state))
eval2$future_state<-str_trim(gsub("[ ]{1,}"," ",eval2$future_state))

eval2$situation<-gsub("(^Outcome:)|(^outcome:)|(<[^>]*>)|(&[a-zA-Z]*;)"," ",str_trim(eval$situation))
eval2$situation<-str_trim(gsub("(&[a-zA-Z]*;)"," ",eval2$situation))
eval2$situation<-str_trim(gsub("[\\n]"," ",eval2$situation))
eval2$situation<-str_trim(gsub("([a-zA-Z]*:)|([a-zA-Z]* :)","",eval2$situation))
eval2$situation<-str_trim(gsub("[ ]{1,}"," ",eval2$situation))

eval2$project_id<-strapplyc(as.character(eval$project_id),"ObjectID\\((.*)\\)",simplify = TRUE)

gap2<-gap

gap2$gap<-gsub("(^Gap:)|(^gap:)|(<[^>]*>)"," ",str_trim(gap$gap))
gap2$gap<-str_trim(gsub("(&[a-zA-Z]*;)"," ",gap2$gap))
gap2$gap<-str_trim(gsub("[\\n]"," ",gap2$gap))
gap2$gap<-gsub("([a-zA-Z]*:)|([a-zA-Z]* :)","",str_trim(gap2$gap))
gap2$gap<-str_trim(gsub("[ ]{1,}"," ",gap2$gap))

gap2$project_id<-strapplyc(as.character(gap2$project_id),"project_id : \\{ \\$oid : (.*) \\}",simplify = TRUE)

View(keyques)
keyques2<-keyques

keyques2$keyquestion<-gsub("(^Keyquestion :)|(^keyquestion :)|(<[^>]*>)"," ",str_trim(keyques$keyquestion))
keyques2$keyquestion<-str_trim(gsub("(&[a-zA-Z]*;)"," ",keyques2$keyquestion))
keyques2$keyquestion<-str_trim(gsub("[\\n]"," ",keyques2$keyquestion))
keyques2$keyquestion<-gsub("([a-zA-Z]*:)|([a-zA-Z]* :)","",str_trim(keyques2$keyquestion))
keyques2$keyquestion<-str_trim(gsub("[ ]{1,}"," ",keyques2$keyquestion))

keyques2$title<-gsub("(^Title:)|(^title:)|(<[^>]*>)"," ",str_trim(keyques$title))
keyques2$title<-str_trim(gsub("(&[a-zA-Z]*;)"," ",keyques2$title))
keyques2$title<-str_trim(gsub("[\\n]"," ",keyques2$title))
keyques2$title<-gsub("([a-zA-Z]*:)|([a-zA-Z]* :)","",str_trim(keyques2$title))
keyques2$title<-str_trim(gsub("[ ]{1,}"," ",keyques2$title))

keyques2$project_id<-strapplyc(as.character(keyques2$project_id),"project_id : \\{ \\$oid : (.*) \\}",simplify = TRUE)

View(eval2)
View(gap2)
View(keyques2)
eval3<-eval2%>%select(situation,future_state,project_id)
gap3<-gap2%>%select(gap,project_id)
keyques3<-keyques2%>%select(keyquestion,title,project_id)
View(eval3)
View(gap3)
View(keyques3)

FinalOutput<-merge(eval3,gap3,by.x = "project_id",by.y = "project_id",all.x = TRUE)
FinalOutput<-merge(FinalOutput,keyques3,by.x = "project_id",by.y = "project_id",all.x = TRUE)
View(FinalOutput)
#FinalOutput<-merge(FinalOutput)


write.csv(FinalOutput,"cleanedSituationGapQuestion.csv")
