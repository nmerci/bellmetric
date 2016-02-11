library(shiny)
library(data.table)
library(ROCR)

data <- read.csv("data/data.csv")
source("helpers.R")

shinyServer(
  function(input, output) {
    output$text1<- renderText({ paste("Threshold =",input$threshold)
    })
    
    output$text2 <- renderText({ paste(c("True negative =","False positive ="), table(as.numeric(data$pred>=input$threshold),data$V1)[1,])
          })
    
    output$text3<- renderText({ paste(c("False negative =","True positive ="),table(as.numeric(data$pred>=input$threshold),data$V1)[2,])
         })
    
    output$text4<- renderText({ paste("F1 Score =",f1(input$threshold,data$V1,data$pred))
    })
    
    output$plot <- renderPlot({ roc(input$threshold,data$V1,data$pred)     
    })
  })
