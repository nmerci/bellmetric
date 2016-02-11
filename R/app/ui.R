library(shiny)

shinyUI(fluidPage(
  titlePanel("threshold"),
  
  sidebarLayout(
    sidebarPanel(
     
      sliderInput("threshold", 
                  label = "Select a threshold",
                  min = 0, max = 1, value = 0.05)
      ),
    
    mainPanel(
      textOutput("text1"),
      textOutput("text2"),
      textOutput("text3"),
      textOutput("text4"),
      plotOutput("plot")  
    )
  )
))