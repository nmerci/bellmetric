client <- function()
{
  while (TRUE)
  {
    con <- socketConnection(host = "localhost", port = 6011, blocking = TRUE, 
                            server = FALSE, open = "r+")
    
    f <- file("stdin")
    open(f)
    
    print("Enter new event, q to quit")
    sendme <- readLines(f, n = 1)
    
    if (tolower(sendme) == "q")
    {
      break
    }
    
    write_resp <- writeLines(sendme, con)
    server_resp <- readLines(con, 1)
    print(paste("Probability to checkout =", server_resp))
    
    Sys.sleep(2)
    
    close(con)
  }
}

client()