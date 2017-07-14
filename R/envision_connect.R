#' Connect to Envision
#'
#' This is a function to obtain the connection detils for a
#' datasource from the envision server by providing the secret
#' key of the datasource
#'
#' @param baseUrl - Envision server URL
#' @param secretKey - Secret key for the datasource
#'                    obtained from the App
#' @export
envision_connect <- function(baseUrl,secretKey) {

  url_length <- stringr::str_length(baseUrl)

  if(substr(baseUrl,url_length,url_length) == "/") {
    baseUrl <- substr(baseurl,0,url_length-1)
  }

  baseUrl <- paste(baseUrl,"/datasource.do?action=dsExtConnect", sep="")

  ua      <- "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:33.0) Gecko/20100101 Firefox/33.0"

  doc <- httr::POST(baseUrl,
              query = list(dstoken=secretKey),
              httr::user_agent(ua))

  res <- httr::content(doc,useInternalNodes=T)

  res
}



