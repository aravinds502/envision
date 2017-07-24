#'Get DataSource Meta data
#'
#' This is a function to obtain the meta data of a
#' datasource from the envision server by providing the secret
#' key of the datasource
#'
#' @param baseUrl - Envision server URL
#' @param secretKey - Secret key for the datasource
#'                    obtained from the App
#' @export
getDataSourceMetaData <- function(baseUrl,secretKey) {

  res <- doHttpCall(baseUrl,secretKey,"dsExtConnect")
  res
}

#'Reload Datasource
#'
#' Function to refresh the datasource in envision, which reflects
#' the latest update
#'
#' @param baseUrl - Envision server URL
#' @param secretKey - Secret key for the datasource
#'                    obtained from the App
#' @export
reload_dataSource <- function(baseUrl,secretKey) {
  res <- doHttpCall(baseUrl,secretKey,"reloadext")
  res
}

doHttpCall <- function(baseUrl,secretKey,identifier) {
  url_length <- stringr::str_length(baseUrl)

  if(substr(baseUrl,url_length,url_length) == "/") {
    baseUrl <- substr(baseUrl,0,url_length-1)
  }

  baseUrl <- paste(baseUrl,"/datasource.do?action=",identifier, sep="")

  ua      <- "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:33.0) Gecko/20100101 Firefox/33.0"

  doc <- httr::POST(baseUrl,
                    query = list(dstoken=secretKey),
                    httr::user_agent(ua))
  res <- httr::content(doc,useInternalNodes=T)

  data <- jsonlite::fromJSON(res)

  data
}

#'Get DataSource Connection
#'
#' This is a function to obtain JDBC connection to a
#' datasource from the envision server by providing the secret
#' key of the datasource
#'
#' @param baseUrl - Envision server URL
#' @param secretKey - Secret key for the datasource
#'                    obtained from the App
#' @export
getDatasourceConnection <- function(baseUrl,secretKey) {

  data <- getDataSourceMetaData(baseUrl,secretKey)
  if (!is.null(data$connect_data)) {
    connect_data <- data$connect_data
    jdbcDetails <- getDriverDetails(connect_data)
    if(is.null(jdbcDetails$driverClass) || is.null(jdbcDetails$driver) || is.null(jdbcDetails$connString))
      stop("Unable to create JDBC connection- required info missing")
    jdbcDriver <- RJDBC::JDBC(driverClass=jdbcDetails$driverClass,
                              classPath=system.file("extdata",jdbcDetails$driver, package = "envision"))
    conn <- RJDBC::dbConnect(jdbcDriver, jdbcDetails$connString, connect_data$username, connect_data$password)
    ftable <- connect_data$ftable
    data <- NULL
    data$ftable <- ftable
    data$jdbc <- conn

  } else {
    stop("Connect data of the datasource not available")
  }
  data
}

getDriverDetails <- function(connect_data) {
  if(is.null(connect_data$engine_type))
    stop("Engine Type not found")
  engineType <- connect_data$engine_type

  if(is.null(connect_data$hostname))
    stop("Host Name not found")
  host_port <- connect_data$hostname

  if(!is.null(connect_data$port))
    host_port <- paste(host_port,connect_data$port, sep=":")

  if(is.null(connect_data$dbName))
    stop("DBName not found")

  jdbcDetails <- NULL
  if(toupper(engineType) == "MONETDB") {
    jdbcDetails$driverClass <- "nl.cwi.monetdb.jdbc.MonetDriver"
    jdbcDetails$driver <- "monetdb-jdbc-2.8.jar"
    jdbcDetails$connString <- paste("jdbc:monetdb://",host_port,"/",connect_data$dbName,sep = "")

  }else if (toupper(engineType) == "MYSQL") {
    jdbcDetails$driverClass <- "com.mysql.jdbc.Driver"
    jdbcDetails$driver <- "mysql-connector-java-5.1.21-bin"
    jdbcDetails$connString <- paste("jdbc:mysql://",host_port,"/",connect_data$dbName,sep = "")

  }else if (toupper(engineType) == "POSTGRESQL" || toupper(engineType) == "REDSHIFT") {
    jdbcDetails$driverClass <- "org.postgresql.Driver"
    jdbcDetails$driver <- "postgresql-9.2-1002.jdbc4.jar"
    jdbcDetails$connString <- paste("jdbc:postgresql://",host_port,"/",connect_data$dbName,sep = "")

  }else if (toupper(engineType) == "SQLSERVER") {
    jdbcDetails$driverClass <- "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    jdbcDetails$driver <- "sqljdbc4.jar"
    jdbcDetails$connString <- paste("jdbc:sqlserver://",getSQLServerConnString(connect_data), sep="")

  }else if (toupper(engineType) == "ORACLE") {
    jdbcDetails$driverClass <- "oracle.jdbc.driver.OracleDriver"
    jdbcDetails$driver <- "ojdbc6.jar"
    jdbcDetails$connString <- paste("jdbc:oracle:thin:@",host_port,"/",connect_data$dbName,sep = "")

  }else if (toupper(engineType) == "SUNDB") {
    jdbcDetails$driverClass <- "sunje.sundb.jdbc.SundbDriver"
    jdbcDetails$driver <- "sundb6.jar"
    jdbcDetails$connString <- paste("jdbc:sundb://",host_port,"/",connect_data$dbName,sep = "")

  }else if (toupper(engineType) == "MARIADB") {
    jdbcDetails$driverClass <- "org.mariadb.jdbc.Driver"
    jdbcDetails$driver <- "mariadb-java-client-1.3.3.jar"
    jdbcDetails$connString <- paste("jdbc:mariadb://",host_port,"/",connect_data$dbName,sep = "")
  }

  jdbcDetails
}

getSQLServerConnString <- function(connect_data) {
  conn_string <- paste(connect_data$hostname,connect_data$dbName, sep="\\")
  if(!is.null(connect_data$port))
    conn_string <- paste(conn_string,connect_data$port,sep=":")

  conn_string
}




