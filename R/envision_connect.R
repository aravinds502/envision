
###############################################################
#'Get datasource connection
#'
#' This is a function to obtain connection to a
#' datasource from the envision server by providing the token
#' obtained from the envision App for the specific datasource
#'
#' @param baseUrl - Envision server URL
#' @param secretKey - Secret key for the datasource
#'                    obtained from the App
#' @export
carriots.analytics.connect <- function(url, token) {

  # //////////////////////////////////////////////////////
  BAConnectionData <- R6::R6Class(
    "BAConnectionData",
    public = list (
      factTable = "",
      jdbc = "",
      columns = "",
      q = "",
      initialize = function(factTable, jdbc, columns,qVar) {
        self$factTable <- factTable
        self$jdbc <- jdbc
        self$columns <- columns
        self$q <- qVar
      },

      quot = function(attribute) {
        tmp <- ""
        tmp <- paste(self$q,attribute,self$q,sep = "")
        tmp
      }
    )
  )

  # //////////////////////////////////////////////////////
  BAConnection <- R6::R6Class(
    "BAConnection",
    private = list(
      conn_data = ''
    ),
    public = list (
      initialize = function(conn_data) {
        private$conn_data <- conn_data
      },
      load = function(...,limit=NA) {
        temp <- as.list(match.call())
        colSelected <- temp[ - which(names(temp) == "limit")]
        colSelected <- colSelected[-1]
        query <- paste("SELECT ",getColumns(colSelected,private$conn_data), sep="");
        query <- paste(query,"FROM",private$conn_data$factTable)
        if(!is.null(limit) & !is.na(limit) & limit > 0)
          query <- paste(query, "LIMIT",limit)

        df <- RJDBC::dbGetQuery(private$conn_data$jdbc,query)
        df
      },

      update = function(where=NULL,set=NULL) {
          if(is.null(set))
            stop("set clause elements are empty")

        setString <- paste("SET",buildSetClause(private$conn_data,set))
        whereString <- paste("WHERE",buildWhereClause(private$conn_data,where))

        query <- paste("UPDATE",private$conn_data$factTable,setString,whereString)

        print(query)
        RJDBC::dbSendUpdate(private$conn_data$jdbc,query)

      },

      getColumnNames = function() {
        cols <- names(private$conn_data$columns)
        cols
      },

      addColumn = function(name=NULL,type=NULL,default = NULL) {
        alterQuery <- "ALTER TABLE"
        if(is.null(name) | is.null(type))
          stop("Column name or type is empty")

        alterQuery <- paste(alterQuery,private$conn_data$factTable,"ADD",name,type)
        if(!is.null(default)) {
          alterQuery <- paste(alterQuery,"NOT NULL DEFAULT")
          if(is.numeric(default))
            alterQuery <- paste(alterQuery," (",default,") ",sep="")
          else
            alterQuery <- paste(alterQuery," '",default,"' ",sep="")
        }
        print(alterQuery)
        RJDBC::dbSendUpdate(private$conn_data$jdbc,alterQuery)

      }
    )
  )

  # //////////////////////////////////////////////////////
  # Call the envision REST API to get the connection data
  # Based on the engine type, get the appropriate JDBC driver
  data <- getDatasourceConnection(url,token)

  jdbc <- data$jdbc
  factTable <- data$ftable
  columns <- data$columns
  quot <- data$quot

  connect_data <- BAConnectionData$new(factTable, jdbc, columns,quot)
  conn <- BAConnection$new(connect_data)
  conn
}

#################################################################################
#' Method to load the table data
#'
#' User can pass the selected column names as the data frame returned by this
#' method will have only the selected columns
#'
#' @param ... - column names
#' @param limit - limit the records in the data frame
#' @param conn  - BAConnection object obtained from connect API
#'
#'@export
carriots.analytics.load = function(...,limit=NA,conn = NULL) {
  conn$load(...,limit=limit)
}

#################################################################################
#' Method to update the data in the table
#'
#' Update the table with the values provided in the data frame. Below is the
#' structure of the data frame for WhereClause
#'
#'   column operator value relate
#'   prod        =    abc  AND
#'   stat        =     1   AND
#'   score       =     2   AND (last relation in the frame Will be ignored)
#'
#'   DataFrame for Set clause
#'
#'   column value
#'   prod     abc
#'   stat     1
#'   score    2
#'
#'  @param where - DataFrame for where Clause
#'  @param set -  DataFrame for Set Clause
#'  @param conn - BAConnection object obtained from connect API
#'
#'@export
carriots.analytics.update = function(where=NULL,set=NULL,conn = NULL) {
  conn$update(where = where, set =set)
}

###############################################################################
#'Reload Datasource
#'
#' Function to refresh the datasource in envision, which reflects
#' the latest update
#'
#' @param baseUrl - Envision server URL
#' @param secretKey - Secret key for the datasource
#'                    obtained from the App
#' @export
carriots.analytics.reload_dataSource <- function(baseUrl,secretKey) {
  res <- doHttpCall(baseUrl,secretKey,"reloadext")
  res
}

##################################################################################
#' UTILITIES
#'
asc <- function(x) { strtoi(charToRaw(x),16L) }

chr <- function(n) { rawToChar(as.raw(n)) }

buildSetClause <- function(con,set) {

  setClause <- ""

  sCols <- set$column
  sVals <- set$value

  colLength <- length(sCols)

  if(is.null(sCols) | is.null(sVals))
    stop("Set clause:Invalid")

  for(i in 1:colLength) {
    if(i > 1)
      setClause <- paste(setClause,",")

    setClause <- paste(setClause, con$quot(sCols[i]),"=")
    setClause <- paste(setClause, " '",sVals[i],"' ", sep = "")

  }

  setClause

}

buildWhereClause <- function(con,where) {
  whereClause <- ""

  if(!is.null(where)) {
    wCols <- where$column
    wOperators <- where$operator
    wVals <- where$value
    wRelate <- where$relate

    colLength <- length(wCols)

    for(i in 1:colLength) {
      whereClause <- paste(whereClause,  con$quot(wCols[i])," ",wOperators[i]," '",wVals[i],"' ", sep = "")
      if(i < colLength)
        whereClause <- paste(whereClause,wRelate[i],"")

    }
  }
  whereClause

}

getColumns <- function(colSelected,conn) {
  cols <- conn$columns
  colString <- ""
  if(!is.null(colSelected) &  length(colSelected) > 0){
    length <- length(colSelected)
    for(i in 1:length) {
      if(i > 1 & i <= length)
        colString <- paste(colString,",")
      if(!is.null(cols[[colSelected[[i]]]])) {
          colString <- paste(colString,conn$quot(cols[[colSelected[[i]]]]))
      }else
          stop(paste("ColName:",colSelected[[i]],"doesn't exists in the table"))
      i <- i+1
    }
  }else
    colString <- "*"

  colString
}

#'Get DataSource Meta data
#'
#' This is a function to obtain the meta data of a
#' datasource from the envision server by providing the secret
#' key of the datasource
#'
#' @param baseUrl - Envision server URL
#' @param token - token for the datasource
#'                    obtained from the App
getDataSourceMetaData <- function(baseUrl,token) {

  res <- doHttpCall(baseUrl,token,"dsExtConnect")
  res
}

#'Get DataSource Connection
#'
#' This is a function to obtain JDBC connection to a
#' datasource from the envision server by providing the secret
#' key of the datasource
#'
#' @param baseUrl - Envision server URL
#' @param token - token for the datasource
#'                    obtained from the App
getDatasourceConnection <- function(baseUrl,token) {

  data <- getDataSourceMetaData(baseUrl,token)
  if (!is.null(data$connect_data)) {
    connect_data <- data$connect_data
    jdbcDetails <- getDriverDetails(connect_data)
    if(is.null(jdbcDetails$driverClass) || is.null(jdbcDetails$driver) || is.null(jdbcDetails$connString))
      stop("Unable to create JDBC connection- required info missing")

    #decrypt password
    passWord <- character()
    encrypt <- strsplit(connect_data$password,"")[[1]]
    for(i in 1 : stringr::str_length(en)) {
      passWord[[i]] <- chr(asc(encrypt[[i]]) - 4)
    }
    decrypt <- paste(passWord,collapse = "")
    passWord <- stringi::stri_reverse(decrypt)
    passWord <- rawToChar(base64enc::base64decode(passWord))

    #passPhrase <- digest::AES(connect_data$ftable,mode="CBC")
    #passPhrase <- substr(passPhrase,0,8)
    #aes <- digest::AES(passPhrase,mode="CBC")
    jdbcDriver <- RJDBC::JDBC(driverClass=jdbcDetails$driverClass,
                              classPath=system.file("extdata",jdbcDetails$driver, package = "envision"),identifier.quote = jdbcDetails$quot)
    conn <- RJDBC::dbConnect(jdbcDriver, jdbcDetails$connString, connect_data$username, passWord)
    ftable <- connect_data$ftable
    data <- NULL
    data$ftable <- ftable
    data$jdbc <- conn
    data$quot <- jdbcDetails$quot
    data$columns <- connect_data$columns

  } else {
    stop("Connect data of the datasource not available")
  }
  data
}

doHttpCall <- function(baseUrl,token,identifier) {
  url_length <- stringr::str_length(baseUrl)

  if(substr(baseUrl,url_length,url_length) == "/") {
    baseUrl <- substr(baseUrl,0,url_length-1)
  }

  baseUrl <- paste(baseUrl,"/datasource.do?action=",identifier, sep="")

  ua      <- "Mozilla/5.0 (Windows NT 6.1; WOW64; rv:33.0) Gecko/20100101 Firefox/33.0"

  doc <- httr::POST(baseUrl,
                    query = list(dstoken=token),
                    httr::user_agent(ua))
  res <- httr::content(doc,useInternalNodes=T)

  data <- jsonlite::fromJSON(res)

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
    jdbcDetails$quot <- "\""

  }else if (toupper(engineType) == "MYSQL") {
    jdbcDetails$driverClass <- "com.mysql.jdbc.Driver"
    jdbcDetails$driver <- "mysql-connector-java-5.1.21-bin"
    jdbcDetails$connString <- paste("jdbc:mysql://",host_port,"/",connect_data$dbName,sep = "")
    jdbcDetails$quot <- "`"

  }else if (toupper(engineType) == "POSTGRESQL" || toupper(engineType) == "REDSHIFT") {
    jdbcDetails$driverClass <- "org.postgresql.Driver"
    jdbcDetails$driver <- "postgresql-9.2-1002.jdbc4.jar"
    jdbcDetails$connString <- paste("jdbc:postgresql://",host_port,"/",connect_data$dbName,sep = "")
    jdbcDetails$quot <- "\""

  }else if (toupper(engineType) == "SQLSERVER") {
    jdbcDetails$driverClass <- "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    jdbcDetails$driver <- "sqljdbc4.jar"
    jdbcDetails$connString <- paste("jdbc:sqlserver://",getSQLServerConnString(connect_data), sep="")
    jdbcDetails$quot <- "\""

  }else if (toupper(engineType) == "ORACLE") {
    jdbcDetails$driverClass <- "oracle.jdbc.driver.OracleDriver"
    jdbcDetails$driver <- "ojdbc6.jar"
    jdbcDetails$connString <- paste("jdbc:oracle:thin:@",host_port,"/",connect_data$dbName,sep = "")
    jdbcDetails$quot <- "\""

  }else if (toupper(engineType) == "SUNDB") {
    jdbcDetails$driverClass <- "sunje.sundb.jdbc.SundbDriver"
    jdbcDetails$driver <- "sundb6.jar"
    jdbcDetails$connString <- paste("jdbc:sundb://",host_port,"/",connect_data$dbName,sep = "")
    jdbcDetails$quot <- "\""

  }else if (toupper(engineType) == "MARIADB") {
    jdbcDetails$driverClass <- "org.mariadb.jdbc.Driver"
    jdbcDetails$driver <- "mariadb-java-client-1.3.3.jar"
    jdbcDetails$connString <- paste("jdbc:mariadb://",host_port,"/",connect_data$dbName,sep = "")
    jdbcDetails$quot <- "\""
  }

  jdbcDetails
}

getSQLServerConnString <- function(connect_data) {
  conn_string <- paste(connect_data$hostname,connect_data$dbName, sep="\\")
  if(!is.null(connect_data$port))
    conn_string <- paste(conn_string,connect_data$port,sep=":")

  conn_string
}




