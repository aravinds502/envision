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
carriots.analytics.connect <- function(url,token) {

  # //////////////////////////////////////////////////////
  BAConnectionData <- R6::R6Class(
    "BAConnectionData",
    public = list (
      factTable = "",
      jdbc = "",
      columns = "",
      engineType = "",
      ba2DBTypes = "",
      q = "",
      username = "",
      initialize = function(factTable, jdbc, columns,ba2DBTypes,qVar,uname) {
        self$factTable <- factTable
        self$jdbc <- jdbc
        self$columns <- columns
        self$ba2DBTypes <- ba2DBTypes
        self$q <- qVar
        self$username <- uname
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
      conn_data = '',
      createTable = function(df,tableName,colName,type) {
        if(is.null(tableName))
          stop("Invalid table name")

        md5table <- digest::digest(tableName,"md5",serialize = FALSE)
        temp <- unlist(strsplit(private$conn_data$factTable,split = "[.]"))
        if(length(temp) > 1)
         md5table <- paste(temp[1],".",private$conn_data$quot(md5table),sep="")
        else
          md5table <- private$conn_data$quot(md5table)

        private$dropIfExists(md5table)

        colNames <- colnames(df)
        colNames <- colNames[colNames!=colName]

        if(!all(colNames %in% self$getColumnNames()))
          stop("Data Frame has more than one new column compared to fact table")

        query <- "CREATE TABLE"
        query <- paste(query,md5table,"AS")
        colString <- paste(private$conn_data$quot(colNames),",",collapse = "")
        colString <- substr(colString,1,nchar(colString)-1)

        orgQuery <- paste("(","SELECT",colString,"FROM",private$conn_data$factTable,"WHERE 1=2",")")
        query <- paste(query,orgQuery)

        print(query)
        RJDBC::dbSendUpdate(private$conn_data$jdbc,query)

        #Add a column first
        private$addColumn(md5table,name=colName,type=type)

        md5table


      },
      addColumn = function(md5table,name=NULL, type =NULL,default = NULL) {

        if(is.null(name) | is.null(type))
          stop("Column name or type is empty")

        if(!(type %in% names(private$conn_data$ba2DBTypes)))
          stop(paste("Invalid Type selected","-",type))

        alterQuery <- "ALTER TABLE"
        alterQuery <- paste(alterQuery,md5table,"ADD COLUMN",private$conn_data$quot(name),private$conn_data$ba2DBTypes[[type]])
        if(!is.null(default)) {
          alterQuery <- paste(alterQuery,"NOT NULL DEFAULT")
          if(is.numeric(default))
            alterQuery <- paste(alterQuery," (",default,") ",sep="")
          else
            alterQuery <- paste(alterQuery," '",default,"' ",sep="")
        }
        print(alterQuery)
        RJDBC::dbSendUpdate(private$conn_data$jdbc,alterQuery)
      },

      insertData = function(tablename,dataframe) {

        if(nrow(dataframe) < 1)
          stop("No data available in dataframe")

        query <- "INSERT INTO"
        query <- paste(query,tablename)
        colNames <- paste("\"",colnames(dataframe),"\"",sep="")
        query <- paste(query,"(",paste(colNames,collapse=","),")")

        query <- paste(query, "VALUES")

        values <- ""

        for(i in 1:nrow(dataframe)) {
          if( i > 1) values <- paste(values,",")
          row <- dataframe[i,]
          values <- paste(values,paste("(",paste(paste("'",row,"'",sep=""),collapse=","),")",sep = ""))
        }

        query <- paste(query,values)
        print(query)

        RJDBC::dbSendUpdate(private$conn_data$jdbc,query)

      },

      dropIfExists = function(tableName) {
        t <- tableName
        temp <- unlist(strsplit(t,split = "[.]"))
        if(length(temp) > 1) {
          RJDBC::dbSendUpdate(private$conn_data$jdbc, paste("set schema",temp[1]))
          t <- temp[2]
        }

        t <- gsub("\"","",t)

        if(RJDBC::dbExistsTable(private$conn_data$jdbc,t)) {
          RJDBC::dbRemoveTable(private$conn_data$jdbc,DBI::dbQuoteIdentifier(private$conn_data$jdbc,t))
        }
      }
    ),
    public = list (
      dataTypes = '',
      initialize = function(conn_data) {
        private$conn_data <- conn_data
        self$dataTypes <- list(STRING = "STRING", NUMERIC = "NUMERIC", INTEGER = "INTEGER",
                               DATE = "DATE", DATETIME = "DATETIME", TIME = "TIME")
      },

      load = function(columns=NULL) {
        query <- paste("SELECT ",getColumns(colSelected=columns,private$conn_data), sep="");
        query <- paste(query,"FROM",private$conn_data$factTable)
        # if(!is.null(limit) & !is.na(limit) & limit > 0)
        #   query <- paste(query, "LIMIT",limit)

        df <- RJDBC::dbGetQuery(private$conn_data$jdbc,query)
        col2Label <- getColumn2Label(private$conn_data$columns)
        orgNames <- names(df)
        for(i in 1:length(orgNames)) {
          names(df)[i] <- col2Label[names(df)[i]]
        }

        df
      },


      updateDataFrame = function(df=NULL,colName=NULL,type=NULL) {

        if(is.na(df) || missing(colName))
          stop("Required parameters were missing")

        if(!(colName %in% colnames(df)))
          stop("Specified column doesnt exists in the data frame")

        #create a duplicate tabel with additional column, named as MD5(<FACTTABLE>_<COLNAME>)
        tableName <- paste(private$conn_data$factTable,colName, sep="_")

        # If module is parameterized append the userName as well _<USERNAME> to tableName
        if(carriots.analytics.isParametrized)
          tableName <- paste(tableName,private$conn_data$username, sep="_")

        md5Table <- private$createTable(df,tableName,colName,type)

        label2Col <-private$conn_data$columns
        orgNames <- names(df)
        for(i in 1:length(orgNames)) {
          if(orgNames[i]!=colName)
          names(df)[i] <- label2Col[orgNames[i]]
        }

       #insert in to new table
        private$insertData(md5Table,df)
      },

      getColumnNames = function() {
        cols <- names(private$conn_data$columns)
        cols
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
  ba2DBTypes <- getDataTypes(data$engineType)

  connect_data <- BAConnectionData$new(factTable, jdbc, columns,ba2DBTypes,quot,data$username)
  conn <- BAConnection$new(connect_data)
  conn
}


#################################################################################
#' Method to load the table data
#'
#' User can pass the selected column names as the data frame returned by this
#' method will have only the selected columns
#'
#' @param columns - vector of selected column names
#' @param conn  - BAConnection object obtained from connect API
#'
#'@export
carriots.analytics.load = function(columns = columns,conn = NULL) {
  conn$load(columns = columns)
}

#################################################################################
#' Method to update the data in the table
#'
#' Update the table with the values provided in the data frame. Below is the
#' structure of the data frame for WhereClause
#'
#'  @param conn - BAConnection object obtained from connect API
#'  @param dataframe - DataFrame for where Clause
#'  @param colname -  colname in dataframe which is to be added in to table
#'  @param type - Data type of column, supports specific types  available in the conn$dataTypes list
#'
#'@export
carriots.analytics.updateFrame = function(conn = NULL,dataframe=NULL,colname=NULL, type = type) {
  conn$updateDataFrame(df=dataframe,colName=colname,type=type)
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
# UTILITIES
##################################################################################
asc <- function(x) { strtoi(charToRaw(x),16L) }

chr <- function(n) { rawToChar(as.raw(n)) }

#supported data types - ba2DBTypes

getDataTypes <- function(engineType) {
  dataTypes <- list(STRING = "VARCHAR(256)", NUMERIC = "DOUBLE", INTEGER = "INT",
                    DATE = "DATE", DATETIME = "TIMESTAMP", TIME = "TIME")

  if(toupper(engineType) == "ORACLE") {
    dataTypes$NUMERIC = "NUMBER(20,4)"
    dataTypes$INTEGER = "NUMBER(20)"
  }
  else if(toupper(engineType) == "MYSQL") {
    dataTypes$NUMERIC = "DECIMAL(20,4)"
    dataTypes$TIMESTAMP = "DATETIME"
  }
  else if(toupper(engineType) == "REDSHIFT") {
    dataTypes$NUMERIC = "DOUBLE PRECISION"
    dataTypes$INTEGER = "BIGINT"
  }

  dataTypes
}

getColumns <- function(colSelected=NULL,conn) {
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

getColumn2Label <- function(colList) {
  column <- paste(colList)
  labels <- names(colList)

  col2Label <- list()
  for(i in 1:length(labels))
    col2Label[column[i]] = labels[i]

  col2Label

}

#-------------------------------------------------------------
#'Get DataSource Meta data
#'
#' This is a function to obtain the meta data of a
#' datasource from the envision server by providing the secret
#' key of the datasource
#'
#' @param baseUrl - Envision server URL
#' @param token - token for the datasource
#'                    obtained from the App
#--------------------------------------------------------------
getDataSourceMetaData <- function(baseUrl,token) {

  res <- doHttpCall(baseUrl,token,"dsExtConnect")
  res
}

#-----------------------------------------------------------------------
#'Get DataSource Connection
#'
#' This is a function to obtain JDBC connection to a
#' datasource from the envision server by providing the secret
#' key of the datasource
#'
#' @param baseUrl - Envision server URL
#' @param token - token for the datasource
#'                    obtained from the App
#-----------------------------------------------------------------------
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
    for(i in 1 : stringr::str_length(connect_data$password)) {
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
    data$username <- connect_data$user_login_name
    data$jdbc <- conn
    data$quot <- jdbcDetails$quot
    data$columns <- connect_data$columns
    data$engineType <- connect_data$engine_type

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




