import groovy.sql.Sql
import dbutils
import dbops
import DBConfig



/**
 * Created by AD3I on 1/21/14.
 */

class dbCore{

    def sqlOutputFile = new File("${DBConfig.FileName}_V.sql".toString())
    def   configurationData = new XmlSlurper().parse(DBConfig.FileName)
    def  tempDatabaseName= "${configurationData.DevBuild.database.Name}tmpupgrade";
    def  originalDatabaseName = "${configurationData.DevBuild.database.Name}";
    def dbmain = Sql.newInstance(configurationData.DevBuild.database.URL.text(),configurationData.DevBuild.database.User.text(),configurationData.DevBuild.database.Password.text(), configurationData.DevBuild.database.Driver.text())
    def databaseFilesPath = DBConfig.dataBaseFilePath as String
    def tempDB = Sql.newInstance("jdbc:sqlserver://localhost:1433;databaseName=${tempDatabaseName}".toString(),configurationData.DevBuild.database.User.text(),configurationData.DevBuild.database.Password.text(), configurationData.DevBuild.database.Driver.text())

    //get the upgraded data from temp database and pass them to closure
    def getUpgradeData(sql,closure){
        println databaseFilesPath
        dbmain.eachRow(sql){row ->
            closure(row);
        }
    }
    // insert the data
    def insertUpgradeData(sql,paramData,closure){
        try{
            if(DBConfig.BuildType != 0 ){
                dbmain.execute(sql,paramData);
            }
            if(closure)
                 closure.call();
            writeIntoLog(convertTOSQl(sql,paramData))
            }
        catch (e){

        }
    }
    def writeIntoLog(message){
        sqlOutputFile.append message;
        sqlOutputFile.append System.getProperty("line.separator")
    }
    def convertTOSQl(sql,paramdata,columnProperties){
        if(paramdata){
            try{
                paramdata.each{key , value->
                    sql = sql.replaceAll(":"+key.toString()){
                       def  istring=dbops.dbiscolumnstring(key,columnProperties)
                        if(istring){
                            if(value && value != "null"){
                                return "'${value}'".toString()
                            }
                            return null
                        }
                        else{
                            return value
                        }
                    }
                }
            }
            catch(e){
                println "failed to convert to sql"
            }
        }
        return sql
    }
    def generateTableNameByEntityName(entityName){
        def sql = """
            select Lvl_CODE as 'Level',Etp_CODE as 'EntityType', Ent_NAME 'EntityName' ,Ent_CODE as 'EntiyCode' from Cc_Sys_Ent_EntityHeader_S
            INNER JOIN dbo.Cc_Sys_Etp_EntityType_S on Etp_ID =Ent_HEADERTYPE
            INNER JOIN Cc_Sys_Lvl_EntityLevel_S on Ent_LEVEL = Lvl_ID
            where Ent_NAME = '${entityName}'
        """.toString()
        HashMap returnValue  = [:]
        try{
            dbmain.eachRow(sql){entityResult ->
                entityResult = entityResult.toRowResult()
                returnValue.tableName= "${entityResult.Level}_${entityResult.EntityType.toString()[0].toUpperCase()}${entityResult.EntityType.toString().substring(1).toLowerCase()}_${entityResult.EntiyCode}_${entityResult.EntityName}_S"
                returnValue.entityLevel = entityResult.Level
            }
        }
        catch(e){}
        return returnValue;
    }
    def createTempDatabase(tempDatabasename){
        def tempDB = Sql.newInstance("jdbc:sqlserver://localhost:1433;databaseName=master",configurationData.DevBuild.database.User.text(),configurationData.DevBuild.database.Password.text(), configurationData.DevBuild.database.Driver.text())
        try{
            def dropDB ="""
                    ALTER DATABASE ${tempDatabasename}  SET SINGLE_USER WITH ROLLBACK IMMEDIATE;drop database ${tempDatabasename};
            """.toString()
            tempDB.execute(dropDB);
        }
        catch (e){
            println "do not have any previous database (or)do not have permission to create a new database"
        }
        try{
            def dropandRecreateDB = """
                        CREATE DATABASE ${tempDatabasename} Collate Latin1_general_CI_AS;
                """.toString()
            tempDB.execute(dropandRecreateDB);
        }
        catch(ex){ println "failed to create new database"}
        //connect to the temp database

        //Execute ddl to create tables
        def ddlPath= "${databaseFilesPath}/tables".toString()
        executeBatchFiles(ddlPath,tempDB,false,"Failed to create all tables", ~/.*\.ddl/)
        //create config data
        def configDataPath= "${databaseFilesPath}/configdata".toString()
        executeBatchFiles(configDataPath,tempDB,false,"Failed to create config data")
        // system data
        def systemDataPath= "${databaseFilesPath}/systemdata".toString()
        executeBatchFiles(systemDataPath,tempDB,false,"Failed to system data")
        //add constraints
        def constraintsPath= "${databaseFilesPath}/constraints".toString()
        executeBatchFiles(constraintsPath,tempDB,false,"Failed to add constraints", ~/.*\.ddl/)
        //execute store procedures
        def storeProceduresPath = "${databaseFilesPath}/storedprocedures".toString()
        executeBatchFiles(storeProceduresPath,tempDB,false, "Failed to create store procedures")

    }
    def executeBatchFiles(filePath,sqlInstance,logFile, errorMessage ="Failed to execute batch files",pattern= ~/.*\.sql/ ){
        def file = new File(filePath);
        if(file.exists()){
            //try
            //{
                file.eachFileMatch(pattern){
                    try{
                        if (it.getName().toString() == "Cc_Opt_Evt_Event_S.sql"){
                            it.getText().toString().split(";").each{line->
                                sqlInstance.execute(line.toString())
                            }
                        }else{
                                sqlInstance.execute( it.getText())
                                if(logFile){
                                    writeIntoLog( it.getText())
                                }
                        }
                    }
                    catch(e){
                        //println  "failed to execute file "
                    }
                }
           // }
            //catch (ex){
               // println  "failed to execute file "
            //}
        }
    }
    def upgradeExistingData(tableName,isLanguageTable =false){
        try{
            def matchID = getIDColumn(tableName)
            def languageColumn =""
            if(matchID){
                createCheckSum(tableName,matchID.NAME)
                def idColumn = matchID.NAME
                 def getChanges = """
                         SELECT temp.*
                        FROM ${tempDatabaseName}.dbo.${tableName} AS temp
                        INNER JOIN  ${originalDatabaseName}.dbo.${tableName} AS B on temp.${idColumn} = b.${idColumn}""".toString()
                if(isLanguageTable){
                     def languageColumnInfo = getlanguageIDColumn(tableName)
                    if(languageColumnInfo){
                        languageColumn =languageColumnInfo.NAME
                        getChanges += " AND temp.${languageColumn} = b.${languageColumn}   ".toString()
                    }
                }
                getChanges+=  """
                                      EXCEPT
                        select * from   ${originalDatabaseName}.dbo.${tableName}
                 """.toString()
                def updateStatement = prepareUpdateStatement(tableName,idColumn,isLanguageTable,languageColumn)
                dbmain.eachRow(getChanges){ updateData->
                    println updateData
                    updateData = updateData.toRowResult()
                    dbmain.execute(updateStatement,updateData){}
                    createUpdateStatementsForLog(tableName,updateData,updateStatement)
                }
            }
        }
        catch(e){
            println "failed to update data for table ${tableName} "
        }
    }
    def getIDColumn(tableName){
        def matchID
        try{
        def getIDColumnSql = """
                    SELECT a.NAME  FROM ${tempDatabaseName}.sys.all_columns AS a  ,${tempDatabaseName}.sys.tables AS b
                    WHERE a.object_id = b.object_id   AND b.NAME = '${tableName}'
                    AND (
                    a.NAME LIKE '[a-z][a-z][a-z]_ID'
                    OR a.NAME LIKE '[a-z][a-z][a-z][a-z]_ID'
                    OR a.NAME LIKE '[a-z][a-z][a-z]_ID_I')
                    ORDER BY a.column_id;
            """.toString()
         matchID=dbmain.firstRow(getIDColumnSql)
        }catch (e){
            println "failed to retrieve ID column for table ${tableName} "
        }
        return matchID
    }
    def getlanguageIDColumn (tableName){
        def matchID
        try{
            def getIDColumnSql = """
                    SELECT a.NAME  FROM ${tempDatabaseName}.sys.all_columns AS a  ,${tempDatabaseName}.sys.tables AS b
                    WHERE a.object_id = b.object_id   AND b.NAME = '${tableName}'
                    AND (
                    a.NAME LIKE '[a-z][a-z][a-z]_LANGUAGE_I'
                    OR a.NAME LIKE '[a-z][a-z][a-z][a-z]_LANGUAGE_I')
                    ORDER BY a.column_id;
            """.toString()
            matchID=dbmain.firstRow(getIDColumnSql)
        }catch (e){
            println "failed to retrieve ID column for table ${tableName} "
        }
        return matchID
    }

    def prepareUpdateStatement(tableName,columnID,isLanguageTable ,languageColumn){
        def updateStatement ="Update ${tableName} SET ".toString()
        def getTableColumns = """
            SELECT a.NAME
            FROM ${tempDatabaseName}.sys.all_columns AS a
            ,${tempDatabaseName}.sys.tables AS b
            WHERE a.object_id = b.object_id
            AND b.NAME = '${tableName}'
            AND a.NAME NOT LIKE '[a-z][a-z][a-z]_ID'
            AND a.NAME NOT LIKE '[a-z][a-z][a-z]_ID_I'
            AND a.NAME NOT LIKE '%_USERID'
            AND a.NAME NOT LIKE '%_TIMESTAMP'
            AND a.NAME NOT LIKE 'CHECKSUM'
            ORDER BY a.column_id
        """.toString()
        List columnsRow = dbmain.rows(getTableColumns)
        if(columnsRow){
            updateStatement += columnsRow.collect{" ${it.NAME} = :${it.NAME}"}.join(",").toString()
            updateStatement +=" where ${columnID} = :${columnID} "
            if(isLanguageTable){
                updateStatement += " AND ${languageColumn} =:${languageColumn}"
            }
        }
        return updateStatement.toString()
    }

    def upgradeNewData(tableName,isLanguageTable= false){
        try{
            def matchID=getIDColumn(tableName)
            def languageColumn =""
            if(matchID){
                def idColumn = matchID.NAME
                def getChanges = """
                         SELECT *
                    FROM ${tempDatabaseName}.dbo.${tableName}
                    WHERE ${tempDatabaseName}.dbo.${tableName}.${idColumn} NOT IN (
                    SELECT ${idColumn}
                    FROM  ${originalDatabaseName}.dbo.${tableName}
                    );
                """.toString()
                def insertStatement = """
                         insert into  ${originalDatabaseName}.dbo.${tableName}
                        SELECT * FROM ${tempDatabaseName}.dbo.${tableName} WHERE ${idColumn} = :uniqueID
                 """.toString()
                if(isLanguageTable){
                    def languageColumnInfo = getlanguageIDColumn(tableName)
                    if(languageColumnInfo){
                        languageColumn =languageColumnInfo.NAME
                        getChanges = """
                                        select temp.* from ${tempDatabaseName}.dbo.${tableName} as temp
                                        LEFT OUTER JOIN ${originalDatabaseName}.dbo.${tableName} as b on b.${idColumn} = temp.${idColumn}
                                        AND b.${languageColumn} = temp.${languageColumn}
                                        where b.${idColumn} IS NULL
                                    """.toString()
                    }
                    insertStatement = """
                         insert into  ${originalDatabaseName}.dbo.${tableName}
                        SELECT * FROM ${tempDatabaseName}.dbo.${tableName} WHERE ${idColumn} = :${idColumn}
                        AND ${languageColumn} = :${languageColumn}
                 """.toString()
                }
                if(isLanguageTable){
                    dbmain.eachRow(getChanges){ updateData->
                        dbmain.execute(insertStatement,updateData){}
                        createInsertStatementForLog(tableName,updateData.toRowResult())
                    }
                }
                else{
                    dbmain.eachRow(getChanges){ updateData->
                        dbmain.execute(insertStatement,[uniqueID:updateData["${idColumn}".toString()]]){}
                        createInsertStatementForLog(tableName,updateData.toRowResult())
                    }
                }
            }
        }
        catch(e){
            println "Failed to insert new data to ${tableName} "
        }
    }
    def createInsertStatementForLog(tableName,data){
        try{
            def columnProps = dbutils.getColumnProperties(dbmain,tableName)
            if(columnProps){
                def insertStatement = "INSERT INTO ${tableName}  ( ".toString()
                        insertStatement += (columnProps.collect{" ${it.columnname}"}.join(",").toString() + " ) VALUES ( ")
                    insertStatement += (columnProps.collect{" :${it.columnname}"}.join(",").toString() + " ) ")
                writeIntoLog convertTOSQl(insertStatement,data,columnProps)
            }
        }
        catch(e){
            printf "failed to create insert statement for ${tableName}"
        }

    }
    def createUpdateStatementsForLog(tableName,data,updateStatement){
        try{
        def columnProps = dbutils.getColumnProperties(dbmain,tableName)
        writeIntoLog convertTOSQl(updateStatement,data,columnProps)
        }
        catch(e){
            printf "failed to create update statement for ${tableName}"
        }
    }
    def createCheckSum(tableName , IdColumn){
        //if original databse has checksum colum add it ti temporary database
        def isColumnCheckSum = """
                             SELECT a.NAME
                    FROM ${originalDatabaseName}.sys.all_columns AS a
                    ,${originalDatabaseName}.sys.tables AS b
                    WHERE a.object_id = b.object_id
                    AND b.NAME = '${tableName}'
                    AND a.NAME  = 'CHECKSUM'
                    ORDER BY a.column_id
            """.toString()
        def checkSumExist = dbmain.firstRow(isColumnCheckSum)
        if(checkSumExist){
            //create also in temporary database
            def addCheckSum ="""
                ALTER TABLE ${tableName} add CHECKSUM AS CHECKSUM(${IdColumn})
            """.toString()
            try{
                //dbmain.execute(addCheckSum)
                dbops.AddChecksum(tempDB,tableName)
            }
            catch(e){
                "failed to add checksum"
            }
        }
    }
}