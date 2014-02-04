import groovy.sql.Sql
import dbCore
import java.util.logging.*


/**
 * Created by AD3I on 1/21/14.
 */

// ***************************************************************************
// Required parameters for running the groovy script
//
//def logger = Logger.getLogger('groovy.sql')
//logger.level = Level.FINE
//logger.addHandler(new ConsoleHandler(level: Level.FINEST))

def filename= "newbusiness.xml";
def appName= "newbusiness";
def client ="pd";
def instance ="dev";
def configurationData=  new XmlSlurper().parse("newbusiness.xml");
def buildType=0;//0--write into sql ,1--upgrade db,2--UpgradeDB and write into sql


def databaseFilesPath = """${configurationData.Environment.Drive}/${client}/${instance}/${appName}/generator/${client}${instance}${appName}/net/Application/${client[0].toUpperCase()}${client.substring(1)}${instance}${appName}/database""".toString()

println databaseFilesPath


def tempDatabaseName= "${configurationData.DevBuild.database.Name}tmpupgrade";
def originalDatabaseName = "${configurationData.DevBuild.database.Name}";

// Set the static data
DBConfig.FileName = filename;
DBConfig.BuildType = buildType
DBConfig.dataBaseFilePath = databaseFilesPath
DBConfig.OriginalDatabaseName = originalDatabaseName
DBConfig.TempDatabaseName =tempDatabaseName




//delete existing log file
new File("${filename}_V.sql".toString()).delete()
// first need to initialize the configaration data
def dbc = new dbCore([appName:appName , databaseFilesPath: databaseFilesPath ,client:client,instance:instance,fileName:filename,buildType:buildType])
//create a temp database
//dbc.createTempDatabase(tempDatabaseName.toString());
//dbc.createTempDatabase("MIkem");
println  "*" * 20 +  "Successfully Created test database" +  "*" * 20
//dbc.createInsertStatementForLog("Cc_Opt_Usr_UserLogin_S",["Usr_ID" :1231231232])
//dbc.createUpdateStatementsForLog("Cc_Opt_Usr_UserLogin_S",["Usr_ID" :1231231232])
// ***************************************************************************
// Entity Levels
// ***************************************************************************
//update existing entity levels
dbc.upgradeExistingData("cc_sys_lvl_entitylevel_s")
//insert new entity levels
dbc.upgradeNewData("cc_sys_lvl_entitylevel_s")
println  "*" * 20 +  "Successfully updated Entity Levels" +  "*" * 20

// ***************************************************************************
// Entity groups
// ***************************************************************************
//update existing entity groups
dbc.upgradeExistingData("cc_sys_grp_entitygroup_s")
//insert new entity groups
dbc.upgradeNewData("cc_sys_grp_entitygroup_s")
println  "*" * 20 +  "Successfully updated Entity Groups" +  "*" * 20

// ***************************************************************************
// Entity Headers
// ***************************************************************************
//update existing entity headers if there are any changes
dbc.upgradeExistingData("Cc_Sys_Ent_EntityHeader_S")
//update existing entity header language table
dbc.upgradeExistingData("Cc_Sys_Ent_EntityHeader_I",true)


println  "*" * 20 +  "Successfully updated existing Entity headers" +  "*" * 20

//Insert new Entity headers
//New Entity header means we need to create table and also insert respective entity details

def getNewEntityHeaderID ="""
        SELECT *
FROM ${tempDatabaseName}.dbo.cc_sys_ent_entityheader_s
WHERE ${tempDatabaseName}.dbo.cc_sys_ent_entityheader_s.Ent_ID NOT IN (
        SELECT Ent_ID
        FROM  ${originalDatabaseName}.dbo.cc_sys_ent_entityheader_s
);
""".toString();


def insertNewEntityHeaderData = """
     insert into  ${originalDatabaseName}.dbo.cc_sys_ent_entityheader_s
         SELECT * FROM ${tempDatabaseName}.dbo.cc_sys_ent_entityheader_s WHERE Ent_ID = :entityID
""".toString()


//Go through each ID in the list
dbc.getUpgradeData(getNewEntityHeaderID){item ->
    item =item.toRowResult()
    //insert entity Header into original database
    dbc.insertUpgradeData(insertNewEntityHeaderData,[entityID:item.Ent_ID]){}
    dbc.createInsertStatementForLog("dbo.cc_sys_ent_entityheader_s",item)

    //create a table directly from generated ddl file.
    String filePath= "${databaseFilesPath}/tables/${item.Ent_NAME}.ddl".toString()
    def file = new File(filePath);
    if(file.exists()){
        dbc.insertUpgradeData( file.getText(),[]){}
        dbc.writeIntoLog(file.getText())
    }
    //insert system (or) Config  data
   def entityInfo =  dbc.generateTableNameByEntityName(item.Ent_NAME)
    if(entityInfo){
        if(entityInfo.Level == "SYS"){
            new File("${databaseFilesPath}/systemdata").eachFileMatch(entityInfo.tableName){
                dbc.insertUpgradeData( it.getText(),[]){}
                dbc.writeIntoLog(it.getText())
            }
        }
        else if (entityInfo.Level == "CFG"){
            new File("${databaseFilesPath}/configdata").eachFileMatch(entityInfo.tableName){
                dbc.insertUpgradeData( it.getText(),[]){}
                dbc.writeIntoLog(it.getText())
            }
        }
    }
    //insert respective entity details
    //insert into  ${originalDatabaseName}.dbo.Cc_Sys_Etd_EntityDetail_S
    def gettEntityDetails ="""
            SELECT * FROM ${tempDatabaseName}.dbo.Cc_Sys_Etd_EntityDetail_S
            WHERE Etd_ENTITY = '${item.Ent_ID}' ;
    """.toString()

    def insertEntityDetails ="""
            insert into  ${originalDatabaseName}.dbo.Cc_Sys_Etd_EntityDetail_S
            SELECT * FROM ${tempDatabaseName}.dbo.Cc_Sys_Etd_EntityDetail_S where Etd_ID = :Etd_ID
        """.toString()

    def insertEntityDetailsLanguageData = """
        insert into  ${originalDatabaseName}.dbo.Cc_Sys_Etd_EntityDetail_I
        SELECT EntityDetailLanguage.* FROM ${tempDatabaseName}.dbo.Cc_Sys_Etd_EntityDetail_S
        INNER JOIN ${tempDatabaseName}.dbo.Cc_Sys_Etd_EntityDetail_I as EntityDetailLanguage on EntityDetailLanguage.Etd_ID_I = Etd_ID
        WHERE Etd_ID_I = :Etd_ID_I AND Etd_LANGUAGE_I = :Etd_LANGUAGE_I
        """.toString()

    def getEntityDetailsLanguageData = """
         SELECT EntityDetailLanguage.* FROM ${tempDatabaseName}.dbo.Cc_Sys_Etd_EntityDetail_S
        INNER JOIN ${tempDatabaseName}.dbo.Cc_Sys_Etd_EntityDetail_I as EntityDetailLanguage on EntityDetailLanguage.Etd_ID_I = Etd_ID
        WHERE Etd_ENTITY = '${item.Ent_ID}'
    """.toString()

    dbc.getUpgradeData(gettEntityDetails){ rowData->
        rowData = rowData.toRowResult()
        dbc.insertUpgradeData(insertEntityDetails,rowData){}
        dbc.createInsertStatementForLog("dbo.Cc_Sys_Etd_EntityDetail_S",rowData)
    }

    dbc.getUpgradeData(getEntityDetailsLanguageData){ rowData->
        rowData = rowData.toRowResult()
        dbc.insertUpgradeData(insertEntityDetailsLanguageData,rowData){}
        dbc.createInsertStatementForLog("dbo.Cc_Sys_Etd_EntityDetail_I",rowData)
    }


    //dbc.insertUpgradeData(insertEntityDetails,[]){}
    //dbc.insertUpgradeData(insertEntityDetailsLanguageData,[]){}

    //execute constraints for new table

    String constraintsFilePath= "${databaseFilesPath}/constraints/${item.Ent_NAME}.ddl".toString()
    def constraintsFile = new File(constraintsFilePath);
    if(constraintsFile.exists()){
        dbc.insertUpgradeData( constraintsFile.getText(),[]){}
        dbc.writeIntoLog(constraintsFile.getText())
    }

}
//insert new entity header language table
dbc.upgradeNewData("Cc_Sys_Ent_EntityHeader_I",true)
println  "*" * 20 +  "Successfully updated New Entity headers" +  "*" * 20


// ***************************************************************************
// Entity Details
// ***************************************************************************
    //Update existing Entity Details
    dbc.upgradeExistingData("Cc_Sys_Etd_EntityDetail_S")
    //Update Existing entity details language details
    dbc.upgradeExistingData("Cc_Sys_Etd_EntityDetail_I",true)
    println  "*" * 20 +  "Successfully updated existing Entity details" +  "*" * 20
    //update New entityDetails
    def getNewEntityDetails ="""
            SELECT *
            FROM ${tempDatabaseName}.dbo.Cc_Sys_Etd_EntityDetail_S
            INNER JOIN ${tempDatabaseName}.dbo.Cc_Sys_Ent_EntityHeader_S ON Ent_ID = Etd_ENTITY
            WHERE ${tempDatabaseName}.dbo.Cc_Sys_Etd_EntityDetail_S.Etd_ID NOT IN (
            SELECT Etd_ID
            FROM  ${originalDatabaseName}.dbo.Cc_Sys_Etd_EntityDetail_S
            );
            """.toString();
    def insertNewEntityDetailsData ="""
        insert into  ${originalDatabaseName}.dbo.Cc_Sys_Etd_EntityDetail_S
             SELECT * FROM ${tempDatabaseName}.dbo.Cc_Sys_Etd_EntityDetail_S WHERE Etd_ID = :entityDetailID
    """.toString()

    //Go through each ID in the list
    dbc.getUpgradeData(getNewEntityDetails){newEntityDetails ->
        newEntityDetails =newEntityDetails.toRowResult()
        //insert entity Header into original database
        def columnMatched = false
        def columnName = "${newEntityDetails.Ent_CODE}_${newEntityDetails.Etd_NAME.toString().toUpperCase()}".toString()
        //alter table to add a new column
        def dbColumnName = "";
        dbc.insertUpgradeData(insertNewEntityDetailsData,[entityDetailID:newEntityDetails.Etd_ID]){}
        dbc.createInsertStatementForLog("Cc_Sys_Etd_EntityDetail_S",newEntityDetails)
        String filePath= "${databaseFilesPath}/tables/${newEntityDetails.Ent_NAME}.ddl".toString()
        def file = new File(filePath);
        file.eachLine {
            it as String;
            if(it.contains(columnName)){
                dbColumnName = it.replace(',','')
                columnMatched = true
            }
        }
        def entityInfo =  dbc.generateTableNameByEntityName(newEntityDetails.Ent_NAME)
        if(entityInfo)  {
            if(columnMatched == true){
                dbc.insertUpgradeData("Alter table ${entityInfo.tableName} add ${dbColumnName}".toString(),[]){}
                dbc.writeIntoLog("Alter table ${entityInfo.tableName} add ${dbColumnName} ;".toString())
             }
        }
    }
dbc.upgradeNewData("Cc_Sys_Etd_EntityDetail_I",true)
    println  "*" * 20 +  "Successfully updated new Entity Details" +  "*" * 20

// ***************************************************************************
// update Relation Details
// ***************************************************************************
    //Update Existing entity details language details
    dbc.upgradeExistingData("Cc_Sys_Etr_EntityRelation_S")
    //Update relation language details
    dbc.upgradeExistingData("Cc_Sys_Etr_EntityRelation_I",true)
    println  "*" * 20 +  "Successfully updated existing relation data Details" +  "*" * 20

    //insert new relation data
    dbc.upgradeNewData("Cc_Sys_Etr_EntityRelation_S")

    //Update relation language details
    dbc.upgradeNewData("Cc_Sys_Etr_EntityRelation_I",true)
    println  "*" * 20 +  "Successfully updated New relation data Details" +  "*" * 20

// ***************************************************************************
// update Artifacts
// ***************************************************************************

//insert decision tables
def decisionTablePath = "${databaseFilesPath}/DecisionTables".toString()
dbc.executeBatchFiles(decisionTablePath,dbc.dbmain,true,"Failed to create decisiontables")
dbc.executeBatchFiles(decisionTablePath,dbc.dbmain,true ,"Failed to create decisiontables",  ~/.*\.ddl/)
println  "*" * 20 +  "Successfully updated decision tables" +  "*" * 20
//insert calc data
def calcsPath = "${databaseFilesPath}/Calcs/config".toString()
dbc.executeBatchFiles(calcsPath,dbc.dbmain,true ,"Failed to create calcs")
dbc.executeBatchFiles(calcsPath,dbc.dbmain,true ,"Failed to create calcs",  ~/.*\.ddl/)
//insert OrchConfig
def orchConfigPath = "${databaseFilesPath}/OrchConfig".toString()
dbc.executeBatchFiles(orchConfigPath,dbc.dbmain,true ,"Failed to create orchConfig")
dbc.executeBatchFiles(orchConfigPath,dbc.dbmain,"Failed to create orchConfig",  ~/.*\.ddl/)
//insert Attributes
def attributesPath = "${databaseFilesPath}/GeneralProduct/Attributes".toString()
dbc.executeBatchFiles(attributesPath,dbc.dbmain,true ,"Failed to create attributes")
dbc.executeBatchFiles(attributesPath,dbc.dbmain,true ,"Failed to create attributes",  ~/.*\.ddl/)
//insert Products
def productsPath = "${databaseFilesPath}/GeneralProduct/Products".toString()
dbc.executeBatchFiles(productsPath,dbc.dbmain,true ,"Failed to create products")
dbc.executeBatchFiles(productsPath,dbc.dbmain,true ,"Failed to create products",  ~/.*\.ddl/)
//insert orchestrations
def orchchestrationsPath = "${databaseFilesPath}/GeneralProduct/Orchestrations".toString()
dbc.executeBatchFiles(orchchestrationsPath,dbc.dbmain,true ,"Failed to create orchestrations")
dbc.executeBatchFiles(orchchestrationsPath,dbc.dbmain,true ,"Failed to create orchestrations",  ~/.*\.ddl/)

//insert documents
def documentsPath = "${databaseFilesPath}/Documents/config".toString()
dbc.executeBatchFiles(documentsPath,dbc.dbmain,true ,"Failed to create document")
dbc.executeBatchFiles(documentsPath,dbc.dbmain,true ,"Failed to create document",  ~/.*\.ddl/)

println  "*" * 20 +  "Successfully updated Artifacts" +  "*" * 20
// ***************************************************************************
// update System data anf config data
// ***************************************************************************

def getSysentities = """
    select Ent_Name
    FROM ${tempDatabaseName}.dbo.Cc_Sys_Ent_EntityHeader_S
    where Ent_HEADERTYPE = (select Etp_ID from ${tempDatabaseName}.dbo.Cc_Sys_Etp_EntityType_S
     where Etp_CODE = 'SYS')
""".toString()

dbc.getUpgradeData(getSysentities){item->
    item =item.toRowResult()
    def entityInfo =  dbc.generateTableNameByEntityName(item.Ent_Name)
    if(entityInfo)  {
        //update data
        dbc.upgradeExistingData(entityInfo.tableName)
        //insert data
        dbc.upgradeNewData(entityInfo.tableName)
    }
}

println  "*" * 20 +  "Successfully updated  system data" +  "*" * 20
def getConfentities = """
    select Ent_Name
    FROM ${tempDatabaseName}.dbo.Cc_Sys_Ent_EntityHeader_S
    where Ent_HEADERTYPE = (select Etp_ID from ${tempDatabaseName}.dbo.Cc_Sys_Etp_EntityType_S
     where Etp_CODE = 'CFG')
""".toString()

dbc.getUpgradeData(getConfentities){item->
    item =item.toRowResult()
    def entityInfo =  dbc.generateTableNameByEntityName(item.Ent_Name)
    if(entityInfo)  {
        //update data
        dbc.upgradeExistingData(entityInfo.tableName.toString())
        //insert data
        dbc.upgradeNewData(entityInfo.tableName.toString())
    }
}
println  "*" * 20 +  "Successfully updated  Config data" +  "*" * 20
