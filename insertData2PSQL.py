import psycopg2
import csv
import math
import sys
import uuid
import os


DATASET_DIR = "D:\\ODM2\\gsnow_china_v1.0\\snow_depth\\"
SITEINFO_DIR = 'D:\\ODM2\\gsnow_china_v1.0\\site_info\\Site_info.csv'
samplingFeatureNames = []
currentSamplingFeatureName = '_'

def listdir1(path, list_name):
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        list_name.append(file_path)
    return list_name

def getSamplingFeatureName(list_name):
    sFName = []
    for path in list_name:
        sFName.append(path[36:40])
    return sFName

list_name = []  # 存放samplingfeature下所有文件绝对路径, 多次用到，不能改变值

conn = psycopg2.connect("host=localhost dbname=ODM2 user=postgres password=Tyj123321")
cur = conn.cursor()

# filename = "C:/Users/nie/Desktop/site_info.csv"

def getSiteInfo():
    with open(SITEINFO_DIR, 'r') as f:
        keys = []
        values = []
        csv_reader = csv.reader(f)
        head_row = next(csv_reader)
        for line in csv_reader:
            all = {'lat': line[1], 'lon': line[2],
                   'alt': line[3], 'receiver_type': line[4], 'gnss_type': line[5],
                   'antenna_height': line[6], 'data_start_year': [7], 'data_end_year': line[8],
                   'data_type': line[9], 'mean_vsm': line[10]}
            values.append(all)
            keys.append(line[0])
        # print(values[keys.index('bjms')]['lat'])
        return values, keys

def getXYZLocation(samplingfeaturename):
    values = []
    keys = []
    locations = []
    (values, keys) = getSiteInfo()
    locations.append(values[keys.index(samplingfeaturename)]['lat'])
    locations.append(values[keys.index(samplingfeaturename)]['lon'])
    locations.append(values[keys.index(samplingfeaturename)]['alt'])
    return locations

def doy2date(year, doy):
    year = int(year)
    doyInt = math.floor(float(doy))  # 取doy整数部分
    doyDem = int((float(doy) - doyInt) * 100)  # 取doy小数部分
    month_leapyear = [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    month_notleap = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]

    if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0):
        for i in range(0, 12):
            if doyInt > month_leapyear[i]:
                doyInt -= month_leapyear[i]
                continue
            if doyInt <= month_leapyear[i]:
                month = i + 1
                day = doyInt
                break
    else:
        for i in range(0, 12):
            if doyInt > month_notleap[i]:
                doyInt -= month_notleap[i]
                continue
            if doyInt <= month_notleap[i]:
                month = i + 1
                day = doyInt
                break

    if doyDem == 25:  # 12小时返回时间
        return "{}-{}-{} 00:00:00".format(year, month, day)
    elif doyDem == 75:  # 12小时返回时间
        return "{}-{}-{} 12:00:00".format(year, month, day)
    else:  # 24小时返回时间
        return "{}-{}-{} 00:00:00".format(year, month, day)


def read_csv_file(filename):
    idName = "valueid"
    tableName = "timeseriesresultvalues"
    with open(filename, 'r') as f:
        csv_reader = csv.reader(f)
        head_row = next(csv_reader)
        for line in csv_reader:
            resultId = getMaxID(idName, tableName)
            dataValue = line[2]
            valueDatetime = doy2date(line[0], line[1])
            valueDatetimeUTCOffset = -4
            censorCodeCV = "Censor Code"
            qualityCodeCV = "Quality Code"
            timeInterval = 24
            timeIntervalID = 8
            insert_query = "INSERT INTO odm2.timeseriesresultvalues(valueid,resultid, \
                    datavalue,valuedatetime,valuedatetimeutcoffset,\
                    censorcodecv,qualitycodecv,timeaggregationinterval,\
                    timeaggregationintervalunitsid) VALUES{}".format((valueId, resultId, \
                                    dataValue, valueDatetime, valueDatetimeUTCOffset, censorCodeCV, \
                                    qualityCodeCV, timeInterval, timeIntervalID))
            cur.execute(insert_query)
            conn.commit()
            valueId = valueId + 1

            # resultId = 12
            # uuidResult = uuid.uuid1()
            # featureActionId = 3
            # resultTypeCV = "pass"
            # variableId = 2
            # unitId = 1
            # processingLevelId = 2
            # resultDatetimeUTCOffset = -4
            # validateDatetimeUTCOffset = -4
            # sampledMediumCV = "Air"
            # valueCount = line[0]
            # cur.execute("""
            #     SELECT year from samplingfeature
            # """)
            # print("{}{}{}".format(line[0], line[1], line[2]))
            # insert_query1 = "INSERT INTO samplingfeature(year, doy, snow_depth, random) VALUES {}".format((line[0], line[1], line[2], uuidResult))
            # cur.execute(insert_query1)
            # insert_query2 = "INSERT INTO dataset(snow_ste, num_of_prns) VALUES {}".format((line[3], line[4]))
            # cur.execute(insert_query2)
            #
            # resultId = resultId + 1

# if "__main__" == __name__:
#     filename = BASE_DIR + r"/aes0_2015_24h.csv"
#     read_csv_file(filename)

def addSamplingFeature(locations):
    idName = "samplingfeatureid"
    tableName = "samplingfeatures"
    samplingFeatureGeotypeCV = "Point"
    featureGeometryWKT = "POINT({} {})".format(locations[1], locations[0])
    elevations_M = float(locations[2])
    elevationDatumCV = "NAVD88"
    samplingFeatureID = getMaxID(idName, tableName)
    samplingFeatureTypeCV = 'Site'
    samplingFeatureName = 'Site'
    global currentSamplingFeatureName
    samplingFeatureOrLocationCode = currentSamplingFeatureName
    insertQuery = "INSERT INTO odm2.samplingfeatures(samplingfeatureid, samplingfeatureuuid, samplingfeaturetypecv, " \
                  "samplingfeaturecode, samplingfeaturename, samplingfeaturegeotypecv, featuregeometry," \
                  "elevation_m, elevationdatumcv) VALUES({}, uuid_generate_v1(), '{}', '{}', '{}', '{}', ST_GeomFromText('{}', 4326), {}, '{}')".format(
        samplingFeatureID, samplingFeatureTypeCV, samplingFeatureOrLocationCode, samplingFeatureName,
        samplingFeatureGeotypeCV, featureGeometryWKT, elevations_M, elevationDatumCV)
    cur.execute(insertQuery)
    conn.commit()
    return samplingFeatureID

def addSamplingFeatureAction(currentSamplingFeatureID, currentActionID):
    idName = "featureactionid"
    tableName = "featureactions"
    featureActionID = getMaxID(idName, tableName)
    samplingFeatureID = currentSamplingFeatureID
    actionID = currentActionID
    insertQuery = "INSERT INTO odm2.featureactions(featureactionid, samplingfeatureid, actionid) VALUES{}".format((
        featureActionID, samplingFeatureID, actionID))
    cur.execute(insertQuery)
    conn.commit()
    return featureActionID

def addActions(currentMethodID):
    idName = "actionid"
    tableName = "actions"
    actionID = getMaxID(idName, tableName)
    actionTypeCV = "Instrument deployment"
    methodID = currentMethodID
    beginDatetime = "2015-01-01  00:00:00"
    beginDatetimeUTCOffset = -4
    endDatetimeUTCOffset = -4
    psqlCommand = "SELECT actiontypecv from odm2.actions"
    cur.execute(psqlCommand)
    conn.commit()
    typeCV = []
    commandResult = cur.fetchall()
    for temp in commandResult:
        typeCV.append(temp[0])
    if actionTypeCV in typeCV:
        return (typeCV.index(actionTypeCV) + 1)
    actiondescription = "This is a action called Instrument deployment related with method."
    insertQuery = "INSERT INTO odm2.actions(actionid, actiontypecv, methodid, begindatetime, begindatetimeutcoffset, " \
                  "enddatetimeutcoffset, actiondescription) VALUES{}".format((actionID, actionTypeCV, methodID, beginDatetime,
                    beginDatetimeUTCOffset, endDatetimeUTCOffset, actiondescription))
    cur.execute(insertQuery)
    conn.commit()
    return actionID

def addMethod():
    idName = "methodid"
    tableName = "methods"
    methodID = getMaxID(idName, tableName)
    methodTypeCV = "Instrument deployment"
    methodCode = "Instrument deployment"
    methodName = "Instrument deployment"
    psqlCommand = "SELECT methodtypecv from odm2.methods"
    cur.execute(psqlCommand)
    conn.commit()
    commandResult = cur.fetchall()
    typeCV = []  # 判断methodtypecv是否已经存在，若存在，返回对应的methodid + 1(0 base)
    for temp in commandResult:
        typeCV.append(temp[0])
    if methodTypeCV in typeCV:
        return (typeCV.index(methodTypeCV) + 1)
    insertQuery = "INSERT INTO odm2.methods(methodid, methodtypecv, methodcode, methodname) VALUES{}".format((
        methodID, methodTypeCV, methodCode, methodName))
    cur.execute(insertQuery)
    conn.commit()
    return methodID

def addDataResult(currentFeatureActionsID, currentVariableID, currentUnitsID, currentProcessingLevel):
    idName = "resultid"
    tableName = "results"
    resultID = getMaxID(idName, tableName)
    resultUUID = uuid.uuid1()
    featureActionID = currentFeatureActionsID
    resultTypeCV = "Pass"
    variableID = currentVariableID
    unitsID = currentUnitsID
    processingLevel = currentProcessingLevel  # 1 fail  2 pass
    resultDatetimeUTCOffset = -4
    validDatetimeUTCOffset  =-4
    sampledMediumCV = "Air"
    NumberOfRecordedValues = 365 # 定义一个全局变量，先遍历csv，设置一个参数，看看出来的是几
    insertQuery = "INSERT INTO odm2.results(resultid, resultuuid, featureactionid, resulttypecv, " \
                  "variableid, unitsid, processinglevelid, resultDatetimeUTCOffset, validDatetimeUTCOffset," \
                  "sampledMediumCV, valuecount) VALUES{}".format((resultID, resultUUID,
                    featureActionID, resultTypeCV, variableID, unitsID, processingLevel, resultDatetimeUTCOffset,
                    validDatetimeUTCOffset, sampledMediumCV, NumberOfRecordedValues))
    cur.execute(insertQuery)
    conn.commit()
    return resultID

def addVariables(currentVariableTypeCV):
    idName = "variableid"
    tableName = "variables"
    variablesID = getMaxID(idName, tableName)
    variableTypeCV = currentVariableTypeCV
    noDataValue = 0
    if variableTypeCV == "Climate":
        variableCode = "Depth of Snow"
        variableNameCV = "Depth, snow"
        variableDefinition = "This variable is defined to express depth of snow."
    elif variableTypeCV == "Count":
        variableCode = "Count"
        variableNameCV = "Count, areal"
        variableDefinition = "Variables associated with the number of GNSS sites."
    elif variableTypeCV == "Estimation":
        variableCode = "Estimation"
        variableNameCV = "Estimation"
        variableDefinition = "The expected value for the STE of snow estimations after estimation."
    psqlCommand = "SELECT variabletypecv from odm2.variables"
    cur.execute(psqlCommand)
    conn.commit()
    commandResult = cur.fetchall()
    typeCV = []
    for temp in commandResult:
        typeCV.append(temp[0])
    if variableTypeCV in typeCV:
        return (typeCV.index(variableTypeCV) + 1)
    insertQuery = "INSERT INTO odm2.variables(variableid, variabletypecv, variablecode, variablenamecv, " \
                  "variabledefinition, nodatavalue) VALUES{}".format((variablesID, variableTypeCV,
                    variableCode, variableNameCV, variableDefinition, noDataValue))
    cur.execute(insertQuery)
    conn.commit()
    return variablesID

def addUnits(currentUnitTypeCV):
    idName = "unitsid"
    tableName = "units"
    unitsid = getMaxID(idName, tableName)
    unitsTypeCV = currentUnitTypeCV
    if unitsTypeCV == "Time":
        unitsAbbreviation = "Time"
        unitsName = "Hour"
    elif unitsTypeCV == "Count":
        unitsAbbreviation = "Count"
        unitsName = "Count"
    elif unitsTypeCV == "Percentage":
        unitsAbbreviation = "Percentage"
        unitsName = "Percentage"
    elif unitsTypeCV == "Length":
        unitsAbbreviation = "meter"
        unitsName = "Depth"
    psqlCommand = "SELECT unitstypecv from odm2.units"
    cur.execute(psqlCommand)
    conn.commit()
    commandResult = cur.fetchall()
    typeCV = []
    for temp in commandResult:
        typeCV.append(temp[0])
    if unitsTypeCV in typeCV:
        return (typeCV.index(unitsTypeCV) + 1)
    insertQuery = "INSERT INTO odm2.units(unitsid, unitstypecv, unitsabbreviation, unitsname) VALUES{}".format((
        unitsid, unitsTypeCV, unitsAbbreviation, unitsName))
    cur.execute(insertQuery)
    conn.commit()
    return unitsid

def addTimeSeriesResult(samplingfeaturename, currentRsultID):
    locations = []
    locations = getXYZLocation(samplingfeaturename)
    resultID = currentRsultID
    xLocation = locations[0]
    yLocation = locations[1]
    zLocation = locations[2]
    aggregationStatisticCV = "Bucket"
    insertQuery = "INSERT INTO odm2.timeseriesresults(resultid, xlocation, ylocation, zlocation, " \
                  "aggregationstatisticcv) VALUES{}".format((currentRsultID, xLocation, yLocation, zLocation, aggregationStatisticCV))
    cur.execute(insertQuery)
    conn.commit()

def addTimeSeriesResultValue(currentResultID, currentDataValue, currentDatetime, currentCensorCodeCV, currentQualityCodeCV, timeInterval, timeUnits):
    idName = "valueid"
    tableName = "timeseriesresultvalues"
    valueid = getMaxID(idName, tableName)
    resultid = currentResultID
    dataValue = currentDataValue
    valueDatetime = currentDatetime
    valueDatetimeUTCOffset = -4
    censorCodeCV = currentCensorCodeCV
    QualityCodeCV = currentQualityCodeCV
    timeAggregationInterval = timeInterval
    timeUnits = timeUnits
    insertQuery = "INSERT INTO odm2.timeseriesresultvalues(valueid, resultid, datavalue, valuedatetime, valuedatetimeutcoffset," \
                  "censorcodecv, qualitycodecv, timeaggregationinterval, timeaggregationintervalunitsid) VALUES{}".format((
                    valueid, resultid, dataValue, valueDatetime, valueDatetimeUTCOffset, censorCodeCV, QualityCodeCV,
                    timeAggregationInterval, timeUnits
    ))
    cur.execute(insertQuery)
    conn.commit()
    # def read_csv_file(filename):
    #     with open(filename, 'r') as f:
    #         csv_reader = csv.reader(f)
    #         head_row = next(csv_reader)
    #         valueId = 1
    #         for line in csv_reader:
    #             resultId = 14
    #             dataValue = line[2]
    #             valueDatetime = doy2date(line[0], line[1])
    #             valueDatetimeUTCOffset = -4
    #             censorCodeCV = "Censor Code"
    #             qualityCodeCV = "Quality Code"
    #             timeInterval = 24
    #             timeIntervalID = 8
    #             insert_query = "INSERT INTO odm2.timeseriesresultvalues(valueid,resultid, \
    #                     datavalue,valuedatetime,valuedatetimeutcoffset,\
    #                     censorcodecv,qualitycodecv,timeaggregationinterval,\
    #                     timeaggregationintervalunitsid) VALUES{}".format((valueId, resultId, \
    #                                     dataValue, valueDatetime, valueDatetimeUTCOffset, censorCodeCV, \
    #                                     qualityCodeCV, timeInterval, timeIntervalID))
    #             cur.execute(insert_query)
    #             conn.commit()
    #             valueId = valueId + 1

def listdir(path, list_name):
    for file in os.listdir(path):
        file_path = os.path.join(path, file)
        if os.path.isdir(file_path):
            listdir(file_path, list_name)
        else:
            list_name.append(file_path)
    return list_name

def classifyFile(list_name):
    filteredFile = []
    filtered0File = []
    rawFile = []
    raw0File = []
    for path in list_name:
        if 'filtered' in path:
            filteredFile.append(path)
        elif 'raw' in path:
            rawFile.append(path)
        if 'filtered0' in path:
            filtered0File.append(path)
        elif 'raw0' in path:
            raw0File.append(path)
    filteredFile = list(set(filteredFile).difference(filtered0File))
    rawFile = list(set(rawFile).difference(raw0File))
    return list(filteredFile), list(filtered0File), list(rawFile), list(raw0File)

def getTableName():
    i = 0
    filteredFile = []
    filtered0File = []
    rawFile = []
    raw0File = []
    filterTableName = []
    filtered0TableName = []
    rawTableName = []
    raw0TableName = []
    (filteredFile, filtered0File, rawFile, raw0File) = classifyFile(list_name)
    filteredFilePath = filteredFile
    rawFilePath = rawFile
    while(i < len(filteredFile)):
        filterTableName.append(filteredFile[i][-17:])
        i = i + 1
    i = 0
    while(i < len(filtered0File)):
        filtered0TableName.append(filtered0File[i][-17:])
        i = i + 1
    i = 0
    while(i < len(rawFile)):
        rawTableName.append(rawFile[i][-17:])
        i = i + 1
    i = 0
    while(i < len(raw0File)):
        raw0TableName.append(raw0File[-i][-17:])
        i = i + 1
    return (filterTableName, rawTableName, filteredFilePath, rawFilePath)
    # return (filterTableName, filtered0TableName, rawTableName, raw0TableName)

def addDataset(currentDatasetCode, currentDatasetTitle, currentDatasetAbstract):  # 需要获取文件名
    idName = "datasetid"
    tableName = "datasets"
    datasetID = getMaxID(idName, tableName)
    datasetUUID = uuid.uuid1()
    datasetTypeCV = "MetaData"
    datasetCode = currentDatasetCode
    datasetTitle = currentDatasetTitle
    datasetAbstract = currentDatasetAbstract
    psqlCommand1 = "SELECT datasetcode from odm2.datasets"
    cur.execute(psqlCommand1)
    conn.commit()
    typeCVResult = cur.fetchall()
    psqlCommand2 = "SELECT datasetid from odm2.datasets"
    cur.execute(psqlCommand2)
    conn.commit()
    idResult = cur.fetchall()
    typeCV = []
    id = []
    for temp in typeCVResult:
        typeCV.append(temp[0])
    for temp in idResult:
        id.append(temp[0])
    if datasetCode in typeCV:
        # return (typeCV.index(datasetCode) + 1)
        return id[typeCV.index(datasetCode)]
    insertQuery = "INSERT INTO odm2.datasets(datasetid, datasetuuid, datasettypecv, " \
                  "datasetcode, datasettitle, datasetabstract) VALUES{}".format((datasetID, datasetUUID,
                    datasetTypeCV, datasetCode, datasetTitle, datasetAbstract))
    cur.execute(insertQuery)
    conn.commit()
    return datasetID

def addDatasetResult(currentDatasetID, currentResultID): # 获取datasetID
    idName = "bridgeid"
    tableName = "datasetsresults"
    bridgeID = getMaxID(idName, tableName)
    datasetID = currentDatasetID
    resultID = currentResultID
    insertQuery = "INSERT INTO odm2.datasetsresults(bridgeid, datasetid, resultid) VALUES{}".format((bridgeID, datasetID, resultID))
    cur.execute(insertQuery)
    conn.commit()

# 获取id最大值
def getMaxID(idName, tableName):
    # insertQuery = "INSERT INTO odm2.timeseriesresults(resultid, xlocation, ylocation, zlocation, " \
    #     #               "aggregationstatisticcv) VALUES()".format((dataResult, xLocation, yLocation, zLocation, aggregationStatisticCV))
    # cur.execute("select featureactionid from odm2.featureactions")
    # id = cur.fetchall()
    # print(max(id[0]))
    # conn.commit()
    IDList = []
    psqlCommand = "SELECT {} from odm2.{}".format(idName, tableName)
    cur.execute(psqlCommand)
    conn.commit()
    id = cur.fetchall()
    if len(id) == 0:  # id列表为空返回1，否则
        return 1
    elif len(id) != 0:
        for i in range(0, len(id)):
            IDList.append(id[i][0])
    maxID = (max(IDList) + 1)
    return maxID
    # print(max(variableIDList))
    # maxID = int(max(id[0])) + 1
    # print(maxID, type(maxID))
    # return maxID  # 最大ID的下一个数开始插入ID

def wholeOverflow():
    # unitsWord = []
    # keyWord = []
    # usefulKeyWord = []
    # filteredTableName = []
    # filtered0TableName = []
    # rawTableName = []
    # raw0TableName = []
    # keyWordInCSVFile = []
    # datasetName = []
    # variablesList = ["Climate", "Count", "Estimation"]
    # unitsList = ["Time", "Count", "Percentage", "Length"]
    # timeAggregationInterval = 0
    # currentSamplingFeatureID = addSamplingFeature()
    # # addSamplingFeatureAction(currentSamplingFeatureID)
    # currentMethodID = addMethod()
    # currentActionID = addActions(currentMethodID)
    # currentFeatureActionsID = addSamplingFeatureAction(currentSamplingFeatureID, currentActionID)
    #
    # # add Variables and Units if NOT exist in PostgreSQL
    #
    # for variables in variablesList:
    #     addVariables(variables)
    # for units in unitsList:
    #     addUnits(units)
    #
    # currentResultID = addDataResult(currentFeatureActionsID, 1, 4)  # TODO
    # listdir(DATASET_DIR + sys.argv[1], list_name)
    # (filteredTableName, filtered0TableName, rawTableName, raw0TableName) = getTableName()
    # for name in filteredTableName:
    #     newName = name[0:13]
    #     datasetName.append(newName)
    # currentDatasetID = addDataset(datasetName[0], datasetName[0], datasetName[0])
    #
    # addDatasetResult(currentDatasetID, currentResultID)
    # addTimeSeriesResult(sys.argv[1], currentResultID)
    #
    # with open(DATASET_DIR + sys.argv[1] + '\\' + 'filtered' + '\\' + filteredTableName[0]) as f:
    #     csv_reader = csv.reader(f)
    #     head_row = next(csv_reader)
    #     for line in csv_reader:
    #         dataValue = line[2]
    #         valueDatetime = doy2date(line[0], line[1])
    #         censorCodeCV = "Censor Code"
    #         qualityCodeCV = "Quality Code"
    #         if '12' in datasetName:
    #             timeAggregationInterval = 12
    #         elif '24' in datasetName:
    #             timeAggregationInterval = 24
    #         insertQuery = "SELECT unitsid,unitstypecv FROM odm2.units"
    #         cur.execute(insertQuery)
    #         conn.commit()
    #         unitsInfo = cur.fetchall()
    #         for i in range(0, len(unitsInfo)):
    #             if 'Time' in unitsInfo[i]:
    #                 currentUnitID = unitsInfo[i][0]
    #         addTimeSeriesResultValue(currentResultID, dataValue, valueDatetime, censorCodeCV,
    #                                  qualityCodeCV, timeAggregationInterval, currentUnitID)

    listdir1(DATASET_DIR, list_name)
    samplingFeatureNames = getSamplingFeatureName(list_name)
    for name in samplingFeatureNames:
        psqlCommand = "SELECT samplingfeaturecode from odm2.samplingfeatures"
        cur.execute(psqlCommand)
        conn.commit()
        typeCV = []
        commandResult = cur.fetchall()
        for temp in commandResult:
            typeCV.append(temp[0])
        if name in typeCV:
            continue
        global currentSamplingFeatureName
        currentSamplingFeatureName = name
        print(currentSamplingFeatureName)

        locations = getXYZLocation(name)
        filteredDatasetName = []  # dataset name + 'filtered' & 'raw'
        rawDatasetName = []  # dataset name + 'filtered' & 'raw'
        listdir(DATASET_DIR + currentSamplingFeatureName, list_name)
        (filteredTableName, rawTableName, filteredFilePath, rawFilePath) = getTableName()
        for name in filteredTableName:
            newName = name[0:13]
            newName = newName + "_filtered"
            filteredDatasetName.append(newName)
        for name in rawTableName:
            newName = name[0:13]
            newName = newName + "_raw"
            rawDatasetName.append(newName)

        currentSamplingFeatureID = addSamplingFeature(locations)
        currentMethodID = addMethod()
        currentActionID = addActions(currentMethodID)
        currentFeatureActionsID = addSamplingFeatureAction(currentSamplingFeatureID, currentActionID)

        for pathName in filteredFilePath:
            if 'filtered' in pathName:
                processingLevelID = 2
            elif 'raw' in pathName:
                processingLevelID = 1
            if '12h' in pathName:
                timeInterval = 12
            elif '24h' in pathName:
                timeInterval = 24
            elif '01h' in pathName:
                timeInterval = 1
            elif '02h' in pathName:
                timeInterval = 2
            elif '03h' in pathName:
                timeInterval = 3
            elif '04h' in pathName:
                timeInterval = 4
            elif '05h' in pathName:
                timeInterval = 5
            elif '06h' in pathName:
                timeInterval = 6
            elif '07h' in pathName:
                timeInterval = 7
            elif '08h' in pathName:
                timeInterval = 8
            elif '09h' in pathName:
                timeInterval = 9
            elif '10h' in pathName:
                timeInterval = 10
            elif '11h' in pathName:
                timeInterval = 11
            currentVariableID = addVariables("Climate")
            currentUnitID = addUnits("Length")
            currentResultID1 = addDataResult(currentFeatureActionsID, currentVariableID, currentUnitID, processingLevelID)
            addTimeSeriesResult(currentSamplingFeatureName, currentResultID1)
            with open(pathName, 'r') as f:
                # print('currentResultID1', currentResultID1)
                csv_reader = csv.reader(f)
                head_row = next(csv_reader)
                for line in csv_reader:
                    if (float(line[1]) > 365) or (float(line[1]) < 1):
                        continue
                    dataValue = line[2]
                    valueDatetime = doy2date(line[0], line[1])
                    censorCodeCV = "Censor Code"
                    qualityCodeCV = "Quality Code"
                    timeUnits = addUnits("Time")
                    print('currentResultID1', currentResultID1)
                    addTimeSeriesResultValue(currentResultID1, dataValue, valueDatetime, censorCodeCV, qualityCodeCV, timeInterval, timeUnits)

            currentVariableID = addVariables("Estimation")
            currentUnitID = addUnits("Percentage")
            currentResultID2 = addDataResult(currentFeatureActionsID, currentVariableID, currentUnitID, processingLevelID)
            addTimeSeriesResult(currentSamplingFeatureName, currentResultID2)
            # print('currentResultID2', currentResultID2)
            with open(pathName, 'r') as f:
                csv_reader = csv.reader(f)
                head_row = next(csv_reader)
                for line in csv_reader:
                    if (float(line[1]) > 365) or (float(line[1]) < 1):
                        continue
                    dataValue = line[3]
                    valueDatetime = doy2date(line[0], line[1])
                    censorCodeCV = "Censor Code"
                    qualityCodeCV = "Quality Code"
                    timeUnits = addUnits("Time")
                    print('currentResultID2', currentResultID2)
                    addTimeSeriesResultValue(currentResultID2, dataValue, valueDatetime, censorCodeCV, qualityCodeCV,
                                             timeInterval, timeUnits)

            currentVariableID = addVariables("Count")
            currentUnitID = addUnits("Count")
            currentResultID3 = addDataResult(currentFeatureActionsID, currentVariableID, currentUnitID, processingLevelID)
            addTimeSeriesResult(currentSamplingFeatureName, currentResultID3)
            # print('currentResultID3', currentResultID3)
            with open(pathName, 'r') as f:
                csv_reader = csv.reader(f)
                head_row = next(csv_reader)
                for line in csv_reader:
                    if (float(line[1]) > 365) or (float(line[1]) < 1):
                        continue
                    dataValue = line[4]
                    valueDatetime = doy2date(line[0], line[1])
                    censorCodeCV = "Censor Code"
                    qualityCodeCV = "Quality Code"
                    timeUnits = addUnits("Time")
                    print('currentResultID3', currentResultID3)
                    addTimeSeriesResultValue(currentResultID3, dataValue, valueDatetime, censorCodeCV, qualityCodeCV,
                                             timeInterval, timeUnits)

            currentDatasetCode = filteredDatasetName[filteredFilePath.index(pathName)]
            currentDatasetTitle = currentDatasetCode
            currentDatasetAbstract = currentDatasetCode
            currentDatasetID = addDataset(currentDatasetCode, currentDatasetTitle, currentDatasetAbstract)
            addDatasetResult(currentDatasetID, currentResultID1)
            addDatasetResult(currentDatasetID, currentResultID2)
            addDatasetResult(currentDatasetID, currentResultID3)

        for pathName in rawFilePath:
            if 'filtered' in pathName:
                processingLevelID = 2
            elif 'raw' in pathName:
                processingLevelID = 1
            if '12h' in pathName:
                timeInterval = 12
            elif '24h' in pathName:
                timeInterval = 24
            currentVariableID = addVariables("Climate")
            currentUnitID = addUnits("Length")
            currentResultID1 = addDataResult(currentFeatureActionsID, currentVariableID, currentUnitID, processingLevelID)
            addTimeSeriesResult(currentSamplingFeatureName, currentResultID1)
            with open(pathName, 'r') as f:
                csv_reader = csv.reader(f)
                head_row = next(csv_reader)
                for line in csv_reader:
                    if (float(line[1]) > 365) or (float(line[1]) < 1):
                        continue
                    dataValue = line[2]
                    valueDatetime = doy2date(line[0], line[1])
                    censorCodeCV = "Censor Code"
                    qualityCodeCV = "Quality Code"
                    timeUnits = addUnits("Time")
                    addTimeSeriesResultValue(currentResultID1, dataValue, valueDatetime, censorCodeCV, qualityCodeCV, timeInterval, timeUnits)

            currentVariableID = addVariables("Estimation")
            currentUnitID = addUnits("Percentage")
            currentResultID2 = addDataResult(currentFeatureActionsID, currentVariableID, currentUnitID, processingLevelID)
            addTimeSeriesResult(currentSamplingFeatureName, currentResultID2)
            with open(pathName, 'r') as f:
                csv_reader = csv.reader(f)
                head_row = next(csv_reader)
                for line in csv_reader:
                    if (float(line[1]) > 365) or (float(line[1]) < 1):
                        continue
                    dataValue = line[3]
                    valueDatetime = doy2date(line[0], line[1])
                    censorCodeCV = "Censor Code"
                    qualityCodeCV = "Quality Code"
                    timeUnits = addUnits("Time")
                    addTimeSeriesResultValue(currentResultID2, dataValue, valueDatetime, censorCodeCV, qualityCodeCV,
                                             timeInterval, timeUnits)

            currentVariableID = addVariables("Count")
            currentUnitID = addUnits("Count")
            currentResultID3 = addDataResult(currentFeatureActionsID, currentVariableID, currentUnitID, processingLevelID)
            addTimeSeriesResult(currentSamplingFeatureName, currentResultID3)
            with open(pathName, 'r') as f:
                csv_reader = csv.reader(f)
                head_row = next(csv_reader)
                for line in csv_reader:
                    if (float(line[1]) > 365) or (float(line[1]) < 1):
                        continue
                    dataValue = line[4]
                    valueDatetime = doy2date(line[0], line[1])
                    censorCodeCV = "Censor Code"
                    qualityCodeCV = "Quality Code"
                    timeUnits = addUnits("Time")
                    addTimeSeriesResultValue(currentResultID3, dataValue, valueDatetime, censorCodeCV, qualityCodeCV,
                                             timeInterval, timeUnits)

            currentDatasetCode = rawDatasetName[rawFilePath.index(pathName)]
            currentDatasetTitle = currentDatasetCode
            currentDatasetAbstract = currentDatasetCode
            currentDatasetID = addDataset(currentDatasetCode, currentDatasetTitle, currentDatasetAbstract)
            addDatasetResult(currentDatasetID, currentResultID1)
            addDatasetResult(currentDatasetID, currentResultID2)
            addDatasetResult(currentDatasetID, currentResultID3)

if "__main__" == __name__:
    wholeOverflow()
