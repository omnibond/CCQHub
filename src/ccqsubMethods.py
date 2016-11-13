# Copyright Omnibond Systems, LLC. All rights reserved.

# This file is part of CCQHub.

# CCQHub is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# CCQHub is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public License
# along with CCQHub.  If not, see <http://www.gnu.org/licenses/>.

import base64
import socket
import traceback
import urllib2
import sys
import json
import time
from random import randint
import datetime
from datetime import timedelta

def checkJobIdAndUserValidity(jobId, userName, isCert):
    import ccqHubMethods

    if str(isCert) == "True":
        values = decodeCertUnPwVals(str(userName), None)
        if values['status'] != "success":
            return {"status": "error", "payload": {"error": "There was a problem trying to decode the credentials!", "traceback": ''.join(traceback.format_stack())}}
        else:
            userName = values['payload']['decUname']
    else:
        userName = decodeString("ccqunfrval", str(userName))

    results = ccqHubMethods.queryObject(None, "RecType-Job-name-" + str(jobId), "query", "dict", "beginsWith")
    if results['status'] == "success":
        results = results['payload']
    else:
        #Need to update the job status here and somehow notify the user the job has failed
        print "Error: QueryErrorException! Unable to get Item!"
        return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}
    for job in results:
        if job['userName'] == str(userName):
            return {"status": "success", "payload": {'jobExists': True, "jobInformation": job}}
        else:
            return {"status": "success", "payload": {'jobExists': False, "message": "You do not have permission to view this job's information!"}}
    return {"status": "success", "payload": {'jobExists': False, "message": "The specified job Id does not exist in the database!"}}

def updateJobInDB(fieldsToAddToJob, jobId):
    #Update the job DB entry with the status of the job!
    import ccqHubMethods
    done = False
    timeToWait = 10
    maxTimeToWait = 120
    timeElapsed = 0
    quit = False
    while not done:
        try:
            results = ccqHubMethods.queryObject(None, "RecType-Job-name-" + str(jobId), "query", "dict", "beginsWith")
            if results['status'] == "success":
                results = results['payload']
            else:
                #Need to update the job status here and somehow notify the user the job has failed
                print "Error: QueryErrorException! Unable to get Item!"
                return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}
            for job in results:
                if fieldsToAddToJob is not None and job['status'] != "Deleting":
                    for key, value in fieldsToAddToJob.iteritems():
                        if key == "status" and value == "Deleting":
                            job[key] = value
                            quit = True
                        if not quit:
                            job[key] = value
                job.save()
            if not quit:
                return {"status": "success", "payload": "Success!"}
            elif quit:
                return {"status": "deleting", "payload": "This job has been deleted by the user and should stop execution and all scaling activities!"}
        except Exception as e:
            if timeElapsed >= maxTimeToWait:
                return {"status": "error", "payload": "Failed to save out job status!"}
            time.sleep(timeToWait)
            timeElapsed += timeToWait

def calculateAvgRunTimeAndUpdateDB(startTime, endTime, instanceType, jobName):
    #Update the job DB entry with the status of the job!
    import ccqHubMethods
    done = False
    timeToWait = 10
    maxTimeToWait = 120
    timeElapsed = 0
    while not done:
        try:
            results = ccqHubMethods.queryObject(None, "RecType-JobScript-name-" + str(jobName), "query", "dict", "beginsWith")
            if results['status'] == "success":
                results = results['payload']
            else:
                #Need to update the job status here and somehow notify the user the job has failed
                print "Error: QueryErrorException! Unable to get Item!"
                return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}
            for jobScript in results:
                runTimes = jobScript['runTimes']
                try:
                    previousAvg = runTimes[instanceType]['avgTime']
                    previousRunTimes = runTimes[instanceType]['runTimes']
                except KeyError:
                    previousAvg = 0
                    previousRunTimes = []

                if previousAvg == 0:
                    td = int(endTime) - int(startTime)
                    td = timedelta(seconds=td)
                    hours, remainder = divmod(td.seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    jobScript['runTimes'][instanceType]['avgTime'] = {"days": int(td.days), "hours": int(hours), "minutes": int(minutes), "seconds": int(seconds)}
                    previousRunTimes.append({"startTime": startTime, "endTime": endTime})
                else:
                    #Need to figure out how to do the average run time of the two date time objects
                    thisRun = int(endTime) - int(startTime)
                    prevAvg = timedelta(days=previousAvg['days'], hours=previousAvg['hours'], minutes=previousAvg['minutes'], seconds=previousAvg['seconds'])
                    newAvgSeconds = (prevAvg.seconds + thisRun.seconds) / 2
                    newAvg = timedelta(seconds=newAvgSeconds)
                    hours, remainder = divmod(newAvg.seconds, 3600)
                    minutes, seconds = divmod(remainder, 60)
                    jobScript['runTimes'][instanceType]['avgTime'] = {"days": int(newAvg.days), "hours": int(hours), "minutes": int(minutes), "seconds": int(seconds)}

                jobScript.save()
            return {"status": "success", "payload": "Successfully calculated the new average times!"}
        except Exception as e:
            if timeElapsed >= maxTimeToWait:
                return {"status": "error", "payload": "Failed to save out job script averages status!"}
            time.sleep(timeToWait)
            timeElapsed += timeToWait

def appendSuffixToJobScriptName(jobName):
    #There are conflicting job names who's MD5 hashes do not match so here we append a number to the end of the
    #jobName in order to make it unique
    import ccqHubMethods
    items = ccqHubMethods.queryObject(None, "RecType-JobScript-name-" + str(jobName), "query", "dict", "beginsWith")
    if items['status'] == "success":
        items = items['payload']
    else:
        print "Error: QueryErrorException! Unable to get Item!"
        return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

    count = 0
    for jobScript in items:
        count += 1

    try:
        temp = str(jobName).split('.')
        jobName = str(temp[0])+str(count)
        for x in range(len(temp)):
            if x == 0:
                pass
            else:
                jobName += str(temp[x])
    except:
        jobName = str(jobName)+str(count)

    return {"status": "success", "payload": {"jobName": jobName}}

def compareJobScriptsMD5s(jobName, newJobMD5):
    import ccqHubMethods
    conflictingJobMD5 = ""
    items = ccqHubMethods.queryObject(None, "RecType-JobScript-name-" + str(jobName), "query", "dict", "beginsWith")
    if items['status'] == "success":
        items = items['payload']
    else:
        print "Error: QueryErrorException! Unable to get Item!"
        return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

    for jobScript in items:
        conflictingJobMD5 = jobScript['jobMD5Hash']
    if conflictingJobMD5 == "":
        return {"status": "error", "payload": {"There was an error retrieving the MD5 Hash for the Job!"}}
    if newJobMD5 == conflictingJobMD5:
        return {"status": "success", "payload": {"sameJob": True}}
    else:
        return {"status": "success", "payload": {"sameJob": False}}

def checkUniqueness(typeToCompare, parameter, jobName=None):
    import ccqHubMethods
    if typeToCompare == "jobScript":
        items = ccqHubMethods.queryObject(None, "RecType-JobScript-name-" + str(parameter), "query", "dict", "beginsWith")
        if items['status'] == "success":
            items = items['payload']
        else:
            print "Error: QueryErrorException! Unable to get Item!"
            return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

        for jobScript in items:
            if jobScript['name'] == parameter:
                return {"status": "success", "payload": {"isTaken": True}}
        return {"status": "success", "payload": {"isTaken": False}}

    elif typeToCompare == "jobId":
        items = ccqHubMethods.queryObject(None, "RecType-Job-name-", "query", "dict", "beginsWith")
        if items['status'] == "success":
            items = items['payload']
        else:
            print "Error: QueryErrorException! Unable to get Item!"
            return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

        for jobId in items:
            if jobId['jobName'] == parameter or jobId['name'] == jobName:
                return {"status": "success", "payload": {"isTaken": True}}
        return {"status": "success", "payload": {"isTaken": False}}

def putJobScriptInDB(obj):
    ccOptionsParsed = obj['ccOptionsParsed']
    jobName = obj['jobName']
    jobScriptText = obj['jobScriptText']
    jobScriptLocation = obj['jobScriptLocation']
    userName = obj['userName']
    jobMD5Hash = obj['jobMD5']

    import ccqHubMethods
    obj = {}
    obj['action'] = "create"
    data = {'name': str(jobName), 'RecType': 'JobScript', 'schedType': str(ccOptionsParsed['schedType']), 'jobScriptText': str(jobScriptText),
            'jobScriptLocation': str(jobScriptLocation), "dateFirstSubmitted": str(time.time()), "runTimes": {}, "numberOfTimesRun": str("0"), "createdByUser": str(userName), "jobMD5Hash": str(jobMD5Hash)}
    for command in ccOptionsParsed:
        if ccOptionsParsed[command] != "None":
            data[command] = ccOptionsParsed[command]
    obj['obj'] = data
    response = ccqHubMethods.handleObj(obj)
    if response['status'] == "success" or response['status'] == "partial":
        item = response['payload']
        return {"status": "success", "payload": "Successfully saved the job script to the database!"}
    else:
        return {"status": "error", "payload": str(response['message'])}

def putJobToRunInDB(obj):
    ccOptionsParsed = obj['ccOptionsCommandLine']
    jobName = obj['jobName']
    jobScriptText = obj['jobScriptText']
    jobScriptLocation = obj['jobScriptLocation']
    userName = obj['userName']
    schedulerToUse = ccOptionsParsed["schedulerToUse"]
    instanceType = ccOptionsParsed["instanceType"]
    schedulerIP = obj['schedulerIP']

    isRemoteSubmit = obj['isRemoteSubmit']

    stdoutFileLocation = ccOptionsParsed["stdoutFileLocation"]
    stderrFileLocation = ccOptionsParsed["stderrFileLocation"]
    jobWorkDir = ccOptionsParsed["jobWorkDir"]

    submitHostInstanceId = obj['submitHostInstanceId']
    schedClusterName = obj['schedClusterName']

    import ccqHubMethods
    generatedJobId = ""
    done = False
    while not done:
        jobNums = [randint(0,9) for p in range(0,4)]
        for digit in jobNums:
          generatedJobId +=str(digit)

        values = checkUniqueness("jobId", generatedJobId)
        if values['status'] == "success" and not values['payload']['isTaken']:
            done = True

    suffixToAddToJobName = 0
    done = False
    while not done:
        if suffixToAddToJobName == 0:
            values = checkUniqueness("jobId", generatedJobId, jobName)
        else:
            jobName = str(jobName)+str(suffixToAddToJobName)
            values = checkUniqueness("jobId", generatedJobId, str(jobName))
        if values['status'] == "success" and not values['payload']['isTaken']:
            done = True
        suffixToAddToJobName += 1

    obj = {'action': "create"}
    data = {'name': str(generatedJobId), "jobName": str(jobName), 'RecType': 'Job', "schedulerUsed": str(schedulerToUse), 'schedType': str(ccOptionsParsed['schedType']), 'jobScriptText': str(jobScriptText),'jobScriptLocation': str(jobScriptLocation), "dateSubmitted": str(time.time()), "startTime": time.time(), "instanceType": str(instanceType), "userName": str(userName), "schedulerIP": str(schedulerIP), "stderrFileLocation": str(stderrFileLocation) , "stdoutFileLocation": str(stdoutFileLocation), "submitHostInstanceId": str(submitHostInstanceId), "schedClusterName": str(schedClusterName), "isRemoteSubmit": str(isRemoteSubmit), "jobWorkDir": str(jobWorkDir)}
    for command in ccOptionsParsed:
        if ccOptionsParsed[command] != "None":
            data[command] = ccOptionsParsed[command]
    obj['obj'] = data
    response = ccqHubMethods.handleObj(obj)
    if response['status'] == "success" or response['status'] == "partial":
        item = response['payload']

        done = False
        timeToWait = 10
        maxTimeToWait = 120
        timeElapsed = 0
        while not done:
            try:
                items = ccqHubMethods.queryObject(None, "RecType-JobScript-name-" + str(jobName), "query", "dict", "beginsWith")
                if items['status'] == "success":
                    items = items['payload']
                else:
                    print "Error: QueryErrorException! Unable to get Item!"
                    return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

                for jobScript in items:
                    jobScript['numberOfTimesRun'] = int(jobScript['numberOfTimesRun'])+1
                    jobScript.save()
                done = True
            except Exception as e:
                if timeElapsed >= maxTimeToWait:
                    return {"status": "error", "payload": "Failed to update number of times the job has run!"}
                time.sleep(timeToWait)
                timeElapsed += timeToWait

        return {"status": "success", "payload": {"jobId": str(generatedJobId)}}
    else:
        return {"status": "error", "payload": {str(response['message'])}}

def checkUserNamePassword(userName, password):
    import ccqHubMethods
    try:
        theUser = None
        res = ccqHubMethods.queryObject(None, "RecType-Collaborator-userName-" + str(userName), "query", "json", "beginsWith")
        if res['status'] == "success":
            results = res['payload']
            for item in results:
                theUser = item
                break
        else:
            return {"status": "error", "payload": "Invalid username or password! Please try again!"}

        if theUser is not None:
            res = decStr(theUser['password'])
            if res['status'] == "success":
                storedPassword = res['payload']['string']
            else:
                return {"status": "error", "payload": "Invalid username or password! Please try again!"}
            if str(password) == str(storedPassword):
                    return {"status": "success", "payload": "Login Successful!"}
            else:
                return {"status": "error", "payload": "Invalid username or password! Please try again!"}
        else:
            return {"status": "error", "payload": "Invalid username or password! Please try again!"}
    except Exception as ex:
        return {"status": "error", "payload": "Invalid username or password! Please try again!"}

def getSchedulerIPInformation(schedName, schedType):
    import ccqHubMethods

    if schedName == "default":
        items = ccqHubMethods.queryObject(None, "RecType-Scheduler-", "query", "dict", "beginsWith")
        if items['status'] == "success":
            items = items['payload']
        else:
            print "Error: QueryErrorException! Unable to get Item!"
            return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

        for scheduler in items:
            if schedType.lower() == str(scheduler['schedType']).lower() and scheduler['defaultScheduler'] == "true":
                isAutoscaling = False
                if scheduler['scalingType'] == "autoscaling":
                    isAutoscaling = True
                return {"status": "success", "payload": {"schedulerIpAddress": str(scheduler['instanceIP']), "clusterName": str(scheduler['clusterName']), "schedulerType": str(scheduler['schedType']), "schedName": str(scheduler['schedName']), "isAutoscaling": isAutoscaling, "instanceName": scheduler['instanceName'], "isDefaultScheduler": scheduler['defaultScheduler'], "schedulerInstanceId": scheduler["instanceID"]}}

    items = ccqHubMethods.queryObject(None, "RecType-Scheduler-schedName-" + str(schedName) + "-", "query", "dict", "beginsWith")
    if items['status'] == "success":
        items = items['payload']
    else:
        print "Error: QueryErrorException! Unable to get Item!"
        return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

    for scheduler in items:
        if schedType.lower() == "default" or schedType.lower() == str(scheduler['schedType']).lower():
            isAutoscaling = False
            if scheduler['scalingType'] == "autoscaling":
                isAutoscaling = True
            return {"status": "success", "payload": {"schedulerIpAddress": str(scheduler['instanceIP']), "clusterName": str(scheduler['clusterName']), "schedulerType": str(scheduler['schedType']), "schedName": str(scheduler['schedName']), "isAutoscaling": isAutoscaling, "instanceName": scheduler['instanceName'], "isDefaultScheduler": scheduler['defaultScheduler'], "schedulerInstanceId": scheduler["instanceID"]}}

    #Need to eventually figure out what the default Scheduler for the different types are and if a Scheduler Name isn't
    #specified then we will choose the default Scheduler for that certain type of job

    return {'status': 'error', 'payload': "The requested scheduler was not found in the Database!"}

def getInstanceType(obj):
    import ccqHubMethods
    ccOptionsParsed = obj['ccOptionsParsed']
    instanceType = ccOptionsParsed['requestedInstanceType']
    print "InstanceType " + str(instanceType)
    if instanceType != "default":
        instanceType = ccOptionsParsed['requestedInstanceType']
        urlResponse = urllib2.urlopen('http://169.254.169.254/latest/meta-data/placement/availability-zone')
        availabilityZone = urlResponse.read()
        values = ccqHubMethods.getRegion(availabilityZone, "")
        if values['status'] != 'success':
            return {"status": "error", "payload": "There was an error trying to determine which region the Scheduler you are using is in. Please try again! " + str(values['payload'])}
        else:
            region = values['payload']
        values = ccqHubMethods.getAllInformationAboutInstancesInRegion(region)
        if values['status'] != 'success':
           return {"status": "error", "payload": "There was a problem getting the Instance Type information from AWS! Please try again in a few minutes!" + str(values['payload'])}
        else:
            instancesInRegion = values['payload']
            try:
                instancesInRegion[instanceType]
                print "The instance type " + str(instanceType) + " is currently available in the " + str(region) + " region."
                return {"status": "success", "payload": {"instanceType": str(instanceType)}}
            except Exception as e:
                return {"status": "error", "payload": "The instance type " + str(instanceType) + " is not currently available in the " + str(region) + " region. Please try again with a different instance type. We are always updating the instances available in each region as Amazon adds more instances."}
    else:
        urlResponse = urllib2.urlopen('http://169.254.169.254/latest/meta-data/placement/availability-zone')
        availabilityZone = urlResponse.read()
        values = ccqHubMethods.getRegion(availabilityZone, "")
        if values['status'] != 'success':
            return {"status": "error", "payload": "There was an error trying to determine which region the Scheduler you are using is in. Please try again! " + str(values['payload'])}
        else:
            region = values['payload']
        values = ccqHubMethods.getAllInformationAboutInstancesInRegion(region)
        if values['status'] != 'success':
           return {"status": "error", "payload": "There was a problem getting the Instance Type information from AWS! Please try again in a few minutes!" + str(values['payload'])}
        else:
            instancesInRegion = values['payload']

            obj = {"numCpusRequested": ccOptionsParsed['numCpusRequested'], "memoryRequested": ccOptionsParsed['memoryRequested'], "networkTypeRequested": ccOptionsParsed['networkTypeRequested'], "instancesInRegion": instancesInRegion, "criteriaPriority": ccOptionsParsed['criteriaPriority'], "optimization": ccOptionsParsed['optimizationChoice']}
            values = ccqHubMethods.calculateInstanceTypeForJob(obj)
            if values['status'] != 'success':
                return {"status": "error", "payload":"There was an error trying to calculate the instance type needed for this job based upon the memory and cpu requirements specified! Please review the requirements and try again!"}
            else:
                instanceType = values['payload']
                return {"status": "success", "payload": {"instanceType": str(instanceType)}}

def readyJobForScheduler(obj):
    jobInDBAlready = False
    jobScriptLocation = obj['jobScriptLocation']
    jobScriptText = obj['jobScriptFile']
    ccOptionsCommandLine = obj['ccOptionsCommandLine']
    jobName = obj["jobName"]
    jobMD5Hash = obj["jobMD5Hash"]
    userName = obj["userName"]
    password = obj["password"]
    schedulerToUse = ccOptionsCommandLine["schedulerToUse"]
    schedType = ccOptionsCommandLine["schedType"]
    isRemoteSubmit = obj["isRemoteSubmit"]

    #Stuff for if authed from cert file
    isCert = obj['isCert']
    if str(isCert) == "True":
        values = decodeCertUnPwVals(str(userName), str(password))
        if values['status'] != "success":
            return {"status": "error", "payload": {"error": "There was a problem trying to decode the credentials!", "traceback": ''.join(traceback.format_stack())}}
        else:
            decodedUserName = values['payload']['decUname']
            decodedPassword = values['payload']['decPass']
    else:
        decodedPassword = decodeString("ccqpwdfrval", str(password))
        decodedUserName = decodeString("ccqunfrval", str(userName))

    schedName = schedulerToUse

    #get the instance ID
    urlResponse = urllib2.urlopen('http://169.254.169.254/latest/meta-data/instance-id')
    instanceId = urlResponse.read()

    values = getSchedulerIPInformation(schedulerToUse, schedType)
    if values['status'] == 'success':
        schedulerIpAddress = values['payload']["schedulerIpAddress"]
        clusterName = values['payload']['clusterName']
        schedName = values['payload']['schedName']
        isAutoscaling = values['payload']['isAutoscaling']
        schedulerInstanceId = values['payload']['schedulerInstanceId']
        schedulerInstanceName = values['payload']['instanceName']
    else:
        return {"status": "error", "payload": values['payload']}
    #print schedulerIpAddress
    #print clusterName

    obj = {"clusterName": str(clusterName), "ccOptionsParsed": ccOptionsCommandLine}
    values = getInstanceType(obj)
    if values['status'] == 'success':
        ccOptionsCommandLine['requestedInstanceType'] = values['payload']["instanceType"]
        ccOptionsCommandLine['instanceType'] = values['payload']["instanceType"]
    else:
        return {"status": "error", "payload": values['payload']}

    values = checkUniqueness("jobScript", jobName)
    if values['status'] == 'success' and values['payload']['isTaken']:
        results = compareJobScriptsMD5s(jobName, jobMD5Hash)
        if results['status'] != 'success':
            return {"status": "error", "payload": results['payload']}
        else:
            if not results['payload']['sameJob']:
                output = appendSuffixToJobScriptName(jobName)
                if output['status'] != 'success':
                    return {"status": "error", "payload": output['payload']}
                else:
                    jobName = output['payload']['jobName']
            else:
                jobInDBAlready = results['payload']['sameJob']

    if not jobInDBAlready:
        #Save the job script object to the DB
        obj = {"clusterName": str(clusterName), "jobScriptLocation": str(jobScriptLocation), "jobScriptText": str(jobScriptText), "jobName": str(jobName), "ccOptionsParsed": ccOptionsCommandLine, "jobMD5": str(jobMD5Hash), "userName": str(decodedUserName)}
        values = putJobScriptInDB(obj)
        if values['status'] == 'success':
            print values['payload']
        else:
            return {"status": "error", "payload": values['payload']}

    #Save the running job to the DB
    newObj = {"jobScriptLocation": str(jobScriptLocation), "jobScriptText": str(jobScriptText), "jobName": str(jobName), "ccOptionsCommandLine": ccOptionsCommandLine, "jobMD5": str(jobMD5Hash), "userName": str(decodedUserName), "password": str(decodedPassword), "schedulerIP": str(schedulerIpAddress), "isRemoteSubmit": str(isRemoteSubmit), "submitHostInstanceId": str(instanceId), "schedClusterName": str(clusterName)}
    values = putJobToRunInDB(newObj)
    if values['status'] != 'success':
        return {"status": "error", "payload": values['payload']}
    else:
        jobId = values['payload']['jobId']
        #print jobId

    #This will hit the Scheduler Web Service with the job parameters and object and then timeout and return the
    #generated job Id

    if str(isCert) == "False":
        userName = encodeString("ccqunfrval", str(decodedUserName))
        password = encodeString("ccqpwdfrval", str(decodedPassword))

    url = "http://" + str(schedulerIpAddress) + "/srv/ccqsub"
    final = {"jobScriptLocation": str(jobScriptLocation), "jobScriptText": str(jobScriptText), "jobId": str(jobId), "jobName": str(jobName), "ccOptionsCommandLine": ccOptionsCommandLine, "jobMD5": str(jobMD5Hash), "userName": str(userName), "password": str(password), "schedName": str(schedName), "isCert": str(isCert)}
    data = json.dumps(final)
    headers = {'Content-Type': "application/json"}
    req = urllib2.Request(url, data, headers)
    try:
        res = urllib2.urlopen(req, timeout=5).read().decode('utf-8')
    except socket.timeout as e:
        return {"status": "success", "payload": "The job has successfully been submitted to the scheduler " + str(schedName) + " and is currently being processed! The job id is: " + str(jobId) + " you can use this id to look up the job status using the ccqstat utility."}
    except Exception as ex:
        print str(ex)
        return {"status": "error", "payload": "There was an error trying to submit your job! " + str(ex)}

    return {"status": "success", "payload": "The job has successfully been submitted to the scheduler " + str(schedName) + " and is currently being processed! The job id is: " + str(jobId) + " you can use this id to look up the job status using the ccqstat utility."}

def getStatusFromScheduler(jobId, userName, password, verbose, instanceId, isCert, schedName=None):
    #This will hit the Scheduler Web Service with the job parameters and object and then timeout and return the
    #generated job Id
    import ccqHubMethods

    if jobId == "all" and schedName is not None:
        schedulerIpAddress = ""
        schedType = ""
        schedulerInstanceName = ""
        schedulerInstanceId = ""

        items = ccqHubMethods.queryObject(None, "RecType-Scheduler-schedName-" + str(schedName), "query", "dict", "beginsWith")
        if items['status'] == "success":
            items = items['payload']
        else:
            print "Error: QueryErrorException! Unable to get Item!"
            return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

        for scheduler in items:
            schedulerIpAddress = scheduler['instanceIP']
            schedType = scheduler['schedType']
            schedulerInstanceId = scheduler['instanceID']
            schedulerInstanceName = scheduler['instanceName']

        if str(isCert) == "False":
                password = encodeString("ccqpwdfrval", str(password))
                userName = encodeString("ccqunfrval", str(userName))

        url = "https://" + str(schedulerIpAddress) + "/srv/ccqstat"


        final = {"jobId": str(jobId), "userName": str(userName), "password": str(password), "verbose": verbose, "instanceId": instanceId, "jobNameInScheduler": None, "schedulerName": schedName, "isCert": str(isCert), 'schedulerType': schedType, 'schedulerInstanceId': schedulerInstanceId, 'schedulerInstanceName': schedulerInstanceName, 'schedulerInstanceIp': schedulerIpAddress}
        data = json.dumps(final)
        headers = {'Content-Type': "application/json"}
        req = urllib2.Request(url, data, headers)
        try:
            res = urllib2.urlopen(req).read().decode('utf-8')
            return {"status": "success", "payload": res}
        except Exception as ex:
            print str(ex)
            return {"status": "error", "payload": "There was an error trying to get the status of the jobs running on the Scheduler " + str(schedName) + "! " + str(ex)}

    values = checkJobIdAndUserValidity(jobId, userName, isCert)
    if values['status'] != "success":
        return {"status": "error", "payload": values['payload']}
    else:
        if not values['payload']['jobExists']:
            print values['payload']['message']
            return {"status": "error", "payload": values['payload']['message']}
        else:
            jobInformation = values['payload']['jobInformation']

    schedulerToUse = jobInformation['schedulerUsed']
    schedType = jobInformation['schedType']

    values = getSchedulerIPInformation(schedulerToUse, schedType)
    if values['status'] == 'success':
        schedulerIpAddress = values['payload']["schedulerIpAddress"]
        clusterName = values['payload']['clusterName']
        schedName = values['payload']['schedName']
        schedulerInstanceId = values['payload']['schedulerInstanceId']
        schedulerInstanceName = values['payload']['instanceName']
    else:
        return {"status": "error", "payload": values['payload']}

    instanceClusterName = None

    items = ccqHubMethods.queryObject(None, instanceId, "get", "dict")
    if items['status'] == "success":
        items = items['payload']
    else:
        instanceClusterName = None
    for item in items:
        instanceClusterName = item['clusterName']

    if instanceClusterName is not None and instanceClusterName == clusterName:
        if str(isCert) == "False":
            userName = encodeString("ccqunfrval", str(userName))
            password = encodeString("ccqpwdfrval", str(password))

        url = "https://" + str(schedulerIpAddress) + "/srv/ccqstat"
        try:
            jobInformation['schedulerJobName']
            final = {"jobId": str(jobId), "userName": str(userName), "password": str(password), "verbose": verbose, "instanceId": instanceId, "jobNameInScheduler": jobInformation['schedulerJobName'], "schedulerName": schedName, "isCert": str(isCert), 'schedulerType': schedType, 'schedulerInstanceId': schedulerInstanceId, 'schedulerInstanceName': schedulerInstanceName, 'schedulerInstanceIp': schedulerIpAddress}
        except:
            final = {"jobId": str(jobId), "userName": str(userName), "password": str(password), "verbose": verbose, "instanceId": instanceId, "jobNameInScheduler": None, "schedulerName": schedName, "isCert": str(isCert), 'schedulerType': schedType, 'schedulerInstanceId': schedulerInstanceId, 'schedulerInstanceName': schedulerInstanceName, 'schedulerInstanceIp': schedulerIpAddress}
        data = json.dumps(final)
        headers = {'Content-Type': "application/json"}
        req = urllib2.Request(url, data, headers)
        try:
            res = urllib2.urlopen(req).read().decode('utf-8')
            return {"status": "success", "payload": res}
        except Exception as ex:
            print str(ex)
            return {"status": "error", "payload": "There ways an error trying to get the status job! " + str(ex)}
    else:
        return {"status": "partial-failure", "payload": "There was an error encountered trying to get the verbose information from the scheduler! The job you requested information on is running on a Cluster that is not the one you are currently using!\n"
                                                        "The non-verbose status of the job " + str(jobId) + " is " + str(jobInformation['status'])}

def deleteJobFromScheduler(jobId, userName, password, instanceId, jobForceDelete, isCert):
    #This will hit the Scheduler Web Service with the job parameters and object and then timeout and return the
    #generated job Id
    import ccqHubMethods

    values = checkJobIdAndUserValidity(jobId, userName, isCert)
    if values['status'] != "success":
        return {"status": "error", "payload": values['payload']}
    else:
        if not values['payload']['jobExists']:
            print values['payload']['message']
            return {"status": "error", "payload": values['payload']['message']}
        else:
            jobInformation = values['payload']['jobInformation']

    schedulerToUse = jobInformation['schedulerUsed']
    schedType = jobInformation['schedType']

    values = getSchedulerIPInformation(schedulerToUse, schedType)
    if values['status'] == 'success':
        schedulerIpAddress = values['payload']["schedulerIpAddress"]
        clusterName = values['payload']['clusterName']
        schedName = values['payload']['schedName']
        schedulerInstanceId = values['payload']['schedulerInstanceId']
        schedulerInstanceName = values['payload']['instanceName']
    else:
        return {"status": "error", "payload": values['payload']}

    instanceClusterName = None

    items = ccqHubMethods.queryObject(None, instanceId, "get", "dict")
    if items['status'] == "success":
        items = items['payload']
    else:
        instanceClusterName = None
    for item in items:
        instanceClusterName = item['clusterName']

    if instanceClusterName is not None and instanceClusterName == clusterName:
        if str(isCert) == "False":
                password = encodeString("ccqpwdfrval", str(password))
                userName = encodeString("ccqunfrval", str(userName))

        url = "https://" + str(schedulerIpAddress) + "/srv/ccqdel"
        try:
            jobInformation['schedulerJobName']
            final = {"jobId": str(jobId), "userName": str(userName), "password": str(password), "instanceId": instanceId, "jobNameInScheduler": jobInformation['schedulerJobName'], "jobForceDelete": jobForceDelete, "isCert": str(isCert), 'schedulerType': schedType, 'schedulerInstanceId': schedulerInstanceId, 'schedulerInstanceName': schedulerInstanceName, 'schedulerInstanceIp': schedulerIpAddress}
        except:
            values = updateJobInDB({"status": "Deleting"}, jobId)
            return {"status": "success", "payload": "The job that you wanted to delete is currently in the process of being deleted!"}
        data = json.dumps(final)
        headers = {'Content-Type': "application/json"}
        req = urllib2.Request(url, data, headers)
        try:
            res = urllib2.urlopen(req).read().decode('utf-8')
            return {"status": "success", "payload": str(res)}
        except Exception as ex:
            print str(ex)
            return {"status": "error", "payload": "There was an error trying to delete job! " + str(ex)}
    else:
        return {"status": "partial-failure", "payload": "There was an error encountered trying to delete the job! The job you want to delete on is running on a Cluster that is not the one you are currently using!\n"}

def decodeString(k, field):
    dchars = []
    field = base64.urlsafe_b64decode(str(field))
    for i in xrange(len(field)):
        k_c = k[i % len(k)]
        dec = chr(abs(ord(field[i])) - ord(k_c) % 256)
        dchars.append(dec)
    ds = "".join(dchars)
    return ds

def checkCertExpired(certExpireDate):
    print "Checking Cert"
    expirationDate = decodeString("ccqdatstrfrcrtfil", str(certExpireDate))

    currentTime = datetime.datetime.now()

    if currentTime > expirationDate:
        return {"status": "error", "payload": "The ccq certificate has expired, please remove it and login again to obtain a new ccq certificate!"}

    else:
        return {"status": "success", "payload": "The ccq certificate is valid, using the credentials specified in the certificate"}

def validateCertificate(userName, password, valKey, dateExpires):
    userName = base64.urlsafe_b64decode(userName)
    password = base64.urlsafe_b64decode(password)
    valKey = base64.urlsafe_b64decode(valKey)

    #Need to decrypt everything now!
    decryptedExpireDate = decodeString("ccqdatstrfrcrtfil", str(dateExpires))
    try:
        splitUser = userName.split(":")
        userObj = {"string": splitUser[0], "key": splitUser[1], "iv": splitUser[2]}

        splitPass = password.split(":")
        passObj = {"string": splitPass[0], "key": splitPass[1], "iv": splitPass[2]}
    except Exception as e:
        return {"status": "error", "payload": "Certificate was unable to be validated!"}

    certDecodedUser = decStr(userObj)
    if certDecodedUser['status'] != "success":
        return {"status": "error", "payload": "Certificate was unable to be validated!"}
    else:
        certDecodedUser = certDecodedUser['payload']['string']

    certDecodedPass = decStr(passObj)
    if certDecodedPass['status'] != "success":
        return {"status": "error", "payload": "Certificate was unable to be validated!"}
    else:
        certDecodedPass = certDecodedPass['payload']['string']
    #Validate valKey
    p1 = str(decryptedExpireDate).split(" ")[1]

    count = 0
    uPlace = 0
    pPlace = 0

    finV = ""
    for l in p1:
        finV += str(l)
        if count % 2 == 0 and uPlace < len(certDecodedUser):
            finV += str(certDecodedUser[uPlace])
            uPlace += 1
        elif pPlace < len(certDecodedPass):
            finV += str(certDecodedPass[pPlace])
            pPlace += 1
        count += 1

    if str(valKey) == str(finV):
        return {"status": "success", "payload": "Certificate Successfully Validated!"}
    else:
        return {"status": "error", "payload": "Certificate was unable to be validated!"}

def decodeCertUnPwVals(userName, password):
    userObj = {}
    passObj = {}
    try:
        if userName is not None:
            userName = base64.urlsafe_b64decode(str(userName))
            splitUser = userName.split(":")
            userObj = {"string": splitUser[0], "key": splitUser[1], "iv": splitUser[2]}

        if password is not None:
            password = base64.urlsafe_b64decode(str(password))
            splitPass = password.split(":")
            passObj = {"string": splitPass[0], "key": splitPass[1], "iv": splitPass[2]}
    except Exception as e:
        return {"status": "error", "payload": "Certificate was unable to be validated!"}

    certDecodedPass = ""
    certDecodedUser = ""

    if userName is not None:
        certDecodedUser = decStr(userObj)
        if certDecodedUser['status'] != "success":
            return {"status": "error", "payload": "Certificate was unable to be validated!"}
        else:
            certDecodedUser = certDecodedUser['payload']['string']

    if password is not None:
        certDecodedPass = decStr(passObj)
        if certDecodedPass['status'] != "success":
            return {"status": "error", "payload": "Certificate was unable to be validated!"}
        else:
            certDecodedPass = certDecodedPass['payload']['string']

    return {"status": "success", "payload": {"decPass": str(certDecodedPass), "decUname": str(certDecodedUser)}}

def encodeString(k, field):
    enchars = []
    for i in xrange(len(field)):
        k_c = k[i % len(k)]
        enc = chr(ord(field[i]) + ord(k_c) % 256)
        enchars.append(enc)
    ens = "".join(enchars)
    return base64.urlsafe_b64encode(ens)

def getControlNodeForCCInstance():
    import ccqHubMethods

    clusterName = ""
    controlNodeIp = ""
    urlResponse = urllib2.urlopen('http://169.254.169.254/latest/meta-data/instance-id')
    instanceId = urlResponse.read()

    items = ccqHubMethods.queryObject(None, instanceId, "get", "dict")
    if items['status'] == "success":
        items = items['payload']
    else:
        return {"status": "error", "payload": "Error: QueryErrorException! Unable to get Item!"}

    for item in items:
        clusterName = item['clusterName']

    items = ccqHubMethods.queryObject(None, "RecType-ControlNode-clusterName-" + str(clusterName)+ "-", "query", "dict")
    if items['status'] == "success":
        items = items['payload']
    else:
        return {"status": "error", "payload": "Error: QueryErrorException! Unable to get Item!"}

    for item in items:
        controlNodeIp = item['publicIP']

    if controlNodeIp != "":
        return {"status": "success", "payload": controlNodeIp}

    else:
        return {"status": "error", "payload": "There was a problem getting the ControlNode IP address!"}

def getSchedulerAndSchedTypeFromJob(jobId):
    import ccqHubMethods
    results = ccqHubMethods.queryObject(None, "RecType-Job-name-" + str(jobId), "query", "dict", "beginsWith")
    if results['status'] == "success":
        results = results['payload']
    else:
        #Need to update the job status here and somehow notify the user the job has failed
        print "Error: QueryErrorException! Unable to get Item!"
        return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}
    for job in results:
        try:
            schedulerToUse = job['schedulerToUse']
            schedType = job['schedType']

            schedulerIpInfo = getSchedulerIPInformation(schedulerToUse, schedType)
            if schedulerIpInfo['status'] == 'success':
                schedulerIpAddress = schedulerIpInfo['payload']["schedulerIpAddress"]
                schedulerInstanceId = schedulerIpInfo['payload']['schedulerInstanceId']
                schedulerInstanceName = schedulerIpInfo['payload']['instanceName']
                return {"status": "success", "payload": str(schedulerIpAddress)}
            else:
                return {"status": "error", "payload": str(schedulerIpInfo['payload'])}
        except:
            return {'status': 'error', 'payload': "Unable to get scheduler information via the Job entry in the DB!"}

    return {"status": "error", "payload": "The job Id requested does not exist in the Database!"}

def getSchedulerIpByName(schedName):
    import ccqHubMethods

    if schedName == "default":
        items = ccqHubMethods.queryObject(None, "RecType-Scheduler-", "query", "dict", "beginsWith")
        if items['status'] == "success":
            items = items['payload']
        else:
            print "Error: QueryErrorException! Unable to get Item!"
            return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

        for scheduler in items:
            if scheduler['defaultScheduler'] == "true":
                return {"status": "success", "payload": {"schedulerIpAddress": str(scheduler['instanceIP'])}}

        return {'status': 'error', 'payload': "The requested default scheduler was not found in the Database!"}

    items = ccqHubMethods.queryObject(None, "RecType-Scheduler-schedName-" + str(schedName), "query", "dict", "beginsWith")
    if items['status'] == "success":
        items = items['payload']
    else:
        print "Error: QueryErrorException! Unable to get Item!"
        return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}

    for scheduler in items:
        return {"status": "success", "payload": {"schedulerIpAddress": str(scheduler['instanceIP'])}}

    #Need to eventually figure out what the default Scheduler for the different types are and if a Scheduler Name isn't
    #specified then we will choose the default Scheduler for that certain type of job

    return {'status': 'error', 'payload': "The requested scheduler was not found in the Database!"}

def calculatePriceForJob(instanceType, numberOfInstances, instanceVolumeSize, instanceVolumeType, purchaseType, isSpotJob, spotPrice):
    try:
        import Cluster

        numberOfInstances = float(numberOfInstances)
        instanceVolumeSize = float(instanceVolumeSize)

        volumeTypes = {"ssd": "gp2", "magnetic": "Magnetic"}

        totalPrice = 0
        storagePricingObj = Cluster.getStoragePricing()
        if storagePricingObj['status'] != 'success':
            return {'status': "error", "payload": storagePricingObj['payload']}
        else:
            storagePricingObj = storagePricingObj['payload']

        cloudyPricing = Cluster.getCloudyPricing()
        if cloudyPricing['status'] != 'success':
            return {'status': "error", "payload": cloudyPricing['payload']}
        else:
            cloudyPricing = cloudyPricing['payload']

        try:
            storagePrice = float(storagePricingObj[str(volumeTypes[instanceVolumeType]).lower()])
            cloudyPrice = float(cloudyPricing[instanceType])

            storagePrice = (storagePrice * instanceVolumeSize) * numberOfInstances
            cloudyPriceTotal = (numberOfInstances * cloudyPrice)

            if not isSpotJob:
                instancePricingObj = Cluster.getInstancePricing()
                if instancePricingObj['status'] != 'success':
                    return {'status': "error", "payload": instancePricingObj['payload']}
                else:
                    instancePricingObj = instancePricingObj['payload']

                instancePrice = float(instancePricingObj[instanceType][purchaseType])

                instancePriceTotal = float(numberOfInstances * instancePrice)

                totalPrice = float(cloudyPriceTotal + instancePriceTotal + storagePrice)

            elif isSpotJob and spotPrice is not None:
                spotPrice = float(spotPrice)
                instancePriceTotal = float(numberOfInstances * spotPrice)

                totalPrice = float(cloudyPriceTotal + instancePriceTotal + storagePrice)

            return {"status": "success", "payload": totalPrice}

        except Exception as e:
            print "There was an error trying to determin the cost of the job!"
            print traceback.print_exc(e)
            return {"status": "error", "payload": {"error": "There was a problem trying to calculate the cost of the job!", "traceback": str(traceback.format_exc(e))}}

    except ImportError as e:
        print "Unable to determine price! Not running on a CC instance!"
        return {"status": "error", "payload": {"error": "Unable to calculate pricing for the job! CCQ not running on a CC instance!", "traceback": str(traceback.format_exc(e))}}
