#Copyright Omnibond Systems, LLC. All rights reserved.

# This file is part of CCQHub.

# CCQHub is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# CCQHub is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.

import os
import sys
import time

from ccqHubMethods import writeCcqVarsToFile

sys.path.append(os.path.dirname(os.path.realpath(__file__))+str("/Schedulers"))
from Slurm import SlurmScheduler
from Torque import TorqueScheduler

import urllib2
import json
import traceback
import commands
from datetime import datetime
from datetime import timedelta
import platform
import ccqHubMethods
import threading
import ccqHubVars
import paramiko
import credentials

tempJobScriptLocation = ""#ClusterMethods.tempScriptJobLocation
tempJobOutputLocation = ""#ClusterMethods.tempJobOutputLocation
logDirectory = ""#ClusterMethods.logFileDirectory

#####################################
#  Possible CCQHub States and meanings #
#####################################
# Pending - The job has been submitted via ccqsub and is in the DB but has not yet been processed by ccqHubHandler.py
# Processing - ccqHubHandler is applying the rules and performing the routing of the job
# Queued - ccqHubHandler is waiting to send the job to the scheduler
# Submitted - ccqHubHandler has sent the job to scheduler and it is now up to the scheduler to handle the execution of the job

# Can have multiple states now depending on the scheduler:
# Pending/Queued/CCQueued/Provisioning/Running/Completed/Error etc. These states will come directly from the remote scheduler

# Completed - the job has finished and the remote scheduler has marked the job as completed. ccqHubHandler will now work to transfer the output files and perform other post processing tasks

# deleting - the job has been deleted via ccqdel and no other actions will be performed on the job. If the job was in the process of creating a compute group or expanding a compute group the creation and expansion will proceed

# Error - ccqHubHandler encountered an error while attempting to run the job. These errors will be logged in a TBD place


def performJobRouting():
    print "Placeholder for actual rules/routing policy evaluation and enforcement."


def createSchedulerObject(schedName, schedType, schedulerInstanceId, schedulerHostName, schedulerIpAddress, clusterName):
    # Create the scheduler object that will be used to call the scheduler specific methods
    kwargs = {"schedName": schedName, "schedType": schedType, "instanceID": schedulerInstanceId,  "clusterName": clusterName, "instanceName": schedulerHostName, "schedulerIP": schedulerIpAddress}
    try:
        if schedType == "Torque":
            scheduler = TorqueScheduler(**kwargs)
        elif schedType == "Slurm":
            scheduler = SlurmScheduler(**kwargs)
        else:
            return {"status": "error", "payload": "Unable to create the scheduler object, unsupported scheduler type " + str(schedType)}
        return {"status": "success", "payload": scheduler}
    except:
        return {"status": "error", "payload": "Unable to create the scheduler object"}


def jobSubmission(jobObj):
    print "The timestamp when starting submitJob is: " + str(time.time())
    jobId = jobObj["name"]
    remoteUserName = jobObj["userName"]
    jobMD5Hash = ""
    ccOptionsParsed = []

    jobName = jobObj['jobName']
    jobScriptLocation = jobObj['jobScriptLocation']
    jobScriptText = jobObj['jobScriptText']

    # We need to get the job script object here because we need certain information that we can't get in just the job obj
    results = ccqHubMethods.queryObj(None, "RecType-JobScript-name-" + str(jobName), "query", "dict")
    if results['status'] == "success":
        results = results['payload']
    else:
        return {'status': 'error', 'payload': results['payload']}
    for jobScript in results:
        jobMD5Hash = jobScript['jobMD5Hash']
        ccOptionsParsed = json.loads(jobScript['ccOptionsParsed'])

    targetAddresses = json.loads(ccqHubVars.jobMappings[jobId]["targetAddresses"])
    targetProxyKeys = json.loads(ccqHubVars.jobMappings[jobId]["targetProxyKeys"])
    targetProtocol = ccqHubVars.jobMappings[jobId]["targetProtocol"]
    targetAuthType = ccqHubVars.jobMappings[jobId]["targetAuthType"]

    encodedUserName = ""
    encodedPassword = ""
    valKey = ""
    dateExpires = ""
    certLength = 1

    # Need to try and submit to all of the targetAddresses. Once one is successfully submitted we quit. If we get an error in the connection we retry with the other addresses
    submittedSuccessfully = False
    # Need to try and submit using all of the target proxy keys. Once one submits successfully we exit, if there is an error due to the key we retry.
    for address in targetAddresses:
        badURL = False
        if not submittedSuccessfully:
            try:
                for proxyKey in targetProxyKeys:
                    if not badURL:
                        final = {"jobScriptLocation": str(jobScriptLocation), "jobScriptFile": str(jobScriptText), "jobName": str(jobName), "ccOptionsCommandLine": ccOptionsParsed, "jobMD5Hash": jobMD5Hash, "userName": str(encodedUserName), "password": str(encodedPassword), "valKey": str(valKey), "dateExpires": str(dateExpires), "certLength": str(certLength), "ccAccessKey": str(proxyKey), "remoteUserName": str(remoteUserName)}
                        data = json.dumps(final)
                        headers = {'Content-Type': "application/json"}
                        url = "https://" + str(address) + "/srv/ccqsub"
                        try:
                            req = urllib2.Request(url, data, headers)
                            res = urllib2.urlopen(req).read().decode('utf-8')
                            res = json.loads(res)
                        except Exception as e:
                            # We couldn't connect to the url so we need to try the other one.
                            badURL = True
                            print ''.join(traceback.format_exc(e))
                        if not badURL:
                            if res['status'] == "failure":
                                if proxyKey is not None:
                                    print "The key is not valid, please check your key and try again."
                            elif res['status'] == "error":
                                #If we encounter an error NOT an auth failure then we exit since logging in again probably won't fix it
                                print res['payload']['message'] + "\n\n"
                            elif res['status'] == "success":
                                ccqJobId = res['payload']['message'].split(":")[1][1:5]
                                with ccqHubVars.ccqHubVarLock:
                                    ccqHubVars.jobMappings[jobId]['status'] = "ccqHubSubmitted"
                                values = ccqHubMethods.updateJobInDB({"status": "ccqHubSubmitted", "jobIdInCcq": str(ccqJobId)}, jobId)
                                if values['status'] != "success":
                                    return {"status": "error", "payload": values['payload']}
                                print "The job has been successfully submitted."
                                return {"status": "success", "payload": "Successfully submitted the job to the scheduler"}
            except Exception as e:
                # We encountered an unexpected exception
                print ''.join(traceback.format_exc(e))
    return {"status": "error", "payload": "Unable to successfully submit the job to the specified scheduler. The key or the target URL is invalid."}


#Determine whether the job needs to create resources, wait, or use existing resources. This method determines what happens to the job
def determineNextStepsForJob(jobId, targetName, schedType):
    #If the job has been killed there is no need to process it
    if ccqHubVars.jobMappings[jobId]['status'] == "Killed":
        return {"status": "success", "payload": {"nextStep": "none"}}

    #Check to see if the job is submittable to any of the currently available resources the scheduler has
    #Also checks jobs already set to expand as well in case things have changed since they were assigned a compute group
    print "The job Id is: " + str(jobId)
    #print "ccqHubVars.jobMappings[jobId][isCreating] is: " + str(ccqHubVars.jobMappings[jobId]['isCreating'])
    if ccqHubVars.jobMappings[jobId]['status'] == "Pending":
        # The job is new and has not been processed by ccqHubLauncher yet. We need to send the job off to the appropriate scheduler
        # Need to get target information here so we can decide what to do with the job, is it local or need to be sent to the Cloud
        targetInfo = {}
        values = ccqHubMethods.getTargetInformation(targetName, schedType)
        if values['status'] != "success":
            print values['payload']
        else:
            targetInfo = values['payload']

        # The scheduler that the job is being submitted to is a Cloud based ccq scheduler so we need the authentication method, the protocol to use, and the DNS name
        targetType = targetInfo['schedulerType']
        targetProtocol = targetInfo['protocol']
        targetAuthType = targetInfo['authType']
        targetProxyKeys = targetInfo['proxyKey']
        targetAddresses = targetInfo['targetAddress']

        if values['status'] != "success":
            return {"status": "error", "payload": values['payload']}

        with ccqHubVars.ccqHubVarLock:
            ccqHubVars.jobMappings[jobId]['status'] = "Processing"
            ccqHubVars.jobMappings[jobId]["targetType"] = str(targetType)
            ccqHubVars.jobMappings[jobId]["targetProtocol"] = str(targetProtocol)
            ccqHubVars.jobMappings[jobId]["targetAuthType"] = str(targetAuthType)
            ccqHubVars.jobMappings[jobId]["targetProxyKeys"] = targetProxyKeys
            ccqHubVars.jobMappings[jobId]["targetAddresses"] = targetAddresses

        values = ccqHubMethods.updateJobInDB({"status": "Processing", "targetType": str(targetType), "targetProtocol": str(targetProtocol), "targetAuthType": str(targetAuthType), "targetProxyKeys": targetProxyKeys, "targetAddresses": targetAddresses}, jobId)
        if values['status'] != "success":
            return {"status": "error", "payload": values['payload']}

        # Now we need to determine what the next step for the job is
        if targetType == "ccq":
            # We need to submit the job to the cloud scheduler and start a thread to monitor the progress of the job
            return {"status": "success", "payload": {"nextStep": "cloudSubmit"}}
        else:
            # We need to do a local submit and start a process to monitor the progress of the job
            return {"status": "success", "payload": {"nextStep": "localSubmit"}}

    #TODO need to add the code for the other types of states that the job can go into
    elif ccqHubVars.jobMappings[jobId]['status'] == "ccqHubSubmitted":
        print "Not yet implemented"
        return {"status": "success", "payload": "Not yet implemented"}
    elif ccqHubVars.jobMappings[jobId]['status'] == "Completed":
        print "Not yet implemented"
        return {"status": "success", "payload": "Not yet implemented"}
    elif ccqHubVars.jobMappings[jobId]['status'] == "Error":
        print "Not yet implemented"
        return {"status": "success", "payload": "Not yet implemented"}
    elif ccqHubVars.jobMappings[jobId]['status'] == "Killed":
        print "Not yet implemented"
        return {"status": "success", "payload": "Not yet implemented"}
    elif ccqHubVars.jobMappings[jobId]['status'] == "Deleting":
        print "Not yet implemented"
        return {"status": "success", "payload": "Not yet implemented"}
    else:
        #The job is still being processed by the scheduler and there is nothing we need to do at this time
        print "Not yet implemented"
        return {"status": "success", "payload": "Not yet implemented"}


def determineJobsToProcess():
    print "Inside of determineJobsToProcess"
    #Check to see which job is next in line to be run
    jobSubmitTimes = {}
    jobs = {}
    results = ccqHubMethods.queryObj(None, "RecType-Job-name-", "query", "dict", "beginsWith")
    if results['status'] == "success":
        results = results['payload']
    else:
        print "Error: QueryErrorException! Unable to get Item!"
        return {'status': 'error', 'payload': results['payload']}
    for job in results:
        print "This is the job[status] " + str(job['status'])
        if job['status'] != "CCQueued" and job['status'] != "Error" and job['status'] != "Deleting" and job['status'] != "deleting" and str(job['isSubmitted']).lower() == "false":
            jobSubmitTimes[job['name']] = job['dateSubmitted']
            jobs[job['name']] = job

    sortedJobsBySubmissionTime = sorted(jobSubmitTimes.items(), key=lambda x: x[1])
    print "The jobs sorted by submission time are: " + str(sortedJobsBySubmissionTime)

    return {"status": "success", "payload": {"jobs": jobs, "sortedJobs": sortedJobsBySubmissionTime}}


def cleanupDeletedJob(jobId):
    #Need to remove the job from all the ccqVarObjects so that we don't assign any instances to it
    print "SOMEHOW MADE IT INTO CLEANUPDELETEDJOB FUNCTION!!!!\n\n\n\n\n"
    with ccqHubVars.ccqHubVarLock:
        ccqHubVars.jobMappings.pop(jobId)
    writeCcqVarsToFile()


def monitorJobs(scheduler):
    #Get the jobs from the list of job mappings maintained by ccqLauncher
    with ccqHubVars.ccqHubVarLock:
        jobsToCheck = ccqHubVars.jobMappings
    writeCcqVarsToFile()
    try:
        for job in jobsToCheck:
            #Check to ensure that the job has finished creating and if so proceed to check if the job has finished running or not and make sure it is not in the Error state or has already been marked as Completed
            print "NOW CHECKING JOB: " + str(job) + " at the start of monitor jobs\n"

            #Check to see if the job is still in the DB if it is not then the user deleted the job through the ccqdel command, we need to delete it from the jobMappings object
            temp = ccqHubMethods.queryObj(None, job, "get", "dict")
            if temp['status'] == "success":
                temp = temp['payload']
            else:
                pass

            found = False
            for DDBItem in temp:
                found = True
            #Need to free the instances belonging to the job and also remove it from any compute groups that are waiting to expand
            if not found:
                with ccqHubVars.ccqHubVarLock:
                    print "Job " + str(job) + " has been deleted from the DB and is now be deleted from the memory object."
                    jobsToCheck[job]['status'] = "deleting"
                    try:
                        for group in ccqHubVars.instanceTypesAndGroups[ccqHubVars.jobMappings[job]['instanceType']]:
                            if job in ccqHubVars.instanceTypesAndGroups[ccqHubVars.jobMappings[job]['instanceType']][group]['jobsWaitingOnGroup']:
                                ccqHubVars.instanceTypesAndGroups[ccqHubVars.jobMappings[job]['instanceType']][group]['jobsWaitingOnGroup'].remove(job)
                    except Exception as e:
                        print "There was a problem removing the job from the jobsWaitingOnGroup variable."
                        print "Maybe compute group for that instance type didn't exist?" + str(job) + " Not fatal error!"
                        print traceback.format_exc(e)
                    try:
                        if job in ccqHubVars.jobsInProvisioningState:
                            ccqHubVars.jobsInProvisioningState.remove(job)
                        ccqHubVars.jobMappings[job]['instancesToUse']
                        for instance in ccqHubVars.jobMappings[job]['instancesToUse']:
                            try:
                                ccqHubVars.instanceInformation[instance]['state'] = "Available"
                                if instance not in ccqHubVars.availableInstances:
                                    ccqHubVars.availableInstances.append(instance)
                            except Exception as e:
                                #This instance has been deleted from the scheduler already
                                pass
                        ccqHubVars.jobMappings[job]['instancesToUse'] = []

                        #Remove the job from the in-memory object since it is gone from the DB now
                        with ccqHubVars.ccqHubVarLock:
                            ccqHubVars.jobMappings.pop(job)
                        writeCcqVarsToFile()

                    except Exception as e:
                        if job in ccqHubVars.jobsInProvisioningState:
                            ccqHubVars.jobsInProvisioningState.remove(job)
                        print "There was a problem changing the state of the instances reserved for the job and adding it to the available instance list."
                        print "There were no instances assigned to the job " + str(job) + "yet? Not fatal error!"
                        print traceback.format_exc(e)
                        #Remove the job from the in-memory object since it is gone from the DB now
                        with ccqHubVars.ccqHubVarLock:
                            ccqHubVars.jobMappings.pop(job)
                        writeCcqVarsToFile()
            else:
                if jobsToCheck[job]['isCreating'] == "completed" and jobsToCheck[job]['status'] != "Error" and jobsToCheck[job]['status'] != "Completed" and jobsToCheck[job]['status'] != "Killed" and jobsToCheck[job]['status'] != "deleting":
                    if jobsToCheck[job]['status'] == "Submitted" or jobsToCheck[job]['status'] == "Running" or jobsToCheck[job]['status'] == "Queued":
                        print "FOUND A JOB THAT NEEDS TO BE CHECKED! NOW CHECKING THE JOB TO SEE IF IT IS STILL RUNNING!\N"
                        jobsToCheck[job]['name'] = str(job)
                        checkToSeeIfJobStillRunningOnCluster(jobsToCheck[job], scheduler)

                elif jobsToCheck[job]['status'] == "Error" or jobsToCheck[job]['status'] == "Completed" or jobsToCheck[job]['status'] == "Killed" or jobsToCheck[job]['status'] == "deleting":
                    print "NOW CHECKING JOB TO SEE IF IT IS PAST TIME TO DELETE IT FROM THE DB: " + "\n"
                    #Need to see if the job has been in the Error or Completed state for more than 1 day and if it has been longer remove it
                    try:
                        #Check to see if the end time was added to the job or not if it errored it does not have an end time then we make the end time now and go from there
                        endTime = jobsToCheck[job]['endTime']
                    except Exception as e:
                        #There is currently no end time on the instance so we add it to the object
                        endTime = time.time()
                        with ccqHubVars.ccqHubVarLock:
                            ccqHubVars.jobMappings[job]['endTime'] = endTime
                        writeCcqVarsToFile()

                    try:
                        if job in ccqHubVars.jobsInProvisioningState:
                            ccqHubVars.jobsInProvisioningState.remove(job)
                        #Check to see if there are any instances remaining in the instancesToUse list for a job in the Error, Completed, or Killed state and if there are set their statuses to Available
                        ccqHubVars.jobMappings[job]['instancesToUse']
                        for instance in ccqHubVars.jobMappings[job]['instancesToUse']:
                            try:
                                ccqHubVars.instanceInformation[instance]['state'] = "Available"
                                if instance not in ccqHubVars.availableInstances:
                                    ccqHubVars.availableInstances.append(instance)
                            except Exception as e:
                                # The instance has been removed from the DB and therefore we should not set the state internally
                                pass
                        ccqHubVars.jobMappings[job]['instancesToUse'] = []
                    except Exception as e:
                        if job in ccqHubVars.jobsInProvisioningState:
                            ccqHubVars.jobsInProvisioningState.remove(job)
                        print "There was a problem trying to release the instances from the job: " + str(job)
                        print traceback.format_exc(e)

                    if jobsToCheck[job]['status'] == "deleting":
                        try:
                            with ccqHubVars.ccqHubVarLock:
                                ccqHubVars.jobMappings.pop(job)
                            writeCcqVarsToFile()
                        except Exception as e:
                            pass
                    else:
                        now = time.time()
                        difference = now - float(endTime)
                        difference = timedelta(seconds=difference)
                        hours, remainder = divmod(difference.seconds, 3600)
                        minutes, seconds = divmod(remainder, 60)

                        totalMinutes = int(hours)*60 + int(minutes)

                        print "THE TOTAL NUMBER OF MINUTES PASSED SINCE THE JOB COMPLETED/ERRORED/WAS KILLED IS: " + str(totalMinutes) + "\n"
                        #If the jobs have been in the DB more than 1 day after entering the Error or Completed state, delete the job from the DB and from the ccqHubVars objects
                        if int(totalMinutes) > 1440:
                            print "THE TOTAL NUMBER OF MINUTES IS GREATER THAN ONE DAY SO WE ARE DELETING THE JOB FROM THE DB\n"
                            obj = {'action': 'delete', 'obj': job}
                            response = ccqHubMethods.handleObj(obj)
                            if response['status'] != 'success':
                                print "There was an error trying to remove the old Jobs from the DB!"
                            else:
                                try:
                                    with ccqHubVars.ccqHubVarLock:
                                        ccqHubVars.jobMappings.pop(job)
                                    writeCcqVarsToFile()
                                except Exception as e:
                                    pass
                                print "The job " + str(job) + " was successfully deleted from the DB because it completed running over 1 day ago!"
    #We have added a new job to the queue so we need to retry
    except RuntimeError as e:
        pass


def checkToSeeIfJobStillRunningOnCluster(job, scheduler):
    try:
        print "NOW CHECKING JOB: " + str(job['name']) + " inside of checkToSeeIfJobStillRunningOnCluster\n"
        checkJobsKwargs = {"job": job}
        results = scheduler.checkJob(**checkJobsKwargs)
        jobInDBStillRunning = results['payload']['jobInDBStillRunning']
        if not jobInDBStillRunning:
            endTime = time.time()

            #Need to update the DB entry for the job to completed and set the timestamp of when the job completed.
            values = ccqHubMethods.updateJobInDB({"status": "Completed", "endTime": str(endTime), "instancesRunningOnIds": [], "instancesRunningOnNames": []}, job['name'])
            if values['status'] != "success":
                print values['payload']
            print "Job " + str(job['name'] + " has finished running!")
            print "Updating the averages in the DB!"

            with ccqHubVars.ccqHubVarLock:
                ccqHubVars.jobMappings[job['name']]['status'] = "Completed"
                ccqHubVars.jobMappings[job['name']]['endTime'] = endTime
                for instance in ccqHubVars.jobMappings[job['name']]['instancesToUse']:
                    ccqHubVars.instanceInformation[instance]['state'] = "Available"
                    if instance not in ccqHubVars.availableInstances:
                        ccqHubVars.availableInstances.append(instance)
                ccqHubVars.jobMappings[job['name']]['instancesToUse'] = []
            writeCcqVarsToFile()

            results = ccqHubMethods.queryObj(None, job['name'], "get", "dict")
            if results['status'] == "success":
                results = results['payload']
            else:
                return {"status": "error", "payload": {"error": "Query Error Exception!\n", "traceback": ''.join(traceback.format_stack())}}

            jobDB = {}
            for thing in results:
                jobDB = thing

            transferred = False
            maxTries = 5
            tries = 0
            while not transferred:
                status = copyJobOutputFilesToSpecifiedLocation(jobDB)
                if status['status'] == "success":
                    transferred = True
                else:
                    #If the error contains AuthenticationException: Authentication failed then run the driver.
                    # if scheduler.schedType == "Slurm":
                    #     instanceIP = str(job['batchHost']).split(".")[0].replace("-", ".").replace("ip.", "")
                    #     print "FAILED TO TRANSFER THE OUTPUT FILES, RUNNING USER AND GROUPS COMMANDS AGAIN ON INSTANCE: " + str(instanceIP)
                    #     print "RUNNING COMMAND: salt \'" + str(instanceIP) + "\' state.sls instance_states.users_groups.groupsAdded"
                    #     print "Unable to verify that the user is on the instance: " + str(instanceIP) + " re-running the group and user states for " + str(instanceIP) + " instance."
                    #     ga = commands.getstatusoutput("salt \'" + str(instanceIP) + "\' state.sls instance_states.users_groups.groupsAdded")
                    #     print "THE OUTPUT OF THE GROUPS ADDED STATE IS: " + str(ga)
                    #     print "RUNNING COMMAND: salt \'" + str(instanceIP) + "\' state.sls instance_states.users_groups.usersAdded"
                    #     ua = commands.getstatusoutput("salt \'" + str(instanceIP) + "\' state.sls instance_states.users_groups.usersAdded")
                    #     print "THE OUTPUT OF THE GROUPS ADDED STATE IS: " + str(ua)
                    if tries > maxTries:
                        transferred = True
                        #ClusterMethods.writeToErrorLog({"messages": ["There was a problem transferring the output files from the job to the requested location. Please check the " + str(ClusterMethods.tempJobOutputLocation) + str(jobDB['userName']) + " on " + str(scheduler.schedName) + " to see if your output files have been transferred there\n"], "traceback": ["Job Output Transfer Error"]}, "ccq Job " + str(jobDB['name']))
                    tries += 1

            values = ccqHubMethods.calculateAvgRunTimeAndUpdateDB(jobDB['startTime'], str(endTime), jobDB['instanceType'], jobDB['jobName'])
            if values['status'] != "success":
                print values['payload']
                print "There was an error calculating the run time for the job!"

        return {"status": "success", "payload": "Finished checking if the job is still running and if not and instances were close to their hour limit and not running other jobs, they were terminated."}
    except Exception as e:
        print traceback.format_exc(e)
        return {'status': 'error', 'payload': {"error": "Error: "+str(e), "traceback": ''.join(traceback.format_exc(e))}}


def copyJobOutputFilesToSpecifiedLocation(job):
    storedPassword = ""
    accessInstanceName = ""
    res = ccqHubMethods.queryObj(None, "RecType-Collaborator-userName-" + str(job['userName']), "query", "json")
    if res['status'] == "success":
        results = res['payload']
        for item in results:
            res = credentials.decryptString(item['password'])
            if res['status'] == "success":
                storedPassword = res['payload']['string']
            else:
                print "Unable to decrypt the password from the Collaborator!"
            break

    try:
        batchHost = job['batchHost']
        if batchHost is not None:
            #Slurm does not copy the output files to where the job was submitted, but since Torque does this, we need to
            #keep the behavior similar between the two so we need to ssh into the batch host node and get the output files
            #for the job and transfer them to the scheduler.
            try:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(str(batchHost), username=job['userName'], password=str(storedPassword), timeout=15)
                sftp = ssh.open_sftp()
                #Copy the job from the stdoutFileLocation and stderrFileLocation to the standard location used by ccq for
                #transfer to the directory the user called ccqsub from.
                sftp.get(str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".o", str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".o")
                sftp.get(str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".e", str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".e")
                sftp.close()
                ssh.close()
            except Exception as e:
                print "Unable to transfer the output files from " + str(batchHost) + "! Due to the following exception: "
                print traceback.format_exc(e)
                print "\n\n"
                return {"status": "error", "payload": "There was an error transferring the job output files."}

    except KeyError as e:
        pass

    #By default the files get transferred into the directory where the job was submitted if no special output location is specified
    #Make sure the file has been transferred before attempting to copy it
    time.sleep(30)
    #If the job was submitted remotely (not from a CC instance) then the job output will be transferred to the user's
    #home directory on the login instance for easy access and storage"
    if job['isRemoteSubmit'] == "True":
        #Get Access Instance IP Address:
        results = ccqHubMethods.queryObj(None, "RecType-WebDav-clusterName-" + str(job['schedClusterName']), "query", "dict", "beginsWith")
        if results['status'] == "success":
            results = results['payload']
        else:
            print "Error trying to obtain the Access Instance to transfer the files from!"
            return {"status": "error", "payload": "Error trying to obtain the Access Instance to transfer the files from."}

        for item in results:
            accessInstanceName = item["accessName"]

        if accessInstanceName != "":
            ccqHubMethods.updateJobInDB({"accessInstanceResultsStoredOn": str(accessInstanceName)}, job['name'])
            try:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(str(accessInstanceName), username=job['userName'], password=str(storedPassword), timeout=15)
                sftp = ssh.open_sftp()
                sftp.put(str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".o", "/home/" + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".o")
                sftp.put(str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".e", "/home/" + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".e")
                sftp.close()
                ssh.close()
                #os.system("rm -rf " + str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".e " + str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".o")
            except Exception as e:
                print "Unable to transfer the output files from the job: " + str(job['name']) + " to " + str(accessInstanceName) + " ! Due to the following exception: "
                print traceback.format_exc(e)
                print "\n\n"
                return {"status": "error", "payload": "There was an error transferring the job output files."}
        else:
            print "Unable to get an Access Instance to copy the results too! Results will stay on the scheduler where the job was submitted!"
    else:
        #Use the submitHostInstanceId parameter to get the IP of the instance to send the information too!
        ipToSendTo = ""
        items = ccqHubMethods.queryObj(None, str(job['submitHostInstanceId']), "get", "dict")
        if items['status'] == "success":
            items = items['payload']

        for instance in items:
            ipToSendTo = instance['instanceIP']
        try:
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(str(ipToSendTo), username=job['userName'], password=str(storedPassword), timeout=15)
            sftp = ssh.open_sftp()

            if str(job["stdoutFileLocation"]) == "default":
                stdoutFileLocation = str(job['jobWorkDir']) + "/" + str(job['jobName']) + str(job['name']) + ".o"
            else:
                stdoutFileLocation = job["stdoutFileLocation"]
            if str(job["stderrFileLocation"]) == "default":
                stderrFileLocation = str(job['jobWorkDir']) + "/" + str(job['jobName']) + str(job['name']) + ".e"
            else:
                stderrFileLocation = job["stderrFileLocation"]

            #Put the job output files in the directory where the ccqsub command was ran from on the instance that invoked the call
            sftp.put(str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".o", stdoutFileLocation)
            sftp.put(str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".e", stderrFileLocation)
            sftp.close()
            ssh.close()
            os.system("rm -rf " + str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".e " + str(tempJobOutputLocation) + str(job['userName']) + "/" + str(job['jobName']) + str(job['name']) + ".o")
            return {"status": "success", "payload": "Output files successfully transferred."}
        except Exception as e:
            print "Unable to transfer the output files from the job: " + str(job['name']) + " to " + str(ipToSendTo) + " ! Due to the following exception: "
            print traceback.format_exc(e)
            print "\n\n"
            return {"status": "error", "payload": "There was an error transferring the job output files."}


def delegateTasks():
    print "The timestamp when starting delegateTasks is: " + str(time.time())
    listOfJobsToProcess = {}
    isStartup=True
    while True:
            #If there are fewer than 10 jobs in the queue wait 20 seconds before attempting to process the next job
            #This prevents spamming the database with queries while the initial instances are creating and allows instances
            #to spin up properly
            if len(listOfJobsToProcess) < 10 and not isStartup:
                time.sleep(20)
            results = determineJobsToProcess()
            isStartup = False
            if results['status'] != "success":
                time.sleep(60)
                print "Failed to find any jobs, sleeping for 20 seconds and checking for jobs again."
            else:
                listOfJobsToProcess = results['payload']['jobs']

                #Process all of the jobs in the list in the order they were submitted
                for job in results['payload']['sortedJobs']:
                    try:
                        # Get job name out of the sorted item
                        currentJob = listOfJobsToProcess[job[0]]
                        # Make sure that the job is in the ccqHubVars.jobMappings list and if it isn't then add it to the list
                        try:
                            ccqHubVars.jobMappings[currentJob['name']]
                            if currentJob['status'] == "Killed":
                                ccqHubVars.jobMappings[currentJob['name']]['status'] = currentJob['status']
                        except:
                            with ccqHubVars.ccqHubVarLock:
                                ccqHubVars.jobMappings[currentJob['name']] = currentJob

                        print "THE CURRENT JOB IS: " + str(currentJob['name'])

                        # Now that we have the job we need to determine what the next step is for the job
                        values = determineNextStepsForJob(currentJob['name'], currentJob['targetName'], currentJob['schedType'])
                        print values
                        if values['status'] != "success":
                            if values['status'] == "error":
                                #ClusterMethods.writeToErrorLog(values['payload'], "ccq Job " + str(currentJob['name']))
                                print values['payload']
                            elif values['status'] == "deleting":
                                #Need to remove the job from all the ccqVarObjects so that we don't assign any instances to it also need to add the instances assigned to the job (if any) back to the available instance pool
                                cleanupDeletedJob(currentJob['name'])
                        else:
                            print values['payload']['nextStep']
                            if values['payload']['nextStep'] == "cloudSubmit":
                                values = jobSubmission(currentJob)
                                print values
                                # with ccqHubVars.ccqHubVarLock:
                                #     if currentJob['name'] in ccqHubVars.jobsInProvisioningState and ccqHubVars.jobMappings[currentJob['name']]['status'] != "Provisioning" and ccqHubVars.jobMappings[currentJob['name']]['status'] != "CCQueued":
                                #         ccqHubVars.jobsInProvisioningState.remove(currentJob['name'])
                                #     else:
                                #         if len(ccqHubVars.jobsInProvisioningState) < 10:
                                #             provisioningThread = threading.Thread(target=jobSubmission, args=(currentJob, scheduler))
                                #             provisioningThread.start()
                                #             #jobSubmission(currentJob, scheduler)
                                #             ccqHubVars.jobsInProvisioningState.append(currentJob['name'])
                                #         else:
                                #             pass

                            elif values['payload']['nextStep'] == "localSubmit":
                                # TODO do stuff here for the local job submission implementation
                                print "Not yet implemented!"
                    except Exception as e:
                        print "Encountered a breaking error on job " + str(listOfJobsToProcess[job[0]])
                        print traceback.format_exc()
                    print "The time at the end of delegate tasks is: " + str(time.time())


def main():
        #Set the global variables that ccq will use throughout its operation
        ccqHubVars.init()
        print "Successfully initialized the global variables for ccq."

        #Set the locks for the threads to ensure data integrity and to ensure multiple threads do not write to the same ccq variable or ccq status file at once
        ccqHubVars.ccqHubVarLock = threading.RLock()
        ccqHubVars.ccqHubFileLock = threading.RLock()
        ccqHubVars.ccqHubDBLock = threading.RLock()

        #ccqCleanupThread = threading.Thread(target=monitorJobs)
        #ccqCleanupThread.start()
        #print "Successfully started the monitorJobsAndInstances thread to check and monitor the jobs."

        ccqDelegateTasksThread = threading.Thread(target=delegateTasks)
        ccqDelegateTasksThread.start()

        #Run forever and check to make sure the threads are running
        while True:
            time.sleep(120)
            if not ccqDelegateTasksThread.is_alive:
                print "UHOH WE DIED AT SOME POINT"
                ccqDelegateTasksThread.start()
            else:
                print "The ccqDelegateTasksThread is running."
            # if not ccqCleanupThread.is_alive:
            #     print "UHOH WE DIED AT SOME POINT"
            #     ccqCleanupThread.start()
            # else:
            #     print "The ccqCleanupThread is running."
main()