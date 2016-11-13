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

# You should have received a copy of the GNU Lesser General Public License
# along with CCQHub.  If not, see <http://www.gnu.org/licenses/>.


import os
import sys
import time

import ccqHubMethods
from ccqsubMethods import writeccqHubVarsToFile

sys.path.append(os.path.dirname(os.path.realpath(__file__))+str("/Schedulers"))
from Slurm import SlurmScheduler
from Torque import TorqueScheduler
from Condor import CondorScheduler
from Openlava import OpenlavaScheduler

import urllib2
import json
import traceback
import ccqsubMethods
import commands
from datetime import datetime
from datetime import timedelta
import platform
import threading
import ccqHubVarsVars
import pytz
import paramiko

tempJobScriptLocation = ccqHubMethods.tempScriptJobLocation
tempJobOutputLocation = ccqHubMethods.tempJobOutputLocation
logDirectory = ccqHubMethods.logFileDirectory

def createSchedulerObject(schedName, schedType, schedulerInstanceId, schedulerHostName, schedulerIpAddress, clusterName):
    # Create the scheduler object that will be used to call the scheduler specific methods
    kwargs = {"schedName": schedName, "schedType": schedType, "instanceID": schedulerInstanceId,  "clusterName": clusterName, "instanceName": schedulerHostName, "schedulerIP": schedulerIpAddress}
    try:
        if schedType == "Torque":
            scheduler = TorqueScheduler(**kwargs)
        elif schedType == "Slurm":
            scheduler = SlurmScheduler(**kwargs)
        elif schedType == "Condor":
            scheduler = CondorScheduler(**kwargs)
        elif schedType == "Openlava":
            scheduler = OpenlavaScheduler(**kwargs)
        else:
            return {"status": "error", "payload": "Unable to create the scheduler object, unsupported scheduler type " + str(schedType)}
        return {"status": "success", "payload": scheduler}
    except:
        return {"status": "error", "payload": "Unable to create the scheduler object"}


def jobSubmission(jobObj, scheduler):
    print "The timestamp when starting submitJob is: " + str(time.time())
    jobScriptText = jobObj['jobScriptText']
    jobId = jobObj["name"]
    jobName = jobObj['jobName']
    userName = jobObj["userName"]
    password = ""

    numCpusRequested = jobObj["numCpusRequested"]
    memoryRequested = jobObj["memoryRequested"]
    jobWorkDir = jobObj["jobWorkDir"]

    with ccqHubVars.ccqVarLock:
        ccqHubVars.jobMappings[jobId]['status'] = "Provisioning"
        instancesToUse = ccqHubVars.jobMappings[jobId]['instancesToUse']
    writeccqHubVarsToFile()

    # Will need to get this from the collaborator object
    results = ccqHubMethods.queryObject(None, "RecType-Collaborator-userName-" + str(userName), "query", "dict", "beginsWith")
    if results['status'] == "success":
        results = results['payload']
    else:
        with ccqHubVars.ccqVarLock:
            ccqHubVars.jobMappings[jobId]['status'] = "Error"
        writeccqHubVarsToFile()
        values = ccqsubMethods.updateJobInDB({"status": "Error"}, jobId)
        ccqHubMethods.writeToErrorLog({"messages": ["Unable to obtain the Cluster Object from the DB!\n"], "traceback": [''.join(traceback.format_stack())]}, "ccq Job " + str(jobId))
    for user in results:
        encPwd = user['password']
        password = encryptionFunctions.decryptString(encPwd)

    hostList = ""
    with ccqHubVars.ccqVarLock:
        hostArray = ccqHubVars.jobMappings[jobId]['instancesToUse']
    writeccqHubVarsToFile()

    kwargs = {"hostList": hostArray, "cpus": numCpusRequested, "memory": memoryRequested}
    values = scheduler.putComputeNodesToRunJobOnInCorrectFormat(**kwargs)
    if values['status'] != "success":
        values = ccqsubMethods.updateJobInDB({"status": "Error"}, jobId)
        with ccqHubVars.ccqVarLock:
            ccqHubVars.jobMappings[jobId]['status'] = "Error"
        writeccqHubVarsToFile()
        ccqHubMethods.writeToErrorLog({"messages": [values['payload']['error']], "traceback": [values['payload']['traceback']]}, "ccq Job " + str(jobId))
        print "The timestamp when ending submitJob is: " + str(time.time())
        return {"status": "error", "payload": "There was an error trying to save new status of the job to the DB."}
    else:
        hostList = values['payload']

    #Need to add the instances that the job is running on to the DB entry
    values = ccqsubMethods.updateJobInDB({"status": "Provisioning"}, jobId)
    if values['status'] == "deleting":
        with ccqHubVars.ccqVarLock:
            ccqHubVars.jobMappings[jobId]['status'] = "deleting"
        writeccqHubVarsToFile()
        print "The timestamp when ending submitJob is: " + str(time.time())
        return {"status": "error", "payload": "The job is marked as deleting in the DB so no further action will be taken!"}
    elif values['status'] != 'success':
        print "There was an error trying to save the instances that the job needs to run on in the DB!"
        with ccqHubVars.ccqVarLock:
            ccqHubVars.jobMappings[jobId]['status'] = "Error"
        writeccqHubVarsToFile()
        values = ccqsubMethods.updateJobInDB({"status": "Error"}, jobId)
        ccqHubMethods.writeToErrorLog({"messages": ["There was an error trying to save new status of the job to the DB.\n"], "traceback": [''.join(traceback.format_stack())]}, "ccq Job " + str(jobId))
        print "The timestamp when ending submitJob is: " + str(time.time())
        return {"status": "error", "payload": "There was an error trying to save new status of the job to the DB."}

    # Check to see if the new compute nodes have registered with the Scheduler before submitting the job!
    done = False
    maxTimeToWait = 500
    timeElapsed = 0
    timeToWait = 20
    while not done:
        try:
            status, output = commands.getstatusoutput('sudo salt-run state.event \'salt/presence/present\' count=1')
            var = output.split("\n")[0].split("\t")[1]
            data = json.loads(var)

            if data is not None:
                listOfMinionsAttached = data['present']
                nodesFound = 0
                totalNodes = len(hostArray)
                for host in hostArray:
                    if host in listOfMinionsAttached:
                        nodesFound += 1
                if totalNodes >= nodesFound:
                    done = True

            if not done:
                if timeElapsed > maxTimeToWait:
                    print "The Compute Nodes for job " + str(jobId) + " have not registered with the Scheduler, aborting now."
                    values = ccqsubMethods.updateJobInDB({"status": "Error"}, jobId)
                    with ccqHubVars.ccqVarLock:
                        ccqHubVars.jobMappings[jobId]['status'] = "Error"
                    writeccqHubVarsToFile()
                    ccqHubMethods.writeToErrorLog({"messages": ["The Compute Nodes for job " + str(jobId) + " have not registered with the Scheduler, aborting now."], "traceback": [''.join(traceback.format_stack())]}, "ccq Job " + str(jobId))
                    print "The timestamp when ending submitJob is: " + str(time.time())
                    return {'status': "error", "payload": "The Compute Nodes for job " + str(jobId) + " have not registered with the Scheduler, aborting now."}
                time.sleep(timeToWait)
                timeElapsed += timeToWait

        except Exception as e:
            print "The Compute Nodes for job " + str(jobId) + " have not registered with the Scheduler, aborting now."
            values = ccqsubMethods.updateJobInDB({"status": "Error"}, jobId)
            with ccqHubVars.ccqVarLock:
                ccqHubVars.jobMappings[jobId]['status'] = "Error"
            writeccqHubVarsToFile()
            ccqHubMethods.writeToErrorLog({"messages": ["The Compute Nodes for job " + str(jobId) + " have not registered with the Scheduler, aborting now."], "traceback": [''.join(traceback.format_exc(e))]}, "ccq Job " + str(jobId))
            print "The timestamp when ending submitJob is: " + str(time.time())
            return {"status": "error", "payload": {"error": "The new compute nodes have not registered as minions to the Scheduler!!\n", "traceback": ''.join(traceback.format_exc(e))}}

    #Need to double check and make sure the user made it onto each of the machines to use by trying to ssh into it with that user.
    if len(hostList) > 0:
        listOfInstancesToCall = ""
        for instance in instancesToUse:
            instanceIP = str(instance).split(".")[0].replace("-", ".").replace("ip.", "")
            try:
                ssh = paramiko.SSHClient()
                ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh.connect(str(instanceIP), username=str(userName), password=str(password), timeout=15)
                ssh.close()
            except Exception as e:
                listOfInstancesToCall += str(instanceIP) + ","

        if str(listOfInstancesToCall) != "":
            print "RUNNING COMMAND: salt -L \'" + listOfInstancesToCall + "\' state.sls instance_states.users_groups.groupsAdded"
            ga = commands.getstatusoutput("salt -L \'" + listOfInstancesToCall + "\' state.sls instance_states.users_groups.groupsAdded")
            print "THE OUTPUT OF THE GROUPS ADDED STATE IS: " + str(ga)

            print "RUNNING COMMAND: salt -L \'" + listOfInstancesToCall + "\' state.sls instance_states.users_groups.usersAdded"
            ua = commands.getstatusoutput("salt -L \'" + listOfInstancesToCall + "\' state.sls instance_states.users_groups.usersAdded")
            print "THE OUTPUT OF THE GROUPS ADDED STATE IS: " + str(ua)

    # Check and see if the job script is accessible from the Scheduler and if not we create a temp script to
    # be used instead.
    tempJobScript = open(str(tempJobScriptLocation) + "/" + str(jobName) + str(jobId), "w")
    tempJobScript.write(jobScriptText)
    tempJobScript.close()
    os.system('chmod +x ' + str(str(tempJobScriptLocation) + "/" + str(jobName) + str(jobId)))

    if not os.path.isdir(str(tempJobOutputLocation) + str(userName)):
        os.system("/bin/mkdir -p " + str(tempJobOutputLocation) + str(userName))
        #os.system("chmod 600 " + str(tempJobOutputLocation) + str(userName))

    # Call the scheduler specific job submission method. This method should take the same parameters for all schedulers!
    if ccqHubVars.jobMappings[jobId]['isAutoscaling']:
        kwargs = {"userName": str(userName), "password": str(password), "tempJobScriptLocation": str(tempJobScriptLocation), "jobId": str(jobId), "hostList": hostList, "jobName": str(jobName), "isAutoscaling": True, "jobWorkDir": str(jobWorkDir)}
        values = scheduler.submitJobToScheduler(**kwargs)
        if values['status'] == "failure":
            #Need to restart job because the nodes didn't quite come up like we wanted them to due to conflicting salt keys or something
            with ccqHubVars.ccqVarLock:
                ccqHubVars.jobMappings[jobId]['status'] = "Pending"
                try:
                    for instance in ccqHubVars.jobMappings[jobId]['instancesToUse']:
                        ccqHubVars.instanceInformation[instance]['state'] = "Available"
                except Exception as e:
                    pass
                ccqHubVars.jobMappings[jobId]['isCreating'] = "none"
                ccqHubVars.jobMappings[jobId]['instancesToUse'] = []
            writeccqHubVarsToFile()
        elif values['status'] != "success":
            print values['payload']
            updateValues = ccqsubMethods.updateJobInDB({"status": "Error"}, jobId)
            with ccqHubVars.ccqVarLock:
                ccqHubVars.jobMappings[jobId]['status'] = "Error"
            writeccqHubVarsToFile()
            ccqHubMethods.writeToErrorLog({"messages": [str(values['payload'])], "traceback": [''.join(traceback.format_stack())]}, "ccq Job " + str(jobId))

    else:
        kwargs = {"userName": str(userName), "password": str(password), "tempJobScriptLocation": str(tempJobScriptLocation), "jobId": str(jobId), "hostList": None, "jobName": str(jobName), "isAutoscaling": False, "jobWorkDir": str(jobWorkDir)}
        values = scheduler.submitJobToScheduler(**kwargs)
        if values['status'] == "failure":
            #Need to restart job because the nodes didn't quite come up like we wanted them to due to conflicting salt keys or something
            with ccqHubVars.ccqVarLock:
                ccqHubVars.jobMappings[jobId]['status'] = "Pending"
                try:
                    for instance in ccqHubVars.jobMappings[jobId]['instancesToUse']:
                        ccqHubVars.instanceInformation[instance]['state'] = "Available"
                except Exception as e:
                    pass
                ccqHubVars.jobMappings[jobId]['isCreating'] = "none"
                ccqHubVars.jobMappings[jobId]['instancesToUse'] = []
            writeccqHubVarsToFile()
        elif values['status'] != "success":
            print values['payload']
            updateValues = ccqsubMethods.updateJobInDB({"status": "Error"}, jobId)
            with ccqHubVars.ccqVarLock:
                ccqHubVars.jobMappings[jobId]['status'] = "Error"
            writeccqHubVarsToFile()
            ccqHubMethods.writeToErrorLog({"messages": [str(values['payload'])], "traceback": [''.join(traceback.format_stack())]}, "ccq Job " + str(jobId))
    print "The timestamp when ending submitJob is: " + str(time.time())
    return {"status": "success", "payload": "Successfully submitted the job to the scheduler"}


#Determine whether the job needs to create resources, wait, or use existing resources. This method determines what happens to the job
def determineNextStepsForJob(jobId, scheduler, clusterName):
    print "SOMETHING"

def determineJobsToProcess(schedName, clusterName):
    print "Inside of determineJobsToProcess"
    #Check to see which job is next in line to be run
    jobSubmitTimes = {}
    jobs = {}
    results = ccqHubMethods.queryObject(None, "RecType-Job-schedulerUsed-" + str(schedName) + "-schedClusterName-" + str(clusterName), "query", "dict", "beginsWith")
    if results['status'] == "success":
        results = results['payload']
    else:
        print "Error: QueryErrorException! Unable to get Item!"
        return {'status': 'error', 'payload': "Error: QueryErrorException! Unable to get Item!"}
    for job in results:
        print "This is the job[status] " + str(job['status'])
        if job['status'] != "CCQueued" and job['status'] != "Error" and job['status'] != "Deleting" and job['status'] != "deleting" and not job['isSubmitted']:
            jobSubmitTimes[job['name']] = job['dateSubmitted']
            jobs[job['name']] = job

    sortedJobsBySubmissionTime = sorted(jobSubmitTimes.items(), key=lambda x: x[1])
    print "The jobs sorted by submission time are: " + str(sortedJobsBySubmissionTime)

    return {"status": "success", "payload": {"jobs": jobs, "sortedJobs": sortedJobsBySubmissionTime}}


def cleanupDeletedJob(jobId):
    #Need to remove the job from all the ccqVarObjects so that we don't assign any instances to it
    print "SOMEHOW MADE IT INTO CLEANUPDELETEDJOB FUNCTION!!!!\n\n\n\n\n"
    with ccqHubVars.ccqVarLock:
        action = ccqHubVars.jobMappings[jobId]['status']
        if action == "ExpandingCG":
            ccqHubVars.instanceTypesAndGroups[str(ccqHubVars.jobMappings[jobId]['instanceType'])][ccqHubVars.jobMappings[jobId]['computeGroup']]['jobsWaitingOnGroup'].remove(jobId)
            ccqHubVars.jobMappings.pop(jobId)
        elif action == "CCQueued":
            for freeInstance in ccqHubVars.jobMappings[jobId]['instancesToUse']:
                if freeInstance not in ccqHubVars.availableInstances:
                    ccqHubVars.availableInstances.append(freeInstance)
                    ccqHubVars.instanceInformation[freeInstance]['state'] = "None"
            ccqHubVars.jobMappings.pop(jobId)
    writeccqHubVarsToFile()


def monitorJobs(scheduler):

    #Get the jobs from the list of job mappings maintained by ccqLauncher
    with ccqHubVars.ccqVarLock:
        jobsToCheck = ccqHubVars.jobMappings
    writeccqHubVarsToFile()
    try:
        for job in jobsToCheck:
            #Check to ensure that the job has finished creating and if so proceed to check if the job has finished running or not and make sure it is not in the Error state or has already been marked as Completed
            print "NOW CHECKING JOB: " + str(job) + " at the start of monitor jobs\n"

            #Check to see if the job is still in the DB if it is not then the user deleted the job through the ccqdel command, we need to delete it from the jobMappings object
            temp = ccqHubMethods.queryObject(None, job, "get", "dict")
            if temp['status'] == "success":
                temp = temp['payload']
            else:
                pass

            found = False
            for DDBItem in temp:
                found = True
            #Need to free the instances belonging to the job and also remove it from any compute groups that are waiting to expand
            if not found:
                with ccqHubVars.ccqVarLock:
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
                        with ccqHubVars.ccqVarLock:
                            ccqHubVars.jobMappings.pop(job)
                        writeccqHubVarsToFile()

                    except Exception as e:
                        if job in ccqHubVars.jobsInProvisioningState:
                            ccqHubVars.jobsInProvisioningState.remove(job)
                        print "There was a problem changing the state of the instances reserved for the job and adding it to the available instance list."
                        print "There were no instances assigned to the job " + str(job) + "yet? Not fatal error!"
                        print traceback.format_exc(e)
                        #Remove the job from the in-memory object since it is gone from the DB now
                        with ccqHubVars.ccqVarLock:
                            ccqHubVars.jobMappings.pop(job)
                        writeccqHubVarsToFile()
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
                        with ccqHubVars.ccqVarLock:
                            ccqHubVars.jobMappings[job]['endTime'] = endTime
                        writeccqHubVarsToFile()

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
                            with ccqHubVars.ccqVarLock:
                                ccqHubVars.jobMappings.pop(job)
                            writeccqHubVarsToFile()
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
                                    with ccqHubVars.ccqVarLock:
                                        ccqHubVars.jobMappings.pop(job)
                                    writeccqHubVarsToFile()
                                except Exception as e:
                                    pass
                                print "The job " + str(job) + " was successfully deleted from the DB because it completed running over 1 day ago!"
    #We have added a new job to the queue so we need to retry
    except RuntimeError as e:
        pass


def monitorJobsAndInstances(scheduler, clusterName):
    #Run for the duration of the ccqLauncher and monitor the jobs and instances for things that need deleted
    while True:
        try:
            time.sleep(60)
            print "The timestamp when start monitorJobs is: " + str(time.time())
            monitorJobs(scheduler)
            print "The timestamp when end monitorJobs is: " + str(time.time())
            print "The timestamp when starting monitorInstances is: " + str(time.time())
            monitorInstances(scheduler, clusterName)
            print "The timestamp when end monitorInstances is: " + str(time.time())
        except Exception as e:
            print "There was an error encountered in monitorJobsAndInstances:\n"
            print traceback.format_exc(e)


def checkToSeeIfJobStillRunningOnCluster(job, scheduler):
    try:
        print "NOW CHECKING JOB: " + str(job['name']) + " inside of checkToSeeIfJobStillRunningOnCluster\n"
        checkJobsKwargs = {"job": job}
        results = scheduler.checkJob(**checkJobsKwargs)
        jobInDBStillRunning = results['payload']['jobInDBStillRunning']
        if not jobInDBStillRunning:
            endTime = time.time()

            #Need to update the DB entry for the job to completed and set the timestamp of when the job completed.
            values = ccqsubMethods.updateJobInDB({"status": "Completed", "endTime": str(endTime), "instancesRunningOnIds": [], "instancesRunningOnNames": []}, job['name'])
            if values['status'] != "success":
                print values['payload']
            print "Job " + str(job['name'] + " has finished running!")
            print "Updating the averages in the DB!"

            with ccqHubVars.ccqVarLock:
                ccqHubVars.jobMappings[job['name']]['status'] = "Completed"
                ccqHubVars.jobMappings[job['name']]['endTime'] = endTime
                for instance in ccqHubVars.jobMappings[job['name']]['instancesToUse']:
                    ccqHubVars.instanceInformation[instance]['state'] = "Available"
                    if instance not in ccqHubVars.availableInstances:
                        ccqHubVars.availableInstances.append(instance)
                ccqHubVars.jobMappings[job['name']]['instancesToUse'] = []
            writeccqHubVarsToFile()

            results = ccqHubMethods.queryObject(None, job['name'], "get", "dict")
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
                        ccqHubMethods.writeToErrorLog({"messages": ["There was a problem transferring the output files from the job to the requested location. Please check the " + str(ccqHubMethods.tempJobOutputLocation) + str(jobDB['userName']) + " on " + str(scheduler.schedName) + " to see if your output files have been transferred there\n"], "traceback": ["Job Output Transfer Error"]}, "ccq Job " + str(jobDB['name']))
                    tries += 1

            values = ccqsubMethods.calculateAvgRunTimeAndUpdateDB(jobDB['startTime'], str(endTime), jobDB['instanceType'], jobDB['jobName'])
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
        results = ccqHubMethods.queryObject(None, "RecType-WebDav-clusterName-" + str(job['schedClusterName']), "query", "dict", "beginsWith")
        if results['status'] == "success":
            results = results['payload']
        else:
            print "Error trying to obtain the Access Instance to transfer the files from!"
            return {"status": "error", "payload": "Error trying to obtain the Access Instance to transfer the files from."}

        for item in results:
            accessInstanceName = item["accessName"]

        if accessInstanceName != "":
            ccqsubMethods.updateJobInDB({"accessInstanceResultsStoredOn": str(accessInstanceName)}, job['name'])
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
        items = ccqHubMethods.queryObject(None, str(job['submitHostInstanceId']), "get", "dict")
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


def delegateTasks(scheduler, schedName, scalingType, vpcId, clusterName):
    print "Still need to implement!"


def main():
    # get the instance ID
    urlResponse = urllib2.urlopen('http://169.254.169.254/latest/meta-data/instance-id')
    schedulerId = urlResponse.read()

    results = ccqHubMethods.queryObject(None, schedulerId, "get", "dict")
    if results['status'] == "success":
        results = results['payload']
    else:
        print "There was an error trying to obtain the scheduler object from the database"
        sys.exit(0)

    for sched in results:
        schedName = sched['schedName']
        schedType = sched['schedType']
        schedulerInstanceId = sched['instanceID']
        schedulerHostName = sched['instanceName']
        schedulerIpAddress = sched['instanceIP']
        clusterName = sched['clusterName']
        vpcId = sched['VPC_id']
        scalingType = sched['scalingType']

        scheduler = createSchedulerObject(schedName, schedType, schedulerInstanceId, schedulerHostName, schedulerIpAddress, clusterName)
        if scheduler['status'] != "success":
            print scheduler
            #ccqHubMethods.writeToErrorLog({"messages": [scheduler['payload']['error']], "traceback": [scheduler['payload']['traceback']]}, "ccqLauncher")
        else:
            scheduler = scheduler['payload']

        count = 0
        for item in results:
            count += 1

        #Set the locks for the threads to ensure data integrity and to ensure multiple threads do not write to the same ccq variable or ccq status file at once
        ccqHubVars.ccqVarLock = threading.RLock()
        ccqHubVars.ccqFileLock = threading.RLock()
        ccqHubVars.ccqInstanceLock = threading.RLock()

        ccqCleanupThread = threading.Thread(target=monitorJobsAndInstances, args=(scheduler, clusterName))
        ccqCleanupThread.start()
        print "Successfully started the monitorJobsAndInstances thread to check and monitor the instances statuses."

        ccqDelegateTasksThread = threading.Thread(target=delegateTasks(scheduler, schedName, scalingType, vpcId, clusterName))
        ccqDelegateTasksThread.start()

        #Run forever and check to make sure the threads are running
        while True:
            time.sleep(120)
            if not ccqDelegateTasksThread.is_alive:
                print "UHOH WE DIED AT SOME POINT"
                ccqDelegateTasksThread.start()
            else:
                print "The ccqDelegateTasksThread is running."
            if not ccqCleanupThread.is_alive:
                print "UHOH WE DIED AT SOME POINT"
                ccqCleanupThread.start()
            else:
                print "The ccqCleanupThread is running."
main()

