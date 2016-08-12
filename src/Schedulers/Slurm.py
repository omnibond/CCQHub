# Copyright Omnibond Systems, LLC. All rights reserved.

# This file is part of OpenCCQ.

# OpenCCQ is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# OpenCCQ is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public License
# along with OpenCCQ.  If not, see <http://www.gnu.org/licenses/>.

import os
import sys
import time
import urllib2

import math

from Scheduler import Scheduler

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import ClusterMethods
import ccqsubMethods
import traceback
import time
import commands

clusterInformationLogFileLocation = ClusterMethods.clusterInformationLogFileLocation
logFileDirectory = ClusterMethods.logFileDirectory
scriptDirectory = ClusterMethods.scriptDirectory
cloudyClusterDefaultSSHUser = ClusterMethods.cloudyClusterDefaultSSHUser

class SlurmScheduler(Scheduler):

    def __init__(self, **kwargs):
        super(SlurmScheduler, self).__init__(**kwargs)

    def checkJobs(self, instancesToCheck, canidatesForTermination, toTerminate, instancesRunningOn, instanceRelations, job, isAutoscalingJob):
        #set the user and username to be ccq so that all jobs can be seen. ccq user is the Torque admin
        os.environ["USER"] = ClusterMethods.ccqUserName
        os.environ["USERNAME"] = ClusterMethods.ccqUserName
        os.environ["LOGNAME"] = ClusterMethods.ccqUserName

        jobInDBStillRunning = False
        status = ""
        output = ""

        if isAutoscalingJob == "true":
            status, output = commands.getstatusoutput('sudo /opt/slurm/bin/scontrol -o show job ' + str(job['schedulerJobName']))
        else:
            status, output = commands.getstatusoutput('sudo /opt/slurm/bin/scontrol -o show job ')

        #This means that isAutoscalingJob is true and the job no longer exists in the DB
        if "Invalid job id" in output:
            print "This job is no longer running and the instances need to be released back into the queue if not near the termination point and not running other jobs!"
            for x in range(len(instancesRunningOn)):
                status, output = commands.getstatusoutput("sudo /opt/slurm/bin/scontrol -o show node=" + str(instancesRunningOn[x]))
                splitText = output.split("NodeName=")
                #The first Item is a blank line so pop it off the stack
                splitText.pop(0)

                nodeState = ""
                for node in splitText:
                    node = node.replace(" \n", "")
                    nodeAttrs = node.split(" ")
                    for nodeAttr in nodeAttrs:
                       keyValue = nodeAttr.split("=")
                       if len(keyValue) > 1:
                           if keyValue[0] == "State":
                               nodeState = keyValue[1]

                if str(nodeState).lower() == "idle" and isAutoscalingJob == "true" and canidatesForTermination[instanceRelations[instancesRunningOn[x]]]['canidate']:
                    os.system('sudo /opt/slurm/bin/scontrol update NodeName=' + str(instancesRunningOn[x]) + ' State=DRAIN Reason=\"Instance marked for possible termination\"')
                    toTerminate.append(instanceRelations[instancesRunningOn[x]])

        #This means that the job is not autoscaling and there are no jobs running
        elif "No jobs" in output:
            #Job is completed or is no longer associated with the scheduler so we say the job is completed and the nodes
            #are marked free for normal use
            print "This job is no longer running and the instances need to be released back into the queue if not near the termination point!"
            for x in range(len(instancesRunningOn)):
                for item in instancesRunningOn:
                    instancesToCheck += str(item).split(".")[0] + ","
                #Strip off trailing comma
                instancesToCheck = instancesToCheck[:len(instancesToCheck)-1]
                status, output = commands.getstatusoutput("sudo /opt/slurm/bin/scontrol -o show node=" + str(instancesToCheck))
                splitText = output.split("NodeName=")
                #The first Item is a blank line so pop it off the stack
                splitText.pop(0)

                listOfNodesAndAttrs = []

                for node in splitText:
                    node = node.replace(" \n", "")
                    nodeAttrs = node.split(" ")
                    tempNodeDict = {}
                    for nodeAttr in nodeAttrs:
                       keyValue = nodeAttr.split("=")
                       if len(keyValue) > 1:
                           tempNodeDict[keyValue[0]] = keyValue[1]
                    listOfNodesAndAttrs.append(tempNodeDict)

                for node in listOfNodesAndAttrs:
                    if str(node['State']).lower() == str("idle").lower() and isAutoscalingJob == "true" and canidatesForTermination[instanceRelations[instancesRunningOn[x]]]['canidate']:
                        os.system('sudo /opt/slurm/bin/scontrol update NodeName=' + str(instancesRunningOn[x]) + ' State=DRAIN Reason=\"Instance marked for possible termination\"')
                        toTerminate.append(instanceRelations[instancesRunningOn[x]])

        else:
            splitText = output.split("\n")
            #The first Item is a blank line so pop it off the stack
            splitText.pop(0)

            listOfJobsAndAttrs = []

            for job in splitText:
                job = job.replace(" \n", "")
                jobAttrs = job.split(" ")
                tempNodeDict = {}
                for jobAttr in jobAttrs:
                   keyValue = jobAttr.split("=")
                   if len(keyValue) > 1:
                       tempNodeDict[keyValue[0]] = keyValue[1]
                listOfJobsAndAttrs.append(tempNodeDict)

            for slurmJob in listOfJobsAndAttrs:
                if slurmJob['JobState'] == "RUNNING" and job['schedulerJobName'] == slurmJob['JobName']:
                    values = ccqsubMethods.updateJobInDB({"status": "Running", "batchHost": slurmJob['BatchHost']}, job['jobId'])
                    if values['status'] != "success":
                        print "There was an error trying to save the new job state to the DB!"
                    jobInDBStillRunning = True

        return {"status" : "success", "payload": {"jobInDBStillRunning": jobInDBStillRunning, "toTerminate": toTerminate}}

    def checkNodeForNodesToPossiblyTerminate(self, instanceNamesToCheck, instanceIdsToCheck):
        #set the user and username to be ccq so that all jobs can be seen. ccq user is the Torque admin
        os.environ["USER"] = ClusterMethods.ccqUserName
        os.environ["USERNAME"] = ClusterMethods.ccqUserName
        os.environ["LOGNAME"] = ClusterMethods.ccqUserName

        toPossiblyTerminate = []
        instancesToCheck = ""
        for item in instanceNamesToCheck:
            instancesToCheck += str(item).split(".")[0] + ","
        #Strip off trailing comma
        instancesToCheck = instancesToCheck[:len(instancesToCheck)-1]

        print instancesToCheck

        #Get all instances that are currently associated with the slurm scheduler and put them in a list
        status, output = commands.getstatusoutput("sudo /opt/slurm/bin/scontrol -o show node=" + str(instancesToCheck))
        splitText = output.split("NodeName=")
        #The first Item is a blank line so pop it off the stack
        splitText.pop(0)

        listOfNodesAndAttrs = []

        instanceNum = 0
        for node in splitText:
            node = node.replace(" \n", "")
            nodeAttrs = node.split(" ")
            tempNodeDict = {}
            for nodeAttr in nodeAttrs:
               keyValue = nodeAttr.split("=")
               if len(keyValue) > 1:
                   tempNodeDict[keyValue[0]] = keyValue[1]
            listOfNodesAndAttrs.append(tempNodeDict)

        for instance in listOfNodesAndAttrs:
            if str(instance['State']).lower() == "idle":
                print "This instance is no longer running any jobs and needs to be released back into the queue if not near the termination point!"
                #If the instance has been tagged as an instance for termination and is free then we kill it
                print "instanceIdsToCheck: " + str(instanceIdsToCheck)
                toPossiblyTerminate.append(instanceIdsToCheck[instanceNum])
            instanceNum += 1
        print toPossiblyTerminate
        return {"status": "success", "payload": toPossiblyTerminate}

    def putComputeNodesToRunJobOnInCorrectFormat(self, hostList, cpus, memory):
        formattedHostList = ""
        for host in hostList:
            formattedHostList = formattedHostList + str(host).split(".")[0] + ","

        #Strip off trailing comma
        formattedHostList = formattedHostList[:len(formattedHostList)-1]

        return {"status": "success", "payload": formattedHostList}

    def submitJobToScheduler(self, userName, jobName, jobId, hostList, tempJobScriptLocation, hostArray, hostIdArray, isAutoscaling, jobWorkDir):
        if isAutoscaling:
            os.environ["USER"] = ClusterMethods.ccqUserName
            os.environ["USERNAME"] = ClusterMethods.ccqUserName
            os.environ["LOGNAME"] = ClusterMethods.ccqUserName

            print "sudo /opt/slurm/bin/sbatch --uid " + str(userName) + " -D " + str(jobWorkDir) + " -w " + str(hostList) + " -o " + str(ClusterMethods.tempJobOutputLocation) + str(userName) + "/" + str(jobName) + str(jobId) + ".o -e " + str(ClusterMethods.tempJobOutputLocation) + str(userName) + "/" + str(jobName) + str(jobId) + ".e " + str(tempJobScriptLocation) + "/" + str(jobName) + str(jobId)
            status, output = commands.getstatusoutput("sudo /opt/slurm/bin/sbatch --uid " + str(userName) + " -D " + str(jobWorkDir) + " -w " + str(hostList) + " -o " + str(ClusterMethods.tempJobOutputLocation) + str(userName) + "/" + str(jobName) + str(jobId) + ".o -e " + str(ClusterMethods.tempJobOutputLocation) + str(userName) + "/" + str(jobName) + str(jobId) + ".e " + str(tempJobScriptLocation) + "/" + str(jobName) + str(jobId))
            jobName = output.split(" ")[3]
            if status == 0:
                pending = True
                timesRan = 0
                while pending:
                    jobStuffStatus, jobStuff = commands.getstatusoutput('sudo /opt/slurm/bin/scontrol -o show job ' + str(jobName))
                    print "jobStuff is: " + str(jobStuff)
                    print "jobName is: " + str(jobName)

                    if "Invalid job id" not in jobStuff and "No jobs" not in jobStuff:
                        splitText = jobStuff.split("\n")

                        listOfJobsAndAttrs = {}

                        for job in splitText:
                            print "Job is: " + str(job)
                            job = job.replace(" \n", "")
                            jobAttrs = job.split(" ")
                            tempNodeDict = {}
                            for jobAttr in jobAttrs:
                               keyValue = jobAttr.split("=")
                               print "The keyValue is: " + str(keyValue)
                               if len(keyValue) > 1:
                                   tempNodeDict[keyValue[0]] = keyValue[1]
                            listOfJobsAndAttrs = tempNodeDict

                        print "ListOfJobsAndAttrs: " + str(listOfJobsAndAttrs)

                        if str(listOfJobsAndAttrs['JobState']).lower() != "pending":
                            try:
                                batchHost = listOfJobsAndAttrs['BatchHost']
                                pending = False
                            except Exception as e:
                                time.sleep(5)
                                if timesRan > 5:
                                    print "Unable to get the BatchHost for the job! The job output files may not transfer correctly."
                                    pending = False
                                timesRan += 1
                    else:
                        batchHost = None
                        if timesRan > 5:
                            print "Unable to get the BatchHost for the job! The job output files may not transfer correctly."
                            pending = False

                #save out job name to the Job DB entry for use by the InstanceInUseChecks
                #Need to add the instances that the job is running on to the DB entry
                values = ccqsubMethods.updateJobInDB({"status": "Submitted", "instancesRunningOnNames": hostArray, "instancesRunningOnIds": hostIdArray, "schedulerJobName": jobName, "batchHost": str(batchHost)}, jobId)
                if values['status'] == "deleting":
                    status, output = commands.getstatusoutput("sudo /opt/slurm/bin/scancel " + str(jobName))
                    print output
                    return {"status": "error", "payload": str(output)}
                elif values['status'] != 'success':
                    print "There was an error trying to save the instances that the job needs to run on in the DB!\n" + values['payload']
                    return {"status": "error", "payload": "There was an error trying to update the job status in the DB!\n" + values['payload']}
            else:
                ccqsubMethods.updateJobInDB({"status": "Error"}, jobId)
                print "There was an error trying to submit the job to the scheduler!\n" + output
                return {"status": "error", "payload": "There was an error trying to submit the job to the scheduler!\n" + output}

            return {'status': "success", "payload": "The job was submitted successfully!"}
        else:
            os.environ["USER"] = ClusterMethods.ccqUserName
            os.environ["USERNAME"] = ClusterMethods.ccqUserName
            os.environ["LOGNAME"] = ClusterMethods.ccqUserName

            status, output = commands.getstatusoutput("sudo /opt/slurm/bin/sbatch --uid " + str(userName) + " -D " + str(jobWorkDir) + " -o " + str(ClusterMethods.tempJobOutputLocation) + str(userName) + "/" + str(jobName) + str(jobId) + ".o -e " + str(ClusterMethods.tempJobOutputLocation) + str(userName) + "/" + str(jobName) + str(jobId) + ".e " + str(tempJobScriptLocation) + "/" + str(jobName) + str(jobId))
            jobName = output.split(" ")[3]
            if status == 0:

                jobStuffStatus, jobStuff = commands.getstatusoutput('sudo /opt/slurm/bin/scontrol -o show job' + str(jobName))

                if "Invalid job id" not in jobStuff and "No jobs" not in jobStuff:
                    splitText = jobStuff.split("\n")
                    #The first Item is a blank line so pop it off the stack
                    splitText.pop(0)

                    listOfJobsAndAttrs = {}

                    for job in splitText:
                        job = job.replace(" \n", "")
                        jobAttrs = job.split(" ")
                        tempNodeDict = {}
                        for jobAttr in jobAttrs:
                           keyValue = jobAttr.split("=")
                           if len(keyValue) > 1:
                               tempNodeDict[keyValue[0]] = keyValue[1]
                        listOfJobsAndAttrs = tempNodeDict

                    batchHost = listOfJobsAndAttrs['BatchHost']
                else:
                    batchHost = None

                #save out job name to the Job DB entry for use by the InstanceInUseChecks
                #Need to add the instances that the job is running on to the DB entry
                values = ccqsubMethods.updateJobInDB({"status": "Submitted", "schedulerJobName": jobName, "batchHost": batchHost}, jobId)
                if values['status'] == "deleting":
                    status, output = commands.getstatusoutput("sudo /opt/slurm/bin/scancel " + str(jobName))
                    print output
                    return {"status": "error", "payload": str(output)}
                elif values['status'] != 'success':
                    print "There was an error trying to save the instances that the job needs to run on in the DB!\n" + values['payload']
                    return {"status": "error", "payload": "There was an error trying to update the job status in the DB!\n" + values['payload']}
            else:
                ccqsubMethods.updateJobInDB({"status": "Error"}, jobId)
                print "There was an error trying to submit the job to the scheduler!\n" + output
                return {"status": "error", "payload": "There was an error trying to submit the job to the scheduler!\n" + output}

            return {'status': "success", "payload": "The job was submitted successfully!"}

    def takeComputeNodeOffline(self, computeNodeInstanceId):
        #set the user and username to be ccq so that all jobs can be seen. ccq user is the Torque admin
        os.environ["USER"] = ClusterMethods.ccqUserName
        os.environ["USERNAME"] = ClusterMethods.ccqUserName
        os.environ["LOGNAME"] = ClusterMethods.ccqUserName

        #Get short hostname since the long one is passed in
        computeNodeInstanceId = str(computeNodeInstanceId).split(".")[0]

        os.system('sudo /opt/slurm/bin/scontrol update NodeName=' + str(computeNodeInstanceId) + ' State=DRAIN Reason=\"Instance marked for possible termination\"')
        return {"status" : "success", "payload": "Disabled the compute node successfully so no new jobs will be assigned to it!"}

    def setComputeNodeOnline(self, computeNodeInstanceId):
        #set the user and username to be ccq so that all jobs can be seen. ccq user is the Torque admin
        os.environ["USER"] = ClusterMethods.ccqUserName
        os.environ["USERNAME"] = ClusterMethods.ccqUserName
        os.environ["LOGNAME"] = ClusterMethods.ccqUserName

        #Get short hostname since the long one is passed in
        computeNodeInstanceId = str(computeNodeInstanceId).split(".")[0]

        os.system('sudo /opt/slurm/bin/scontrol update NodeName=' + str(computeNodeInstanceId) + ' State=RESUME')
        return {"status" : "success", "payload": "Enabled the compute node successfully so new jobs will be assigned to it!"}

    def checkIfInstancesAlreadyAvailableInScheduler(self, requestedInstanceType, numberOfInstancesRequested):
        urlResponse = urllib2.urlopen('http://169.254.169.254/latest/meta-data/placement/availability-zone')
        availabilityZone = urlResponse.read()
        values = ClusterMethods.getRegion(availabilityZone, "")

        if values['status'] != 'success':
            return {"status": "error", "payload": "There was an error trying to determine which region the Scheduler you are using is in. Please try again! " + str(values['payload'])}
        else:
            region = values['payload']
        instancesInRegion =[]
        values = ClusterMethods.getAllInformationAboutInstancesInRegion(region)
        if values['status'] != 'success':
           return {"status": "error", "payload": "There was a problem getting the Instance Type information from AWS!" + str(values['payload'])}
        else:
            instancesInRegion = values['payload']

            try:
                instanceInfo = instancesInRegion[requestedInstanceType]
                memoryInfo = instanceInfo['Memory'].replace("," , "")
                memInfo = memoryInfo.split(" ")
                requestedMemory = 0
                if str(memInfo[1]).lower()== "gib":
                    requestedMemory = float(memInfo[0])*float(1000)
                elif str(memInfo[1]).lower()== "tb":
                    requestedMemory = float(memInfo[0])*float(10000)
                requestedCores = float(instanceInfo['Cores'])
            except Exception as e:
                return {"status": "error", "payload": "There was a problem getting the Instance Type information from AWS! The requested Instance Type: " + str(requestedInstanceType) + " is not currently supported!"}

        instanceNamesFree = []

        os.environ["USER"] = ClusterMethods.ccqUserName
        os.environ["USERNAME"] = ClusterMethods.ccqUserName
        os.environ["LOGNAME"] = ClusterMethods.ccqUserName

        #Get all instances that are currently associated with the slurm scheduler and put them in a list
        status, output = commands.getstatusoutput("sudo /opt/slurm/bin/scontrol -o show node")
        splitText = output.split("NodeName=")
        #The first Item is a blank line so pop it off the stack
        splitText.pop(0)

        listOfNodesAndAttrs = []

        for node in splitText:
            node = node.replace(" \n", "")
            nodeAttrs = node.split(" ")
            tempNodeDict = {}
            for nodeAttr in nodeAttrs:
               keyValue = nodeAttr.split("=")
               if len(keyValue) > 1:
                   tempNodeDict[keyValue[0]] = keyValue[1]
            listOfNodesAndAttrs.append(tempNodeDict)

        instanceIdList = {}
        instancesComputeGroupList = {}

        #The length of the listOfNodesAndAttrs is the number of nodes that are currently in the cluster. If this number
        #is less than the number of requested nodes then we have to create more and cannot use what we already have
        if len(listOfNodesAndAttrs) < int(numberOfInstancesRequested):
            return {"status": "success", "payload": {"instancesFound": False, "instances": None}}
        else:
            results = ClusterMethods.queryObject(None, "RecType-HPCNode-clusterName-" + str(self.clusterName) + "-name-", "query", "dict", "beginsWith")
            if results['status'] == "success":
                results = results['payload']
            else:
                return {"status": "success", "payload": {"instancesFound": False, "instances": None}}
            for DDBitem in results:
                instanceIdList[DDBitem['instanceName'].split(".")[0]] = DDBitem['instanceID']
                instancesComputeGroupList[DDBitem['instanceName'].split(".")[0]] = DDBitem['groupName']

        listOfComputeGroupInstanceTypes = {}
        results = ClusterMethods.queryObject(None, "RecType-ComputeGroupConfig-clusterName-", "query", "dict", "beginsWith")
        if results['status'] == "success":
            results = results['payload']
        else:
            print "Unable to obtain information on Compute Groups, no instances could be found!"
            return {"status": "success", "payload": {"instancesFound": False, "instances": None}}
        for DDBitem in results:
            listOfComputeGroupInstanceTypes[DDBitem['groupName']] = DDBitem['instanceType']

        instancesFree = 0
        newInstanceNamesAndIds = {}


        status, output = commands.getstatusoutput("sudo /opt/slurm/bin/scontrol -o show node")
        splitText = output.split("NodeName=")
        #The first Item is a blank line so pop it off the stack
        splitText.pop(0)

        listOfNodesAndAttrs = []

        for node in splitText:
            node = node.replace(" \n", "")
            nodeAttrs = node.split(" ")
            tempNodeDict = {}
            for nodeAttr in nodeAttrs:
               keyValue = nodeAttr.split("=")
               if len(keyValue) > 1:
                   tempNodeDict[keyValue[0]] = keyValue[1]
            listOfNodesAndAttrs.append(tempNodeDict)

        for node in listOfNodesAndAttrs:
            #Get state of the instance (idle or allocated or down or offline or drained)
            stateOfInstance = node['State']

            #Get number of CPUs and amount of Memory of the instance type (instance reporting is too good for what we
            #need it shows the actual memory on the machine (e.g 991MB instead of 1000MB for t2.micro) so we must get
            #this information from the instanceTypes file to ensure that it is accurate.

            #gets the instance's compute group name from the instancesComputeGroupList object, then uses the compute group
            #name to get the instance type for that compute group and then gets the instance type information using the
            #instance type of the compute group
            cores = float(instancesInRegion[listOfComputeGroupInstanceTypes[instancesComputeGroupList[node['NodeHostName']]]]["Cores"])
            memoryInfo = instancesInRegion[listOfComputeGroupInstanceTypes[instancesComputeGroupList[node['NodeHostName']]]]["Memory"].split(" ")
            memory = 0
            if str(memoryInfo[1]).lower()== "gib":
                memory = float(memoryInfo[0])*float(1000)
            elif str(memoryInfo[1]).lower()== "tb":
                memory = float(memoryInfo[0])*float(10000)

            #cores = float(node['CPUTot'])
            #memory = float(node['RealMemory'])

            if str(stateOfInstance).lower() == "idle" and float(cores) >= float(requestedCores) and float(memory) >= float(math.ceil(requestedMemory)):
                instancesFree += 1
                if int(len(instanceNamesFree)) != int(numberOfInstancesRequested):
                    instanceNamesFree.append(node['NodeHostName'])
                    newInstanceNamesAndIds[instanceIdList[node['NodeHostName']]] = node['NodeHostName']

        if int(len(instanceNamesFree)) != int(numberOfInstancesRequested):
            return {"status": "success", "payload": {"instancesFound": False, "newInstanceNamesAndIds": None}}
        else:
            return {"status": "success", "payload": {"instancesFound": True, "newInstanceNamesAndIds": newInstanceNamesAndIds}}

    def getJobStatusFromScheduler(self, jobId, jobNameInScheduler, userName):
        os.environ["USER"] = ClusterMethods.ccqUserName
        os.environ["USERNAME"] = ClusterMethods.ccqUserName
        os.environ["LOGNAME"] = ClusterMethods.ccqUserName
        if jobId == "all":
            status, output = commands.getstatusoutput('sudo /opt/slurm/bin/squeue -u ' + str(userName))
        else:
            status, output = commands.getstatusoutput('sudo /opt/slurm/bin/squeue --jobs ' + str(jobNameInScheduler))

        if output == "" and jobId == "all":
            output = "There are currently no jobs in the queue!"

        return {"status": "success", "payload": output}

    def deleteJobFromScheduler(self, jobForceDelete, jobNameInScheduler):
        if jobForceDelete:
            os.environ["USER"] = ClusterMethods.ccqUserName
            os.environ["USERNAME"] = ClusterMethods.ccqUserName
            os.environ["LOGNAME"] = ClusterMethods.ccqUserName
            status, output = commands.getstatusoutput("sudo /opt/slurm/bin/scancel -f " + str(jobNameInScheduler))
            print output
        else:
            os.environ["USER"] = ClusterMethods.ccqUserName
            os.environ["USERNAME"] = ClusterMethods.ccqUserName
            os.environ["LOGNAME"] = ClusterMethods.ccqUserName
            status, output = commands.getstatusoutput("sudo /opt/slurm/bin/scancel " + str(jobNameInScheduler))
            print output
        return {"status": "success", "payload": "Successfully issued the job deletion command! Job should be terminating, if not please try the force or purge option!"}
