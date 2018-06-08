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
import os
import commands
import traceback
import logging

from Scheduler import Scheduler
import src.ccqHubMethods as ccqHubMethods

#logFileDirectory = "/var/logs/CCQHub"

#logging.basicConfig(filename=str(logFileDirectory) + 'torque.log', level=logging.DEBUG)


class TorqueScheduler(Scheduler):

    def __init__(self, **kwargs):
        super(TorqueScheduler, self).__init__(**kwargs)

        #Set the environment variable for the Torque commands to use, it is undefined otherwise and it won't work
        os.environ["PBS_SERVER"] = str(self.instanceName)

    def checkJobs(self, instancesToCheck, canidatesForTermination, toTerminate, instancesRunningOn, instanceRelations, job, isAutoscalingJob):
        #TODO This will have to change for local submission stuff
        #set the user and username to be ccq so that all jobs can be seen. ccq user is the Torque admin
        # os.environ["USER"] = ccqHubMethods.ccqUserName
        # os.environ["USERNAME"] = ccqHubMethods.ccqUserName
        # os.environ["LOGNAME"] = ccqHubMethods.ccqUserName

        jobInDBStillRunning = False
        if isAutoscalingJob == "true":
                status, instanceStates = commands.getstatusoutput('/opt/torque/bin/pbsnodes' + str(instancesToCheck) + ' | grep \" state = \"')
        else:
            status, instanceStates = commands.getstatusoutput('/opt/torque/bin/pbsnodes | grep \" state = \"')

        splitOutput = instanceStates.split('\n')

        instanceNum = 0
        for instance in splitOutput:
            state = instance.replace(" ", "")

            state = state.split('=')

            if state[0] != "power_state":
                if state[1] == "free":
                    print "This instance is no longer running any jobs and needs to be released back into the queue if not near the termination point!"
                    #If the instance has been tagged as an instance for termination and is free then we kill it
                    print canidatesForTermination[instanceRelations[instancesRunningOn[instanceNum]]]['canidate']
                    if isAutoscalingJob == "true" and canidatesForTermination[instanceRelations[instancesRunningOn[instanceNum]]]['canidate']:
                        os.system('/opt/torque/bin/pbsnodes -o ' + str(instancesRunningOn[instanceNum]))
                        toTerminate.append(instanceRelations[instancesRunningOn[instanceNum]])

                if state[1] == "job-exclusive":
                    if len(instancesRunningOn) > 0:
                        status, output = commands.getstatusoutput('/opt/torque/bin/pbsnodes ' + str(instancesRunningOn[instanceNum]) + ' | grep \"jobs = \"')
                    else:
                        status, output = commands.getstatusoutput('/opt/torque/bin/pbsnodes | grep \"jobs = \"')

                    splitOutput = output.split('\n')
                    for jobInstance in splitOutput:
                        state = jobInstance.replace(" ", "")
                        state = state.split('/')
                        state = state[1]
                        if state == job['schedulerJobName']:
                            values = ccqHubMethods.updateJobInDB({"status": "Running"}, job['jobId'])
                            if values['status'] != "success":
                                print "There was an error trying to save the new job state to the DB!"
                            print "Instance: " + str(instancesRunningOn[instanceNum]) + " is still running the job in the DB named: " + str(job['jobName'])
                            jobInDBStillRunning = True
                        else:
                            if len(instancesRunningOn) > 0:
                                print "Instance: " +str(instancesRunningOn[instanceNum]) + " is no longer running the job in the DB. But is running another job now named: " + str(state[1])

                    instanceNum += 1
        return {"status" : "success", "payload": {"jobInDBStillRunning": jobInDBStillRunning, "toTerminate": toTerminate}}

    def checkNodeForNodesToPossiblyTerminate(self, instanceNamesToCheck, instanceIdsToCheck):
        #TODO This will have to change for local submission stuff
        #set the user and username to be ccq so that all jobs can be seen. ccq user is the Torque admin
        # os.environ["USER"] = ccqHubMethods.ccqUserName
        # os.environ["USERNAME"] = ccqHubMethods.ccqUserName
        # os.environ["LOGNAME"] = ccqHubMethods.ccqUserName

        toPossiblyTerminate = []
        instancesToCheck = ""
        for item in instanceNamesToCheck:
            instancesToCheck += " " + str(item)

        print instancesToCheck

        status, instanceStates = commands.getstatusoutput('/opt/torque/bin/pbsnodes' + str(instancesToCheck) + ' | grep \" state = \"')
        splitOutput = instanceStates.split('\n')

        instanceNum = 0
        print splitOutput
        print instanceNamesToCheck
        for instance in splitOutput:
            state = instance.replace(" ", "")
            print state
            state = state.split('=')

            if state[0] != "power_state":
                if state[1] == "free":
                    print "This instance is no longer running any jobs and needs to be released back into the queue if not near the termination point!"
                    #If the instance has been tagged as an instance for termination and is free then we kill it
                    print "instanceIdsToCheck: " + str(instanceIdsToCheck)
                    toPossiblyTerminate.append(instanceIdsToCheck[instanceNum])

            instanceNum += 1
        return {"status": "success", "payload": toPossiblyTerminate}

    def removeComputeNodesFromScheduler(self, computeNodes):
        torqueNodeList = []

        with open('/var/spool/torque/server_priv/nodes', 'r') as torqueNodeFile:
            readConfFile = torqueNodeFile.read()
            for line in readConfFile.splitlines():
                torqueNodeList.append(line.strip())

        for torqueNode in torqueNodeList:
            tempDeleteNodeFlag = None
            os.system('echo \"here is the current torqueNode: ' + str(torqueNode) + '\" >> /opt/CloudyCluster/logs/apache_logs/newNodesLog')
            for nodeInDB in computeNodes:
                os.system('echo \"here is the current nodeInDB: ' + nodeInDB['instanceName'] + '\" >> /opt/CloudyCluster/logs/apache_logs/newNodesLog')
                if str(torqueNode) in str(nodeInDB['instanceName']):
                    tempDeleteNodeFlag = False
            os.system('echo \"here is tempDeleteNodeFlag: ' + str(tempDeleteNodeFlag) + '\" >> /opt/CloudyCluster/logs/apache_logs/newNodesLog')
            if tempDeleteNodeFlag is None:
                for nodeInDB in computeNodes:
                    if str(torqueNode) in str(nodeInDB['instanceName']):
                        # we couldn't find this torqueNode in the database nodes so delete the node from the scheduler
                        os.system('qmgr -c \"delete node ' + str(torqueNode) + "\"")
                        #TODO clean up hosts, hosts.equiv, and ssh_known_hosts files as well
                        computeNodes.remove(nodeInDB)
        return {"status" : "success", "payload": computeNodes}

    def takeComputeNodeOffline(self, computeNodeInstanceId):
        #set the user and username to be ccq so that all jobs can be seen. ccq user is the Torque admin
        #TODO This will have to change for local submission stuff
        #set the user and username to be ccq so that all jobs can be seen. ccq user is the Torque admin
        # os.environ["USER"] = ccqHubMethods.ccqUserName
        # os.environ["USERNAME"] = ccqHubMethods.ccqUserName
        # os.environ["LOGNAME"] = ccqHubMethods.ccqUserName

        os.system('/opt/torque/bin/pbsnodes -o ' + str(computeNodeInstanceId))
        return {"status" : "success", "payload": "Disabled the compute node successfully so no new jobs will be assigned to it!"}

    def setComputeNodeOnline(self, computeNodeInstanceId):
        #TODO This will have to change for local submission stuff
        #set the user and username to be ccq so that all jobs can be seen. ccq user is the Torque admin
        # os.environ["USER"] = ccqHubMethods.ccqUserName
        # os.environ["USERNAME"] = ccqHubMethods.ccqUserName
        # os.environ["LOGNAME"] = ccqHubMethods.ccqUserName

        os.system('/opt/torque/bin/pbsnodes -c ' + str(computeNodeInstanceId))
        return {"status" : "success", "payload": "Enabled the compute node successfully so new jobs will be assigned to it!"}

    def submitJobToScheduler(self, userName, jobName, jobId, hostList, tempJobScriptLocation, hostArray, hostIdArray, isAutoscaling, jobWorkDir):
        #TODO This will have to change for local submission stuff
        #set the user and username to be ccq so that all jobs can be seen. ccq user is the Torque admin
        # os.environ["USER"] = ccqHubMethods.ccqUserName
        # os.environ["USERNAME"] = ccqHubMethods.ccqUserName
        # os.environ["LOGNAME"] = ccqHubMethods.ccqUserName
        os.system("sudo /opt/torque/sbin/trqauthd")

        if isAutoscaling:
            print "sudo /opt/torque/bin/qsub -P " + str(userName) + " -o " + str(ccqHubMethods.tempJobOutputLocation) + str(userName) + "/" + str(jobName) + str(jobId) + ".o -e " + str(ccqHubMethods.tempJobOutputLocation) + str(userName) + "/" + str(jobName) + str(jobId) + ".e -l nodes=" + str(hostList) + " " + str(tempJobScriptLocation) + "/" + str(jobName) + str(jobId)
            status, output = commands.getstatusoutput("sudo /opt/torque/bin/qsub -P " + str(userName) + " -o " + str(ccqHubMethods.tempJobOutputLocation) + str(userName) + "/" + str(jobName) + str(jobId) + ".o -e " + str(ccqHubMethods.tempJobOutputLocation) + str(userName) + "/" + str(jobName) + str(jobId) + ".e -l nodes=" + str(hostList) + " " + str(tempJobScriptLocation) + "/" + str(jobName) + str(jobId))
            jobName = output
            if status == 0:
                #save out job name to the Job DB entry for use by the InstanceInUseChecks
                #Need to add the instances that the job is running on to the DB entry
                values = ccqHubMethods.updateJobInDB({"status": "Submitted", "instancesRunningOnNames": hostArray, "instancesRunningOnIds": hostIdArray, "schedulerJobName": jobName}, jobId)
                if values['status'] == "deleting":
                    status, output = commands.getstatusoutput("/opt/torque/bin/qdel " + str(jobName))
                    print output
                    return {"status": "error", "payload": str(output)}
                elif values['status'] != 'success':
                    print "There was an error trying to save the instances that the job needs to run on in the DB!\n" + values['payload']
                    return {"status": "error", "payload": "There was an error trying to update the job status in the DB!\n" + values['payload']}

                return {"status" : "success", "payload": "The job has been successfully submitted to the scheduler!"}
            else:
                ccqHubMethods.updateJobInDB({"status": "Error"}, jobId)
                print "There was an error trying to submit the job to the scheduler!\n" + output
                return {"status": "error", "payload": "There was an error trying to submit the job to the scheduler!\n" + output}
        else:
            print "/opt/torque/bin/qsub -P " + str(userName) + " -o " + str(ccqHubMethods.tempJobOutputLocation) + str(userName) + "/" + str(jobName) + str(jobId) + ".o -e " + str(ccqHubMethods.tempJobOutputLocation) + str(userName) + "/" + str(jobName) + str(jobId) + ".e " + str(tempJobScriptLocation) + "/" + str(jobName) + str(jobId)
            status, output = commands.getstatusoutput("sudo /opt/torque/bin/qsub -P " + str(userName) + " -o " + str(ccqHubMethods.tempJobOutputLocation) + str(userName) + "/" + str(jobName) + str(jobId) + ".o -e " + str(ccqHubMethods.tempJobOutputLocation) + str(userName) + "/" + str(jobName) + str(jobId) + ".e " + str(tempJobScriptLocation) + "/" + str(jobName) + str(jobId))
            jobName = output
            if status == 0:
                #save out job name to the Job DB entry for use by the InstanceInUseChecks
                #Need to add the instances that the job is running on to the DB entry
                values = ccqHubMethods.updateJobInDB({"status": "Submitted", "schedulerJobName": jobName}, jobId)
                if values['status'] == "deleting":
                    status, output = commands.getstatusoutput("/opt/torque/bin/qdel " + str(jobName))
                    print output
                    return {"status": "error", "payload": str(output)}
                elif values['status'] != 'success':
                    print "There was an error trying to save the instances that the job needs to run on in the DB!\n" + values['payload']
                    return {"status": "error", "payload": "There was an error trying to update the job status in the DB!\n" + values['payload']}

                return {"status" : "success", "payload": "The job has been successfully submitted to the scheduler!"}
            else:
                ccqHubMethods.updateJobInDB({"status": "Error"}, jobId)
                print "There was an error trying to submit the job to the scheduler!\n" + output
                return {"status": "error", "payload": "There was an error trying to submit the job to the scheduler!\n" + output}

    def putComputeNodesToRunJobOnInCorrectFormat(self, hostList, cpus, memory):
        formattedHostList = ""
        for host in hostList:
            formattedHostList = formattedHostList + str(host) + ",mem=" + str(memory) + "mb:ppn=" + str(cpus) + "+"

        #Strip off trailing plus
        formattedHostList = formattedHostList[:len(formattedHostList)-1]

        return {"status": "success", "payload": formattedHostList}

    def checkIfInstancesAlreadyAvailableInScheduler(self, requestedInstanceType, numberOfInstancesRequested):
        print "TODO Implement a different thing here"
        # urlResponse = urllib2.urlopen('http://169.254.169.254/latest/meta-data/placement/availability-zone')
        # availabilityZone = urlResponse.read()
        # values = ccqHubMethods.getRegion(availabilityZone, "")
        #
        # if values['status'] != 'success':
        #     return {"status": "error", "payload": "There was an error trying to determine which region the Scheduler you are using is in. Please try again! " + str(values['payload'])}
        # else:
        #     region = values['payload']
        # values = ccqHubMethods.getAllInformationAboutInstancesInRegion(region)
        # if values['status'] != 'success':
        #    return {"status": "error", "payload": "There was a problem getting the Instance Type information from AWS!" + str(values['payload'])}
        # else:
        #     instancesInRegion = values['payload']
        #
        #     try:
        #         instanceInfo = instancesInRegion[requestedInstanceType]
        #         requestedMemory = float(instanceInfo['Memory'])
        #         requestedCores = float(instanceInfo['Cores'])
        #     except Exception as e:
        #         return {"status": "error", "payload": "There was a problem getting the Instance Type information from AWS! The requested Instance Type: " + str(requestedInstanceType) + " is not currently supported!"}
        #
        # instanceNamesFree = []
        #
        # #TODO This will have to change for local submission stuff
        # #set the user and username to be ccq so that all jobs can be seen. ccq user is the Torque admin
        # # os.environ["USER"] = ccqHubMethods.ccqUserName
        # # os.environ["USERNAME"] = ccqHubMethods.ccqUserName
        # # os.environ["LOGNAME"] = ccqHubMethods.ccqUserName
        # os.system("sudo /opt/torque/sbin/trqauthd")
        # status, instanceStates = commands.getstatusoutput('/opt/torque/bin/pbsnodes')
        # splitOutput = instanceStates.split('\n')
        # instanceIdList = {}
        #
        # if len(splitOutput)/9 < int(numberOfInstancesRequested):
        #     return {"status": "success", "payload": {"instancesFound": False, "instances": None}}
        # else:
        #     results = ccqHubMethods.queryObj(None, "RecType-HPCNode-clusterName-" + str(self.clusterName) + "-name-", "query", "dict", "beginsWith")
        #     if results['status'] == "success":
        #         results = results['payload']
        #     else:
        #         return {"status": "success", "payload": {"instancesFound": False, "instances": None}}
        #     for DDBitem in results:
        #         instanceIdList[DDBitem['instanceName']] = DDBitem['instanceID']
        #
        # instancesFree = 0
        # instanceName = ""
        # instanceNumberOn = 0
        # newInstanceNamesAndIds = {}
        #
        # status, instanceStates = commands.getstatusoutput('/opt/torque/bin/pbsnodes')
        # splitOutput = instanceStates.split('\n')
        #
        # cores = 0
        # memory = 0
        # while instanceNumberOn < len(splitOutput)/9:
        #     #Get state of the instance (free or job exclusive or down or offline)
        #     stateLine = splitOutput[(instanceNumberOn*9)+1]
        #     if "state = " not in str(splitOutput[(instanceNumberOn*9)+1]):
        #         stateLine = splitOutput[(instanceNumberOn*9)+2]
        #         stateLine = stateLine.replace(" ", "")
        #     stateSplit = stateLine.split("=")
        #     stateOfInstance = stateSplit[1].strip()
        #
        #     #Get number of CPUs and amount of Memory of the instance
        #     instanceInfoLine = splitOutput[(instanceNumberOn*9)+5]
        #     instanceInfoLineSplit = instanceInfoLine.split(",")
        #     if "jobs = " in str(instanceInfoLineSplit[0]):
        #         instanceInfoLine = splitOutput[(instanceNumberOn*9)+6]
        #         instanceInfoLineSplit = instanceInfoLine.split(",")
        #     for thing in range(len(instanceInfoLineSplit)):
        #         if "ncpus=" in instanceInfoLineSplit[thing]:
        #             coresLine = instanceInfoLineSplit[thing]
        #             coresSplit = coresLine.split("=")
        #             cores = float(coresSplit[1])
        #
        #     for thing in range(len(instanceInfoLineSplit)):
        #         if "physmem=" in instanceInfoLineSplit[thing]:
        #             memoryLine = instanceInfoLineSplit[thing]
        #             memorySplit = memoryLine.split("=")
        #             memory = memorySplit[1]
        #             memory = memory[:len(memory)-2]
        #             memory = float(memory)/1000000
        #
        #     if str(stateOfInstance) == "free" and float(cores) >= float(requestedCores) and float(memory) >= float(math.ceil(requestedMemory)):
        #         instancesFree += 1
        #         if int(len(instanceNamesFree)) != int(numberOfInstancesRequested):
        #             instanceNamesFree.append(splitOutput[(instanceNumberOn*9)])
        #             newInstanceNamesAndIds[instanceIdList[splitOutput[(instanceNumberOn*9)]]] = splitOutput[(instanceNumberOn*9)]
        #     instanceNumberOn += 1
        #
        # if int(len(instanceNamesFree)) != int(numberOfInstancesRequested):
        #     return {"status": "success", "payload": {"instancesFound": False, "newInstanceNamesAndIds": None}}
        # else:
        #     return {"status": "success", "payload": {"instancesFound": True, "newInstanceNamesAndIds": newInstanceNamesAndIds}}

    def getJobStatusFromScheduler(self, jobId, jobNameInScheduler, userName):
        #TODO This will have to change for local submission stuff
        #set the user and username to be ccq so that all jobs can be seen. ccq user is the Torque admin
        # os.environ["USER"] = ccqHubMethods.ccqUserName
        # os.environ["USERNAME"] = ccqHubMethods.ccqUserName
        # os.environ["LOGNAME"] = ccqHubMethods.ccqUserName
        os.system("sudo /opt/torque/sbin/trqauthd")
        if jobId == "all":
            status, output = commands.getstatusoutput('/opt/torque/bin/qstat -u ' + str(userName))
        else:
            status, output = commands.getstatusoutput('/opt/torque/bin/qstat ' + str(jobNameInScheduler))

        if output == "" and jobId == "all":
            output = "There are currently no jobs in the queue!"

        return {"status": "success", "payload": output}

    def deleteJobFromScheduler(self, jobForceDelete, jobNameInScheduler):
        if jobForceDelete:
            #TODO This will have to change for local submission stuff
            #set the user and username to be ccq so that all jobs can be seen. ccq user is the Torque admin
            # os.environ["USER"] = ccqHubMethods.ccqUserName
            # os.environ["USERNAME"] = ccqHubMethods.ccqUserName
            # os.environ["LOGNAME"] = ccqHubMethods.ccqUserName
            os.system("sudo /opt/torque/sbin/trqauthd")
            status, output = commands.getstatusoutput("/opt/torque/bin/qdel -p " + str(jobNameInScheduler))
            print output
        else:
            #TODO This will have to change for local submission stuff
            #set the user and username to be ccq so that all jobs can be seen. ccq user is the Torque admin
            # os.environ["USER"] = ccqHubMethods.ccqUserName
            # os.environ["USERNAME"] = ccqHubMethods.ccqUserName
            # os.environ["LOGNAME"] = ccqHubMethods.ccqUserName
            os.system("sudo /opt/torque/sbin/trqauthd")
            status, output = commands.getstatusoutput("/opt/torque/bin/qdel " + str(jobNameInScheduler))
            print output
        return {"status": "success", "payload": "Successfully issued the job deletion command! Job should be terminating, if not please try the force or purge option!"}
