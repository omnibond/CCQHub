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
import sys
import time
import urllib2

import math

from Scheduler import Scheduler

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import ccqHubMethods
import traceback
import time
import commands


class SlurmScheduler(Scheduler):
    def __init__(self, **kwargs):
        super(SlurmScheduler, self).__init__(**kwargs)

    def checkJobs(self, job):
        # Check to see if the job is still running on the local Slurm Cluster, if it is update the status of the job to Running, if not then we return not running and it will be processed as completed.
        jobInDBStillRunning = False
        status = ""
        output = ""

        status, output = commands.getstatusoutput('sudo /opt/slurm/bin/scontrol -o show job ' + str(job['schedulerJobName']))
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
                values = ccqHubMethods.updateJobInDB({"status": "Running", "batchHost": slurmJob['BatchHost']}, job['jobId'])
                if values['status'] != "success":
                    print "There was an error trying to save the new job state to the DB."
                jobInDBStillRunning = True
        return {"status": "success", "payload": {"jobInDBStillRunning": jobInDBStillRunning}}

    def submitJobToScheduler(self, userName, jobName, jobId, tempJobScriptLocation):
        status, output = commands.getstatusoutput("su -" + str(userName) + " -c \' /opt/slurm/bin/sbatch " + str(tempJobScriptLocation) + "/" + str(jobName) + str(jobId) + "\'")
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
            values = ccqHubMethods.updateJobInDB({"status": "Submitted", "schedulerJobName": jobName, "batchHost": batchHost}, jobId)
            if values['status'] == "deleting":
                status, output = commands.getstatusoutput("sudo /opt/slurm/bin/scancel " + str(jobName))
                print output
                return {"status": "error", "payload": str(output)}
            elif values['status'] != 'success':
                print values['payload']
                return {"status": "error", "payload": "There was an error trying to update the job status in the DB.\n" + values['payload']}
        else:
            ccqHubMethods.updateJobInDB({"status": "Error"}, jobId)
            print "There was an error trying to submit the job to the scheduler!\n" + output
            return {"status": "error", "payload": "There was an error trying to submit the job to the Target.\n" + output}

        return {'status': "success", "payload": "The job was submitted successfully."}

    def getJobStatusFromScheduler(self, jobId, jobNameInScheduler, userName):
        if jobId == "all":
            status, output = commands.getstatusoutput('sudo /opt/slurm/bin/squeue -u ' + str(userName))
        else:
            status, output = commands.getstatusoutput('sudo /opt/slurm/bin/squeue --jobs ' + str(jobNameInScheduler))

        if output == "" and jobId == "all":
            output = "There are currently no jobs in the queue."

        return {"status": "success", "payload": output}

    def deleteJobFromScheduler(self, jobForceDelete, jobNameInScheduler):
        status, output = commands.getstatusoutput("sudo /opt/slurm/bin/scancel -f " + str(jobNameInScheduler))
        print output
        return {"status": "success", "payload": "Successfully issued the job deletion command."}

    # Methods that may be required for other implementations (ccq)
    def checkNodeForNodesToPossiblyTerminate(self, instanceNamesToCheck, instanceIdsToCheck):
        return {"status": "success", "payload": "Not required for local submission via ccqHub."}

    def putComputeNodesToRunJobOnInCorrectFormat(self, hostList, cpus, memory):
        return {"status": "success", "payload": "Not required for local submission via ccqHub."}

    def removeComputeNodesFromScheduler(self, computeNodes):
        return {"status": "success", "payload": "Not required for local submission via ccqHub."}

    def setComputeNodeOnline(self, computeNodeInstanceId):
        return {"status": "success", "payload": "Not required for local submission via ccqHub."}

    def takeComputeNodeOffline(self, computeNodeInstanceId):
        return {"status": "success", "payload": "Not required for local submission via ccqHub."}

    def checkIfInstancesAlreadyAvailableInScheduler(self, requestedInstanceType, numberOfInstancesRequested):
        return {"status": "success", "payload": "Not required for local submission via ccqHub."}

