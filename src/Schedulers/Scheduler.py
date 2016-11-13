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


import os, sys
sys.path.append('../Database')
import time
import traceback

import sqlLite3Database

ccqHubMethods = None

clusterInformationLogFileLocation = ccqHubMethods.clusterInformationLogFileLocation
logFileDirectory = ccqHubMethods.logFileDirectory
scriptDirectory = ccqHubMethods.scriptDirectory

sleepTime = 30  # how long to sleep between checks for number of nodes registered in seconds
timeOut = 600  # max time to wait in seconds per node

class Scheduler(object):

    def __init__(self, schedName, schedType, instanceID, clusterName, instanceName, schedulerIP):
        self.schedName = schedName
        self.schedType = schedType
        self.instanceID = instanceID
        self.clusterName = clusterName
        self.instanceName = instanceName
        self.schedulerIP = schedulerIP

    #Base classes
    def checkJobs(self, **kwargs):
        return {"status" : "error", "payload": "Base Scheduler Class Not Called Error In: checkJobs"}

    def removeComputeNodesFromScheduler(self, **kwargs):
        return {"status" : "error", "payload": "Base Scheduler Class Not Called Error In: removeComputeNodesFromScheduler"}

    def setComputeNodeOnline(self, **kwargs):
        return {"status" : "error", "payload": "Base Scheduler Class Not Called Error In: setComputeNodeOnline"}

    def takeComputeNodeOffline(self, **kwargs):
        return {"status" : "error", "payload": "Base Scheduler Class Not Called Error In: takeComputeNodeOffline"}

    def checkNodeForNodesToPossiblyTerminate(self, **kwargs):
        return {"status" : "error", "payload": "Base Scheduler Class Not Called Error In: checkNodeForNodesToPossiblyTerminate"}

    # This method should take the same parameters for all schedulers! This is critical for CCQ
    def submitJobToScheduler(self, **kwargs):
        return {"status" : "error", "payload": "Base Scheduler Class Not Called Error In: submitJobToScheduler"}

    def checkIfInstancesAlreadyAvailableInScheduler(self, **kwargs):
        return {"status" : "error", "payload": "Base Scheduler Class Not Called Error In: checkIfInstancesAlreadyAvailableInScheduler"}

    def getJobStatusFromScheduler(self, **kwargs):
        return {"status" : "error", "payload": "Base Scheduler Class Not Called Error In: getJobStatus"}

    def deleteJobFromScheduler(self, **kwargs):
        return {"status" : "error", "payload": "Base Scheduler Class Not Called Error In: deleteJobFromScheduler"}

    def putComputeNodesToRunJobOnInCorrectFormat(self, **kwargs):
        return {"status" : "error", "payload": "Base Scheduler Class Not Called Error In: putComputeNodesToRunJobOnInCorrectFormat"}
