# Copyright Omnibond Systems, LLC. All rights reserved.

# This file is part of OpenCCQ.

# OpenCCQ is free software: you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published by
# the Free Software Foundation, either version 2 of the License, or
# (at your option) any later version.

# OpenCCQ is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Lesser General Public License for more details.

# You should have received a copy of the GNU Lesser General Public License
# along with OpenCCQ.  If not, see <http://www.gnu.org/licenses/>.

import os
import sys
from Scheduler import Scheduler

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
import ClusterMethods
import traceback
import time
import commands

clusterInformationLogFileLocation = ClusterMethods.clusterInformationLogFileLocation
logFileDirectory = ClusterMethods.logFileDirectory
scriptDirectory = ClusterMethods.scriptDirectory
cloudyClusterDefaultSSHUser = ClusterMethods.cloudyClusterDefaultSSHUser


class OpenlavaScheduler(Scheduler):

    def __init__(self, **kwargs):
        super(OpenlavaScheduler, self).__init__(**kwargs)

    def checkJobs(self):
        return {"status" : "success", "payload": "The checkJobs function is not implemented for the Openlava Scheduler yet!"}
