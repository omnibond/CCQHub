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


import src.ccqHubMethods as ccqHubMethods
import src.ccqHubVars as ccqHubVars
import src.credentials as credentials


class Deletion:
    def __init__(self):
        ccqHubVars.init()
        pass

    def cleanupDeletedJob(self, jobId, logger):
        #Need to remove the job from all the ccqVarObjects so that we don't assign any instances to it
        logger.info("SOMEHOW MADE IT INTO CLEANUPDELETEDJOB FUNCTION!!!!\n\n\n\n\n")
        with ccqHubVars.ccqHubVarLock:
            ccqHubVars.jobMappings.pop(jobId)
        ccqHubMethods.writeCcqVarsToFile()
