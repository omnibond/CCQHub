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


class Submission:
    def __init__(self):
        ccqHubVars.init()
        pass

    def performJobRouting(self, logger):
        logger.info("Placeholder for actual rules/routing policy evaluation and enforcement.")
