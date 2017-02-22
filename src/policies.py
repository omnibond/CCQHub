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

def getValidActionsAndRequiredAttributes():
    # These identify the valid actions and what attributes the Identity is required to have in order to be granted permission to perform the action
    actionsAndRequiredAttributes = {"ccqHubAdmin": {"groups": [], "attributes": {"ccqHubAdmin": "True"}},
                                    "proxyJob": {"groups": [], "attributes": {"proxyJob": "True"}},
                                    "submitJob": {"groups": [], "attributes": {"submitJob": "True"}}
                                   }
    return actionsAndRequiredAttributes