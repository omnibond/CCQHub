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

# These identify the valid actions and what attributes the Identity is required to have in order to be granted permission to perform the action
actionsAndRequiredAttributes = {"ccqHubAdmin": {"groups": [], "attributes": {"ccqHubAdmin": "True"}},
                                "proxyUser": {"groups": [], "attributes": {"proxyUser": "True"}},
                                "listAllUserJobs": {"groups": [], "attributes": {"listAllUserJobs": "True"}},
                                "submitJob": {"groups": [], "attributes": {"submitJob": "True"}}
                               }

def getValidActionsAndRequiredAttributes():
    return actionsAndRequiredAttributes


def getValidActions(singleLine):
    tempList = []
    for key in actionsAndRequiredAttributes:
        tempList.append(key)
    if singleLine:
        returnString = ""
        for item in tempList:
            returnString += item + ", "
        return returnString[:len(returnString)-2]
    else:
        return tempList