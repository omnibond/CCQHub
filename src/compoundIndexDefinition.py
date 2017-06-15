#!/usr/bin/python2.7
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


def returnCompoundIndexDefinition():
    return [
        {"JobScript": [
            "name"
        ]},
        {"Job": [
            "name",
            "jobName"
        ]},
        {"Job": [
            "userName",
            "name"
        ]},
        {"Identity": [
            "userName",
            "name"
        ]},
        {"Identity": [
            "keyId",
            "name"
        ]},
        {"Identity": [
            "name"
        ]},
        {"Target": [
            "targetName",
            "schedulerType",
            "targetAddress",
            "name"
        ]},
        {"Target": [
            "targetAddress",
            "targetName",
            "schedulerType",
            "name"
        ]},
        {"Target": [
            "schedulerType",
            "name"
        ]},
        {"Target": [
            "targetAddress",
            "schedulerType",
            "name"
        ]},
        {"Target": [
            "name"
        ]},
        {"Target": [
            "targetName",
            "name"
        ]},
        {"DefaultHubTargets": [
            "name"
        ]}
    ]
