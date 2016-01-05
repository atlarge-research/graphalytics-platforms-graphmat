#!/bin/sh
#
# Copyright 2015 Delft University of Technology
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


# Ensure the configuration file exists
if [ ! -f "$config/graphmat.properties" ]; then
	echo "Missing mandatory configuration file: $config/graphmat.properties" >&2
	exit 1
fi

# Get the first specification of hadoop.home
graphmathome=$(grep -E "^graphmat.home[	 ]*[:=]" $config/graphmat.properties | sed 's/graphmat.home[\t ]*[:=][\t ]*\([^\t ]*\).*/\1/g' | head -n 1)
if [ ! -d "$graphmathome/bin/" ]; then
	echo "Invalid definition of graphmat.home: $graphmatphome" >&2
	echo "Expecting both a \"bin\" and \"lib\" subdirectory." >&2
	exit 1
fi
export LD_LIBRARY_PATH=$graphmathome/lib:$LD_LIBRARY_PATH

# Set the "platform" variable
export platform="graphmat"

