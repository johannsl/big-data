# big-data
TDT4305

## Setup on OS X

### Install Spark
brew install apache-spark

### Install py4j
pip install py4j

### Run pyspark code
spark-submit >pythonfile.py<

### Errors
problem:
NativeCodeLoader: Unable to load native-hadoop library for your platform.

solution:
It appears that the WARN message should be disregarded on Mac OS X as the native library does simply not exist for the platform.
http://stackoverflow.com/questions/23572724/why-does-bin-spark-shell-give-warn-nativecodeloader-unable-to-load-native-had


<a href="http://www.wtfpl.net/"><img src="http://www.wtfpl.net/wp-content/uploads/2012/12/logo-220x1601.png" align="left" height="30" width="30" ></a>
<br>

```
DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
Version 2, December 2004

Copyright (C) 2016 Johan Slettevold <johan.slettevold@gmail.com>

Everyone is permitted to copy and distribute verbatim or modified
copies of this license document, and changing it is allowed as long
as the name is changed.

DO WHAT THE FUCK YOU WANT TO PUBLIC LICENSE
TERMS AND CONDITIONS FOR COPYING, DISTRIBUTION AND MODIFICATION

0. You just DO WHAT THE FUCK YOU WANT TO.
```
