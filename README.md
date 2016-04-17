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
It appears that the WARN message should be disregarded on Mac OS X as the native library doesn't simply exist for the platform.
http://stackoverflow.com/questions/23572724/why-does-bin-spark-shell-give-warn-nativecodeloader-unable-to-load-native-had
