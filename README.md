Hive UDTF for parsing User-Agent string.
****


**How to build and run**

- `gradlew jar` to build jar file. 
- Execute `ADD JAR build/libs/UserAgentUDF-1.0-SNAPSHOT.jar` in HIVE
- Execute `CREATE TEMPORARY FUNCTION parseUserAgent as 'com.mylnikov.UserAgentUDTF'`

****

**Usage**

You can use use function like this `SELECT parseUserAgent(user-agent-string-column) FROM tbl;`

The output  table will contain columns: device, browser, os. 
