# MicroPyDatabase
A low-memory json-based database for MicroPython

Install prerequisites:  
`micropython -m upip install micropython-os`  
`micropython -m upip install micropython-os.path`  

Using on Mac:
Create a file like sandbox.tmp.py like so:

```
from micropydatabase import *

print(Database().another_func())
print(Table().do_something())
```
Run `micropython sandbox.tmp.py`
