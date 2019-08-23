# MicroPyDatabase
A low-memory json-based database for MicroPython.
Data is stored in a folder structure in json for easy inspection.

Install prerequisites:  
`micropython -m upip install micropython-os`  
`micropython -m upip install micropython-os.path`  

or
```
>>> import upip
>>> upip.install("micropython-os”)
>>> upip.install("micropython-os.path”)
```

Using on a :

```
import micropydatabase
```
Create a new database:
```
db_object = micropydatabase.Database.create("mydb")
```
Open an existing database:
```
db_object = micropydatabase.Database.open("mydb")
```
Create a new table (specifying column names):
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.create_table("mytable", ["name", "password"])
```
Insert data into table:
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable"])
db_table.insert({"name": "lucy", "password": "coolpassword"})
```
Multi-insert data into table:
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable"])
db_table.insert([{"name": "john", "password": "apassword"}, {"name": "john", "password": "apassword"}, {"name": "bob", "password": "thispassword"}, {"name": "sally", "password": "anotherpassword"}])
```
Find (returns first result):
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable"])
db_table.find({"name": "john", "password": "apassword"})
```
Query (returns all results):
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable"])
db_table.query({"name": "john", "password": "apassword"})
```
Update Row (search query, updated row data):
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable"])
db_table.update({"name": "bob", "password": "thispassword"}, {"name": "george", "password": "somethingelse"})
```
Delete Row:
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable"])
db_table.delete({"name": "george", "password": "somethingelse"})
```
Scan (iterate through each row in table):
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable"])
f = db_table.scan()
f.__next__()
```
Scan with Query (iterate through rows that match query):
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable"])
f = db_table.scan({"name": "john", "password": "apassword"})
f.__next__()
```
Truncate Table (delete all table contents):
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable"])
db_table.truncate()
```


Using on Mac:
Create a file like sandbox.tmp.py like so:

```
from micropydatabase import *

#Database examples:
db_object = Database.create("mydb2")
db_object = Database.open("mydb")

#Table examples:
db_table = db_object.create_table("mytable", ["name", "password"])
db_table = db_object.open_table("mytable")
db_table.truncate()

#Insert examples:
db_table.insert({"name": "nate", "password": "coolpassword"})
db_table.insert([{"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}])

#Low-level operations using internal row_id:
db_table.find_row(5)
db_table.update_row(300, {'name': 'bob'})
db_table.delete_row(445)
db_table.find_by_column_value("name", "bob")
f = db_table.scan()
f.__next__()

#High-level operations using queries:
db_table.find({"name": "blah", "password": "something"})
db_table.query({"name": "blah", "password": "something"})
db_table.update({"name": "blah8", "password": "yeah8"}, {"name": "blah9", "password": "yeah9"})
db_table.delete({"name": "blah9", "password": "yeah9"})
```
Run `micropython sandbox.tmp.py`
