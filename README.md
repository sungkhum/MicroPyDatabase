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

Usage instructions :

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
Create a new table (specifying column names [and types if you need it]):  
*(Table column definition supported types are **str**, **int** and **bool**. Default is **str**.)*
```
db_object = micropydatabase.Database.open("mydb")
db_object.create_table("mytable", ["name", "password"])
db_object.create_table("mytable", {
        "name":str, 
        "age":int, 
        "isMember":bool
})
```
Insert data into table:
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable")
db_table.insert({"name": "lucy", "password": "coolpassword"})   # as dict
db_table.insert(["Rose", "MySecret"])                           # as list
```
Multi-insert data into table:
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable")
db_table.insert([
    {"name": "john", "password": "apassword"}, 
    {"name": "john", "password": "apassword"}, 
    {"name": "bob", "password": "thispassword"}, 
    {"name": "sally", "password": "anotherpassword"}
])
```
Find (returns first result):
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable")
db_table.find({"name": "john", "password": "apassword"})
```
Query (returns all results):
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable")
db_table.query({"name": "john", "password": "apassword"})
```
Update Row (search query, updated row data):
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable")
db_table.update(
    {"name": "bob", "password": "thispassword"},        #find what
    {"name": "george", "password": "somethingelse"}     # change with
)
```
Delete Row:
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable")
db_table.delete({"name": "george", "password": "somethingelse"})
```
Scan (iterate through each row in table):
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable")
f = db_table.scan()
f.__next__()
```
Scan with Query (iterate through rows that match query):
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable")
f = db_table.scan({"name": "john", "password": "apassword"})
f.__next__()
```
Truncate Table (delete all table contents):
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable")
db_table.truncate()
```

Vaccum Table (reorganize all content):
```
db_object = micropydatabase.Database.open("mydb")
db_table = db_object.open_table("mytable")
db_table.vaccum()
```
