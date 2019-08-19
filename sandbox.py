"""
Sandbox for MicroPyDatabase
A low-memory json-based databse for MicroPython.
Data is stored in a folder structure in json for easy inspection.
This file contains sample usage.
"""
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




