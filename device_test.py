"""
A file that contains tests of MicroPyDatabase features to ensure nothing 
is broken when updates are written.

Run on device with:
with open("device_test.py") as f:
    exec(f.read(), globals())
"""

import micropydatabase as mdb
import gc
import time


def test_database_open_exception():
    try:
        db_object = mdb.Database.open("testblahdb")
    except Exception:
        return 'Success.'
    else:
        return 'Error.'

def test_database_creation():
    try:
        gc.collect()
        before = gc.mem_free()
        start_time = time.ticks_ms()
        db_object = mdb.Database.create("testdb")
        gc.collect()
        after = gc.mem_free()
        end_time = time.ticks_diff(time.ticks_ms(), start_time)
        print("Database creation took", end_time, "ms to run")
        print("Database creation took up", before - after, "bytes")
    except Exception:
        return 'Error.'
    else:
        return 'Success.'

def test_database_creation_exception():
    try:
        db_object = mdb.Database.create("testdb")
    except Exception:
        return 'Success.'
    else:
        return 'Error.'

def test_table_open_exception():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testblahtable")
    except Exception:
        return 'Success.'
    else:
        return 'Error.'

def test_table_creation():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.create_table("testtable", ["name", "password"])
    except Exception:
        return 'Error.'
    else:
        return 'Success.'

def test_table_open():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
    except Exception:
        return 'Error.'
    else:
        return 'Success.'

def test_insert_row():
    i = 1
    try:
        all_memory = []
        all_time = []
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
        for x in range(550):
            gc.collect()
            before = gc.mem_free()
            start_time = time.ticks_ms()
            db_table.insert({"name": "bob", "password": "coolpassword"})
            gc.collect()
            after = gc.mem_free()
            end_time = time.ticks_diff(time.ticks_ms(), start_time)
            all_memory.append(before - after)
            all_time.append(end_time)
            i += 1
    except Exception:
        return 'Error.'
    else:
        print("Average memory used per insert was", sum(all_memory) / len(all_memory), "bytes.")
        print("Average time per insert was", sum(all_time) / len(all_time), "ms.")
        all_time.sort()
        all_memory.sort()
        print("Most memory used was", all_memory[-1], "bytes.")
        print("Longest insert time was", all_time[-1], "ms.")
        return 'Success.'

def test_insert_multiple_rows():
    try:
        gc.collect()
        before = gc.mem_free()
        start_time = time.ticks_ms()
        db_object = mdb.Database.open("testdb")
        gc.collect()
        after = gc.mem_free()
        end_time = time.ticks_diff(time.ticks_ms(), start_time)
        print("Database open took", end_time, "ms to run.")
        print("Database open took up", before - after, "bytes.")
        gc.collect()
        before = gc.mem_free()
        start_time = time.ticks_ms()
        db_table = db_object.open_table("testtable")
        gc.collect()
        after = gc.mem_free()
        end_time = time.ticks_diff(time.ticks_ms(), start_time)
        print("Opening table with 550 rows took", end_time, "ms.")
        print("Opening table with 550 rows took", before - after, "bytes.")
        gc.collect()
        before = gc.mem_free()
        start_time = time.ticks_ms()
        db_table.insert([{"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}])
        gc.collect()
        after = gc.mem_free()
        end_time = time.ticks_diff(time.ticks_ms(), start_time)
        print("Multi-inserting 28 rows took", end_time, "ms to run.")
        print("Multi-inserting 28 rows took up", before - after, "bytes.")
        gc.collect()
        before = gc.mem_free()
        start_time = time.ticks_ms()
        db_table.insert([{"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}, {"name": "whothere", "password": "ohyeah"}])
        gc.collect()
        after = gc.mem_free()
        end_time = time.ticks_diff(time.ticks_ms(), start_time)
        print("Multi-inserting 30 rows took", end_time, "ms to run.")
        print("Multi-inserting 30 rows took up", before - after, "bytes.")
    except Exception:
        return 'Error.'
    else:
        return 'Success.'

def test_update_row_exception_row():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
        db_table.update_row(1000, {"name": "john"})
    except Exception:
        return 'Success.'
    else:
        return 'Error.'

def test_update_row_exception_column():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
        db_table.update_row(300, {"what": "john"})
    except Exception:
        return 'Success.'
    else:
        return 'Error.'

def test_update_row():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
        gc.collect()
        before = gc.mem_free()
        start_time = time.ticks_ms()
        db_table.update_row(300, {"name": "george", "password": "anotherone"})
        gc.collect()
        after = gc.mem_free()
        end_time = time.ticks_diff(time.ticks_ms(), start_time)
        print("Updating by row id took", end_time, "ms to run.")
        print("Updating by row id took", before - after, "bytes.")
    except Exception:
        return 'Error.'
    else:
        return 'Success.'

def test_update():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
        gc.collect()
        before = gc.mem_free()
        start_time = time.ticks_ms()
        db_table.update({"name": "george", "password": "anotherone"}, {"name": "sally", "password": "whatisthis"})
        gc.collect()
        after = gc.mem_free()
        end_time = time.ticks_diff(time.ticks_ms(), start_time)
        print("Updating by query (at row 300) took", end_time, "ms to run.")
        print("Updating by query (at row 300) took", before - after, "bytes.")
    except Exception:
        return 'Error.'
    else:
        return 'Success.'

def test_update_exception():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
        db_table.update({"name": "whowhowho", "password": "anotherone"}, {"name": "sally", "password": "whatisthis"})
    except Exception:
        return 'Success.'
    else:
        return 'Error.'

def test_delete_row():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
        gc.collect()
        before = gc.mem_free()
        start_time = time.ticks_ms()
        db_table.delete_row(200)
        gc.collect()
        after = gc.mem_free()
        end_time = time.ticks_diff(time.ticks_ms(), start_time)
        print("Delete row by row id took", end_time, "ms to run.")
        print("Delete row by row id took", before - after, "bytes.")
    except Exception:
        return 'Error.'
    else:
        return 'Success.'

def test_delete_row_exception():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
        db_table.delete_row(3300)
    except Exception:
        return 'Success.'
    else:
        return 'Error.'

def test_delete():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
        gc.collect()
        before = gc.mem_free()
        start_time = time.ticks_ms()
        db_table.delete({"name": "sally", "password": "whatisthis"})
        gc.collect()
        after = gc.mem_free()
        end_time = time.ticks_diff(time.ticks_ms(), start_time)
        print("Delete by query (at row 300) took", end_time, "ms to run.")
        print("Delete by query (at row 300) took", before - after, "bytes.")
    except Exception:
        return 'Error.'
    else:
        return 'Success.'

def test_delete_exception():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
        db_table.delete({"name": "sallywho", "password": "whatisthis"})
    except Exception:
        return 'Success.'
    else:
        return 'Error.'

def test_find_row():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
        gc.collect()
        before = gc.mem_free()
        start_time = time.ticks_ms()
        db_table.find_row(150)
        gc.collect()
        after = gc.mem_free()
        end_time = time.ticks_diff(time.ticks_ms(), start_time)
        print("Find by row id (row 150) took", end_time, "ms to run.")
        print("Find by row id (row 150) took", before - after, "bytes.")
    except Exception:
        return 'Error.'
    else:
        return 'Success.'

def test_find_row_exception():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
        db_table.find_row(4500)
    except Exception:
        return 'Success.'
    else:
        return 'Error.'

def test_query():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
        db_table.update_row(250, {"name": "blah", "password": "something"})
        db_table.update_row(101, {"name": "blah", "password": "something"})
        gc.collect()
        before = gc.mem_free()
        start_time = time.ticks_ms()
        return_list = db_table.query({"name": "blah", "password": "something"})
        gc.collect()
        after = gc.mem_free()
        end_time = time.ticks_diff(time.ticks_ms(), start_time)
        print("Query with two results (row 101 and 250) took", end_time, "ms to run.")
        print("Query with two results (row 101 and 250) took", before - after, "bytes.")
    except Exception:
        return 'Error.'
    if len(return_list) == 2:
        return 'Success.'
    
    else:
        return 'Error.'

def test_find():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
        gc.collect()
        before = gc.mem_free()
        start_time = time.ticks_ms()
        db_table.find({"name": "blah", "password": "something"})
        gc.collect()
        after = gc.mem_free()
        end_time = time.ticks_diff(time.ticks_ms(), start_time)
        print("Find by query (row 101) took", end_time, "ms to run.")
        print("Find by query (row 101) took", before - after, "bytes.")
    except Exception:
        return 'Error.'
    else:
        return 'Success.'

def test_scan_no_query():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
        db_table.update_row(1, {"name": "tom", "password": "alright"})
        gc.collect()
        before = gc.mem_free()
        start_time = time.ticks_ms()
        scan_return = db_table.scan()
        the_scan = scan_return.__next__()
        gc.collect()
        after = gc.mem_free()
        end_time = time.ticks_diff(time.ticks_ms(), start_time)
        print("Scan returning first row took", end_time, "ms to run.")
        print("Scan returning first row took", before - after, "bytes.")
    except Exception:
        return 'Error.'
    if the_scan['name'] == 'tom':
        return 'Success.'
    else:
        return 'Error.'

def test_scan_with_query():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
        gc.collect()
        before = gc.mem_free()
        start_time = time.ticks_ms()
        scan_return = db_table.scan({"name": "blah", "password": "something"})
        the_scan = scan_return.__next__()
        gc.collect()
        after = gc.mem_free()
        end_time = time.ticks_diff(time.ticks_ms(), start_time)
        print("Scan with query returning first result (row 101) took", end_time, "ms to run.")
        print("Scan with query returning first result (row 101) took", before - after, "bytes.")
    except Exception:
        return 'Error.'
    if the_scan['name'] == 'blah':
        return 'Success.'
    else:
        return 'Error.'

#A test to be sure data row files were created correctly.
def check_data_file_name():
    location = mdb.os.listdir('testdb/testtable')
    #Remove non-data files from our list of dirs.
    location = [element for element in location if 'data' in element]
    #Check we have the correct number of data page files
    if len(location) == 61:
        pass
    else:
        raise Exception('Error.')
    #Sort as integers so we get them in the right order.
    location = sorted(location, key=lambda x: int(x.split('.')[0].split('_')[1]), reverse = True)
    for f in location:
        if f in ['data551_560.dat', 'data1_10.dat', 'data11_20.dat', 'data21_30.dat', 'data31_40.dat', 'data41_50.dat', 'data51_60.dat', 'data61_70.dat', 'data71_80.dat', 'data81_90.dat', 'data91_100.dat', 'data101_110.dat', 'data111_120.dat', 'data121_130.dat', 'data131_140.dat', 'data141_150.dat', 'data151_160.dat', 'data161_170.dat', 'data171_180.dat', 'data181_190.dat', 'data191_200.dat', 'data201_210.dat', 'data211_220.dat', 'data221_230.dat', 'data231_240.dat', 'data241_250.dat', 'data251_260.dat', 'data261_270.dat', 'data271_280.dat', 'data281_290.dat', 'data291_300.dat', 'data301_310.dat', 'data311_320.dat', 'data321_330.dat', 'data331_340.dat', 'data341_350.dat', 'data351_360.dat', 'data361_370.dat', 'data371_380.dat', 'data381_390.dat', 'data391_400.dat', 'data401_410.dat', 'data411_420.dat', 'data421_430.dat', 'data431_440.dat', 'data441_450.dat', 'data451_460.dat', 'data461_470.dat', 'data471_480.dat', 'data481_490.dat', 'data491_500.dat', 'data501_510.dat', 'data511_520.dat', 'data521_530.dat', 'data531_540.dat', 'data541_550.dat', 'data561_570.dat', 'data571_580.dat', 'data581_590.dat', 'data591_600.dat', 'data601_610.dat']:
            continue
        else:
            raise Exception('Error.')
    return 'Success.'

def check_current_row():
    try:
        db_object = Database.open("testdb")
        db_table = db_object.open_table("testtable")
        current_row = db_table.__calculate_current_row()
    except Exception:
        return 'Error.'
    if int(current_row) == 610:
        return 'Success.'
    else:
        return 'Error.'

#A test to make sure the data files have the correct number of rows in the files
def test_data_files():
    location = mdb.os.listdir('testdb/testtable')
    #Remove non-data files from our list of dirs.
    location = [element for element in location if 'data' in element]
    #Sort as integers so we get them in the right order.
    location = sorted(location, key=lambda x: int(x.split('.')[0].split('_')[1]), reverse = True)
    for f in location:
        if f != 'data601_610.dat':
            with open('testdb/testtable/' + f, 'r') as output_file:
                i = 0
                for line in output_file:
                    i += 1
                if i == 10:
                    pass
                else:
                    raise Exception('Error.')
        else:
            with open('testdb/testtable/' + f, 'r') as output_file:
                i = 0
                for line in output_file:
                    i += 1
                if i == 10:
                    pass
                else:
                    raise Exception('Error.')
    return 'Success.'

def test_truncate():
    try:
        db_object = mdb.Database.open("testdb")
        db_table = db_object.open_table("testtable")
        db_table.truncate()
    except Exception:
        return 'Error.'
    for file_name in mdb.os.listdir('testdb/testtable'):
        if file_name[0:4] == 'data':
            return 'Error.'
    else:
        return 'Success.'

#Clean up all the test data
def remove_test_database_files():
    try:
        for file_name in mdb.os.listdir('testdb/testtable'):
            mdb.os.remove('testdb/testtable/' + file_name)
        mdb.os.rmdir('testdb/testtable')
        for file_name in mdb.os.listdir('testdb'):
            mdb.os.remove('testdb/' + file_name)
        mdb.os.rmdir('testdb')
    except Exception:
        return 'Failed to delete test data.'

print("Testing")
print("------")
assert test_database_open_exception() == "Success.", "Open database that doesn't exist: Error."
assert test_database_creation() == "Success.", "Create database: Error."
assert test_database_creation_exception() == "Success.", "Create database with same name: Error."
assert test_table_open_exception() == "Success.", "Open table that doesn't exist: Error."
assert test_table_creation() == "Success.", "Create table: Error."
assert test_table_open() == "Success.","Open table: Error."
assert test_insert_row() == "Success.","Insert row: Error."
assert test_insert_multiple_rows() == "Success.", "Insert multiple rows: Error."
assert test_update_row_exception_row() == "Success.","Update row that doesn't exist: Error."
assert test_update_row_exception_column() == "Success.","Update row with column that doesn't exist: Error."
assert test_update_row() == "Success.","Update row: Error."
assert test_update() == "Success.", "Update: Error."
assert test_update_exception() == "Success.", "Update with query that doesn't match: Error."
assert test_delete_row() == "Success.","Delete row: Error."
assert test_delete_row_exception() == "Success.","Delete row exception: Error."
assert test_find_row() == "Success.","Find row: Error."
assert test_find_row_exception() == "Success.","Find row exception: Error."
assert test_query() == "Success.","Query exception: Error."
assert test_find() == "Success.","Find exception: Error."
assert test_scan_no_query() == "Success.","Scan without query: Error."
assert test_scan_with_query() == "Success.","Scan with query: Error."
assert check_data_file_name() == "Success.", "Data row files: Error"
assert test_truncate() == "Success.","Truncate: Error."
remove_test_database_files()
print("------")
print("All tests passed.")
