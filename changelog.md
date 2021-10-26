# Changelog#

## 2021-10-18 ##
> Breaking changes!
* Shaved 8 bytes from each line in database, by changing `data` and `row_id` to `d` and `r`. This is incompatible with already existing data files.
* Added database method `Database.exist("database_name")` for easy check.
* changed string concatenation to use `format()` function, to save RAM.
* Shortened exception texts.

## 2020-12-05 ##

* Fixed method `Table.__return_query` so now querying by `tbl.query({"name": "Bob"})` works correctly. Expanded capability (read bellow). Backward compatibility left. **Note**: Value strings are Case Sensitive!
* Make compatible with full python (windows, linux).
* Table creattion method inherit `rows_per_page` and `max_rows` from database schema settings.
* Do not increase `tbl.current_row` counter if data has not passed validation yet.
* New method `tbl.vacuum()` to optimize pagefiles. Worth to use after some data has been deleted.
* updated method `Table.__return_query` so now is possible to search by multiple keys and values. Imagine following **persons_table** data:
    | fname | lname | age |
    |---|---|---|
    | John | Smith | 37 |
    | Nicole | Smith | *None* |
    | Kim | Smith| 7 |
    | John | Lee | *None* |
    | Nicole | Lee | 32 |
    | Bart | Lee | 3  |
    We want to get John and Nicole Smiths, but dont want all Smiths and no Lees. Following query `tbl.query({"fname":["John", "Nicole"], "lname": "Smith"})` and here is the result:
    ``` python
    [{'fname': 'John', 'lname': 'Smith', 'age': 34},
    {'fname': 'Nicole', 'lname': 'Smith', 'age': None}]
    ```
    You may add any number of search parameter in your query and all of them will be `AND`. In sql definition upper search query represent following SQL:
    ```sql
    select * from persons_table where fname in ("John", "Nicole") and lname = "Smith"
    ```
* Changed `__insert_modify_data_file` so writing to files would be more efficient and faster in exchange in reliability if something unexpected happens. 
* Annotated all function's parameters
* Fixed `tests/test.py` file to correcttly calculatee current row in `check_current_row()` function

`test/test.py` and `device_test.py` passed succesfully on
> MicroPython v1.9.4-2922-gce1dfde98-dirty on 2020-11-26; ESP32 module with ESP32
