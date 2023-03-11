"""Low-memory json-based databse for MicroPython.
Data is stored in a folder structure in json for easy inspection.
Indexing multiple columns is supported, and RAM usage is optimized
for embedded systems.
Database examples:
db_object = Database.create("mydb")
db_object = Database.open("mydb")
Table examples:
db_object.create_table("mytable", ["name", "password"])
this case all fields will be string type. If you want to define columns
precisely- pass dict of fields with types defined. Supported: str, int, float, bool
db_object.create_table("mytable", {"name":str, "age":int, "height":float, "isMember":bool})
db_table = db_object.open_table("mytable")
db_table.truncate()
Insert examples:
db_table.insert({"name": "nate", "password": "coolpassword"})
or you can use dict for fields:
db_table.insert(["John", 37, True])
Query data
db_table.query({"name": "nate"})
In case you need to get row_id with your data, pass second optional boolean parameter
db_table.insert({"name": "nate", "password": "coolpassword"}, True)
You'll get additional column '_row' with your data. Works with scan() find() and query()
Low-level operations using internal row_id:
db_table.find_row(5)
db_table.update_row(300, {'name': 'bob'})
db_table.delete_row(445)
"""
import json as json
import os


class OutOfMemoryError(Exception):
    opt = ''
    msg = ''

    def __init__(self, msg, opt=''):
        self.msg = msg
        self.opt = opt
        Exception.__init__(self, msg, opt)

    def __str__(self):
        return self.msg


def file_exists(path):
    try:
        f = open(path, 'r') 
        f.close()
        return True
    except OSError:
        return False


def dir_exists(path):
    try:
        return os.stat(path)[0] & 0o170000 == 0o040000
    except OSError:
        return False


class Database:
    def __init__(self, database: str, rows_per_page: int, max_rows: int,
                 storage_format_version: int):
        self.path = database
        self.rows_per_page = rows_per_page
        self.max_rows = max_rows
        self.storage_format_version = storage_format_version

        if not dir_exists(self.path):
            raise Exception("Database not found at {}".format(self.path))

    @staticmethod
    def create(database: str, rows_per_page: int = 10, max_rows: int = 10000):
        """
        store the current version of the MicroPyDB to prevent future upgrade
        errors.
        """
        version = 1
        # Check if database already exists.
        if not dir_exists(database):
            os.mkdir(database)
            # If database doesn't exist, create the directory structure and the
            # Schema json file with default values.
            with open("{}/schema.json".format(database), 'w') as f:
                data = {
                    'max_rows': max_rows,
                    'storage_format_version': version,
                    'rows_per_page': rows_per_page
                }
                f.write(json.dumps(data))
                return Database(database, rows_per_page, max_rows, version)
        else:
            raise Exception("Database {} is already in use".format(database))

    @staticmethod
    def open(database: str):
        # Check if database exists.
        schema_path = '{}/schema.json'.format(database)
        if file_exists(schema_path):
            with open(schema_path) as json_file:
                data = json.load(json_file)
                rows_per_page = data['rows_per_page']
                max_rows = data['max_rows']
                storage_format_version = data['storage_format_version']
                return Database(database, rows_per_page, max_rows,
                                storage_format_version)
        else:
            raise Exception("Database {} does not exist".format(database))

    def create_table(self, table: str, columns: any,
                     rows_per_page: int = None,
                     max_rows: int = None):
        # Convert all column names to lowercase
        # columns = [element.lower() for element in columns]  # logic moved to Table.create_table()
        if rows_per_page is None:
            rows_per_page = self.rows_per_page
        max_rows = max_rows if max_rows is not None else self.max_rows
        Table.create_table(self, table.lower(), columns, rows_per_page,
                           max_rows=self.max_rows)

    def open_table(self, table_name: str):
        return Table.open_table(self, table_name)

    def list_tables(self) -> list:
        """
        Get the list of available tables
        """
        tables_list = []
        for item in os.listdir(self.path):
            if dir_exists("{}/{}".format(self.path, item)):
                tables_list.append(item)
        return tables_list

    @staticmethod
    def exist(database: str) -> bool:
        return True if dir_exists(database) else False


class Table:
    def __init__(self, database: str, table: str,
                 columns: list, rows_per_page: int,
                 max_rows: int):
        self.database = database
        self.name = table.lower()
        self.columns = columns
        self.rows_per_page = rows_per_page
        self.max_rows = max_rows
        self.path = '{}/{}'.format(database.path, table)
        self.current_row = self.__calculate_current_row()

        # TODO: validate and self-heal to recover from data corruption

    @staticmethod
    def create_table(database, table: str, columns: any,
                     rows_per_page: int = None, max_rows: int = None):
        """
        Create a table in a database that already exists.
        Takes string input for table name and a comma seperated list
        for column names.
        """
        # Inherit rows_per_page and max_rows from database metadata
        if rows_per_page is None:
            rows_per_page = int(database.rows_per_page)
        max_rows = int(database.max_rows) if max_rows is None else max_rows

        table_folder = "{}/{}".format(database.path, table)
        # Check if table already exists, if it doesn't, then proceed,
        # ortherwise throw an error.
        if not dir_exists(table_folder):
            # Add our hard-coded meta-ida to the beginning of the
            # column name variable.
            # columns.insert(0, "meta_id")
            # create the table json file and populate it.
            data = {
                'settings': {
                    'rows_per_page': rows_per_page,
                    'max_rows': max_rows,
                },
                'columns': {}
            }
            
            # dictionary style columns declaration, all types default to str
            if(isinstance(columns, list)):
                for col in columns:
                    data['columns'][col.lower()] = {'data_type': 'str', 'max_length': 10000}

            # dictionary style columns declaration, together with types
            elif(isinstance(columns, dict)):
                for col in columns:
                    if(columns[col].__name__ == 'str'):
                        data['columns'][col.lower()] = {'data_type': 'str', 'max_length': 10000}
                    elif(columns[col].__name__ in ["int", "float", "bool"]):
                        data['columns'][col.lower()] = {'data_type': columns[col].__name__}
                    else:
                        raise Exception("Data type '{}' for column '{}' is not suported".format(
                            columns[col].__name__, col))
            else:
                raise Exception("Columns definition is incorrect")
            os.mkdir(table_folder)
            with open('{}/definition.json'.format(table_folder), 'w') as f:    
                f.write(json.dumps(data))
                return Table(database, table, columns, rows_per_page, max_rows)
        else:
            raise Exception("Table {} already exists".format(table))

    @staticmethod
    def open_table(database, table: str):
        path = '{}/{}'.format(database.path, table)
        if dir_exists(path):
            with open('{}/definition.json'.format(path)) as json_file:
                definition = json.load(json_file)
            # Check to make sure there are not any temporary files left over
            # from previous session.
            for file_name in os.listdir(path):
                if file_name[-4:] in ['temp', 'vacu']:
                    raise Exception("Some temporary data page files are still"
                                    " in your table. Delete temp and vacu files manually")
            return Table(database, table, definition['columns'],
                         definition['settings']['rows_per_page'],
                         definition['settings']['max_rows'])
        else:
            raise Exception("Table {} does not exist in {}".format(
                table, database.path))

    def stats(self) -> dict:
        with open("{}/definition.json".format(self.path)) as json_file:
            definition = json.load(json_file)
            table_size = 0
            for entry in os.ilistdir(self.path):
                if not entry[0] == 'definition.json':
                    table_size +=entry[3]
        return {
            'Settings': definition['settings'],
            'Columns': definition['columns'],
            'Pages_Count': len(os.listdir(self.path)) - 1,
            'Current_row': self.__calculate_current_row(),
            'Data_Size' : table_size
        }

    def insert(self, data: any) -> bool:
        """
        Inserts new data in a table
        """
        # Check for multiple row insert and prepare for each
        if isinstance(data, list) and isinstance(data[0], dict):
            total = len(data) - 1
            new_data = data

            if isinstance(data[0], dict):
                for x in range(len(new_data)):
                    if self.__scrub_data(new_data[x]):
                        pass
                    else:
                        raise Exception("Data element {} is not formatted correctly".format(x))

            # while we still have data to insert
            while total > 0:
                current_line = self.__row_id_in_file(self.current_row)+1
                # calculate how many lines we will insert on the first loop
                insert_number = int(self.rows_per_page) - int(current_line)
                if insert_number == 0:
                    insert_number = int(self.rows_per_page)
                # Check that we aren't at max rows:
                if self.current_row + insert_number > self.max_rows:
                    raise Exception("Table {} can not fit all those"
                                    " rows".format(self.name))
                # populate first_data based on how many total rows are being
                # inserted and how much room is on data page
                if insert_number < total:
                    # grab how many we need to fill the current data page
                    first_data = data[:insert_number]
                    del data[:insert_number]
                else:
                    first_data = data
                # record how many rows we are inserting this time:
                number_rows_to_insert = len(first_data)
                # prepare data
                first_data_string = ''
                first_path = self.__data_file_for_row_id(
                    int(self.current_row) + 1)
                for x in range(len(first_data)):
                    self.current_row += 1
                    first_data_string = "{0}{{\"r\": {1}, \"d\": {2}}}\n" \
                        .format(first_data_string,str(self.current_row),
                            json.dumps(first_data[x]))
                if not self.__multi_append_row(first_data_string, first_path):
                    raise Exception("There was a problem inserting "
                                    "multiple rows")
                if not self.__is_multi_insert_success(first_data_string,
                                                        first_path,
                                                        number_rows_to_insert,
                                                        current_line):
                    raise Exception("There was a problem validating the "
                                    "write during multiple row insert")
                total -= insert_number
            return True
        # If not multi-insert
        else:
            data = self.__scrub_data(data)
            if data:
                self.current_row += 1
                row_id = self.current_row
                path = self.__data_file_for_row_id(row_id)
                # Check that we aren't at max rows:
                if self.current_row < self.max_rows:
                    if self.__insert_modify_data_file(path, data):
                        return True
                    else:
                        raise Exception("There was a problem inserting "
                                        "row at {}".format(row_id))
                else:
                    raise Exception("Table {} is full".format(self.name))
            else:
                raise Exception("Data you tried to insert is invalid")

    def update(self, query_conditions: dict, data: dict):
        """
        Update data based on matched query
        """
        matched_queries = self.__return_query('update', query_conditions)
        if matched_queries is None:
            raise Exception("Query did not match any data")
        else:
            # Loop through and update each row where the query returned true
            for row_id in matched_queries:
                # Check to make sure all the column names given by user match
                # the column names in the table.
                self.update_row(row_id, data)

    def update_row(self, row_id: int, update_data: any):
        """
        Update data based on row_id.
        """
        # get data from database and update with user provided
        combined = self.find_row(row_id)["d"]
        combined.update(update_data)

        # Check to make sure all the column names given by user
        # match the column names in the table.
        data = self.__scrub_data(combined, False)
        path = self.__data_file_for_row_id(row_id)
        if data:
            # Create a temp data file with the updated row data.
            if self.__modify_data_file(path, {row_id: data}, 'update'):
                pass
            else:
                raise Exception("There was a problem updating "
                                "row at {}".format(row_id))
        else:
            raise Exception("Data you tried to insert is invalid")

    def delete(self, query_conditions: dict) -> bool:
        """
        Delete row based on query search.
        """
        matched_queries = self.__return_query('delete', query_conditions)
        if matched_queries is None:
            raise Exception("Query did not match any data")
        else:
            # Loop through and update each row where the query returned true
            for found_row in matched_queries:
                row_id = found_row['r']
                self.delete_row(row_id)
            return True

    def delete_row(self, row_id: int):
        """
        Delete data based on row_id.
        """
        if self.__modify_data_file(self.__data_file_for_row_id(row_id),
                                   {row_id: None}, 'delete'):
            pass
        else:
            raise Exception("There was a problem deleting "
                            "row at {}".format(row_id))

    def truncate(self):
        """
        Nuke all data in the table.
        """
        for file_name in os.listdir(self.path):
            if file_name[0:4] == 'data':
                os.remove('{}/{}'.format(self.path, file_name))
        self.current_row = 0

    def find_row(self, row_id: int):
        """
        Find data based on row_id.
        """
        # Calculate what line in the file the row_id will be found at
        looking_for_line = self.__row_id_in_file(row_id)
        # Prevous method of counting lines is not reliable
        if looking_for_line is not None:
            with open(self.__data_file_for_row_id(row_id), 'r') as f:
                for current_line, line in enumerate(f):
                    if current_line == looking_for_line:
                        return json.loads(line)
        else:
            raise Exception("Could not find row_id {}".format(row_id))

    def query(self, queries: dict, show_row: bool = False):
        """
        Search through the whole table and return all rows where column
        data matches searched for value.
        """
        final_result = []
        results = self.__return_query('query', queries, show_row)
        if results is None:
            return []
        else:
            if len(results) > 1:
                for result in results:
                    final_result.append(result)
            else:
                final_result = results
        return final_result

    def find(self, queries: dict, show_row: bool = False):
        """
        Search through the whole table and return first row where column
        data matches searched for value.
        """
        return self.__return_query('find', queries, show_row)

    def scan(self, queries: any = None, show_row: bool = False):
        """
        Iterate through the whole table and return data by line
        """
        if queries:
            queries = self.__scrub_data(queries, False)
        location = os.listdir(self.path)
        # Remove non-data files from our list of dirs.
        location = [element for element in location if 'data' in element]
        # Sort as integers so we get them in the right order
        # (not reversed because we want smallest first).
        location = sorted(location, key=lambda x:
                          int(x.split('.')[0].split('_')[1]))
        for f in location:
            with open("{}/{}".format(self.path, f), 'r') as data:
                for line in data:
                    if line != "\n":    # empty lines fails to json.loads()
                        current_data = json.loads(line)
                        # If we are not searching for anything
                        if not queries:
                            if show_row:
                                current_data['d']['_row'] = current_data['r']
                            yield current_data['d']
                        else:
                            for query in queries:
                                if current_data['d'][query] == queries[query]:
                                    if show_row:
                                        current_data['d']['_row'] = current_data['r']
                                    yield current_data['d']
                                else:
                                    break

    def vacuum(self) -> bool:
        """
        This will reorganize your data files- remove spaces after records has
        been deleted
        NOTE: this also change row ID of your data
        """
        location = os.listdir(self.path)
        # Remove non-data files from our list of dirs.
        location = [element for element in location if 'data' in element]
        # Sort as integers so we get them in the right order.
        location = sorted(location,
                          key=lambda x: int(x.split('.')[0].split('_')[1]),
                          reverse=False)
        for f in location:
            os.rename('{}/{}'.format(self.path, f),
                      '{}/{}.vacu'.format(self.path, f))
        # Reset row id counter
        self.current_row = 0
        for f in location:
            with open("{}/{}.vacu".format(self.path, f), 'r') as data:
                for line in data:
                    if line != '\n':
                        current_data = json.loads(line)
                        self.insert(current_data['d'])
            # delete temporary files
            os.remove('{}/{}.vacu'.format(self.path, f))
        return True

    def drop(self):
        for filename in os.ilistdir(self.path):
            location = "{}/{}".format(self.path, filename[0])
            os.remove(location)
        os.remove(self.path)
    
    def __return_query(self, search_type: str, queries: any = None, show_row: bool = False) -> list:
        """
        Helper function to process a query and return the result.
        """
        if queries:
            queries = self.__scrub_data(queries, False)

        for query in queries:
            if type(queries[query]) is not list:
                queries[query] = [queries[query]]

        # different handling logic must be performed if there are mutiple keys
        multiple_keys = True if len(list(queries.keys())) > 1 else False
        result = []
        location = os.listdir(self.path)
        # Remove non-data files from our list of dirs.
        location = [element for element in location if 'data' in element]
        # Sort as integers so we get them in the right order.
        location = sorted(location,
                          key=lambda x: int(x.split('.')[0].split('_')[1]),
                          reverse=True)
        found = False
        for f in location:
            with open("{}/{}".format(self.path, f), 'r') as data:
                for line in data:
                    # Make sure the line isn't blank (ex. if it was deleted).
                    if line != '\n':
                        found = False
                        cur_data = json.loads(line)
                        for query in queries:
                            if query in cur_data['d'].keys() and \
                                    cur_data['d'][query] in queries[query]:
                                found = True
                                if not multiple_keys:
                                    break
                            else:
                                found = False
                                if multiple_keys:
                                    break
                        if found:
                            if show_row:
                                cur_data['d']['_row'] = cur_data['r']
                            if search_type == 'find':
                                return cur_data['d']
                            elif search_type == 'query':
                                result.append(cur_data['d'])
                            # for delete and update commands- only row id is needed
                            elif search_type in ['update', 'delete']:
                                result.append(cur_data['r'])
        if result:
            return result
        else:
            return None
    


    # def __check_write_success(self, data, page: str, method: str) -> bool:
    #     """
    #     Checks to make sure the previous update or delete was successful.
    #     """
    #     # Calculate what line will have the row we are looking for.
    #     looking_for_line = self.__row_id_in_file(list(data)[0])
    #     row_id = list(data)[0]
    #     # open file at path
    #     with open(page, 'r') as f:
    #         for current_line, line in enumerate(f):
    #             if current_line == looking_for_line:
    #                 if method == "update":
    #                     json_line = json.loads(line)
    #                     if json_line['r'] == row_id and \
    #                             json_line['d'] == data[row_id]:
    #                         return True
    #                 elif method == "delete":
    #                     if line == "\n":
    #                         return True
    #     # There was a problem writing, so return false
    #     return False

    # def __check_write_success_insert(self, data: dict, page: str) -> bool:
    #     """
    #     Checks to make sure the previous insert was successful.
    #     """
    #     last_line = None
    #     with open(page, 'r') as f:
    #         for line in f:
    #             if len(line) > 1:
    #                 last_line = line
    #         if last_line:
    #             json_line = json.loads(last_line)
    #             if data['r'] == json_line['r'] and data['d'] == json_line['d']:
    #                 return True
    #     return False

    def __is_multi_insert_success(self, data: dict, page: str,
                                  rows_added: int, start_line: int) -> bool:
        """
        Checks to make sure the previous insert was successful.
        """
        line_counter = 1
        # if we were at the end of a data page, then start at the first
        # line of the file
        if int(start_line) == int(self.rows_per_page):
            start_line = 0
        temp_data = ''
        with open(page, 'r') as f:
            for line in f:
                # check if we are past the line we started the insert at in
                # the data page
                if line_counter > start_line:
                    # If we are, then add the line to our temp_data so we
                    # can compare it with the data insert
                    temp_data = '{}{}'.format(temp_data, line)
                line_counter += 1
        if temp_data == data:
            return True
        return False

    def __multi_append_row(self, data: dict, page: str) -> bool:
        """
        This function assumes the data has already been scrubbed!
        """
        # Write the row to the data page file ('a' positions the stream at the
        # end of the file).
        # temp_current_row = self.current_row
        with open(page, 'a+') as f:
            f.write(data)
        return True

    def __append_row(self, data: dict, page: str) -> bool:
        """
        This function assumes the data has already been scrubbed!
        """
        # Write the row to the data page file ('a' positions the stream at the
        # end of the file).
        with open(page, 'a+') as f:
            f.write(json.dumps({'r': self.current_row, 'd': data}))
            f.write('\n')
        return True
        # if self.__check_write_success_insert(new_data, page):
        #     return True
        # else:
        #     raise Exception("Data was corrupted at row: {}".format(
        #         self.current_row))
        #     return False

    def __calculate_current_row(self) -> int:
        """
        We don't want to write table metadata to disk every insert,
        so just find it when we open the table and keep it in memory.
        """
        # last_data_file = None
        location = os.listdir(self.path)
        # Remove non-data files from our list of dirs.
        location = [element for element in location if 'data' in element]
        # Sort as integers so we get them in the right order.
        location = sorted(location,
                          key=lambda x: int(x.split('.')[0].split('_')[1]),
                          reverse=True)
        for f in location:
            if f[0:4] == 'data':
                last_line = None
                with open("{}/{}".format(self.path, f), 'r') as f:
                    for line in f:
                        if len(line) > 1:
                            last_line = line
                if last_line:
                    return json.loads(last_line)['r']

        return 0

    def __insert_modify_data_file(self, page: str, data: dict = None,
                                  fast: bool = True) -> bool:
        """
        Insert data page write helper
        """
        if fast:
            if self.__append_row(data, page):
                return True
        else:
            # not recomended, becouse its heavy on flash storage!
            # First we copy the data page file to a temp file if
            # the data page file exists
            temp_path = "{}.temp".format(page)
            piece_size = 512    # 512 bytes at a time
            if file_exists(page):
                with open(page, 'rb') as in_file,\
                        open(temp_path, 'wb') as out_file:
                    while True:
                        piece = in_file.read(piece_size)
                        if piece == b'':
                            break    # end of file
                        out_file.write(piece)

            elif not file_exists(page):
                open(temp_path, 'w').close

            if self.__append_row(data, temp_path):
                # make it compatible with PC and
                try:
                    if os.path.exists(page):
                        os.remove(page)
                except AttributeError:
                    pass
                os.rename(temp_path, page)
        return True

    def __modify_data_file(self, path: str, update_data, action: str) -> bool:
        """
        Creates a duplicate of a data file in the same path and appends
        ".temp". You also can specify whether or not to replace certain rows
        with new data. update_data = {334: {name: John}} will update the
        "name" field of row 334 update_data = {334: None} will delete row 334
        """
        # Check that data page file exists
        if not file_exists(path):
            raise Exception("Cannot {} a row that "
                            "does not exist".format(action))

        temp_path = "{}.temp".format(path)
        current_data = ''
        row_id = next(iter(update_data))
        # Open the master data page file
        with open(path, 'r') as input_file:
            # Create a temporary data page file
            with open(temp_path, 'w') as output:
                for line_num, line in enumerate(input_file):
                    if line != "\n":
                        current_data = json.loads(line)
                        # If this is our line
                        if current_data['r'] == row_id:
                            # Write the modified line to the file
                            if action == 'delete':
                                # output.write('\n')
                                pass
                            # If we are updating a row:
                            else:
                                if current_data['r'] == row_id:
                                    current_data['d'].update(update_data[row_id])
                                    output.write(json.dumps(current_data))
                                    output.write('\n')
                                else:
                                    raise Exception("Woah we thought {} was row_id"
                                                        " {} and almost stomped the "
                                                        "wrong row's data!".format(
                                                            line, line_num))
                        # Otherwise, write the line to the file as-is, skipping empty lines
                        else:
                            output.write(line)
        # imposible to check as we have ommited empty lines
        os.remove(path)
        os.rename(temp_path, path)
        return True


    def __data_file_for_row_id(self, row_id: int) -> str:
        """
        Calculate what data file we are currently in and return the path
        to that file.
        """
        file_row_id = int(row_id) % int(self.rows_per_page)
        if file_row_id == 0:
            second_number = (int(row_id) // int(self.rows_per_page)) *\
                int(self.rows_per_page)
            first_number = second_number - int(self.rows_per_page) + 1
        else:
            first_number = (int(row_id) // int(self.rows_per_page)) *\
                int(self.rows_per_page) + 1
            second_number = first_number + int(self.rows_per_page) - 1
        return '{}/data{}_{}.dat'.format(self.path, first_number,
                                         second_number)

    def __row_id_in_file(self, row_id: int) -> int:
        """
        Calculates the line in a data page file that row will be found at.
        """
        # if our table doesn't have any rows yet
        if row_id == 0:
            return 0
        else:
            with open(self.__data_file_for_row_id(row_id), 'r') as f:
                for line_num, line in enumerate(f):
                    if line != "\n":
                        if int(json.loads(line)['r']) == int(row_id):
                            return line_num
        return None

    def __scrub_data(self, data: any, fill_missing: bool = True):
        """
        Check to see if user data input contains valid column data for
        current table.
        a) Validates column names actually exist in table
        b) Fills in null values for missing columns.
        c) Downcases column names.
        """
        all_columns = list(self.columns.keys())
        result = {}
        column = None

        # if received list of values - make a dict with fields
        if(isinstance(data, list)):
            ndata = {}
            for k, v in zip(self.columns, data):
                ndata[k]=v
            data = ndata
            del(ndata)
        try:
            for column, value in data.items():
                column = column.lower()
                # column_definition = self.columns[column]
                # validate type/length
                if(self.columns[column]['data_type'] == "str" and isinstance(value, str)):
                    if(len(value)>self.columns[column]["max_length"]):
                        raise Exception("Max_length of {} exceeded for {} ".format(
                            self.columns[column]['max_length'], column))
                elif(self.columns[column]['data_type'] == "int" and isinstance(value, int)):
                    pass
                elif(self.columns[column]['data_type'] == "float" and isinstance(value, float)):
                    pass
                elif(self.columns[column]['data_type'] == "bool" and isinstance(value, bool)):
                    pass
                else:
                    raise Exception("Data types mismath: Expected <class '{}'> but received {} for column '{}'".format(
                        self.columns[column]['data_type'], type(value), column))

                all_columns.remove(column)
                result[column] = value
        except KeyError:
            raise Exception("Column {} does not exist in {}".format(
                column, self.name))

        # now add default values
        if fill_missing:
            for lefover_column in all_columns:
                result[lefover_column] = None

        return result
