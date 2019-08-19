"""Low-memory json-based databse for MicroPython.
Data is stored in a folder structure in json for easy inspection.
Indexing multiple columns is supported, and RAM usage is optimized for embedded systems.
Database examples:
db_object = Database.create("mydb")
db_object = Database.open("mydb")
Table examples:
db_table = db_object.create_table("mytable", ["name", "password"])
db_table = db_object.open_table("mytable")
db_table.truncate()
Insert examples:
for x in range(550):
   db_table.insert({"name": "nate", "password": "coolpassword"})
Low-level operations using internal row_id:
db_table.find_row(5)
db_table.update_row(300, {'name': 'bob'})
db_table.delete_row(445)
"""
import ujson as json
import os
import uerrno as errno

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
        f = open(path, "r")
        exists = True
        f.close()
    except OSError:
        exists = False
    return exists

def dir_exists(path):
    try:
        f = os.listdir(path)
        if f != []:
            exists = True
        else:
            exists = False
    except OSError:
        exists = False
    return exists

class Database:
    def __init__(self, path, rows_per_page, max_rows, storage_format_version):
        self.path = path
        self.rows_per_page = rows_per_page
        self.max_rows = max_rows
        self.storage_format_version = storage_format_version

        if not dir_exists(self.path):
            raise Exception("Sorry, that database not found at " + self.path) 

    @staticmethod
    def create(path, rows_per_page = "10", max_rows = "10000"):
        #store the current version of the MicroPyDB to prevent future upgrade errors.
        version = "1"
        #Check if database already exists.
        if not dir_exists(path):
            os.mkdir(path)
            #If database doesn't exist, create the directory structure and the Schema json file with
            #default values.
            with open(path + '/schema.json', 'w') as f:
                data = {
                    "max_rows": max_rows,
                    "storage_format_version": version,
                    "rows_per_page": rows_per_page
                    }
                f.write(json.dumps(data))
                print('Database \"' + path + '\" created successfully.')
                return Database(path, rows_per_page, max_rows, version)
        else:
            raise Exception('Sorry, that database \"' + path + '\" is already in use. Please choose another name.')

    @staticmethod
    def open(path):
        #Check if database exists.
        schema_path = path + '/schema.json'
        if file_exists(schema_path):
            with open(schema_path) as json_file:
                data = json.load(json_file)
                rows_per_page = data['rows_per_page']
                max_rows = data['max_rows']
                storage_format_version = data['storage_format_version']
                return Database(path, rows_per_page, max_rows, storage_format_version)
        else:
            raise Exception('Sorry, the database, \"' + path + '\" doesn\'t exist. Please try again.')

    def create_table(self, table_name, column_names, rows_per_page = None):
        #Convert all column names to lowercase
        column_names = [element.lower() for element in column_names]
        if rows_per_page == None:
            rows_per_page = self.rows_per_page
        Table.create_table(self, table_name.lower(), column_names, rows_per_page = self.rows_per_page)
    
    def open_table(self, table_name):
        return Table.open_table(self, table_name)
    
        
class Table:
    def __init__(self, database, name, columns, rows_per_page, max_rows):
        self.database = database
        self.name = name.lower()
        self.columns = columns
        self.rows_per_page = rows_per_page
        self.max_rows = max_rows
        self.path = database.path + '/' + name
        self.current_row = self.__calculate_current_row()

        #TODO: validate and self-heal to recover from data corruption

    @staticmethod
    def create_table(database, name, columns, rows_per_page = 10, max_rows = 10000):
        """
        Create a table in a database that already exists.
        Takes string input for table name and a comma seperated list for column names.
        """
        table_folder = database.path + '/' + name
        #Check if table already exists, if it doesn't, then proceed, ortherwise throw an error.
        if not dir_exists(table_folder):
            #Add our hard-coded meta-ida to the beginning of the column name variable.
            #columns.insert(0, "meta_id")
            #create the table json file and populate it.
            os.mkdir(table_folder)

            current_row = 0
            with open(table_folder + '/definition.json', 'w') as f:
                data = {
                    "settings": {
                        "rows_per_page": rows_per_page,
                        "max_rows": max_rows,                    
                        },
                    "columns": {}
                }
                for x in range(len(columns)):
                    data["columns"][columns[x]] = {"data_type": "str", "max_length": 10000}
                f.write(json.dumps(data))
                print('Table \"' + name + '\" created succuessfully.')

                return Table(database, name, columns, rows_per_page, max_rows)
        else:
            raise Exception('Sorry, the table, \"' + name + '\" already exists. Please choose another name.')
        
    @staticmethod
    def open_table(database, name):
        path = database.path + '/' + name
        if dir_exists(path):
            with open(path + '/definition.json') as json_file:
                table = json.load(json_file)
            #Check to make sure there are not any temporary files left over from previous session.
            for file_name in os.listdir(path):
                if file_name[-4:] == 'temp':
                    raise Exception("Some temporary data page files are still in your table.")
            return Table(database, name, table['columns'], table['settings']['rows_per_page'], table['settings']['max_rows'])
        else:
            raise Exception('Sorry, the table, \"' + name + '\" does not exist in the database, \"' + database.name + '\". Please try again.')

    def insert(self, data):
        """
        Inserts new data in a table
        """
        #Check for multiple row insert and prepare for each
        if isinstance(data, list):
            i = 0
            total = len(data) - 1
            static_total = total
            new_data = data
            multi = True
            inserted = 0
            for x in range(len(new_data)):
                if self.__scrub_data(new_data[x]):
                    pass
                else:
                    raise Exception('Your insert data is not formatted correctly.')
        #Not multi-insert
        else:
            multi = False
        #if we are doing a multi insert
        if multi:
            #while we still have data to insert
            while total > 0:
                current_line = self.__row_id_in_file(self.current_row)
                #calculate how many lines we will insert on the first loop
                insert_number = int(self.rows_per_page) - int(current_line)
                if insert_number == 0:
                    insert_number = int(self.rows_per_page)
                #Check that we aren't at max rows:
                if self.current_row + insert_number > self.max_rows:
                    raise Exception('Sorry, the table, \"' + self.name + '\" can\'t fit all those rows.')
                #populate first_data based on how many total rows are being inserted and how much room is on data page
                if insert_number < total:
                    #grab how many we need to fill the current data page
                    first_data = data[:insert_number]
                    del data[:insert_number]
                else:
                    first_data = data
                #record how many rows we are inserting this time:
                number_rows_to_insert = len(first_data)
                #prepare data
                first_data_string = ''
                first_path = self.__data_file_for_row_id(int(self.current_row) + 1)
                for x in range(len(first_data)):
                    self.current_row += 1
                    first_data_string = first_data_string + "{\"data\": " + json.dumps(first_data[x]) + ", \"row_id\": " + str(self.current_row) + "}\n"
                if self.__multi_append_row(first_data_string, first_path):
                    if self.__check_write_success_multi_insert(first_data_string, first_path, number_rows_to_insert, current_line):
                        pass
                    else:
                        raise Exception("There was a problem validating the write during multiple row insert.")
                else:
                    raise Exception("There was a problem inserting multiple rows.")
                total -= insert_number
            print(str(static_total) + ' rows have been added.')
        #If not multi-insert
        else:
            self.current_row += 1
            if self.__scrub_data(data):
                row_id = self.current_row
                path = self.__data_file_for_row_id(row_id)
                #Check that we aren't at max rows:
                if self.current_row < self.max_rows:
                    if self.__insert_modify_data_file(path, data):
                        print('Row ' + str(row_id) + ' has been added.')
                    else:
                        raise Exception('There was a problem inserting row at ' + str(row_id) +'.')
                else:
                    raise Exception('Sorry, the table, \"' + self.name + '\" is full.')
            else:
                raise Exception('Sorry, the data you tried to insert is invalid.')

    def update(self, query_conditions, cols_vals_to_update):
        """
        Update data based on matched query
        """
        matched_queries = self.__return_query('query', query_conditions)
        if matched_queries == None:
            raise Exception('Sorry, your query did not match any data.')
        else:
            #Loop through and update each row where the query returned true
            for found_row in matched_queries:
                #Check to make sure all the column names given by user match the column names in the table.
                row_id = found_row['row_id']
                self.update_row(row_id, cols_vals_to_update)


    def update_row(self, row_id, update_data):
        """
        Update data based on row_id.
        """
        #Check to make sure all the column names given by user match the column names in the table.
        data = self.__scrub_data(update_data)
        path = self.__data_file_for_row_id(row_id)
        if data:
            #Create a temp data file with the updated row data.
            if self.__modify_data_file(path, {row_id: data}, 'update'):
                print('Row ' + str(row_id) + ' has been updated.')            
            else:
                raise Exception('There was a problem updating row at ' + str(row_id) +'.')
        else:
            raise Exception('Sorry, the data you tried to insert is invalid.')

    def delete(self, query_conditions):
        """
        Delete row based on query search.
        """
        matched_queries = self.__return_query('query', query_conditions)
        if matched_queries == None:
            raise Exception('Sorry, your query did not match any data.')
        else:
            #Loop through and update each row where the query returned true
            for found_row in matched_queries:
                row_id = found_row['row_id']
                self.delete_row(row_id)

    def delete_row(self, row_id):
        """
        Delete data based on row_id.
        """
        if self.__modify_data_file(self.__data_file_for_row_id(row_id), {row_id: None}, 'delete'):        
            print('Row ' + str(row_id) + ' has been deleted.')
        else:
            raise Exception('There was a problem deleting row at ' + str(row_id) +'.')

    def truncate(self):
        """
        Nuke all data in the table.
        """
        for file_name in os.listdir(self.path):
            if file_name[0:4] == 'data':
                os.remove(self.path + '/' + file_name)
        self.current_row = 0

    def find_row(self, row_id):
        """
        Find data based on row_id.        
        """
        #Calculate what line in the file the row_id will be found at
        looking_for_line = self.__row_id_in_file(row_id)

        #Initiate line-counter
        current_line = 1
        with open(self.__data_file_for_row_id(row_id), 'r') as f:
            for line in f:
                if current_line == looking_for_line:
                    return json.loads(line)
                current_line += 1

        raise Exception('Could not find row_id ' + row_id)

    def query(self, queries):
        """
        Search through the whole table and return all rows where column data matches
        searched for value.
        """
        final_result = []
        results = self.__return_query('query', queries)
        if results == None:
            return None
        else:
            if len(results) > 1:
                for result in results:
                    final_result.append(result['data'])
            else:
                final_result = results
        return final_result

    def find(self, queries):
        """
        Search through the whole table and return first row where column data matches
        searched for value.
        """
        return self.__return_query('find', queries)

    def scan(self, queries = None):
        """
        Iterate through the whole table and return data by line
        """
        if queries:
            queries = self.__scrub_data(queries)
        location = os.listdir(self.path)
        #Remove non-data files from our list of dirs.
        location = [element for element in location if 'data' in element]
        #Sort as integers so we get them in the right order (not reversed because we want smallest first).
        location = sorted(location, key=lambda x: int(x.split('.')[0].split('_')[1]))
        found = False
        while True:
            for f in location:
                with open(self.path + '/' + f, 'r') as data:
                    for line in data:
                        current_data = json.loads(line)
                        #If we are not searching for anything
                        if not queries:
                            yield current_data['data']
                        else:
                            found = False
                            for query in queries:
                                if current_data['data'][query] == queries[query]:
                                    found = True
                                else:
                                    found = False
                                    break
                        if found:
                            yield current_data['data']

    def __return_query(self, search_type, queries = None):
        """
        Helper function to process a query and return the result.
        """
        if queries:
            queries = self.__scrub_data(queries)
        result = []
        location = os.listdir(self.path)
        #Remove non-data files from our list of dirs.
        location = [element for element in location if 'data' in element]
        #Sort as integers so we get them in the right order.
        location = sorted(location, key=lambda x: int(x.split('.')[0].split('_')[1]), reverse = True)
        found = False
        for f in location:
            with open(self.path + '/' + f, 'r') as data:
                for line in data:
                    #Make sure the line isn't blank (ex. if it was deleted).
                    if line != '\n':
                        current_data = json.loads(line)
                        for query in queries:
                            if current_data['data'][query] == queries[query]:
                                found = True
                            else:
                                found = False
                                break
                        if found:
                            if search_type == 'find':
                                return current_data['data']
                            elif search_type == 'query':
                                result.append(current_data)
                    else:
                        break
        if result:
            return result
        else:
            return None

    def __check_write_success(self, update_data, path, method):
        """
        Checks to make sure the previous update or delete was successful.
        """
        #Initiate line-counter
        current_line = 1
        #Calculate what line will have the row we are looking for.
        looking_for_line = self.__row_id_in_file(list(update_data)[0])
        row_id = list(update_data)[0]
        #open file at path
        with open(path, 'r') as f:
            for line in f:
                if current_line == looking_for_line:
                    if method == 'update':
                        if json.loads(line)['row_id'] == row_id and json.loads(line)['data'] == update_data[row_id]:
                            return True
                    elif method == 'delete':
                        if line == '\n':
                            return True
                current_line += 1
        #There was a problem writing, so return false
        return False

    def __check_write_success_insert(self, data, path):
        """
        Checks to make sure the previous insert was successful.
        """
        with open(path, 'r') as f:
            for line in f:
                if len(line) > 1:
                    last_line = line
        if last_line:
            if data["row_id"] == json.loads(line)["row_id"] and data["data"] == json.loads(line)["data"]:
                return True
        return False

    def __check_write_success_multi_insert(self, data, path, rows_added, start_line):
        """
        Checks to make sure the previous insert was successful.
        """
        line_counter = 1
        #if we were at the end of a data page, then start at the first line of the file
        if int(start_line) == int(self.rows_per_page):
            start_line = 0
        temp_data = ''
        with open(path, 'r') as f:
            for line in f:
                #check if we are past the line we started the insert at in the data page
                if line_counter > start_line:
                    #If we are, then add the line to our temp_data so we can compare it with the data insert
                    temp_data = str(temp_data) + str(line)
                line_counter += 1
        if temp_data == data:
            return True
        return False

    def __multi_append_row(self, data, path):
        """
        This function assumes the data has already been scrubbed!
        """
        #Write the row to the data page file ('a' positions the stream at the end of the file).
        temp_current_row = self.current_row
        with open(path, 'a') as f:
            f.write(data)
        #if self.__check_write_success_insert(new_data, path):
        #    return True
        #else:
        #    print('Data was corrupted at row: ' + temp_current_row)
        #    return False
        return True

    def __append_row(self, data, path):
        """
        This function assumes the data has already been scrubbed!
        """
        #Write the row to the data page file ('a' positions the stream at the end of the file).
        temp_current_row = self.current_row
        with open(path, 'a') as f:
            new_data = { "row_id": temp_current_row, "data": data }
            f.write(json.dumps(new_data))
            f.write('\n')
        if self.__check_write_success_insert(new_data, path):
            return True
        else:
            print('Data was corrupted at row: ' + temp_current_row)
            return False

    def __calculate_current_row(self):
        """
        We don't want to write table metadata to disk every insert, 
        so just find it when we open the table and keep it in memory.
        """
        last_data_file = None
        location = os.listdir(self.path)
        #Remove non-data files from our list of dirs.
        location = [element for element in location if 'data' in element]
        #Sort as integers so we get them in the right order.
        location = sorted(location, key=lambda x: int(x.split('.')[0].split('_')[1]), reverse = True)
        for f in location:
            if f[0:4] == 'data':
                last_line = None
                with open(self.path + '/' + f, 'r') as f:
                    for line in f:
                        if len(line) > 1:
                            last_line = line
                if last_line:
                    return json.loads(line)['row_id']

        return 0
    
    def __insert_modify_data_file(self, path, update_data = None, method = None):
        """
        Insert data page write helper
        """
        #First we copy the data page file to a temp file if the data page file exists
        temp_path = path + '.temp'
        piece_size = 512 # 512 bytes at a time
        if file_exists(path):
            with open(path, 'rb') as in_file, open(temp_path, 'wb') as out_file:
                while True:
                    piece = in_file.read(piece_size)
            
                    if piece == b'':
                        break # end of file
    
                    out_file.write(piece)

        elif not file_exists(path):
            open(temp_path, 'w').close

        if self.__append_row(update_data, temp_path):
            os.rename(temp_path, path)
        return True



    def __modify_data_file(self, path, update_data, method):
        """
        Creates a duplicate of a data file in the same path and appends ".temp". 
        You also can specify whether or not to replace certain rows with new data.
        update_data = {334: {name: John}} will update the "name" field of row 334
        update_data = {334: None} will delete row 334
        """
        #Check that data page file exists
        if not file_exists(path):
            raise Exception('Cannot ' + method + ' a row that does not exist.')

        temp_path = path + '.temp'
        wrote_to_file = False
        current_data = ''
        current_line = 1
        file_row_offset = ''
        #Calculate the row offset for current data page file
        for number in path.split('/')[-1][4:]:
            if number in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
                file_row_offset = str(file_row_offset) + str(number)
            else:
                break
        file_row_offset = int(file_row_offset) - 1
        #Open the master data page file
        with open(path, 'r') as input_file:
            #Create a temporary data page file
            with open(temp_path, 'w') as output:
                for line in input_file:
                    current_row_id = current_line + file_row_offset
                    #If we are at the row we need to modify
                    if current_row_id in update_data: 
                        #If we are deleting a row:
                        if method == 'delete':
                            output.write('\n')
                            wrote_to_file = True
                        #If we are updating a row:
                        else:
                            current_data = json.loads(line)
                            if current_data['row_id'] == current_row_id:
                                #Find a match for the column name the user wants to update with new data.
                                for x in current_data['data']:
                                    for y in update_data[current_row_id]:
                                        #If the column the user specified equals the column in the current data
                                        if y == x:
                                            #Then update the data with new user input
                                            current_data['data'][x] = update_data[current_row_id][y]
                                #and write to temp file
                                output.write(json.dumps(current_data))
                                output.write('\n')
                                wrote_to_file = True
                            else:
                                raise Exception("Woah we thought " + line + " was row_id " + current_row_id + " and almost stomped the wrong row's data!")
                    #Else we are not at the row we are updating, so just copy the previous data to the temp file.
                    else:
                        output.write(line)
                    current_line += 1
        #If we performed an update, check to make sure we actually wrote the update to the temp file
        if wrote_to_file:
            if self.__check_write_success(update_data, temp_path, method):
                os.rename(temp_path, path)
                return True
            else:
                return False


    def __data_file_for_row_id(self, row_id):
        """Calculate what data file we are currently in and return the path to that file."""
        file_row_id = int(row_id) % int(self.rows_per_page)
        if file_row_id == 0:
            second_number = (int(row_id) // int(self.rows_per_page)) * int(self.rows_per_page)
            first_number = second_number - int(self.rows_per_page) + 1
        else:
            first_number = (int(row_id) // int(self.rows_per_page)) * int(self.rows_per_page) + 1
            second_number = first_number + int(self.rows_per_page) - 1
        path = self.path + '/data' + str(first_number) + '_' + str(second_number) + '.dat'
        return path

    def __row_id_in_file(self, row_id):
        """
        Calculates the line in a data page file that row will be found at.
        """
        #if our table doesn't have any rows yet
        if row_id == 0:
            return 0
        else:
            file_row_id = int(row_id) % int(self.rows_per_page)
            if file_row_id == 0:
                file_row_id =  int(file_row_id) + int(self.rows_per_page)
            return file_row_id

    def __scrub_data(self, data):
        """
        Check to see if user data input contains valid column data for current table.
        a) Validates column names actually exist in table
        b) Fills in null values for missing columns.
        c) Downcases column names.
        """
        all_columns = list(self.columns.keys())
        
        result = {}
        try:
            for column, value in data.items():
                column = column.lower()
                column_definition = self.columns[column]
                #TODO: validate type/length/etc
                all_columns.remove(column)
                result[column] = value
        except KeyError:
            raise Exception('Sorry, the column, \"' + column + '\" doesn\'t exist in the table, \"' + self.name + '\".')
        
        #now add default values
        for lefover_column in all_columns:
            result[lefover_column] = None

        return result