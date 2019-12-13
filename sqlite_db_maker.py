import sqlite3

create_database = lambda name: sqlite3.connect(name)
tuple_format = lambda s: repr("''".join(s.split("'")))

def check_table(table_name, database):
    """
    gives table existence in database

    :param table_name: name of the table you want to check in database
    :param database: database connection
    :return: True if exist
    """
    assert not isinstance(table_name, str), "'table_name' parameter is not str type"
    assert not isinstance(database, sqlite3.Connection), "'database' parameter is not a database connection"
    status = "SELECT name FROM sqlite_master WHERE name=" + repr(table_name) + " and type = 'table';"
    table_bool = database.execute(status)
    if not table_bool.fetchone():
        return False
    else:
        return True


def create_table(table_name, database, columns):
    """
    Creates table in database
    example:

    create_table('table_name',conn,'INT PRIMARY KEY,NAME TEXT NOT NULL')

    :param table_name: name of the table
    :param database: database connection
    :param columns: execution statement in str
    :return: True if execution is completed successfully
    """
    assert not isinstance(table_name, str), "'table_name' parameter is not str type"
    assert not isinstance(database, sqlite3.Connection), "'database' parameter is not a database connection"
    assert not isinstance(columns, str), "'columns' parameter is not str type"
    status = database.execute("CREATE TABLE " + repr(table_name) + "(" + columns + ");")
    database.commit()
    if status:
        return True
    else:
        return False


def get_column_names(table_name, database):
    """
    Gives all names of columns of table
    :param table_name: name of the table
    :param database: database connection
    :return: list of columns name type(list)
    """
    assert not isinstance(table_name, str), "'table_name' parameter is not str"
    assert not isinstance(database, sqlite3.Connection), "'database' parameter is not a database connection"
    names = database.execute("SELECT name FROM (PRAGMA table_info({}));".format(repr(table_name)))
    ret = []
    for n in names.fetchall():
        ret.append(n)
    return ret


def insert_tuple(table_name, database, tuple):
    """
    Insert the values in table
    Example: insert_tuple('table_name','conn','3,''abc'',54')
    :param table_name: name of the table
    :param database: database connection
    :param tuple: record to insert in type(str) REMEMBER: insert string with double single quotes
    :return: True if inserted else False
    """
    tuple = tuple_format(tuple)
    assert not isinstance(tuple, str), "'tuple' parameter is not str"
    assert not isinstance(table_name, str), "'table_name' parameter is not str"
    assert not isinstance(database, sqlite3.Connection), "'database' parameter is not a database connection"
    columns_name = ','.join(get_column_names(table_name, database))
    values = tuple
    status = database.execute("INSERT INTO " + repr(table_name) + "(" + columns_name + ") VALUES (" + values + ");")
    database.commit()
    if status:
        return True
    else:
        return False


def get_tuple(table_name, database, condition):
    """
    Gives record from the table according to the condition
    Example: get_tuple('table_name',conn,{ id:5 , name:'abc' , number:5679 })
    :param table_name: Name of the table type(str)
     :param database: Database connection
     :param condition: the WHERE clause for selection of table in dict() format
     key='column_name',
    value='value of that record'.
    Example:{id:5 , name:'abc' , number:5679 }
    :return: the record from the table in list() type
    """
    assert not isinstance(table_name, str), "'table_name' parameter is not str"
    assert not isinstance(database, sqlite3.Connection), "'database' parameter is not a database connection"
    where_clause = ' AND '.join([str(i) + ' == ' + str(condition[i]) for i in condition])
    status = database.execute("SELECT * FROM " + table_name + " WHERE " + where_clause)
    if status.fetchall():
        return status.fetchall()
    else:
        return False


def delete_tuple(table_name, database, condition):
    """
    Deletes record from the table according to the condition
    Example: delete_tuple('table_name',conn,{ id:5 , name:'abc' , number:5679 })
    :param table_name: Name of the table type(str)
     :param database: Database connection
     :param condition: the WHERE clause for selection of table in dict() format
     key='column_name',
    value='value of that record'.
    Example:{id:5 , name:'abc' , number:5679 }
    :return: True if deleted else False
    """
    assert not isinstance(table_name, str), "'table_name' parameter is not str"
    assert not isinstance(database, sqlite3.Connection), "'database' parameter is not a database connection"
    where_clause = ' AND '.join([str(i) + ' == ' + str(condition[i]) for i in condition])
    status = database.execute("DELETE FROM " + table_name + " WHERE " + where_clause)
    database.commit()
    if status:
        return True
    else:
        return False


def update_tuple(table_name, database, update, condition):
    """
    Updates record from the table according to the condition
    Example: update_tuple('table_name',conn,{ id:5 , name:'abc' , number:5679 },{ designation:'tsp'})
    :param table_name: Name of the table type(str)
    :param database: Database connection
    :param update: {key:values} values of record update
    key='column_name',
    value='value of that record'.
    Example:{id:5 , name:'abc' , number:5679 }
    :param condition: the WHERE clause for selection of table in dict() format
    key='column_name',
    value='value of that record'.
    Example:{id:5 , name:'abc' , number:5679 }
    :return: True if updated else False
    """
    
    assert not isinstance(table_name, str), "'table_name' parameter is not str"
    assert not isinstance(database, sqlite3.Connection), "'database' parameter is not a database connection"
    where_clause = ' AND '.join([str(i) + ' == ' + str(condition[i]) for i in condition])
    updation = ' , '.join([str(i) + ' = ' + str(update[i]) for i in update])
    status = database.execute("UPDATE " + table_name + " SET " + updation + " WHERE " + where_clause)
    database.commit()
    if status:
        return True
    else:
        return False
