# -*- coding: utf-8 -*-
import mysql.connector
import json
from datetime import datetime

class dbBasic():
    def __init__(self, tbl_name, primary_key = "id"):
        file = open("/home/project/ductn/src/db_config.json", "r")
        content = file.read()
        file.close()
        json_content = json.loads(content)
        tbl_info = json_content[tbl_name]
        self.db_host = tbl_info["db_host"]
        self.db_user = tbl_info["db_user"]
        self.db_password = tbl_info["db_password"]
        self.db_database = tbl_info["db_name"]
        self.tbl = tbl_info["table_name"]
        self.pkey = primary_key

    def execute_sql(self, sql_string, fetch_mode = None, list_data = []):
        conn = mysql.connector.connect(
            host = self.db_host,
            user = self.db_user,
            password = self.db_password,
            database = self.db_database,
            charset="utf8mb4",
            use_unicode=True
        )
        cursor = conn.cursor(dictionary=True)
        error = False
        try:
            if not fetch_mode and len(list_data) > 0:
                cursor.execute(sql_string, tuple(list_data))
                conn.commit()
                return True
            else:
                cursor.execute(sql_string)
                if fetch_mode == 1:
                    results = cursor.fetchone()
                elif fetch_mode == 2:
                    results = cursor.fetchall()
        except Exception as err:
            error = err
            
        try:
            cursor.close()
            conn.close()
        except Exception as err:
            print('------ mysql error close')
            pass
        if error: return error
        else: return results
        
    def get_all(self, qry=None, field = "*", return_sql = False):
        sql = f"SELECT {field} FROM {self.tbl} WHERE {qry}"
        if return_sql: return sql
        return self.execute_sql(sql, 2)

    def count(self, qry, field="id"):
        sql = f"SELECT COUNT({field}) FROM {self.tbl} WHERE {qry}"
        results = self.execute_sql(sql, 1)
        for key in results:
            return int(results[key])
        return False
            
    def find(self, qry, field = "*", return_sql = False):
        try: 
            _id = int(qry)
            sql = f"SELECT {field} FROM {self.tbl} WHERE id = {_id} limit 1"
        except:
            sql = f"SELECT {field} FROM {self.tbl} WHERE {qry} limit 1"
        if return_sql: return sql
        results = self.execute_sql(sql, 1)
        return results

    def is_exist(self, qry):
        sql = f"SELECT EXISTS (SELECT {self.pkey} FROM {self.tbl} WHERE {qry} order by {self.pkey} DESC limit 1)"
        results = self.execute_sql(sql, 1)
        for key in results:
            if results[key] > 0:
                return True
            else:
                return False

    def insert(self, _dict, return_sql = False, created_at = False):
        if created_at:
            _dict["created_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        list_keys = _dict.keys()
        list_values = []
        list_column = ",".join(list_keys)
        value_string = ""
        for key in (list_keys):
            list_values.append(_dict[key])
            value_string += "%s,"
        value_string = value_string[:-1]
        sql = f"INSERT INTO {self.tbl} ({list_column}) VALUES ({value_string})"
        if return_sql: return(sql, list_values)
        return self.execute_sql(sql, list_data = list_values)

    def insert_many(self, _list, return_sql = False, created_at = False):
        _dict = dict(_list[0])
        list_values = []
        
        list_keys = _dict.keys()
        list_column = ",".join(list_keys) + ""
        if created_at:
            time_string = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            list_column += ",created_at"
        value_string = ""

        for _dict in _list:
            value_string += "("
            for key in (list_keys):
                list_values.append(_dict[key])
                value_string += "%s,"

            # add created_at
            if created_at:
                list_values.append(time_string)
                value_string += "%s),"
            else:
                value_string = value_string[:-1] + "),"

        value_string = value_string[:-1]
        sql = f"INSERT IGNORE INTO {self.tbl} ({list_column}) VALUES {value_string}"
        if return_sql: return(sql, list_values)
        return self.execute_sql(sql, list_data = list_values)

    def update(self, row_id, _dict, return_sql=False, updated_at=False):
        list_values = []
        if updated_at:
            _dict["updated_at"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        sql = f"UPDATE {self.tbl} SET "
        for key in _dict:
            value = _dict[key]
            list_values.append(value)
            sql += f"{key}=%s,"
        sql = sql[:-1] + f" WHERE {self.pkey} = {row_id}"
        if return_sql: return(sql)
        return self.execute_sql(sql, list_data=list_values)

    def update_many_same_value(self, update_column_name, update_column_value, column_name, list_column_value, return_sql = False, updated_at=False):
        updated_at_string = ""
        if updated_at:
            updated_at_string = f", updated_at = '{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}'"
        value_string = ""
        list_data = [update_column_value]
        for value in list_column_value:
            value_string += "%s,"
            list_data.append(value)
        value_string = value_string[:-1]
        sql = f'''UPDATE {self.tbl} SET {update_column_name} = %s {updated_at_string} WHERE {column_name} IN ({value_string})'''
        if return_sql: return sql
        return self.execute_sql(sql, list_data=list_data)

    def update_many(self, _list, return_sql=False):
        conn = mysql.connector.connect(host = self.db_host,user = self.db_user,password = self.db_password,database = self.db_database, charset="utf8mb4", use_unicode=True)
        cursor = conn.cursor(dictionary=True)
        error = False
        on_update_str = ""
        list_keys = []
        for key in _list[0]:
            list_keys.append(key)
            on_update_str += " {} = VALUES({}),".format(key, key)
        on_update_str = on_update_str[:-1]
        columns_name = ",".join(list_keys)
        
        values_str = ""
        list_tuple_value = []
        for item in _list:
            values_str += "("
            tmp_list = []
            for key in item:
                value = item[key]
                list_tuple_value.append(value)
                values_str += "%s,"
                # value_type = type(value)
                # if value_type == int or value_type == float:
                #     values_str += "{},".format(value)
                # else:
                #     values_str += "'{}',".format(value)
            values_str = values_str[:-1]
            values_str += "),"
        list_tuple_value = tuple(list_tuple_value)
        values_str = values_str[:-1]

        sql = '''INSERT INTO {tbl} ({columns_name}) VALUES {values_str} ON DUPLICATE KEY UPDATE {on_update_str}'''.format(tbl=self.tbl, columns_name=columns_name, values_str=values_str, on_update_str=on_update_str)
        if return_sql: return sql
        
        try:
            cursor.execute(sql, list_tuple_value)
            conn.commit()
            cursor.close()
            conn.close()
            return True
        except Exception as err:
            return err
