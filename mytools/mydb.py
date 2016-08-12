import sqlite3

def _divide(sample, length):
    lsm = len(sample)
    tp = lsm%length
    anchor = lsm/length+1 if tp else lsm/length
    for _ in xrange(anchor):
        yield sample[:length]
        sample = sample[length:]

class SimpleDB(object):
    sql = sqlite3
    
    def __init__(self, dbpath):
        self.dbpath = dbpath
        self.tbname = None
        self.cur = None
        self.cxn = None
        
    def connect_db(self):
        self.cxn = self.sql.connect(self.dbpath)
        self.cur = self.cxn.cursor()
        
    def _execute(self, cmd):
        self.cur.execute(cmd)
        self._commit()
        
    def create_table(self, tbname, force=False, *forms):
        '''
        Create Database Table.
        
        *tbname*    - Table name.
        *force*     - True, delete existed table with the same name.
        *forms*     - Item names, all column are in type of TEXT.
        '''
        self.tbname = tbname
        if self._is_table_existing(self.tbname):
            if force:
                self.drop_table(self.tbname)
            else:
                raise AssertionError('Table {0} is already existed.'.format(self.tbname))
            
        _forms = ', '.join([str(col)+' TEXT' for col in forms])
        sqlcmd = 'CREATE TABLE {0}(id INTEGER PRIMARY KEY, {1})'.format(self.tbname, _forms)
        self._execute(sqlcmd)
    
    def create_table_free(self, tbname, force=False, *forms):
        '''
        Create Database Table.
        
        *tbname*    - Table name.
        *force*     - True, delete existed table with the same name.
        *forms*     - Item names, e.g.: name==TEXT, age==INTEGER.
        '''
        self.tbname = tbname
        if self._is_table_existing(self.tbname):
            if force:
                self.drop_table(self.tbname)
            else:
                raise AssertionError('Table {0} is already existed.'.format(self.tbname))
            
        _forms = ', '.join([col.replace('==', ' ') for col in forms])
        sqlcmd = 'CREATE TABLE {0}(id INTEGER PRIMARY KEY, {1})'.format(self.tbname, _forms)
        self._execute(sqlcmd)
        
    def create_table_manual(self, tbname, force=False, *forms):
        '''
        Create Database Table.
        The CMD will not be executed in this method, user needs call commit() method to
        execute the CMD.
        
        *tbname*    - Table name.
        *force*     - True, delete existed table with the same name.
        *forms*     - Item names, all column are in type of TEXT.
        '''
        self.tbname = tbname
        drop_count = 0
        while self._is_table_existing(self.tbname):
            if drop_count > 10:
                raise AssertionError('Table {0} is already existed.'.format(self.tbname))
                break
            else:
                drop_count += 1
            if force:
                self.drop_table(self.tbname)
            
        _forms = ', '.join([str(col)+' TEXT' for col in forms])
        sqlcmd = 'CREATE TABLE {0}(id INTEGER PRIMARY KEY, {1})'.format(self.tbname, _forms)
        self.cur.execute(sqlcmd)

    def _is_table_existing(self, tbname):
        self._execute("SELECT tbl_name FROM sqlite_master WHERE type='table'")
        msg = [each[0] for each in self.fetchall()]
        if  msg and tbname in msg:
            return True
        else:
            return False
        
    def add_column(self, col_name, typee):
        """
        Add new column.
        """
        cmd = "ALTER TABLE {0} ADD COLUMN {1} {2}".format(self.tbname, col_name, typee)
        self._execute(cmd)
        
    def get_table_line_count(self):
        """
        Return an integer of all line count in current table.
        """
        cmd = "SELECT COUNT(*) FROM {0}".format(self.tbname)
        self._execute(cmd)
        return self.fetchone()[0]
    
    def get_db_line_count(self):
        """
        Return an integer of all line count in current db.
        """
#        substatements = ["SELECT COUNT(*) a FROM {0}".format(tbname) for tbname in self.get_all_tablenames()]
#        cmd = "SELECT sum(a) FROM ({0})".format(' union all '.join(substatements))
#        self._execute(cmd)
#        return self.fetchone()[0]

        count = 0
        for sub_tbnames in _divide(self.get_all_tablenames(), 300):
            substatements = ["SELECT COUNT(*) a FROM {0}".format(tbname) for tbname in sub_tbnames]
            cmd = "SELECT sum(a) FROM ({0})".format(' union all '.join(substatements))
            self._execute(cmd)
            count += self.fetchone()[0]
        
        return count
    
    def get_from_last_2(self, colu_name='*'):
        """
        Return the value of *colu_name* from last 2 lines of current table.
        If *colu_name* is not given, return the whole last 2 lines.
        
        <This method could be extend to *get_from_last_n*>
        
        """
        cmd = "SELECT {0} FROM {1} WHERE id=(SELECT max(id)-1 FROM {1}) OR id=(SELECT max(id) FROM {1}) GROUP BY id".format(colu_name, self.tbname)
        self._execute(cmd)
        return self.fetchall()
    
    def get_all_tablenames(self):
        """
        Get a list of all table names.
        """
        cmd = "SELECT tbl_name FROM sqlite_master WHERE type='table' ORDER BY tbl_name"
        self._execute(cmd)
        msg = [each[0] for each in self.fetchall()]
        return msg
        
    def get_tablename_by(self, column, value):
        """
        Retrun the tablename in which table there is item *column*==*value*.
        """
        for tbname in self.get_all_tablenames():
            self.select_table(tbname)
            column_all = self.query_column(column)
            if value in column_all:
                return tbname
        return False
        
    def _remove_internal_index(self, set_of_data):
        '''
        Return the data list, which the first internal index value is removed.
        e.g.:
            *set_of_data*    : (1, u'Nanoha', u'16', u'AB0')
            return           : [u'Nanoha', u'16', u'AB0']
        '''
        return list(set_of_data)[1:]
    
    def _format_tuple2single(self, group_of_data):
        """
        Return the data list, which the inner tuple data is transformed into single.
        e.g.:
            *group_of_data*    : [(u'01 05 07 11 02',), (u'11 06 05 04 02',), (u'08 02 06 03 10',)]
            return             : ['01 05 07 11 02', '11 06 05 04 02', '08 02 06 03 10',]
        """
        return [sub[0] for sub in group_of_data]
        
    def select_table(self, tbname):
        '''
        Select current processing table.
        '''
        self.tbname = tbname
        
    def drop_table(self, tbname):
        '''
        Delete *tbname* table.
        '''
        self._execute("DROP TABLE {0}".format(tbname))
        self._commit()
        
    def insert(self, *values):
        '''
        Insert data to current table.
        *values*    - Values of each column, all of them would be changed into string.
        '''
        _v = ', '.join(["'{0}'".format(value) for value in values])
        cmd = "INSERT INTO {0} VALUES (NULL, {1})".format(self.tbname, _v)
        self._execute(cmd)
        
    def insert_manual(self, *values):
        '''
        Insert data to current table. This method doesn't commit the CMD into database, user needs
        call commit() method to execute the CMD after calling this method. User can call insert_manual()
        several times and call commit() once, in this way the writing data into database is faster than 
        calling insert() method once.
        
        *values*    - Values of each column, all of them would be changed into string.
        '''
        _v = ', '.join(["'{0}'".format(value) for value in values])
        cmd = "INSERT INTO {0} VALUES (NULL, {1})".format(self.tbname, _v)
        self.cur.execute(cmd)
        
    def delete_by(self, column, value):
        """
        Delete a record which *column* == *value*.
        """
        cmd = "DELETE FROM {0} WHERE {1}='{2}'".format(self.tbname, column, value)
        self._execute(cmd)
        
    def update_single_by(self, condition, new):
        """
        Update the record which match the *condition*.
        e.g.:
            *condition* - name==nanoha
            *new* - age==16
        """
        condition_colum, condition_value = condition.split('==')
        new_colum, new_value = new.split('==')
        cmd = "UPDATE {0} SET {1}='{2}' WHERE {3}='{4}'".format(self.tbname,
                                                                new_colum, new_value,
                                                                condition_colum, condition_value)
        self._execute(cmd)
    
    def update_single_by_manual(self, condition, new):
        """
        Update the record which match the *condition*.
        e.g.:
            *condition* - name==nanoha
            *new* - age==16
        """
        condition_colum, condition_value = condition.split('==')
        new_colum, new_value = new.split('==')
        cmd = "UPDATE {0} SET {1}='{2}' WHERE {3}='{4}'".format(self.tbname,
                                                                new_colum, new_value,
                                                                condition_colum, condition_value)
        self.cur.execute(cmd)
        
    def update_multi_by(self, condition, new_data):
        """
        Update the record which match the *condition*.
            *condition* -  a string of the new pair data, e.g.: "name==nanoha"
            *new* -    a list of the new pair data, e.g.: ["age==16",
                                                           "ability==nb"]
        """
        condition_colum, condition_value = condition.split('==')
        new_dataa = []
        for each in new_data:
            new_dataa.append("{0}='{1}'".format(*each.split('==')))
        
        cmd = "UPDATE {0} SET {1} WHERE {2}='{3}'".format(self.tbname,
                                                                ','.join(new_dataa),
                                                                condition_colum, condition_value)
        
        self._execute(cmd)
        
    def query_column(self, column):
        '''
        Return a list of data in the *column*.
        '''
        cmd = "SELECT {0} FROM {1}".format(column, self.tbname)
        self._execute(cmd)
        return self._format_tuple2single(self.fetchall())
        
    def query_all(self):
        '''
        Return a 2-D list of all data in the table.
        '''
        cmd = "SELECT * FROM {0}".format(self.tbname)
        self._execute(cmd)
        return [self._remove_internal_index(each) for each in self.fetchall()]
        
    def query_by(self, column, value):
        '''
        Return a list of the record where its *column* == *value*.
        '''
        cmd = "SELECT * FROM {0} WHERE {1}='{2}'".format(self.tbname, column, value)
        self._execute(cmd)

        _values = self.fetchall()

        return [self._remove_internal_index(_value) for _value in _values]

    def query_previous_x(self, col_val, x):
        '''
        Locate at the line which is specified with *col_val*, query and return a list of
        its previous *x* lines of data.

        If the previous data is less than *x*, return None.
        If the *col_val* locate multiple data, locate the first data.

        *col_val*   -   strings pair of ['=', '>', '<', '>=', '<=', '<>'].
                            e.g.:
                                NAME=Nanoha AGE=16
                                TYPE=Car PRICE>150000
        *x*         -   integer, indicate how many lines of data to query.
        '''
        result = []

        cmd = "SELECT id FROM {0} WHERE {1}".format(self.tbname, col_val)
        self._execute(cmd)
        locater = int(self.fetchone()[0])

        cmd = "SELECT * FROM {0} WHERE {1}<=id AND id<={2}".format(self.tbname, locater-x, locater-1)
        self._execute(cmd)
        for each in  self.fetchall():
            result.append(self._remove_internal_index(each))

        if len(result) < x:
            return None
        else:
            return result

        
    def query_by_multi(self, *col_val):
        '''
        Return a list of the record where conditions match *col_val*.
        *col_val*    -    strings pair of <column_name><operator><value>.
                          <operator> in ['=', '>', '<', '>=', '<=', '<>'].
                            e.g.:
                                NAME=Nanoha AGE=16
                                TYPE=Car PRICE>150000
        '''
        cvs = []
        
        for each in col_val:

            for operator in ['=', '>', '<', '>=', '<=', '<>']:
                if operator in each:
                    break
            
            col, val = each.split(operator)
            cvs.append(col+operator+"'"+val+"'")
            
        cv = ' and '.join(cvs)
        
        cmd = "SELECT * FROM {0} WHERE {1}".format(self.tbname, cv)
        self._execute(cmd)
        _values = self.fetchone()
        
        if _values:
            return self._remove_internal_index(_values)
        else:
            return []
        
    def change_default_cache_size(self, size=2000):
        self._execute('PRAGMA default_cache_size = {0}'.format(size))
                            
    def fetchone(self):
        return self.cur.fetchone()
    
    def fetchall(self):
        return self.cur.fetchall()
    
    def close(self):
        self._commit()
        self.done()
        
    def commit(self):
        self._commit()
    
    def _commit(self):
        self.cxn.commit()
        
    def done(self):
        self.cxn.close()
        
        
if __name__ == '__main__':
    from codetimer import codetimer
    
    sdb = SimpleDB('testdb.db')
    sdb.connect_db()
    sample = ['Nanoha', 'Hayate', 'Fate', 'Miya', 'Atugo']
    for name in sample:
        sdb.insert(name, 16+sample.index(name), 'AB'+str(sample.index(name)))

    print sdb.query_by('NAME', 'Nanoha')
    print sdb.query_by_multi('NAME=Nanoha', 'AGE>18')
      
    sdb.update_single_by('NAME==Hayate', 'AGE==secret')
    sdb.update_multi_by('NAME==Atugo', ['NAME==Atusa',
                                        'AGE==14',
                                        'ABILITY==AA'])
  
    print sdb.query_all()
    print sdb.get_table_line_count()
    print sdb.get_db_line_count()
    print sdb.get_from_last_2('NAME')
    print sdb.get_tablename_by('NAME', 'Nanoha')    
    
    codetimer.start()
    
    for tbname in ['Strikers'+str(each) for each in xrange(10)]:
        sdb.create_table_manual(tbname, True, 'NAME', 'AGE', 'ABILITY')
    
        sample = [str(each) for each in xrange(20000)]
        for name in sample:
            sdb.insert_manual(name, 16+sample.index(name), 'AB'+str(sample.index(name)))
    sdb.commit()
    
    codetimer.stop()

    
    sdb.done()
    
    
    