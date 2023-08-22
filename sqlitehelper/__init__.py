"""
sqlitehelper -- helper library for sqlite3

SH is the primary class that is used.
Subclassing SH and adding a __schema__ list of DBTable objects permit creation of the DB schema.
See README for further details of an example

For SQL queries used, set the logging library level to DEBUG.
"""

import sqlite3
import logging
import time

# For converters/adapters
import datetime
import json
import uuid


__all__ = ['SH', 'DBTable', 'DBCol', 'DBColUnique', 'DBColROWID']

# Serialized permits full sharing of connections and cursors between threads
sqlite3.threadsafety = 3


class DBTable:
	"""
	Represents a database table of DBCol columns.
	Create in a list of __schema__ in subclass of SH.
	Pass an number of DBCol objects to the constructor to add columns.
	"""

	def __init__(self, dbname, *cols):
		self._dbname = dbname
		self._cols = cols

	@property
	def DBName(self): return self._dbname

	@property
	def Cols(self): return self._cols

	@property
	def SQL(self):
		"""
		Returns the SQL neede to generate this table.
		"""

		cols = [_.SQL for _ in self.Cols]

		cols = ",".join(cols)
		return "CREATE TABLE `%s` (%s)" % (self.DBName, cols)

class DBCol:
	"""
	Represents a database column.
	Pass any number of these to DBTable to define the columns of the table.
	The sqlite type is passed as a string to @typ (eg, "text", "integer").
	"""

	def __init__(self, dbname, typ):
		self._dbname = dbname
		self._typ = typ
		self._unique = False

	@property
	def DBName(self): return self._dbname

	@property
	def Typ(self): return self._typ

	@property
	def IsUnique(self): return self._unique

	@property
	def SQL(self):
		"""
		Returns the SQL used in CREATE TABLE for this column.
		"""

		# TODO: add something for handling the unique foreign key constraint
		return "`%s` %s" % (self.DBName, self.Typ)

class DBColUnique(DBCol):
	"""
	Represents a database column that should have unique values.
	Pass any number of these to DBTable to define the columns of the table.
	The sqlite type is passed as a string to @typ (eg, "text", "integer").
	"""

	def __init__(self, name, typ):
		super().__init__(name, typ)
		self._unique = True

class DBColROWID(DBCol):
	"""
	Special DBCol object that permits renaming the default primary key in sqlite from rowid to anything.
	Inclusion of this in the DBTable constructor without providing an alternate name has implications in sqlite.
	As there is always a primary key, explicit inclusion means the rowid is returned in SELECT * queries.
	"""
	def __init__(self, name='rowid'):
		super().__init__(name, 'integer')
	
	@property
	def SQL(self):
		"""
		Returns the SQL used in CREATE TABLE for this column.
		Defining the primary key requires an extra couple keywords in the CREATE TABLE statement.
		"""

		return super().SQL + " primary key"


class SH_sub:
	"""
	Utility sub class for SH that permits object like access to SH classes to select tables.
	Can subclass this and provide the class object to the SH constructor to provide an alternate template for these objects.
	"""
	_dbname = None

	@property
	def DBName(self): return self._dbname

	def __init__(self, db, schema, ex, sel, sel_one, ins, up, dlt, num):
		self.db = db
		self._schema = schema
		self._dbname = schema.DBName

		self._execute = ex
		self._select = sel
		self._select_one = sel_one
		self._insert = ins
		self._update = up
		self._delete = dlt
		self._num_rows = num

		# Find primary key column
		pkey = None
		for col in schema.Cols:
			if isinstance(col, DBColROWID):
				pkey = col

		# Only create these functions if there's a primary key (what would they return otherwise?)
		if pkey is not None:
			# This is for non-unique columns since it can return multiple results
			def rowidorempty(res):
				if res is None:
					return []
				else:
					return [_['rowid'] for _ in res]

			# This is for unique columns since it should return only one result
			def rowidornone(res):
				if res is None:
					return None
				else:
					return res['rowid']

			# This is for unique columns since it should return only one result
			def dictornone(res):
				if res is None:
					return None
				else:
					return dict(res)

			# Have to do this for the late binding in python
			# This is for non-unique columns
			def getbycolumn(self, k):
				def _(v):
					return rowidorempty(self.select('rowid', '`%s`=?' % k, [v]))
				return _

			# This is for unique columns
			def getbycolumnunique(self, k):
				def _(v):
					return rowidornone(self.select_one('rowid', '`%s`=?' % k, [v]))
				return _

			# This is for GetById
			def getbypkey(self, k):
				def _(v):
					return dictornone(self.select_one('*', '`%s`=?' % k, [v]))
				return _

			# Create GetBy* function for each column
			# If you don't want this function then delete in the sub-class constructor of SH
			# after invoking super().__init__
			for col in schema.Cols:
				# Special GetById for the primary key
				if isinstance(col, DBColROWID):
					setattr(self, 'GetById', getbypkey(self, 'rowid'))
				else:
					# Shouldn't have much trouble with function name requirements of python and column
					# name requirements in sqlite.
					# TODO: Could get in trouble though since they don't exactly match up
					fname = 'GetBy' + col.DBName
					if col.IsUnique:
						setattr(self, fname, getbycolumnunique(self, col.DBName))
					else:
						setattr(self, fname, getbycolumn(self, col.DBName))

	def HasDBColumnName(self, k):
		"""Checks if tables has a column named @k"""
		for col in self._schema.Cols:
			if col.DBName == k:
				return True

		return False

	def GetDBColumnNames(self):
		"""Returns a list of strings of the column names"""
		return [col.DBName for col in self._schema.Cols]

	def setup(self, db):
		""" Called after all SH_sub classes are created."""
		pass

	def select(self, cols, where=None, vals=None, order=None):
		return self._select(self.DBName, cols, where, vals, order)

	def select_one(self, cols, where=None, vals=None, order=None):
		return self._select_one(self.DBName, cols, where, vals, order)

	def insert(self, **cols):
		return self._insert(self.DBName, **cols)

	def update(self, where, vals):
		return self._update(self.DBName, where, vals)

	def delete(self, where):
		return self._delete(self.DBName, where)

	def num_rows(self, where=None, vals=None):
		return self._num_rows(self.DBName, where, vals)

class SH:
	"""
	Sqlite3 helper class.
	Does basics for handling select, insert, update, and delete functions to reduce need to write SQL everywhere.
	"""

	_objects = None

	def __init__(self, fname, sub_constructor=SH_sub):
		self._fname = fname
		self._db = None
		self._sub_cls = sub_constructor
		self._rowfact = None

		# Get converters and register one for datetime.datetime & json
		cons = [_.lower() for _ in sqlite3.converters]

		# datetime objects are stored pretty much in full as ASCII strings
		if 'datetime' not in cons:
			def dtconverter(txt):
				txt = txt.strip()
				try:
					return datetime.datetime.strptime(txt.decode('ascii'), "%Y-%m-%d %H:%M:%S.%f %z")
				except:
					return datetime.datetime.strptime(txt.decode('ascii'), "%Y-%m-%d %H:%M:%S.%f")

			sqlite3.register_adapter(datetime.datetime, lambda dt: dt.strftime("%Y-%m-%d %H:%M:%S.%f %z"))
			sqlite3.register_converter("datetime", dtconverter)

		# As strings representing JSON are still just str() objects, no adapter can be defined
		if 'json' not in cons:
			sqlite3.register_converter("json", lambda txt: json.loads(txt))

		# uuid objects are stored as strings
		if 'uuid' not in cons:
			sqlite3.register_adapter(uuid.UUID, lambda u: str(u))
			sqlite3.register_converter("uuid", lambda txt: uuid.UUID(txt.decode('ascii')))

		# bool is stored as 0/1 in sqlite, so just provide the type conversion
		if 'bool' not in cons:
			sqlite3.register_adapter(bool, lambda x: int(x))
			sqlite3.register_converter("bool", lambda x: bool(int(x)))

		# No transaction to start
		self._cursor = None

		# Generate the SH_sub objects
		self._objects = []
		self.GenerateSchema()

		for o in self._objects:
			o.setup(self)

	def GenerateSchema(self):
		# For exach DBTable, add an object to this object that wraps the table name to reduce parameter bloat when using this library
		# Ie: db.employee.select("*") is the same as db.select("employee", "*")
		if not hasattr(self, '__schema__'):
			return

		# TODO: Check for duplicate DBTable.DBName amongst the list
		# TODO: Check for duplicate DBCol.DBName within a table
		# TODO: Check that there's only, at most, one DBColROWID
		finalschema = []
		for o in self.__schema__:
			if isinstance(o, type):
				subo = o(self, None, self.execute, self.select, self.select_one, self.insert, self.update, self.delete, self.num_rows)
				setattr(self, subo.DBName, subo)
				finalschema.append( subo.BuildSchema() )

			elif hasattr(self, o.DBName):
				# Prefix with db_ is table name is already chosen (eg, select, insert)
				if hasattr(self, 'db_' + o.DBName):
					raise Exception("Object has both %s and db_%s, cannot assign SH_sub object" % (o.DBName, o.DBName))
				else:
					subo = SH_sub(self, o, self.execute, self.select, self.select_one, self.insert, self.update, self.delete, self.num_rows)
					setattr(self, 'db_' + o.DBName, subo)
					finalschema.append(o)

			else:
				subo = self._sub_cls(self, o, self.execute, self.select, self.select_one, self.insert, self.update, self.delete, self.num_rows)
				setattr(self, o.DBName, subo)
				finalschema.append(o)

			self._objects.append(subo)

		#self.__schema__.clear()
		#self.__schema__ += finalschema

	@property
	def Filename(self): return self._fname

	@property
	def DB(self): return self._db


	def reopen(self):
		#self.close()
		self._db = None
		self.open(self._rowfact)

	def open(self, rowfactory=None):
		"""
		Opens the database connection.
		Can provide ":memory:" to use sqlite's ability to use a database in memory (or anything else it accepts).
		Can override this function to call MakeDatabaseSchema if file doesn't exist.
		"""

		# Store in case reopen() is called
		self._rowfact = rowfactory

		if self.DB:
			raise Exception("Already opened to database '%s'" % self.Filename)

		# Open database
		self._db = sqlite3.connect(self.Filename, detect_types=sqlite3.PARSE_DECLTYPES)

		# Change row factory (default is an indexable row by column name)
		if rowfactory:
			self.DB.row_factory = rowfactory
		else:
			self.DB.row_factory = sqlite3.Row

	def close(self):
		"""
		Closes the database connection.
		"""

		if not self.DB:
			raise Exception("Not opened to database '%s', cannot close it" % self.Filename)

		self.DB.close()
		self._db = None

	def MakeDatabaseSchema(self):
		"""
		Creates the database schema in the named database
		"""

		logging.debug("SH: Making schema")

		if not hasattr(self, '__schema__'):
			raise Exception("Class %s doesn't have __schema__ attribute")

		ret = self.execute(None, 'schema', "select name from sqlite_master where type='table'")
		tnames = [_['name'] for _ in ret]

		for o in self.__schema__:
			if isinstance(o, DBTable):
				# Table already exists, skip it
				if o.DBName in tnames: continue

				self.begin()
				self.execute(None, 'schema', o.SQL)
				self.commit()
			elif isinstance(o, type) and issubclass(o, SH_sub):
				subo = o(self, None, self.execute, self.select, self.select_one, self.insert, self.update, self.delete, self.num_rows)
				# Table already exists, skip it
				if subo.DBName in tnames: continue

				sql = subo.BuildSchema().SQL

				self.begin()
				self.execute(None, 'schema', sql)
				self.commit()
			else:
				raise TypeError("Unrecognized schema type '%s'" % type(o))



	def begin(self):
		"""
		Begin a transaction.
		"""

		if self._cursor is None:
			logging.debug("SH: BEGIN")
			self._cursor = self._db.cursor()
		else:
			raise Exception("Already in a transaction")

	def commit(self):
		"""
		Commit a transaction.
		"""

		if self._cursor is None:
			pass
		else:
			logging.debug("SH: COMMIT")
			self._db.commit()
			self._cursor = None

	def rollback(self):
		"""
		Rollback a transaction.
		"""

		if self._cursor is None:
			pass
		else:
			logging.debug("SH: ROLLBACK")
			self._db.rollback()
			self._cursor = None



	def execute(self, tname, cmd, sql, vals=None):
		"""
		Execute any SQL statement desired with @vals as the iterable container of python values corresponding to ? parameter values in the SQL statement.
		"""

		if vals is None:
			logging.debug("SH: SQL: %s ()" % (sql,))
			vals = tuple()
		else:
			logging.debug("SH: SQL: %s %s" % (sql, vals))

		return self._execute(tname, cmd, sql, vals)

	def _execute(self, tname, cmd, sql, vals):
		# Try up to 10 times if locked (probably from another process)
		cnt = 0
		while cnt < 10:
			try:
				return self.DB.execute(sql, vals)
			except sqlite3.OperationalError as e:
				if 'database is locked' in e.args[0]:
					logging.error("Locked database count %d" % cnt)

					time.sleep(cnt)
					cnt += 1
					continue
				else:
					# Some other error
					raise

	def select(self, tname, cols, where=None, vals=None, order=None):
		"""
		SELECT statement to retrieve information.

		@tname is a string representing the table name to select from
		@cols is a string or list selecting columns to return. An empty string or "*" return all columns.
		@where is a string eseentially a SQL where clause (eg, "`rowid`=?", "`rowid` in (?)")
		@vals is an iterable container of python values to be substituted into ? parameters in the query
		@order is a string used to sort the returned results (eg, "`lname` asc, `fname` asc")
		"""

		# Assume nothing needs to be passed
		if vals is None:
			vals = []

		# Select all
		if cols is None or cols == '*':
			cols = '*'
		else:
			if type(cols) == str:
				cols = [cols]
			elif type(cols) == list:
				if not all([type(_) is str for _ in cols]):
					raise Exception("All columns are are expected to be a list of stirngs")
			else:
				raise Exception("Unrecognized columns input")

			# Comma separate list of names
			cols = ','.join( ["`%s`"%_ for _ in cols] )

		# Start formatting of sql string
		sql = "SELECT %s FROM `%s`" % (cols, tname)

		if where:
			sql += " WHERE %s" % where
		if order:
			sql += " ORDER BY %s" % order

		return self.execute(tname, 'select', sql, vals)

	def select_one(self, tname, cols, where, vals=None, order=None):
		"""
		Identical to select() except fetchone() is called to return the first result.
		"""

		res = self.select(tname, cols, where, vals, order)
		if res is None:
			return None
		return res.fetchone()

	def insert(self, tname, **cols):
		"""
		INSERT statement to add new information.

		@tname is a string representing the table name to select from
		@cols is a kwargs style passing of columns and values

		All values are ultimately passed in using ? style parameters
		"""

		names = []
		vals = []

		for k,v in cols.items():
			names.append(k)
			vals.append(v)

		vnames = ','.join( ['?']*len(names) )

		# Format column names
		cols = ",".join( ["`%s`" % _ for _ in names] )

		# Format SQL
		sql = "INSERT INTO `%s` (%s) VALUES (%s)" % (tname,cols,vnames)

		res = self.execute(tname, 'insert', sql, vals)

		return res.lastrowid

	def update(self, tname, where, vals, joiner='AND'):
		"""
		UPDATE statement to alter infromation.

		@tname is a string representing the table name to select from
		@where is a dictionary of column name/value pairs to limit which rows are updated (ie, the WHERE clause)
			{"rowid": 10, 'name': 'John'} -> "`rowid`=? AND `name`=?" and [10,'John'] are passed as values to execute()
		@vals is a dictionary of column name/value pairs to update matched rows to (ie, the SET clause)
			{"age": 20, "height": 70} -> "`age`=?, `height`=?" and [20, 70] are passed as values to execute()

		All values are ultimately passed in using ? style parameters
		"""

		s_cols = []
		s_vals = []

		w_cols = []
		w_vals = []

		# SET clause
		for k,v in vals.items():
			s_cols.append('`%s`=?' % k)
			s_vals.append(v)

		# WHERE clause
		for k,v in where.items():
			w_cols.append('`%s`=?' % k)
			w_vals.append(v)

		if joiner.strip().lower() == 'and':
			w = " AND ".join(w_cols)
		elif joiner.strip().lower() == 'or':
			w = " OR ".join(w_cols)
		else:
			raise ValueError("joiner parameter must be AND or OR")

		s = ",".join(s_cols)

		sql = "UPDATE `%s` SET %s WHERE %s" % (tname, s, w)

		return self.execute(tname, 'update', sql, s_vals + w_vals)

	def delete(self, tname, where, joiner='AND'):
		"""
		DELETE statement to remove information

		@tname is a string representing the table name to select from
		@where is a dictionary of column name/value pairs to limit which rows are updated (ie, the WHERE clause)
			{"rowid": 10, 'name': 'John'} -> "`rowid`=? AND `name`=?" and [10,'John'] are passed as values to execute()
		"""

		w_cols = []
		w_vals = []

		for k,v in where.items():
			w_cols.append('`%s`=?' % k)
			w_vals.append(v)

		if joiner.strip().lower() == 'and':
			w = " AND ".join(w_cols)
		elif joiner.strip().lower() == 'or':
			w = " OR ".join(w_cols)
		else:
			raise ValueError("joiner parameter must be AND or OR")

		sql = "DELETE FROM `%s` WHERE %s" % (tname, w)

		return self.execute(tname, 'delete', sql, w_vals)

	def num_rows(self, tname, where=None, vals=None):
		"""
		Do a fancy select to get the number of rows.
		If a where clause is provided, then use that to limit the rows.
		"""

		sql = "SELECT count(*) as `count` FROM `%s`" % tname

		if where:
			sql += " WHERE %s" % where

		if vals is None:
			res = self.execute(tname, 'select', sql)
		else:
			res = self.execute(tname, 'select', sql, vals)
		return res.fetchone()['count']

