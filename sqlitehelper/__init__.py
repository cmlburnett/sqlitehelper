
import sqlite3
import logging

# For converters/adapters
import datetime
import json


__all__ = ['SH', 'DBTable', 'DBCol', 'DBColROWID']


class DBTable:
	"""
	Represents a database table of DBCol columns.
	"""

	def __init__(self, name, *cols):
		self._name = name
		self._cols = cols

	@property
	def Name(self): return self._name

	@property
	def Cols(self): return self._cols

	@property
	def SQL(self):
		cols = [_.SQL for _ in self.Cols]

		cols = ",".join(cols)
		return "CREATE TABLE `%s` (%s)" % (self.Name, cols)

class DBCol:
	"""
	Represents a database column.
	"""

	def __init__(self, name, typ):
		self._name = name
		self._typ = typ

	@property
	def Name(self): return self._name

	@property
	def Typ(self): return self._typ

	@property
	def SQL(self):
		return "`%s` %s" % (self.Name, self.Typ)

class DBColROWID(DBCol):
	def __init__(self, name='rowid'):
		super().__init__(name, 'integer')
	
	@property
	def SQL(self):
		return super().SQL + " primary key"

class SH_sub:
	"""
	Sub class for SH that permits object like access to SH classes to select tables.
	"""

	def __init__(self, name, ex, sel, sel_one, ins, up, dlt):
		self._name = name

		self._execute = ex
		self._select = sel
		self._select_one = sel_one
		self._insert = ins
		self._update = up
		self._delete = dlt

	@property
	def Name(self): return self._name

	def select(self, cols, where=None, vals=None, order=None):
		return self._select(self.Name, cols, where, vals, order)

	def select_one(self, cols, where=None, vals=None):
		return self._select_one(self.Name, cols, where, vals)

	def insert(self, **cols):
		return self._insert(self.Name, **cols)

	def update(self, where, vals):
		return self._update(self.Name, where, vals)

	def delete(self, where, vals):
		return self._delete(self.Name, where, vals)

class SH:
	"""
	Sqlite3 helper class.
	Does basics for handling select, insert, update, and delete functions to reduce need to write SQL everywhere.
	"""

	def __init__(self, fname):
		self._fname = fname
		self._db = None

		# Get converters and register one for datetime.datetime & json
		cons = [_.lower() for _ in sqlite3.converters]
		if 'datetime' not in cons:
			sqlite3.register_adapter(datetime.datetime, lambda dt: dt.strftime("%Y-%m-%d %H:%M:%S.%f").encode('ascii'))
			sqlite3.register_converter("datetime", lambda txt: datetime.datetime.strptime(txt.decode('ascii'), "%Y-%m-%d %H:%M:%S.%f"))

		if 'json' not in cons:
			sqlite3.register_converter("json", lambda txt: json.loads(txt))

		if 'bool' not in cons:
			sqlite3.register_adapter(bool, lambda x: int(x))
			sqlite3.register_converter("bool", lambda x: bool(int(x)))

		if hasattr(self, '__schema__'):
			for o in self.__schema__:
				if hasattr(self, o.Name):
					if hasattr(self, 'db_' + o.Name):
						raise Exception("Object has both %s and db_%s, cannot assign SH_sub object" % (o.Name, o.Name))
					else:
						setattr(self, 'db_' + o.Name, SH_sub(o.Name, self.execute, self.select, self.select_one, self.insert, self.update, self.delete))
				else:
					setattr(self, o.Name, SH_sub(o.Name, self.execute, self.select, self.select_one, self.insert, self.update, self.delete))

		# No transaction to start
		self._cursor = None

	@property
	def Filename(self): return self._fname

	@property
	def DB(self): return self._db


	def open(self, rowfactory=None):
		"""
		Opens the database connection.
		"""

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

		for o in self.__schema__:
			if type(o) is DBTable:
				self.begin()
				self.execute(o.SQL)
				self.commit()
			else:
				raise TypeError("Unrecognized schema type '%s'" % type(o))



	def begin(self):
		if self._cursor is None:
			logging.debug("SH: BEGIN")
			self._cursor = self._db.cursor()
		else:
			raise Exception("Already in a transaction")

	def commit(self):
		if self._cursor is None:
			pass
		else:
			logging.debug("SH: COMMIT")
			self._db.commit()
			self._cursor = None

	def rollback(self):
		if self._cursor is None:
			pass
		else:
			logging.debug("SH: ROLLBACK")
			self._db.rollback()
			self._cursor = None



	def execute(self, sql, vals=None):
		if vals is None:
			logging.debug("SH: SQL: %s ()" % (sql,))
			res = self.DB.execute(sql)
		else:
			logging.debug("SH: SQL: %s %s" % (sql, vals))
			res = self.DB.execute(sql, vals)
		return res

	def select(self, tname, cols, where=None, vals=None, order=None):
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

		return self.execute(sql, vals)

	def select_one(self, tname, cols, where, vals=None):
		res = self.select(tname, cols, where, vals)
		return res.fetchone()

	def insert(self, tname, **cols):
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

		self.execute(sql, vals)

	def update(self, tname, where, vals):
		s_cols = []
		s_vals = []

		w_cols = []
		w_vals = []

		for k,v in vals.items():
			s_cols.append('`%s`=?' % k)
			s_vals.append(v)

		for k,v in where.items():
			w_cols.append('`%s`=?' % k)
			w_vals.append(v)

		s = ",".join(s_cols)
		w = ",".join(w_cols)

		sql = "UPDATE `%s` SET %s WHERE %s" % (tname, s, w)

		return self.execute(sql, s_vals + w_vals)

	def delete(self, tname, where, vals):
		w_cols = []
		w_vals = []

		for k,v in where.items():
			w_cols.append('`%s`=?' % k)
			w_vals.append(v)

		w = ",".join(w_cols)

		sql = "DELETE FROM `%s` WHERE %s" % (tname, w)

		return self.execute(sql, vals)



