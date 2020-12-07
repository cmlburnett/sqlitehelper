"""Example included in the README"""

import datetime
import os.path
from sqlitehelper import SH,DBTable,DBCol

class mydb(SH):
	__schema__ = [
		DBTable('employee',
			DBCol('name', 'text'),
			DBCol('DOB', 'datetime'),
			DBCol('awesome', 'bool')
		),
		DBTable('address',
			DBCol('street', 'text'),
			DBCol('city', 'text'),
			DBCol('state', 'text'),
			DBCol('country', 'text'),
			DBCol('visits', 'integer')
		)
	]

	def open(self):
		"""
		Inject the above schema if file not found.
		"""
		ex = os.path.exists(self.Filename)
		super().open()

		if not ex:
			self.MakeDatabaseSchema()

if __name__ == '__main__':
	db = mydb("foo.db")
	db.open()

	db.employee.insert(name='Ethyl', DOB=datetime.datetime.utcnow(), awesome=True)
	db.employee.insert(name='Bob', DOB=datetime.datetime.utcnow(), awesome=True)
	db.employee.insert(name='John', DOB=datetime.datetime.utcnow(), awesome=False)

	print("Current employees: %s" % (",".join( sorted([_['name'] for _ in db.employee.select("name")]) )))

	for e in db.employee.select('name', '`awesome`=?', [True]):
		print("Awesome employee: %s" % e['name'])
	for e in db.employee.select(['rowid','name'], '`awesome`=?', [False]):
		print("Decidedly NOT awesome employee: %s" % e['name'])
		# Get rid of that employee
		db.employee.delete({'rowid': '?'}, [e['rowid']])

	print("Current employees: %s" % (",".join( sorted([_['name'] for _ in db.employee.select("name")]) )))

	print("----------------")

	db.address.insert(street='1600 Pennsylvania Ave', city='Washington', state='DC', country='USA', visits=5)
	db.address.insert(street='10 Downing Street', city='London', country='England', visits=0)

	for a in db.address.select("*"):
		print("Address I want to visit: %s, %s, %s; been there %d times" % (a['street'], a['city'], a['country'], a['visits']))

	print("Go visit all of Europe")
	for a in db.address.select(["rowid","visits"], "`country`=?", ['England']):
		db.address.update({"rowid": a['rowid']}, {"visits": a['visits']+1})

	for a in db.address.select("*"):
		print("Address I want to visit: %s, %s, %s; been there %d times" % (a['street'], a['city'], a['country'], a['visits']))

