from distutils.core import setup

majv = 1
minv = 2

setup(
	name = 'sqlitehelper',
	version = "%d.%d" %(majv,minv),
	description = "Python module that wraps sqlite3",
	author = "Colin ML Burnett",
	author_email = "cmlburnett@gmail.com",
	url = "https://github.com/cmlburnett/sqlitehelper",
	packages = ['sqlitehelper'],
	package_data = {'sqlitehelper': ['sqlitehelper/__init__.py']},
	classifiers = [
		'Programming Language :: Python :: 3.7'
	]
)
