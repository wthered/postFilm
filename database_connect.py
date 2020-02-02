import psycopg2 as psy
from database_credentials import *
import psycopg2.extras


class Database:

	def __init__(self):
		""" Connect to the PostgreSQL database server """
		self.connection = None
		try:
			# Connect to the Database server
			self.connection = psy.connect(host=configuration['hostname'], database=configuration['database'], user=configuration['username'], password=configuration['password'])

			# create a cursor
			self.cursor = self.connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
			self.query = "SELECT version()"
			self.success = self.cursor.execute(self.query, [])
			self.results = self.cursor.fetchone()
		except (Exception, psy.DatabaseError) as error:
			print(error)
		finally:
			if self.connection is not None:
				print('Connected into {}'.format(self.results['version']))

	def select(self, parameters, single=True, show=False):
		try:
			self.success = self.cursor.execute(self.query, parameters)
			if show:
				print(self.cursor.mogrify(self.query, parameters))
			if single:
				self.results = self.cursor.fetchone()
			else:
				self.results = self.cursor.fetchall()
		except (Exception, psy.DatabaseError) as db_error:
			print("[Database:37]\tError:\t{}".format(db_error))
			self.success = False
		finally:
			return {'success': self.success, 'many': self.cursor.rowcount, 'rows': self.results}

	def insert(self, parameters, commit, show):
		try:
			self.success = self.cursor.execute(self.query, parameters)
			if show:
				print(self.cursor.mogrify(self.query, parameters))
			if commit:
				self.connection.commit()
			self.success = self.cursor.rowcount > 0
		except (Exception, psy.DatabaseError) as db_error:
			print("[Database:50]\tError:\t{}\nRolling Back!!!!\n{}".format(db_error, self.cursor.mogrify(self.query, parameters)))
			with open("database_errors.sql", "a+") as error_handler:
				error_handler.write(self.cursor.mogrify(self.query, parameters))
				error_handler.write("-- [Database:53]\t{}".format(db_error))
			error_handler.close()
			self.connection.rollback()
			self.success = False
		finally:
			return dict({'success': self.success, 'rows': self.cursor.rowcount})

	def update(self, param, commit, show):
		return self.insert(param, commit,show)

	def delete(self, p, c, s):
		return self.insert(p, c, s)
