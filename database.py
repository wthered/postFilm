import psycopg2
from psycopg2.extras import DictCursor


class Database:
	def __init__(self):
		self.conn_params = {
			'host': 'localhost',
			'database': 'movies',
			'user': 'wthered',
			'password': '!p9lastiras',
			'port': 5432}
		"""Initialize database connection parameters."""
		self.conn = None
		self.cursor = None
		self.last_query = ""
		self.results = None
		self.connect()


	def connect(self):
		"""Establish a connection to the database."""
		try:
			self.conn = psycopg2.connect(**self.conn_params)
			self.cursor = self.conn.cursor(cursor_factory=DictCursor)  # DictCursor returns results as dictionaries
			print("Database connection successful!")
		except psycopg2.Error as e:
			print(f"Error connecting to database: {e}")


	def select(self, parameters=None, show=False, many=False):
		"""Execute a SELECT query and return results."""
		if parameters is None:
			parameters = []
		try:
			self.cursor.execute(self.last_query, parameters)
			if show:
				print("[Database:37 - SELECT] {}".format(self.cursor.mogrify(self.last_query, parameters)))
			if many:
				# print("Selected to fetch all")
				# Rises ValueError: dictionary update sequence element #0 has length 7; 2 is required
				# self.results = dict(self.cursor.fetchall())
				self.results = [dict(row) for row in self.cursor.fetchall()]
			else:
				# print("Selected to fetch one")
				line = self.cursor.fetchone()
				if line:
					self.results = dict(line)
				else:
					self.results = None
			return self.results
		except psycopg2.Error as e:
			print("Error executing SELECT: {}\n{}\n{}".format(e, self.last_query, parameters))
			self.conn.rollback()
			return None


	def insert(self, parameters=None, show=False, commit=False):
		"""Execute an INSERT query and commit the transaction."""
		try:
			self.cursor.execute(self.last_query, parameters)
			if show:
				print("[Database:63 - INSERT] {}\n{}".format(self.cursor.mogrify(self.last_query, parameters), self.cursor.statusmessage))
			if commit:
				self.conn.commit()
		except psycopg2.Error as e:
			print("\n\n\nError executing INSERT: {}\n{}".format(e, self.cursor.mogrify(self.last_query, parameters)))
			self.conn.rollback()


	def update(self, parameters, show=False, commit=False):
		"""Execute an UPDATE query and commit the transaction."""
		try:
			self.cursor.execute(self.last_query, parameters)
			if show:
				print("[Database:75 - UPDATE] {}".format(self.cursor.mogrify(self.last_query, parameters)))
			if commit:
				self.conn.commit()
		except psycopg2.Error as e:
			print("\n\n\nError executing UPDATE: {}\n{}".format(e, self.cursor.mogrify(self.last_query, parameters)))
			self.conn.rollback()


	def delete(self, parameters=None, show=False, commit=False):
		"""Execute a DELETE query and commit the transaction."""
		try:
			self.cursor.execute(self.last_query, parameters)
			if show:
				print("[Database:88 - DELETE] {}".format(self.cursor.mogrify(self.last_query, parameters)))
			if commit:
				self.conn.commit()
			print(self.cursor.statusmessage)
		except psycopg2.Error as e:
			print("\n\n\nError executing DELETE: {}\n{}".format(e, self.cursor.mogrify(self.last_query, parameters)))
			self.conn.rollback()


	def close(self):
		"""Close the database connection."""
		if self.cursor:
			self.cursor.close()
		if self.conn:
			self.conn.close()
		print("Database connection closed.")
