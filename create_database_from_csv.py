from peewee import *


# Connect to a Postgres database.
db = PostgresqlDatabase('db_name', user='user', password='password', 
	host='host_address', port="port")


# returns model name
def get_table_name(model_class):
	"""	
	Function, that returns Peewee model name

	Args:
	----------
	model_class: our model class
	model_class.__name__: name of the model

	Returns:
	----------
	model_class.__name__
	"""
	return model_class.__name__


def csv_values():
	"""	
	Function, that returns CSV file components, both headers and the values

	Args:
	----------
	file: csv file with data
	first_line: the first line, which contains headers for our table
	headers: complete headers list for Peewee model
	values = list, which will contain main data except for headers

	Returns:
	----------
	Function returns headers and values lists
	"""

	file = open("sample_disk:\\sample_user\\sample_folder\\sample_file.csv")

	first_line = file.readline()

	# removing `\n` if there is one in elements
	first_line = first_line.strip()

	# headers for our future table
	headers = first_line.split(";")

	values = []

	# collecting data to values list
	for line in file:
		line = line.strip()
		line = line.split(';')
		values.append(line)

	file.close()

	return headers, values


# Peewee model
def class_generator(class_name, headers):
	"""	
	Class, which represents peewee model for constructing SQL table in DB
	There are samples of table column data types in the model like DateField, DecimalFiled etc.
	Data types are needed for further correct data distribution into each column in the table

	Args:
	----------
	headers: headers list for Peewee model
	value_dict = dictionary for data distribution in table model


	Returns:
	----------
	Peewee SQL table model
	"""

	value_dict = {}

	for header in headers:

		# making auto serial id column set in peewee model
		value_dict["id"] = AutoField(primary_key=True)

		if header == "date_from":
			value_dict[f"{header}"] = DateField(null=True, formats=['%Y-%m-%d'])

		elif (header == "kpi_revenue_plan_value" or header == "kpi_receipt_plan_count" or
				header == "kpi_unit_plan_count" or header == "kpi_traffic_plan_value"):
			value_dict[f"{header}"] = DecimalField(default=None, null=True)

		elif "entity" in header or "flag" in header or "name" in header:
			value_dict[f"{header}"] = CharField(null=True)

		else:
			value_dict[f"{header}"] = IntegerField(null=True)

	value_dict['Meta'] = type('Meta', (object, ), {'database': db,
			'table_function': get_table_name})

	# returning the class! class == table in peewee(for making new table in SQL)
	return type(class_name, (Model, ), value_dict)


if __name__ == '__main__':

	
	# saving our headers and values from file
	headers, values = csv_values()

	# replacing empty values with None
	for line in values:
		for index, value in enumerate(line):
			if value == "":
				line[index] = None

	# import ipdb; ipdb.set_trace()

	# saving peewee model
	data = class_generator('kpi_main_division_hour_task', headers)
	
	# table with headers
	data.create_table()

	# creating columns
	for line in values:

		line = dict(zip(headers, line))

		data.create(**line)




