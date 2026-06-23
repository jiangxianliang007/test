#coding=utf-8
#!/usr/bin/python
import ConfigParser


class DBConfig(object):
	"""Container for database and Kafka connection settings."""
	def __init__(self, host, port, user, password, kafka_hosts):
		self.host = host
		self.port = port
		self.user = user
		self.password = password
		self.kafka_hosts = kafka_hosts

	def log_message(self):
		return "dbhost:%s dbport%s dbuser:%s dbpwd:%s broker_hosts:%s" % (
			self.host, self.port, self.user, self.password, self.kafka_hosts)


def load_db_config(path="db.conf"):
	"""Load the common DB/Kafka settings used by log consumers."""
	cf = ConfigParser.ConfigParser()
	cf.read(path)
	return DBConfig(
		cf.get("db", "db_host"),
		cf.getint("db", "db_port"),
		cf.get("db", "db_user"),
		cf.get("db", "db_pass"),
		cf.get("kafka", "broker_hosts"),
	)
