#coding=utf-8
#!/usr/bin/ python
import string, os, sys
from kafka import KafkaConsumer
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import ConfigParser
import json
import re
import MySQLdb
import sqlalchemy
session=None
kafka_hosts=[]
def InitialDB():
	global kafka_hosts
	cf = ConfigParser.ConfigParser()
	try:
		cf.read("db.conf")
		db_host = cf.get("db", "db_host")
		db_port = cf.getint("db", "db_port")
		db_user = cf.get("db", "db_user")
		db_pass = cf.get("db", "db_pass")
		kafka_hosts = cf.get("kafka","broker_hosts")
	except Exception, e:
		print Exception,":",e
		exit(0)
	
	print "dbhost:%s dbport%s dbuser:%s dbpwd:%s broker_hosts:%s"%(db_host,db_port,db_user,db_pass,kafka_hosts)
	global session
	DB_CONNECT_STRING = "mysql+mysqldb://%s:%s@%s:%s/imsuibo?charset=utf8" % (db_user,db_pass,db_host,db_port) 
	engine = create_engine(DB_CONNECT_STRING, echo=True)
	DB_Session = sessionmaker(bind=engine)
	session = DB_Session()
	try:
		session.execute("SET NAMES 'utf8mb4'")
	except Exception, e:
		print Exception,":",e
		exit(0)

def  CloseDB():
	global session
	session.close()




def savedbsqlalchemy(sql):
	global session
	try:
		result = session.execute(sql)
		session.commit()
		return result.rowcount
	except Exception, e:
		print Exception,":",e
		return False
	


def Split():
	global kafka_hosts
	consumer = KafkaConsumer('suibomobileslogs',
						 group_id='suibomobile',
                         client_id="suibomobile",
                         bootstrap_servers=kafka_hosts,value_deserializer=lambda m: json.loads(m.decode('utf-8')),auto_offset_reset="earliest", enable_auto_commit=True)
	for message in consumer:
		sqlstr=""
		try:
			if message.value['eventId'] == 30006 and message.value['uin']>0: #手机机型
				sqlstr = "update suibo_user_info set terminal_type='%s' where uin = %d" % (message.value['content']['Model'],message.value['uin'])
				if savedbsqlalchemy(sqlstr) == 0:
					sqlstr = "insert into suibo_user_info(terminal_type,uin) values('%s',%d)" % (message.value['content']['Model'],message.value['uin'])
					savedbsqlalchemy(sqlstr)
		except Exception, e:
			print Exception,":",e
			continue
		
	
def main():
	InitialDB()
	Split()
	

if __name__ == "__main__":
	main()
