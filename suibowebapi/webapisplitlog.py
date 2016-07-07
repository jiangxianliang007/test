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
import datetime
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
	engine = create_engine(DB_CONNECT_STRING, echo=False)
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
	
def GetRegUinInfo(message):
	#得到注册时间
	if ('eventid' in message.value) and ('content' in message.value) and ('serTime' in message.value):
		if message.value['eventid'] == 10025:
			if 'retData' in message.value['content']:
				retjson=json.JSONDecoder().decode(message.value['content']['retData'])
				if ('lt_uin' in retjson) and ('first_authorization' in retjson) and (retjson['first_authorization']==1):
					return {'uin':int(retjson['lt_uin']),'time':message.value['serTime']}
				else:
					print retjson['first_authorization']
		elif message.value['eventid'] == 100154:
			if 'retData' in message.value['content']:
				retjson=json.JSONDecoder().decode(message.value['content']['retData'])
				if 'lt_uin' in retjson:
					return {'uin':int(retjson['lt_uin']),'time':message.value['serTime']}
	return None



def Split():
	global kafka_hosts
	consumer = KafkaConsumer('suibowebapilogs',
						 group_id='suibwebapi',
                         client_id="suibwebapi",
                         bootstrap_servers=kafka_hosts,value_deserializer=lambda m: json.loads(m.decode('utf-8')),auto_offset_reset="earliest", enable_auto_commit=True)
	for message in consumer:
		sqlstr=""
		ret = GetRegUinInfo(message)
		if ret==None:
			continue
		try:
			sqlstr = "update suibo_user_info set regtime='%s' where uin = %d" % (ret['time'],ret['uin'])
			if savedbsqlalchemy(sqlstr) == 0:
				sqlstr = "insert into suibo_user_info(regtime,uin) values('%s',%d)" % (ret['time'],ret['uin'])
				savedbsqlalchemy(sqlstr)
		except Exception, e:
			print Exception,":",e
		
	
def main():
	InitialDB()
	Split()
	

if __name__ == "__main__":
	main()
