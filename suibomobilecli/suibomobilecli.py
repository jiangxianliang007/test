#coding=utf-8
#!/usr/bin/ python
import string, os, sys
sys.path.append('../comm')
from kafka import KafkaConsumer
import ConfigParser
import json
import re
import MySQLdb
import taolelogs
from dbhelper import TaoleSessionDB
session=None
kafka_hosts=[]
kafka_topic = ''
def InitialDB():
	global kafka_hosts
	global kafka_topic
	cf = ConfigParser.ConfigParser()
	try:
		cf.read("db.conf")
		db_host = cf.get("db", "db_host")
		db_port = cf.getint("db", "db_port")
		db_user = cf.get("db", "db_user")
		db_pass = cf.get("db", "db_pass")
		kafka_hosts = cf.get("kafka","broker_hosts")
		#kafka_topic = cf.get("kafka",'topic')
	except Exception, e:
		print Exception,":",e
		taolelogs.logroot.warn(e)
		exit(0)
	
	print "dbhost:%s dbport%s dbuser:%s dbpwd:%s broker_hosts:%s"%(db_host,db_port,db_user,db_pass,kafka_hosts)
	global session
	session = TaoleSessionDB(db_host,db_port,db_user,db_pass,'imsuibo')



def Split():
	global kafka_hosts
	global session
	global kafka_topic
	consumer = KafkaConsumer('suibomobileslogs',
						 group_id='suibomobile',
                         client_id="suibomobile",
                         bootstrap_servers=kafka_hosts,value_deserializer=lambda m: json.loads(m.decode('utf-8')),auto_offset_reset="earliest", enable_auto_commit=True)
	for message in consumer:
		sqlstr=""
		if ('eventId' in message.value) and ('uin' in message.value) and ('content' in message.value) and ('Model' in message.value['content']) and ('func' in message.value['content']):
			try:
				if message.value['eventId'] == 20000 and message.value['uin']>0 and ('/mapi/msgpush/settoken.html' in message.value['content']['func']): #手机机型
					sqlstr = "update suibo_user_info set terminal_type='%s' where uin = %d" % (message.value['content']['Model'],message.value['uin'])
					sqlret = session.excute(sqlstr)
					if sqlret!=None and sqlret.rowcount == 0:
						sqlstr = "insert into suibo_user_info(terminal_type,uin) values('%s',%d)" % (message.value['content']['Model'],message.value['uin'])
						session.excute(sqlstr)
			except Exception, e:
				print Exception,":",e
				taolelogs.logroot.warn(e)
				continue
		
	
def main():
	taolelogs.InitailLogs('suibomobilecli')
	InitialDB()
	Split()
	

if __name__ == "__main__":
	main()
