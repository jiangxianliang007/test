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
from confighelper import load_db_config
session=None
kafka_hosts=[]
kafka_topic = ''
def InitialDB():
	global kafka_hosts
	global kafka_topic
	try:
		config = load_db_config()
	except Exception, e:
		print Exception,":",e
		taolelogs.logroot.warn(e)
		exit(0)
	kafka_hosts = config.kafka_hosts
	
	print config.log_message()
	global session
	session = TaoleSessionDB(config.host,config.port,config.user,config.password,'imsuibo')



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
