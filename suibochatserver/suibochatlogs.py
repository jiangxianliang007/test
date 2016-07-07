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
from sqlalchemy.pool import NullPool
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
		session.execute(sql)
		session.commit()
	except Exception, e:
		print Exception,":",e
	


def Split():
	global kafka_hosts
	consumer = KafkaConsumer('suibochatserverlogs',
						 group_id='suibo',
                         client_id="suibochat",
                         bootstrap_servers=kafka_hosts,value_deserializer=lambda m: json.loads(m.decode('utf-8')),auto_offset_reset="earliest", enable_auto_commit=True)
	for message in consumer:
		regstr = "(?P<date>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})\| type:suibo version:(\d+.\d+.\d+.\d+) module:TEXT_CHAT eventid:768 pid:(\d+) servid:(?P<sid>\d+) roomid:(?P<roomid>\d+) anchorUin:(?P<anchoruin>\d+) vid:(?P<vid>\d+_\d+) srcUin:(?P<srcuin>\d+) dstUin:(?P<dstuin>\d+) msgtype:(\d+) is_public:(\d+) msg:(?P<text>.*)"
		pattern = re.compile(regstr)
		match = pattern.search(message.value['message'])
		if match:
			str = "insert into suibo_room_liaot_info(tjdate,roomid,anchorUin,vid,srcUin,msg) values('%s',%d,%d,'%s',%d,'%s')" % (match.group('date'),int(match.group('roomid')),int(match.group('anchoruin')),match.group('vid'),int(match.group('srcuin')),match.group('text'))
			savedbsqlalchemy(str)


def main():
	InitialDB()
	Split()
	

if __name__ == "__main__":
	main()
