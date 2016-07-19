#coding=utf-8
#!/usr/bin/python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy
from sqlalchemy.pool import NullPool

class dbhelper(object):
	"""docstrinclass dbhelper"""
	def __init__(self,host,port, usr,pwd,dbname):
		object.__init__(self)
		DB_CONNECT_STRING = "mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8" % (usr,pwd,host,port,dbname) 
		try:
			engine = create_engine(DB_CONNECT_STRING, echo=False)
			self.connect = engine.connect()
		except Exception, e:
			print Exception,e
		

	

	def close(self):
		try:
			self.connect.close()
		except Exception, e:
			print Exception,e

	def __del__(self):
		#object.__del__(self)
		self.close()
	def excute(self,sqlstr):
		try:
			ret = self.connect.execute(sqlstr)
			return ret
		except Exception, e:
			print Exception,":",e

helper = dbhelper('140.205.102.3','3306','taoledb','b1D0B1TZDta','immoney1')
ret = helper.excute('SELECT giftid,giftprice,giftname FROM gift_partner WHERE partnerid=1 AND gift_mobile=1')
rows=ret.fetchall()
helper.close()
print rows

