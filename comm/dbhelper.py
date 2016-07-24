#coding=utf-8
#!/usr/bin/python
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import sqlalchemy
from sqlalchemy.pool import NullPool
import taolelogs
class TaoleSessionDB(object):
	def __init__(self, host,port,usr,pwd,dbname):
		object.__init__(self)
		DB_CONNECT_STRING = "mysql+mysqldb://%s:%s@%s:%s/%s?charset=utf8" % (usr,pwd,host,port,dbname) 
		engine = create_engine(DB_CONNECT_STRING, echo=True)
		DB_Session = sessionmaker(bind=engine)
		self.session = DB_Session()

	def excute(self,sqlstr):
		result = None
		try:
			result = self.session.execute(sqlstr)
			self.session.commit()
		except Exception, e:
			print Exception,":",e
			taolelogs.logroot.warn(e)
			result = None
		return result

	def close(self):
		try:
			self.session.close()
		except Exception, e:
			print Exception,e
			taolelogs.logroot.warn(e)
			return False

	def __del__(self):
		self.close()

	
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
			taolelogs.logroot.warn(e)
	def close(self):
		try:
			self.connect.close()
		except Exception, e:
			print Exception,e
			taolelogs.logroot.warn(e)

	def __del__(self):
		self.close()
	def excute(self,sqlstr):
		try:
			ret = self.connect.execute(sqlstr)
			return ret
		except Exception, e:
			print Exception,":",e
			taolelogs.logroot.warn(e)
