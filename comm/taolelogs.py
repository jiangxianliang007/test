# encoding:utf-8
#!/usr/bin/ python
import os,sys
import time
import logging
import logging.handlers
from logging.handlers import TimedRotatingFileHandler
from EventsDefine import EvensIDS
logroot=None
def InitailLogs(logname,level=logging.WARN):
	global logroot
	if not os.path.exists('./logs/'):
		os.makedirs('./logs/')
	format = '%(asctime)s %(levelname)s %(module)s.%(funcName)s Line:%(lineno)d %(message)s'  
	logroot = logging.getLogger(logname)
	logroot.setLevel(level)
	if not logroot.handlers:
		hdlr = TimedRotatingFileHandler("./logs/"+logname + '.log',"D")
		fmt = logging.Formatter(format)
		hdlr.setFormatter(fmt)
		logroot.addHandler(hdlr)
	logroot.propagate = False
	return logroot
	
def GetTaoleLog():
	global logroot
	return logroot