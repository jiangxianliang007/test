#coding=utf-8
#!/usr/bin/python
import string, os, sys
import urllib2
import json
def SuiboGetIP(IPV4):
	retstr =""
	url = "http://int.dpool.sina.com.cn/iplookup/iplookup.php?format=json&ip=%s"%IPV4
 	try:
 		result = urllib2.urlopen(url,timeout=20)
 		data = result.read()
 		datadict = json.loads(data, encoding='utf-8')
 		if (int(datadict['ret'])>0):
 			if ('country' in datadict and len(datadict['country'])>0):
 				retstr +=datadict['country']
 			if ('district' in datadict):
 				retstr += datadict['district']
 			if ('province' in datadict and len(datadict['province'])>0):
 				retstr += datadict['province']
 			if ('city' in datadict and len(datadict['city'])>0):
 				retstr += datadict['city']
 			if ('isp' in datadict and len(datadict['isp'])>0):
				retstr += datadict['isp']
	except Exception, e:
		print Exception,e
	return retstr

def GetTimeStr(iseconds):
	inseconds = int(iseconds)
	hour = int(inseconds/3600)
	if hour>0:
		ntemp = inseconds - 3600 * hour
		inseconds = ntemp
	nsecond = int(inseconds%60)
	minute = int(inseconds/60)
	retstr = u'%d小时%d分钟%d秒'%(hour,minute,nsecond)
	return retstr
