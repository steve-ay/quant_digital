# -*- coding: utf-8 -*-
import sys,os,time,datetime,json
import requests,configparser
import threading
import zmq,ssl
import traceback
from websocket import create_connection
from common.logger import logger

class binanceserver(object):
	def __init__(self):
		cf=configparser.ConfigParser()
		cf.read('config.ini')

		self.logger=logger('binance',os.path.join(os.getcwd(),"log","%s.log"%(time.strftime('%Y%m%d%H',time.localtime(time.time()))))).logger
		self.logger.info('=================================binance data server============================')
		self.instruments=json.loads(cf.get('config','instruments'))

		context = zmq.Context()
		self.socket = context.socket(zmq.PUB)
		self.socket.bind("tcp://*:30000")
		self.status=0

	def run(self):
		while True:
			self.run_forever()
			self.logger.error("exception run exit....")
			time.sleep(5)

	def run_forever(self):
		threads=[]
		if 'd' in self.instruments:
			th=threading.Thread(target=self.futures_thread,args=('d',))
			threads.append(th)
		if 'f' in self.instruments:
			th=threading.Thread(target=self.futures_thread,args=('f',))
			threads.append(th)
		for t in threads:
			t.setDaemon(True)
			t.start()
		for t in threads:
			t.join()
		self.status=0

	def futures_thread(self,tp='d'):
		lastk={}
		for ins in self.instruments[tp]:
			for gral in self.instruments[tp][ins]:
				while True:
					data=requests.get('https://%sapi.binance.com/%sapi/v1/klines?symbol=%s&interval=%s'%(tp,tp,ins,gral)).json()
					self.logger.info('https://%sapi.binance.com/%sapi/v1/klines?symbol=%s&interval=%s'%(tp,tp,ins,gral))
					if len(data)>0:
						data=sorted(data,key=lambda y:y[0])
						path='data/binance#%s@kline_%s.csv'%(ins,gral)
						lasttime=0
						if os.path.exists(path):
							cmd = "tail -n 1 %s"%path
							f = os.popen(cmd)
							txt = f.readline()
							if len(txt.strip())>0:
								lasttime=int(txt.strip().split(',')[0])
						f=open(path,'a')
						for row in data:
							if int(row[0])>lasttime and row!=data[-1]:
								f.write('%s,%s,%s,%s,%s,%s\n'%(row[0],row[1],row[2],row[3],row[4],row[5]))
							lastk['%s@kline_%s'%(ins,gral)]={'stream': '%s@kline_%s'%(ins,gral), 'data': [row]}
						self.logger.info('%s,%s,%s,%s'%(ins,gral,lastk['%s@kline_%s'%(ins,gral)],len(data)))
						f.close()
						break
		ws = None
		st=''
		for ins in self.instruments[tp]:
			st+='/%s@depth5'%ins
			for gl in self.instruments[tp][ins]:
				st+='/%s@kline_%s'%(ins,gl)
		ws_url = "wss://%sstream.binance.com/stream?streams=%s"%(tp,st[1:])
		while True:
			try:
				ws = create_connection(ws_url, sslopt={"cert_reqs": ssl.CERT_NONE})
				break
			except:
				self.logger.error('binance connect ws error,retry...')
				time.sleep(5)
		while True:
			try:
				result=ws.recv()
				#self.logger.info(result)
				if 'ping' in result:
					ws.send(result.replace('ping','pong'))
				else:
					data=json.loads(result)
					if 'stream' in data:
						if '@depth5' in data['stream']:
							if data['data']['e']=='depthUpdate':
								newdata={'stream':data['stream'],'data':[{'bids':data['data']['b'],'asks':data['data']['a'],'ts':int(data['data']['E']),'instrument_id':data['data']['s'].lower()}]}
								self.socket.send_string(json.dumps(newdata))
						elif '@kline' in data['stream']:
							canins=data['stream']
							if 'data' in data and lastk[canins]['data'][0][0]!=data['data']['k']['t']:
								self.socket.send_string(json.dumps(lastk[canins]))
								self.logger.info('push new->%s'%lastk[canins])
								path='data/binance#%s.csv'%canins
								f=open(path,'a')
								f.write('%s,%s,%s,%s,%s,%s\n'%(lastk[canins]['data'][0][0],lastk[canins]['data'][0][1],lastk[canins]['data'][0][2],lastk[canins]['data'][0][3],lastk[canins]['data'][0][4],lastk[canins]['data'][0][5]))
								f.close()
							lastk[canins]={'stream':data['stream'],'data':[[data['data']['k']['t'],data['data']['k']['o'],data['data']['k']['h'],data['data']['k']['l'],data['data']['k']['c'],data['data']['k']['v'],data['stream'].split('@')[0]]]}
						else:
							self.logger.info(data)
					elif 'event' in data and data['event']=='subscribe':
							self.logger.info(data)
					else:
						self.logger.info('ERROR:%s'%data)
				if self.status>0:
					break
			except Exception as e:
				self.logger.error("binance run->traceback=%s"%traceback.format_exc())
				self.status+=1
				break    

if __name__ == '__main__':
	binanceserver().run()
