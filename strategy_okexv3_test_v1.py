import sys,os,time,json
import zlib
from websocket import create_connection

class strategy(object):
	def __init__(self):
		self.url='wss://real.okex.com:8443/ws/v3'
		#定义变量
		
	def on_depth_data(self,data):
		print('----->depth:%s'%data)
		#计算指标
		#编写策略

	def on_kbar_data(self,data):
		print('=====>kbar:%s'%data)
		#计算指标
		#编写策略

	def run(self):
		while True:			#有时websocket会断开，需要自动重连
			self.run_forever()
			print('run error,retry...')
			time.sleep(5)

	def run_forever(self):
		ws = create_connection(self.url)
		ins='BTC-USDT'			#合约
		gral='60'			#k线的周期，按秒计
		depthStr = {"op": "subscribe", "args":["spot/depth5:%s"%ins,"spot/candle%ss:%s"%(gral,ins)]}
		ws.send(json.dumps(depthStr))
		while True:
			compressData=ws.recv()
			result=self.gzip_decode(compressData)
			if 'ping' in result:
				ws.send(result.replace('ping','pong'))
			elif result=='pong':
				pass
			else:
				data=json.loads(result)
				if 'table' in data:
					if 'depth' in data['table']:
						self.on_depth_data(data['data'])
					elif 'candle' in data['table']:
						self.on_kbar_data(data['data'])

	def gzip_decode(self,data):
		decompress = zlib.decompressobj(-zlib.MAX_WBITS)
		inflated = decompress.decompress(data)
		inflated += decompress.flush()
		return inflated.decode('utf-8')		

if __name__ == '__main__':			#策略入口，运行策略
	strategy().run()
