import logging

class logger(object):
        def __init__(self,name,path):
                #define logger
                self.logger=logging.getLogger(name)
                self.logger.setLevel(level=logging.INFO)
                handler=logging.FileHandler(path)
                handler.setLevel(logging.INFO)
                formatter=logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
                handler.setFormatter(formatter)

                console=logging.StreamHandler()
                console.setLevel(logging.INFO)
                self.logger.addHandler(handler)
                self.logger.addHandler(console)
