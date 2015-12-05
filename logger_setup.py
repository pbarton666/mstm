import logging
import logging.handlers

#log file name - lives in the script directory
LOG_FILE='db_loader.log'

#**********************************************************

logger = logging.getLogger('trans_logger')
h = logging.handlers.RotatingFileHandler(LOG_FILE, 
                                         'a', 
                                         maxBytes=2*1024*1024, 
                                         backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(levelname)s - '
                              '%(filename)s:%(lineno)s - %(message)s',
                              datefmt="%Y-%m-%d %H:%M:%S")
h.setFormatter(formatter)
logger.addHandler(h)
