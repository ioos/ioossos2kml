

import urllib2
import logging
import logging.config

def main():
  
  #logging.config.fileConfig('/Users/danramage/Documents/workspace/EyeOnEarth/eyeOnEarthKMLReCacheDebug.conf')
  logging.config.fileConfig('/home/xeniaprod/config/eyeOnEarthKMLReCache.conf')  
  logger = logging.getLogger("kmlrecacher_logging")
  logger.info("Log file opened.")
  
  urlList = ['http://utility.arcgis.com/sharing/kml?url=http://129.252.139.124/mapping/xenia/feeds/ioos/eoe/SECOORA_EyeOnEarth_LatestObs.kml&folders=&callback=&outSR=&refresh=true',
             'http://utility.arcgis.com/sharing/kml?url=http://habu.apl.washington.edu/mayorga/nvs/eyeonearth/NANOOS_EyeOnEarth_LatestObs.kml&folders=&callback=&outSR=&refresh=true'
             ]
  for urlEntry in urlList:
    try:
      logger.info("Getting url: %s" % (urlEntry))
      response = urllib2.urlopen(urlEntry)
      
      json = response.read()
      
      logger.info(json + '\n')
    except Exception,e:
      logger.exception(e)
  
  logger.info("Log file closed.")
if __name__ == "__main__":
  main()
