"""
Revisions
Date: 2013-06-24
Changes: Verify we have the platform_type fields filled out in the database.
"""
import sys
import logging.config
import optparse
import ConfigParser
import datetime
import codecs

from mako.template import Template
from mako import exceptions as makoExceptions
from geoalchemy import * 
from sqlalchemy import or_
from xeniatools.xeniaSQLAlchemy import xeniaAlchemy, multi_obs, organization, platform, uom_type, obs_type, m_scalar_type, m_type, sensor
from xeniatools.xenia import uomconversionFunctions



def main():
  
  parser = optparse.OptionParser()
  parser.add_option("-c", "--ConfigFile", dest="configFile",
                    help="INI Configuration file." )
  (options, args) = parser.parse_args()
  
  if(options.configFile is None):
    parser.print_help()
    sys.exit(-1)
    
  configFile = ConfigParser.RawConfigParser()
  configFile.read(options.configFile)
  
  try:
    logger = None
    logConfFile = configFile.get('logging', 'configFile')
    if(logConfFile):
      logging.config.fileConfig(logConfFile)
      logger = logging.getLogger("eye_on_earth_logging")
      logger.info("Log file opened.")
  except ConfigParser.Error, e:
    print("No log configuration file given, logging disabled.")
    
  try:      
    bbox = configFile.get('output', 'bbox')
    observations = configFile.get('output', 'observations').split(',')
    templateFilepath = configFile.get('output', 'kmltemplatefile')
    kmlOutfilename = configFile.get('output', 'kmlfilename')
    uomConversionFilename = configFile.get('output', 'uomconversionfilename')
    dbUser = configFile.get('Database', 'user')
    dbPwd = configFile.get('Database', 'password')
    dbHost = configFile.get('Database', 'host')
    dbName = configFile.get('Database', 'name')
    dbConnType = configFile.get('Database', 'connectionstring')
  except ConfigParser.Error, e:
    if(logger):
      logger.exception(e)
  else:
    
    uomConverter = uomconversionFunctions(uomConversionFilename)
        
    db = xeniaAlchemy()      
    if(db.connectDB(dbConnType, dbUser, dbPwd, dbHost, dbName, False) == True):
      if(logger):
        logger.info("Succesfully connect to DB: %s at %s" %(dbName,dbHost))
    else:
      logger.error("Unable to connect to DB: %s at %s. Terminating script." %(dbName,dbHost))
    
    kmlData = {}
    kmlData['iconStyles'] = [{'id' : 'buoy', 'url' : 'http://129.252.37.86/rcoos/resources/images/legend/buoy-default.png'},
                             {'id' : 'shore_station', 'url' : 'http://129.252.37.86/rcoos/resources/images/legend/shore_station-default.png'},
                             {'id' : 'land_station', 'url' : 'http://129.252.37.86/rcoos/resources/images/legend/land_station-default.png'},
                             {'id' : 'estuary_station', 'url' : 'http://129.252.37.86/rcoos/resources/images/legend/estuary_station_default.png'},
                             {'id' : 'river_gauge', 'url' : 'http://129.252.37.86/rcoos/resources/images/legend/river_gauge-default.png'}
                             ]
    kmlData['IOOSRA'] = {'shortName' : "SECOORA",
                         'URL' : "http://www.secoora.org",
                         'imgLogo' : "http://129.252.37.86/rcoos/resources/images/legend/logo_secoora_eoe.png",
                         'ioosLogo' : "http://129.252.37.86/rcoos/resources/images/legend/logo_ioos_eoe.png"
                         }
    mTypeIds = []
    for obsuom in observations:
      obs,uom = obsuom.split(' ')
      typeId = db.mTypeExists(obs,uom)
      if(typeId):
        mTypeIds.append(typeId)
      else:
        if(logger):
          logger.error("Observation: %s UOM: %s not found in database." % (obs,uom))
    
    bboxPoly = 'POLYGON((%s))' % (bbox)    
    dateOffset = (datetime.datetime.utcnow() - datetime.timedelta(hours=3)).strftime("%Y-%m-%dT%H:00:00")
    try:
      obsRecs = db.session.query(multi_obs)\
        .join(sensor,sensor.row_id == multi_obs.sensor_id)\
        .join(platform,platform.row_id == sensor.platform_id)\
        .join(organization,organization.row_id == platform.organization_id)\
        .join(m_type,m_type.row_id == multi_obs.m_type_id)\
        .join(m_scalar_type,m_scalar_type.row_id == m_type.m_scalar_type_id)\
        .join(obs_type,obs_type.row_id == m_scalar_type.obs_type_id)\
        .join(uom_type,uom_type.row_id == m_scalar_type.uom_type_id)\
        .filter(multi_obs.m_date > dateOffset)\
        .filter(multi_obs.m_type_id.in_(mTypeIds))\
        .filter(multi_obs.d_top_of_hour == 1)\
        .filter(sensor.s_order == 1)\
        .filter(platform.active < 3)\
        .filter(platform.the_geom.within(WKTSpatialElement(bboxPoly, -1)))\
        .order_by(platform.row_id)\
        .order_by(multi_obs.m_date.desc())\
        .all()
    except Exception,e:
      if(logger):
        logger.exception(e)
    else:
      platforms = {}
      for obsRec in obsRecs:
        platRec = {}
        observations = []
        if(obsRec.sensor.platform.short_name in platforms):
          platRec = platforms[obsRec.sensor.platform.short_name]
          observations = platRec['observations']
        else:        
          if(logger):
            logger.info("%s processing observations." % (obsRec.sensor.platform.short_name))
          platforms[obsRec.sensor.platform.short_name] = platRec
          platRec['longName'] = obsRec.sensor.platform.short_name
          if(obsRec.sensor.platform.description and len(obsRec.sensor.platform.description)):
            platRec['description'] = obsRec.sensor.platform.description
          else:
            platRec['description'] = obsRec.sensor.platform.short_name
            
          platRec['latestTimeUTCstr'] = obsRec.m_date.strftime("%Y-%m-%d %H:%M:%S")
          platRec['platformURL'] = obsRec.sensor.platform.url
          platRec['longitude'] = obsRec.sensor.platform.fixed_longitude
          platRec['latitude'] = obsRec.sensor.platform.fixed_latitude
          # DWR 2013-06-24
          # Verify platform has platform_type info. When it doesn't we log a message below. We should
          # make sure to add the data to the db. 
          if(obsRec.sensor.platform.platform_type and 
             obsRec.sensor.platform.platform_type.type_name.lower() == 'buoy'):
            platRec['type'] = 'Buoy'          
            platRec['iconName'] = 'buoy'
            platRec['iconURL'] = 'http://129.252.37.86/rcoos/resources/images/legend/buoy-default.png'
          else:
            if(obsRec.sensor.platform.organization.short_name.lower() == 'usgs'):
              platRec['type'] = 'River Gauge'
              platRec['iconName'] = 'river_gauge'
              platRec['iconURL'] = 'http://129.252.37.86/rcoos/resources/images/legend/river_gauge-default.png'
            else:  
              platRec['type'] = 'Shore Station'
              platRec['iconName'] = 'shore_station'
              platRec['iconURL'] = 'http://129.252.37.86/rcoos/resources/images/legend/shore_station-default.png'
            # DWR 2013-06-24
            # Logout message whenever the platform doesn't have the platform_type entry in the database.
            if(obsRec.sensor.platform.platform_type == None):
              if(logger):
                logger.error("Platform: %s has no platform_type" % (obsRec.sensor.platform.short_name))
          platRec['operator'] = obsRec.sensor.platform.organization.short_name
          platRec['operatorURL'] = obsRec.sensor.platform.organization.url
          platRec['observations'] = observations
        
  
        displayLabel = uomConverter.getDisplayObservationName(obsRec.sensor.m_type.scalar_type.obs_type.standard_name)
        if(displayLabel == None):
          displayLabel = obsRec.sensor.m_type.scalar_type.obs_type.standard_name
          
        #IIf the observation already exists in the array, that is the latest, so skip this one.
        addObs = True
        for obs in observations:
          if(obs['longName'] == displayLabel and platRec['latestTimeUTCstr'] != obsRec.m_date.strftime("%Y-%m-%d %H:%M:%S")):
            addObs = False
            break
        if(addObs):
          #Verify the depth is in the depth range we are interested in.
          #depthInRange = True          
          #if(obsRec.m_z != None and obsRec.m_z != -99999.0 and obsRec.m_z != -9999.0):
          #  if(abs(obsRec.m_z) > 5):
          #    depthInRange = False
          
          #if(depthInRange):
          displayUOM = uomConverter.getUnits(obsRec.sensor.m_type.scalar_type.uom_type.standard_name, obsRec.sensor.m_type.scalar_type.uom_type.standard_name )
          if(displayUOM == None):
            displayUOM = obsRec.sensor.m_type.scalar_type.uom_type.standard_name
          depthstr = 'N/A'
          depthDescrTitle = ''
          if(obsRec.m_z != None and obsRec.m_z != -99999.0 and obsRec.m_z != -9999.0):
            depthstr = obsRec.m_z
            
          obs = {'longName' : displayLabel,
                  #'uom' : displayUOM.encode('utf-8', errors='xmlcharrefreplace'),
                  'uom' : displayUOM,
                  'valuestr' : obsRec.m_value,
                  'timeUTCstr' : obsRec.m_date.strftime("%Y-%m-%d %H:%M:%S"),
                  'depthDescrTitle' : depthDescrTitle,
                  'depthstr' : depthstr
                }
          observations.append(obs)
      
      platformList = []
      for platformKey in platforms:
        platformList.append(platforms[platformKey])
      kmlData['platforms'] = platformList

      try:  
        mytemplate = Template(filename=templateFilepath)
        
        kmlOutFile = codecs.open(kmlOutfilename, mode='w', encoding='utf-8')  
        #kmlFileBuf = mytemplate.render(kmlData=kmlData)
        kmlOutFile.write(mytemplate.render(kmlData=kmlData))
      except IOError,e:
        if(logger):
          logger.exception(e)  
      except Exception,e:
        if(logger):
          logger.exception(e)
        
    #Disconnect from the database.    
    try:
      db.disconnect()
    except Exception,e:
      if(logger):
        logger.exception(e)    
    if(logger):
      logger.info("Closing log file")    
if __name__ == "__main__":
  main()      
