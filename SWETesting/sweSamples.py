from pyoos.collectors.ioos.swe_sos import IoosSweSos
from datetime import datetime, timedelta

def main():
  #The observations we're interested in.
  obsList = ['sea_water_temperature','sea_water_salinity']
  """
  Create a data collection object.
  Contructor parameters are:
    url - THe SWE endpoint we're interested in
    version - Optional default is '1.0.0' The SWE version the endpoint.
    xml - Optional default is None - The XML response from a GetCapabilities query of the server.
  """
  dataCollector = IoosSweSos(url='http://ioossos.axiomalaska.com/52n-sos-ioos-stable/sos/kvp', xml=None)

  #Loop through the offerings from the server.
  #Offerings are the stations/platforms/data device that the server house data from.
  offerings = dataCollector.server.offerings
  for offer in offerings:
    #We skip over the 'all' offering.
    #all "*network*" offerings represent aggregations of stations;
    # unlike all other offerings we've dealt with basically correspond to one station.
    if(offer.name.split(':')[-1] != 'all'):

      print "Offering: %s %s Obs: %s"\
            % (offer.name, offer.description, offer.observed_properties)


      #Check to see if the station offered has the observation we're interested in.
      #Loop through the observed_properties attribute and compare.
      obsFilterList = []
      for obs in offer.observed_properties:
        #Providers may or may not have there observed property in a mmi link format
        #like: 'http://mmisw.org/ont/cf/parameter/air_temperature'
        #We split it up just in case so we can get just the observation name.
        property = obs.rsplit('/', 1)
        if(len(property) > 1):
          property = property[1]
        if(property in obsList):
          obsFilterList.append(obs)

      if(len(obsFilterList)):
        #We create a filter based on the observations of interest,
        # and a time frame of the last 2 hours.
        dataCollector.filter(
                             variables=obsFilterList,
                             start=datetime.utcnow() - timedelta(hours=2))
        response = dataCollector.collect(offerings=[offer.name])
        print response

  return

if __name__ == "__main__":
  main()