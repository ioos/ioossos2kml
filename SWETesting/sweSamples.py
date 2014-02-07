from pyoos.collectors.ioos.swe_sos import IoosSweSos
from owslib import ows
from datetime import datetime, timedelta

import traceback

def main():

  #The observations we're interested in. THis is another gotcha, is it water_temperature or sea_water_temperature...
  obsList = ['water_temperature','salinity']
  """
  Create a data collection object.
  Contructor parameters are:
    url - THe SWE endpoint we're interested in
    version - Optional default is '1.0.0' The SWE version the endpoint.
    xml - Optional default is None - The XML response from a GetCapabilities query of the server.
  """
  dataCollector = IoosSweSos(url='http://129.252.139.124/thredds/sos/carocoops.cap2.buoy.nc', xml=None)

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
      #We want to query just the observations n the obsOfInterest list.
      #The observer_properties should be coded with an MMI href, however due
      #to ncSOS bug: https://github.com/asascience-open/ncSOS/issues/107 it adds
      #a URN pattern. Regardless, an end user should just need to know the observation
      #name and not the MMI url, so this tries to do a simple
      #string find of our desired observation in the observed_property.
      for obsOfInterest in obsList:
        for observedProperty in offer.observed_properties:
          if(observedProperty.find(obsOfInterest) != -1):
            #Because of the ncSOS bug 107, we don't add the observedProperty. When
            #that bug gets addressed, we would add the observedProperty.
            #obsFilterList.append(observedProperty)
            obsFilterList.append(obsOfInterest)
            break

      if(len(obsFilterList)):
        #We create a filter based on the observations of interest,
        # and a time frame of the 12 hours of data from yesterday.
        #There is an ncSOS bug that affects getting the current data
        #Issue: https://github.com/asascience-open/ncSOS/issues/112
        """
        Filter function params:
        bbox - Bounding box, not implemented for SOS calls currently.
        start  - start date for records
        end - end date for records
        features - I assume it's a list of the stations of interest. Not sure why this is here
          and then the offerings parameter in the collect() function.
        variables - Poorly named parameter to house the observed_properties
        """
        try:
          dataCollector.filter(
                                 variables=obsFilterList,
                                 start=datetime.utcnow() - timedelta(hours=24),
                                 end=datetime.utcnow()  - timedelta(hours=12))
          """
          collect() params:
          offering - Station list

          responseFormat - The desired return format for the SOS call.
          eventTime - If the start/end times are not provided in the filter(), this param
            can be passed in. Has to be properly formatted: %Y-%m-%dT%H:%M:%SZ/%Y-%m-%dT%H:%M:%SZ for a start/end time/date.

          returns a list of OmObservation objects. OmObservation object has no member functions.
          """
          response = dataCollector.collect(offerings=[offer.name])
          for obsRec in response:
            stationRec = obsRec.feature
            print "Station: %s Location: %s" % (stationRec.name, stationRec.get_location())

            #The elements are a list of the observed_properties returned wrapped in a Point object.
            for obsProp in stationRec.get_elements():
              print "Observation Date/Time: %s" % (obsProp.get_time())
              #print "Member names: %s" % (obsProp.get_member_names())
              for member in obsProp.get_members():
                for key,value in member.iteritems():
                  print "%s = %s" % (key, value)

        except ows.ExceptionReport,e:
          traceback.print_exc(e)

        except Exception,e:
          traceback.print_exc(e)


  return

if __name__ == "__main__":
  main()