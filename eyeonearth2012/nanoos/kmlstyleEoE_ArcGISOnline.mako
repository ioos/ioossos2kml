<?xml version="1.0" encoding="UTF-8"?>
<kml xmlns="http://www.opengis.net/kml/2.2">
<Document>
<!-- kmlstyleEoE_ArcGISOnline.mako. 
3/25/2013.  Emilio. Updated to new IOOS domain name, www.ioos.noaa.gov
11/20/2012. Emilio. Updates, polish. Tweaked and centered update_time_latest table row class.
10/25/2012. Emilio. Reworked the IOOS and RA logo images section.
10/24/2012. Emilio. Based on Dan's version from 10/19 -->
  % if kmlData['iconStyles']:
    % for iconStyle in kmlData['iconStyles']:
      <Style id="${iconStyle['id']}">
        <IconStyle>
          <Icon>
            <href>${iconStyle['url']}</href>
          </Icon>
        </IconStyle>
      </Style>      
    % endfor
  % endif
  <!-- kmlData['platforms'] is a list of dictionaries (one dict per platform) -->
  % for platform in kmlData['platforms']:
    <Placemark>
      <name>${platform['longName']}</name>
      <description>
        <![CDATA[
          <style type="text/css">
            .platform_block
            {
              width: 237px;
            }
            hr.divider {
              border: 0;
              height: 0;
              border-top: 1px solid rgba(0, 0, 0, 0.1);
              border-bottom: 1px solid rgba(255, 255, 255, 0.3);
            }

            .header_title
            {
              text-align: center;
              font-weight:bold;
              #font-size: 1.2em;
              font-size: 0.9em;
              padding-top: 0px;
              padding-bottom: 0px;
            }
            .header_subtitle
            {
              text-align: center;
              font-weight:bold;
              font-size: 1.0em;
              padding-bottom: 2px;
            }

            .platform_info
            {
              padding-top: 2px;
              padding-bottom: 2px;
            }
            table.platform_table
            {
              width: 100%;
              border : 1;
              background-color: #E8E6E7;
              padding-bottom: 5px;
            }
            td.platform_desc_col
            {
              font-weight:bold;
              font-size: 1.0em;
              width: 45%;
            }
            td.platform_desc
            {
              #font-size: 0.9em;
              font-size: 0.8em;
              text-align: center;
            }
            .observations
            {
              padding-top: 2px;
              padding-bottom: 0px;
            }
            table.observations_table
            {
              width: 100%;
              border : 1;
              background-color: #E8E6E7;
              padding-bottom: 10px;
            }
            td.update_time_latest
            {
              font-weight: bold;
              font-size: 0.9em;
              text-align: center;
            }
            td.update_time_gmt
            {
              font-style: italic;
              font-size: 0.9em;
              text-align: center;
            }
            td.obs_name
            {
              width: 60%;
              font-size: 0.9em;
              font-weight: bold;
            }
            td.obs_properies
            {
              font-size: 0.9em;
              text-align: right;
            }
            td.obs_properies_units
            {
              font-size: 0.9em;
            }
            td.obs_properies_dtandz
            {
              font-size: 0.8em;
              text-align: right;
            }

            //.platform_link {
            //  padding-top: 2px;
            //  padding-bottom: 2px;
            //  font-size: 0.8em;
            //    }

            .footer
            {
              padding-top: 2px;
              padding-bottom: 0px;
            }
            table.ra_info_table
            {
              font-size: 0.8em;
              padding-top: 0px;
              padding-bottom: 0px;
            }
            td.ra_info
            {
              font-size: 0.8em;
              text-align: center;
              padding-top: 0px;
              padding-bottom: 0px;
            }
            //td.platform_link {
            //  padding-top: 0px;
            //  padding-bottom: 0px;
            //  font-size: 1em;
            //  text-align: center;
            //}
          </style>
        <div class="platform_block">
          
          <!-- HEADER BLOCK -->
          <div class="header">
            <div class="header_title">IOOS Surface Ocean Conditions</div>
            <hr class="divider" />
            <div class="header_subtitle"><img align="top" src="${platform['iconURL']}">${platform['description']}</div>
            <div class="platform_info">
              <table class="platform_table">
                <tr>
                  <td class="platform_desc"><b>${platform['type']}</b> sensor platform <b>operated</b> by <a href="${platform['operatorURL']}">${platform['operator']}</a> (USA). <b>Data distributed</b> by <a href="${kmlData['IOOSRA']['URL']}">${kmlData['IOOSRA']['shortName']}</a>. <br />
                  <b>Additional</b> <a href="${platform['platformURL']}">platform data and information are available.</a></td>
                </tr>
              </table>
            </div>
          </div>
          <!-- END HEADER BLOCK -->
          
          <!-- OBSERVATIONS (BODY) BLOCK -->
          <div class="observations">
            <table class="observations_table">
              <tr>
                <td colspan="4" class="update_time_latest">Updated: ${platform['latestTimeUTCstr']}</th>
              </tr>
              <tr>
                <td colspan="4" class="update_time_gmt">Times are in UTC/GMT</td>
              </tr>
              <!-- platform['observations'] is a list of dictionaries (one dict per observation) -->
              % for observation in platform['observations']:
                <tr>
                  <td class="obs_name">${observation['longName']}:</td>
                  <td class="obs_properies">${observation['valuestr']}</td>
                  <td class="obs_properies_units">${observation['uom']}</td>
                  <!-- If we have an observation time that doesn't match the platform time, then let's create a new row so we can display the time and depth. -->
                  % if observation['timeUTCstr'] != platform['latestTimeUTCstr']:
                    </tr>
                    <tr>
                      <td colspan="3" class="obs_properies_dtandz">${observation['timeUTCstr']}, ${observation['depthstr']}</td>
                  % else:
                    <td class="obs_properies_dtandz" title="${observation['depthDescrTitle']}">(${observation['depthstr']})</td>
                  % endif
                </tr>
              % endfor
            </table>
          </div>
          <!-- END OBSERVATIONS (BODY) BLOCK -->
          
          <!-- FOOTER BLOCK -->
          <div class="footer">
            <table class="ra_info_table">
              <tr>
                <td width="50%" align="middle">
                  <a href="${kmlData['IOOSRA']['URL']}">
                  <img src="${kmlData['IOOSRA']['imgLogo']}" title="${kmlData['IOOSRA']['shortName']}"></a>
                </td>
                <td align="middle">
                  <a href="http://www.ioos.noaa.gov">
                  <img src="${kmlData['IOOSRA']['ioosLogo']}" title="US IOOS"></a>
                </td>
              </tr>
              <tr>
                <td class="ra_info" colspan="2" align="center"><a href="${kmlData['IOOSRA']['URL']}">${kmlData['IOOSRA']['shortName']}</a> is a <a href="http://www.ioos.noaa.gov/regional.html">Regional Association</a> of the <a href="http://www.ioos.noaa.gov">U.S. Integrated Ocean Observing System (IOOS)</a></td>
              </tr>
            </table>
          </div>
          <!-- END FOOTER BLOCK -->
        
        </div>
        ]]>
      </description>
      
      <styleUrl>#${platform['iconName']}</styleUrl>
      <Point>
        <coordinates>
          ${platform['longitude']}, ${platform['latitude']}, 0
        </coordinates>
      </Point>
    </Placemark>
  %endfor
</Document>
</kml>
