import csv
import requests
import xml.etree.ElementTree as ET

import vault_structure as vs
cruise_name = 'KM0703'
vs.cruise_leaf_structure(f"{vs.r2r_cruise}{cruise_name}")
cruise_raw = f"{vs.r2r_cruise}{cruise_name}/raw/"
url = 'https://www.marine-geo.org/services/xml/datafileservice.php?data_set_uid=18306'
resp = requests.get(url)
with open(cruise_raw+'KM0703_backscatter_metadata.xml', 'wb') as f:
        f.write(resp.content)

#https://www.convertcsv.com/xml-to-csv.htm


# xml_file = cruise_raw+'KM0703_backscatter_metadata.xml'
# tree = ET.parse(xml_file)
# root = tree.getroot()
# latlon = []
# for item in root.findall('file'):
#     for mitem in item.findall('metadata'):
#         for child in mitem:
#             if child.tag == 'start':
#                     print(child.attrib)
# for item in root:        
#         spatial = {}
#         for child in item:
#             if child.tag == 'file':
#                   print(child.attrib)
                  
#             print(child.tag)
#             if child.tag == '{http://search.yahoo.com/mrss/}content':
#                 spatial['media'] = child.attrib['url']


# import xml.dom.minidom
# xml_doc = xml.dom.minidom.parse(xml_file)
# fls = xml_doc.getElementsByTagName('metadata')
# ch_el = fls.item(0)
# for m in fls:
#       m.getElementsByTagName('start')[0].childNodes[0].data