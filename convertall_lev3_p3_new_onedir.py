#!/awips2/python/bin/python

#convertall_lev3_p2.py: Converts all level 3 radar files in specified directory to a specified new radar site.
#python 2 version
#Written by: Bryan A. Mroczka
#Science and Operations Officer / Meteorologist
#National Weather Service Tampa Bay Area
#7/11/2019
#Version 1.0

from __future__ import print_function
import os
import subprocess
import sys
from pathlib import Path
import os.path
#from __future__ import print_function
import bz2
from collections import defaultdict, namedtuple, OrderedDict
import contextlib
import datetime
import logging
import re
import struct
from struct import Struct
#from xdrlib import Unpacker
import gzip
import numpy as np

#def convert(old_stid, new_stid, path):

def convert(path, filename, new_stid, newfilename):

# Read in text file (rpg_id.txt) that comes with program defining each rpg id, lat, lon, and height per line. Assign the rpg ids, lats, lons, etc into lists for the purpose of indexing each based on the command line new_stid (new station id) argument.

    print(filename)
    print(new_stid)
    print(newfilename)
    print(path)

    with open("rpg_id.txt", "r") as ins:
         radars=[]
         latid=[]
         lonid=[]
         heightid=[]
         radid=[]
         for line in ins:
             radar=line.split(' ')[0]
             radar=radar.upper()
             radar=radar[1:]
             radars.append(radar)

             lat=line.split(' ')[1]
             lat=int(lat)
             latid.append(lat)

             lon=line.split(' ')[2]
             lon=int(lon)
             lonid.append(lon)

             hgt=line.split(' ')[3]
             hgt=int(hgt)
             heightid.append(hgt)

             rpg=line.split(' ')[4]
             rpg=int(rpg)
             radid.append(rpg)

    print(radars)
    print(radid)
    print(latid)
    print(lonid)
    print(heightid)

#find index number of the new station from the radars list based on the new_stid command line argument given. 

    x = radars.index(new_stid.upper())
    print('index num is '+str(x))

#base on index number, assign the new_stid to the newsrc_id variable, assign the newlat from the latid list, the newlon from the lonid list, and the newheight from the heightid list.

    newsrc_id=radid[int(x)]
    newsrc_id2=str(newsrc_id)
    print('newsrc_id = '+newsrc_id2)
    newlat=latid[int(x)]
    newlat2=str(newlat)
    print('newlat = '+newlat2)
    newlon=lonid[int(x)]
    newlon2=str(newlon)
    print('newlon = '+newlon2)
    newheight=heightid[int(x)]
    newheight2=str(newheight)
    print('newheight = '+newheight2)

#Tallahassee has a weird naming convention for level 3 filenames and WFO header. It uses both TAE and TLH. Here we set up for that potential so everything mimics real world.

    if new_stid == "TAE":
       new_stid2="TLH"

    newsrc_id2=str(newsrc_id)

    print("new_stid = "+new_stid)
    print("newsrc_id = "+newsrc_id2)

# Read in original level 3 data file specified on command line.

    fobj = open(filename, 'rb')

# Utilize the struct.unpack methodology to break down the file piece by piece and assign specific segments to variables. Byte structures for each binary segment were determined by studying the metpy Level3File utility.

#Start breakdown of ascii WMO header

    wmo1 = struct.unpack('7s', fobj.read(7))[0]
    wmo1_2 = wmo1.decode("utf-8")
    print('wmo line 1 ='+wmo1_2)

    wmo2 = struct.unpack('5s', fobj.read(5))[0]
    wmo2_2 = wmo2.decode("utf-8")
    print('wmo line 2 ='+wmo2_2)
    newwmo2='K'+new_stid+' '

    wmo3 = struct.unpack('7s', fobj.read(7))[0]
    wmo3_2 = wmo3.decode("utf-8")
    print('wmo line 3 ='+wmo3_2)

    wmo4 = struct.unpack('5s', fobj.read(5))[0]
    wmo4_2 = wmo4.decode("utf-8")
    print('wmo line 4 ='+wmo4_2)

    wmo5 = struct.unpack('3s', fobj.read(3))[0]
    wmo5_2 = wmo5.decode("utf-8")
    print('wmo line 5 ='+wmo5_2)
    newwmo5=new_stid
    if new_stid == "TAE":
       newwmo5=new_stid2

    wmo6 = struct.unpack('3s', fobj.read(3))[0]
    wmo6_2 = wmo6.decode("utf-8")
    print('wmo line 6 ='+wmo6_2)

#Finish breakdown of ascii WMO header

#Begin breakdown on binary portion of the file. We will only break down and assign to variables those portions of the binary that will need to be changed...basically the station identification and product identification information. The remainder portion of the file (all the metadata) left in the fobj variable after the upacking below will be held as a single unit.

    code = struct.unpack('>H', fobj.read(2))[0]
    code2=str(code)
    print('code = '+code2)
    date = struct.unpack('>H', fobj.read(2))[0]
    date2=str(date)
    print('date = '+date2)
    time = struct.unpack('>l', fobj.read(4))[0]
    time2=str(time)
    print('time = '+time2)
    msg_len = struct.unpack('>L', fobj.read(4))[0]
    msg_len2=str(msg_len)
    print('msg_len = '+msg_len2)
    src_id = struct.unpack('>h', fobj.read(2))[0]
    src_id2=str(src_id)
    print('src_id = '+src_id2)
    dest_id = struct.unpack('>h', fobj.read(2))[0]
    dest_id2=str(dest_id)
    print('dest_id = '+dest_id2)
    num_blks = struct.unpack('>H', fobj.read(2))[0]
    num_blks2=str(num_blks)
    print('num_blks = '+num_blks2)
    divider = struct.unpack('>h', fobj.read(2))[0]
    divider2=str(divider)
    print('divider = '+divider2)
    lat = struct.unpack('>l', fobj.read(4))[0]
    lat2=str(lat)
    print('lat = '+lat2)
    lon = struct.unpack('>l', fobj.read(4))[0]
    lon2=str(lon)
    print('lon = '+lon2)
    height = struct.unpack('>h', fobj.read(2))[0]
    height2=str(height)
    print('height = '+height2)
    prod_code = struct.unpack('>h', fobj.read(2))[0]
    prod_code2=str(prod_code)
    print('prod_code = '+prod_code2)
    op_mode = struct.unpack('>h', fobj.read(2))[0]
    op_mode2=str(op_mode)
    print('op_mode = '+op_mode2)
    vcp = struct.unpack('>h', fobj.read(2))[0]
    vcp2=str(vcp)
    print('vcp = '+vcp2)
    seq_num = struct.unpack('>h', fobj.read(2))[0]
    seq_num2=str(seq_num)
    print('seq_num = '+seq_num2)
    vol_num = struct.unpack('>h', fobj.read(2))[0]
    vol_num2=str(vol_num)
    print('vol_num = '+vol_num2)
    vol_date = struct.unpack('>h', fobj.read(2))[0]
    vol_date2=str(vol_date)
    print('vol_date = '+vol_date2)
    vol_start_time = struct.unpack('>l', fobj.read(4))[0]
    vol_start_time2=str(vol_start_time)
    print('vol_start_time = '+vol_start_time2)
    prod_gen_date = struct.unpack('>h', fobj.read(2))[0]
    prod_gen_date2=str(prod_gen_date)
    print('prod_gen_date = '+prod_gen_date2)
    prod_gen_time = struct.unpack('>l', fobj.read(4))[0]
    prod_gen_time2=str(prod_gen_time)
    print('prod_gen_time = '+prod_gen_time2)

#begin writing of new level 3 file with the name given from the command line argument.
    print('######################################################################')
    print(newfilename)
    output = open(newfilename, 'wb')

# _2's are for python 3 since it does not have encode for "bytes", only "str". The _2's represent the original "byte" data that has been converted to "str". Remove _2's for script is run with python 2, since python 2 has encode for bytes.

###Begin re-encoding of WMO header for new station site.

    output.write(struct.pack('7s', wmo1_2.encode('utf-8'))) #_2's
    output.write(struct.pack('5s', newwmo2.encode('utf-8')))
    output.write(struct.pack('7s', wmo3_2.encode('utf-8')))
    output.write(struct.pack('5s', wmo4_2.encode('utf-8')))
    output.write(struct.pack('3s', newwmo5.encode('utf-8')))
    output.write(struct.pack('3s', wmo6_2.encode('utf-8')))

###Finish re-encodign of WMO header for new station site.

###Begin re-encoding of binary station identification and product identification data.

    output.write(struct.pack('>H', code))
    output.write(struct.pack('>H', date))
    output.write(struct.pack('>l', time))
    output.write(struct.pack('>L', msg_len))
    output.write(struct.pack('>h', newsrc_id))
    output.write(struct.pack('>h', dest_id))
    output.write(struct.pack('>H', num_blks))
    output.write(struct.pack('>h', divider))
    output.write(struct.pack('>l', newlat))
    output.write(struct.pack('>l', newlon))
    output.write(struct.pack('>h', newheight))
    output.write(struct.pack('>h', prod_code))
    output.write(struct.pack('>h', op_mode))
    output.write(struct.pack('>h', vcp))
    output.write(struct.pack('>h', seq_num))
    output.write(struct.pack('>h', vol_num))
    output.write(struct.pack('>h', vol_date))
    output.write(struct.pack('>l', vol_start_time))
    output.write(struct.pack('>h', prod_gen_date))
    output.write(struct.pack('>l', prod_gen_time))

###Finish re-encoding of binary station identification and product identification data.

###Re-Encode the remaining binary metadata left over in the fobj variable.

    output.write(fobj.read())

#Finish and close new level 3 file

    output.close()

#This section of used in the building of this code to view output from original level 3 file using the metpy Level3File utility. It is commented out now since it does not do anything to actually aid the conversion of original to new level 3 file.

    #fobj = open(filename, 'rb')
    #f = Level3File(filename)
    #print(f)

###take in command line arguments for old radar site, new radar site, and path to files needing conservsion.

def main(argv):
    if len(argv) != 5:
        print("<old_stid> <new_stid> <path> <product string>")
        print("BMX TBW /home/bryan/Convert_Level3/data/ all")
        print("or...")
        print("BMX TBW /home/bryan/Convert_Level3/data/ NQ0")
        return
    old_stid = argv[1]
    print(old_stid)

    new_stid = argv[2]
    print(new_stid)

    path = argv[3]
    print(path)

    product_string = argv[4]
    print(product_string)

###loop through all files in directory and convert from old radar site to new radar site. Place new files in
###same directory. Create a new directory for the new files named with the new_stid...if it does not already exist.

    if not os.path.exists(path+new_stid):
       os.makedirs(path+new_stid)  
       print(path+new_stid)   

    if product_string != 'all':

      for filename in os.listdir(path):

        if product_string in filename:
           print(filename)
 
           newfilename=filename

           cmd1 = 's/'+old_stid+'/'+new_stid+'/g'
           cmd2 = 'sed \''+cmd1+'\' <<<"'+newfilename+'"'
        
           print('cmd = '+cmd2)
           newfilename=subprocess.check_output(cmd2, shell=True)

           newfilename=newfilename.decode('utf-8')

           if old_stid == "TAE":

              old_stid2="TLH"
              cmd1 = 's/'+old_stid2+'/'+new_stid+'/g'
              cmd2 = 'sed \''+cmd1+'\' <<<"'+newfilename+'"'
              newfilename=subprocess.check_output(cmd2, shell=True)

              newfilename=newfilename.decode('utf-8')

           if new_stid == "TAE":

              new_stid2="TLH"
              cmd1 = 's/'+old_stid+'/'+new_stid2+'/g'
              cmd2 = 'sed \''+cmd1+'\' <<<"'+newfilename+'"'
              newfilename=subprocess.check_output(cmd2, shell=True)
  
              newfilename=newfilename.decode('utf-8')

#           if os.path.isdir(filename):
#              print("this is a directory. Moving on...")
#           else:
#              print('filename = '+newfilename)
#              cmd = './l3convert_p3.py '+new_stid+' '+path+filename+' '+path+new_stid+'/'+newfilename
#              os.system(cmd)          

           if filename.find(old_stid) == -1:
              print('not the correct file')
           else:
              print('correct file')
#              cmd = './convert_lev3_p3.py '+new_stid+' '+path+filename+' '+path+new_stid+'/'+newfilename
#              os.system(cmd)  

              convert(path, path+filename, new_stid, path+new_stid+'/'+newfilename)

    elif product_string == 'all':

      for filename in os.listdir(path):

           print(filename)
 
           newfilename=filename

           cmd1 = 's/'+old_stid+'/'+new_stid+'/g'
           cmd2 = 'sed \''+cmd1+'\' <<<"'+newfilename+'"'
        
           print('cmd = '+cmd2)
           newfilename=subprocess.check_output(cmd2, shell=True)

           newfilename=newfilename.decode('utf-8')

           if old_stid == "TAE":

              old_stid2="TLH"
              cmd1 = 's/'+old_stid2+'/'+new_stid+'/g'
              cmd2 = 'sed \''+cmd1+'\' <<<"'+newfilename+'"'
              newfilename=subprocess.check_output(cmd2, shell=True)

              newfilename=newfilename.decode('utf-8')

           if new_stid == "TAE":

              new_stid2="TLH"
              cmd1 = 's/'+old_stid+'/'+new_stid2+'/g'
              cmd2 = 'sed \''+cmd1+'\' <<<"'+newfilename+'"'
              newfilename=subprocess.check_output(cmd2, shell=True)
  
              newfilename=newfilename.decode('utf-8')

#           if os.path.isdir(filename):
#              print("this is a directory. Moving on...")
#           else:
#              print('filename = '+newfilename)
#              cmd = './l3convert_p3.py '+new_stid+' '+path+filename+' '+path+new_stid+'/'+newfilename
#              os.system(cmd)          

           if filename.find(old_stid) == -1:
              print('not the correct file')
           else:
              print('correct file')
#              cmd = './convert_lev3_p3.py '+new_stid+' '+path+filename+' '+path+new_stid+'/'+newfilename
#              os.system(cmd)  

              convert(path, path+filename, new_stid, path+new_stid+'/'+newfilename)

    else:
      print('not a valid product string input')

##############################################################

    for filename in os.listdir(path+new_stid):
    # Clean the filename: remove quotes and $'\n'
     cleaned_filename = path+new_stid+'/'+filename.replace("'","").replace("$\\n", "").strip()

    # Full path for old and new filenames
     old_file_path = os.path.join(path+new_stid, filename)
     new_file_path = os.path.join(path+new_stid, cleaned_filename)

    # Rename the file if the cleaned filename is different from the original
     if old_file_path != new_file_path:
        os.rename(old_file_path, new_file_path)
        print(f"Renamed: {old_file_path} -> {new_file_path}")

###send arguments to convert function 

#    convert(old_stid, new_stid, path)

if __name__ == '__main__':
    main(sys.argv)
