import psycopg2
import json
import pathlib
import sys
import magic
import os
import shutil
import subprocess
import re
from datetime import datetime


# TODO
#
# WELL
# 1. Install Pandas
# 2. Process the images but don't copy them
# 3. Save metadata (in json), name, size, path, HASH

#
# parse target folder and build up hash database from there. This way the process can be stopped / restarted.
# unterminated quoted string -- file names with ' or " ?
# Handle UNADJUSTEDNONRAW thumb and mini images (delete / not copy them)
# save original folder in logs -- and see if there is a discrepancy between the date there and the date in the exif
# handle .thm images (delete them?)
# handle images without exif date but date in the filename

#get timestamp
now = datetime.now() # current date and time
tstamp = now.strftime("%m%d%Y%H%M%S")

img_processed = {}



def connect_db():
    try:
        connection = psycopg2.connect(user = "andras2",
                                  password = "",
                                  host = "127.0.0.1",
                                  port = "5432",
                                  database = "phoebe")


    except (Exception, psycopg2.Error) as error :
        print ("Error while connecting to PostgreSQL", error)
    return connection

def process_exif(fname, filetype):
  
    #check if the filename has spaces
    print("vvvvvvvvvvvvvvvvvvvvvvvvvv")
    print(fname)

    #xfcommand = 'exiftool "' + str(fname) + '" | grep Date'
    xfcommand = 'exiftool -x ThumbnailImage "' + str(fname) + '"'
    stream = os.popen(xfcommand)
    output = stream.read()


    print(output)

    # Find creation date
    exifdata = {}
    output = re.split("\n",output)
    for x in output:
        t = re.split(":",x)
        t = list(filter(None,t))
        if len(t) > 1:
            k = t.pop(0).strip()
            v = ((":").join(t)).strip()
            exifdata[k] = v

    print(exifdata)

    print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")

    orig_times=['Date/Time Original','Profile Date Time','Create Date','Sony Date Time']
    year  = "0000"
    month = "00"
    day   = "00"
    for x in orig_times:
        if x in exifdata:
            #separate date and time
            stamp = exifdata[x].split(" ")
            date = stamp[0].split(":")
            year = date[0].strip()
            month = date[1].strip()
            day = date[2].strip()
            break

    folder = createdirs(year,month,day)
    target = folder + "/" + fname.split("/")[-1]
    process_file(fname,target,filetype)

def sha(f):
    f = str(f)
    
    stream = os.popen('sha1sum "' + str(f) +'"')
    res = stream.read()
    return res.split(" ")[0]

def process_file(s,t,filetype):
    # check if hashs exists. If so, we don't need to process further.
    h = sha(s)
    # check in db
    #q_hash = "select count(*) from images where hash = %s;"
    q_hash = "select count(*) from images where hash = 'x';"
    try:
        cursor = connection.cursor()
        cursor.execute(q_hash,(h))
        r = cursor.fetchall() 
        print("vvvvvvvvvvvvvvv")
        print(r[0])
        print("^^^^^^^^^^^^^^^^")
    except (Exception, psycopg2.Error) as error:
        print("Error fetching data from PostgreSQL table", error)

    if os.path.exists(t):
        # target file exists
        # get filename
        target = t.split("/")
        filename = target.pop(-1)
        fparts = filename.split(".")    # filename, parts separated by dot(s)
        fpart1 = fparts.pop(0)          # first part

        # add _dupe to the name or increase counter
        if "_dupename" in fpart1:
            # separate by _
            tparts = fpart1.split("_")
            tcounter = int(tparts.pop(-1))
            tcounter = tcounter + 1 
            tparts.append(str(tcounter))
            fpart1 = "_".join(tparts)
        else:
            fpart1 = fpart1 + "_dupename_0"

        # put it all together
        fparts.insert(0,fpart1)
        fparts = ".".join(fparts)
        target.append(fparts)
        target = "/".join(target)
        process_file(s,target,filetype) # recursive!!!
    else:
        # target file does not exist
        #print("does not exist!")

        if "_dupename" in t:
            result = "d"
        else:
            result = "c"

        l = result + " " + filetype+","+s+","+t
        log_results(result,l)

        # ADD DB RECORD
        shutil.copy2(s,t)


def log_results(r,l):
    img_processed[r] += 1
    logfile.write(l+"\n")
    global foldername
    print("["+foldername+":"+str(foldercounter)+"] c: "+str(img_processed["c"])+" | d: "+str(img_processed["d"])+" | x:"+str(img_processed["x"]), end="\r")

def createdirs(y,m,d):
    #create year folder
    p = targetpath+"processed/"+y.strip()
    try:
        os.makedirs(p)
    except FileExistsError as e:
        pass
        #if e.errno != errno.EEXIST:
    except:
        raise
    
    p += "/"+m.strip()
    try:
        os.makedirs(p)
    except FileExistsError as e:
        pass
        #if e.errno != errno.EEXIST:
    except:
        raise

    p += "/"+d.strip()
    try:
        os.makedirs(p)
    except FileExistsError as e:
        pass
        #if e.errno != errno.EEXIST:
    except:
        raise

    return(p)

def process_files(folder):
    folder=sourcepath+folder
    result = list(pathlib.Path(folder).rglob("*"))

    global foldercounter 
    foldercounter = len(result)

    for f in result:
        fname = (str(f))
        #print(fname)

        # is it a folder?
        if os.path.isdir(fname): continue

        # is it jpeg?

        filetype = ""
        try:
            filetype = magic.from_file(fname, mime=True)
            #find datetime
        except:
            print("Unexpected error:", sys.exc_info()[0], fname)

        if filetype=="image/jpeg" or filetype.split("/")[0]=="video":
            #print(str(c)+" + "+filetype+" "+fname)
            process_exif(fname,filetype)

        foldercounter = foldercounter - 1
#
# START
#


sourcepath = str(sys.argv[1])
targetpath = str(sys.argv[2])
logfile = open(targetpath+"phoebe_"+tstamp+".log","a") 
folders = os.listdir(sourcepath)
foldercounter = 0
foldername = ""

img_processed["c"]=0
img_processed["d"]=0
img_processed["x"]=0

createdirs('0000','00','00')

connection = connect_db()

cursor = connection.cursor()
# Print PostgreSQL Connection properties
print ( connection.get_dsn_parameters(),"\n")



print(folders)
for folder in folders:
    foldername = folder
    process_files(folder)

print (" ")
logfile.close()

if(connection):
    cursor.close()
    connection.close()

