## This script will connect to the NOAA Database and extract temperature data for Whitewater, WI
## It will then convert that data into an format that is easier to work with within Tableau/Excel
## This script is programmed to run with both Python 3.x and 2.x interpreters for compatibility.
## Created By: Thomas Hunt
## Last Updated: 02/26/2020
##
## Notes:
##      The link to the index of the NOAA database is: ftp://ftp.ncdc.noaa.gov/pub/data/ghcn/daily/
##      Whitewaters Station Code is: USC00479190
##
## Worklog:
##  02/19/2020: Created Script
##              Implemented download functions for Python 2 & 3
##              Implemented convertToCSV function
##              Implemented main & helper functions
##  
##  02/26/2020: Added Documentation
##              Fixed convertToCSV function
##              Implemented parsing function to turn the temp CSV file into Month objects
##              Implemented a Parser class that is used to manipulate and output the data
##              Implemented a toCSV function that turns the data held in the Parser object into an array of CSV lines
##  03/02/2020: Fixed issue where missing data was stored as 999.9, throwing off calculations. Is now represented by an empty string
## TODO: Finish Documentation

######################################## Imports #########################################################################################################################################################

import sys
import os

######################################## Main Function Definition #########################################################################################################################################
""" Main driver function to run the script. It is called at the bottom of the script. """
def main():
    # Define FTP variables
    ftp_url = "ftp.ncdc.noaa.gov"
    file_path = '/pub/data/ghcn/daily/all/'
    file_name = "USC00479190.dly"

    ## Check Python Interpreter version and run the approprate download code
    try:
        if(checkVersion() == 2):
            downloadPY2(ftp_url, file_path, file_name)
        else:
            downloadPY3(ftp_url, file_path, file_name)
    except:
        ## If a download error occured, stop the script
        print("\nAn error has occured, stopping script.")
        return 

    ## Convert Space-Delimeted file to CSV for to make things easier
    print("Converting to CSV...")
    convertToCSV(file_name)
    print("Done!")

    ## Process the csv
    print("Processing Data...")
    processData('temp.csv')
    print("Done!")

    ## Cleanup Temporary files
    cleanup()

######################################## Class Definitions ################################################################################################################################################
class Day:
    def __init__(self, day):
        self.day = day

class Month:
    def __init__(self, date):
        self.date = date
        self.TMAX = []
        self.TMIN = []
        self.TOBS = []
        self.PRCP = []
        self.SNOW = []
        self.SNWD = []
        print(self.date)

    def add_data(self, data):
        data = data.split(',')
        header = data[0]
        data = data[1:]
        if(header == "TMAX"):
            self.TMAX = data
        elif(header == "TMIN"):
            self.TMIN = data
        elif(header == "TOBS"):
            self.TOBS = data
        elif(header == "PRCP"):
            self.PRCP= data
        elif(header == "SNOW"):
            self.SNOW = data
        elif(header == "SNWD"):
            self.SNWD = data

    def __str__(self):
        return self.date

class Parser:
    def __init__(self):
        self.Months = []

    def parse_date(self, datestring):
        year = datestring[:4]
        month = datestring[4:]
        date = (year, month)
        return date

    def addDate(self, data):
        datestring = data.split(',')[0][11:17]
        data = data[17:].strip()
        date = self.parse_date(datestring)
        mExists = False
        for m in self.Months:
            if m.date == date:
                mExists = True
                m.add_data(data)
        if not mExists:
            self.Months.append(Month(date))
            self.Months[len(self.Months)-1].add_data(data)

    def toCSV(self):
        lines = []
        for M in self.Months:
            numDays = len(M.PRCP)
            for D in range(0, numDays):
                s_date  = "{}/{}/{}".format(M.date[1], D+1, M.date[0])
                v_TMAX, v_TMIN, v_TOBS, v_PRCP, v_SNOW, v_SNWD = "","","","","",""
                m_TMAX, m_TMIN, m_TOBS, m_PRCP, m_SNOW, m_SNWD = "","","","","",""
                q_TMAX, q_TMIN, q_TOBS, q_PRCP, q_SNOW, q_SNWD = "","","","","",""
                s_TMAX, s_TMIN, s_TOBS, s_PRCP, s_SNOW, s_SNWD = "","","","","",""
                
                ## Parse TMAX
                if(len(M.TMAX) > 0):
                    d_TMAX = M.TMAX[D].split('|')
                    v_TMAX = str((float(d_TMAX[0])/10)) if ((float(d_TMAX[0])/10) < 999) and ((float(d_TMAX[0])/10) > -999) else ""
                    m_TMAX = str(d_TMAX[1])
                    q_TMAX = str(d_TMAX[2])
                    s_TMAX = str(d_TMAX[3])

                ## Parse TMIN
                if(len(M.TMIN) > 0):
                    d_TMIN = M.TMIN[D].split('|')
                    v_TMIN = str((float(d_TMIN[0])/10)) if ((float(d_TMIN[0])/10) < 999) and ((float(d_TMIN[0])/10) > -999) else ""
                    m_TMIN = str(d_TMIN[1])
                    q_TMIN = str(d_TMIN[2])
                    s_TMIN = str(d_TMIN[3])

                ## Parse TOBS
                if(len(M.TOBS) > 0):
                    d_TOBS = M.TOBS[D].split('|')
                    v_TOBS = str((float(d_TOBS[0])/10)) if ((float(d_TOBS[0])/10) < 999) and ((float(d_TOBS[0])/10) > -999) else ""
                    m_TOBS = str(d_TOBS[1])
                    q_TOBS = str(d_TOBS[2])
                    s_TOBS = str(d_TOBS[3])

                ## Parse PRCP
                if(len(M.PRCP) > 0):
                    d_PRCP = M.PRCP[D].split('|')
                    v_PRCP = str((float(d_PRCP[0])/10)) if ((float(d_PRCP[0])/10) < 999) and ((float(d_PRCP[0])/10) > -999) else ""
                    m_PRCP = str(d_PRCP[1])
                    q_PRCP = str(d_PRCP[2])
                    s_PRCP = str(d_PRCP[3])

                ## Parse SNOW
                if(len(M.SNOW) > 0):
                    d_SNOW = M.SNOW[D].split('|')
                    v_SNOW = str(d_SNOW[0]) if (float(d_SNOW[0]) < 999) and (float(d_SNOW[0]) > -999) else ""
                    m_SNOW = str(d_SNOW[1])
                    q_SNOW = str(d_SNOW[2])
                    s_SNOW = str(d_SNOW[3])

                ## Parse SNWD
                if(len(M.SNWD) > 0):
                    d_SNWD = M.SNWD[D].split('|')
                    v_SNWD = str(d_SNWD[0]) if (float(d_SNWD[0]) < 999) and (float(d_SNWD[0]) > -999) else ""
                    m_SNWD = str(d_SNWD[1])
                    q_SNWD = str(d_SNWD[2])
                    s_SNWD = str(d_SNWD[3])

                ## Concatenates values
                o_TMAX = "{},{},{},{}".format(v_TMAX, m_TMAX, q_TMAX, s_TMAX)
                o_TMIN = "{},{},{},{}".format(v_TMIN, m_TMIN, q_TMIN, s_TMIN)
                o_TOBS = "{},{},{},{}".format(v_TOBS, m_TOBS, q_TOBS, s_TOBS)
                o_PRCP = "{},{},{},{}".format(v_PRCP, m_PRCP, q_PRCP, s_PRCP)
                o_SNOW = "{},{},{},{}".format(v_SNOW, m_SNOW, q_SNOW, s_SNOW)
                o_SNWD = "{},{},{},{}".format(v_SNWD, m_SNWD, q_SNWD, s_SNWD)

                ## Add line for date into final lines array
                lines.append("{},{},{},{},{},{},{}".format(s_date, o_TMAX, o_TMIN, o_TOBS, o_PRCP, o_SNOW, o_SNWD))

        out = '\n'.join(lines)
        return out
                
######################################## Logic Function Definitions #######################################################################################################################################

""" Downloads the .dly file for processing using the FTP protocol. Included for backwards compatibility with Python 2.
    Args:
        server = hostname of the FTP server to download from
        path   = path from the root of the server to the directory that contains the desired file
        file_name = name of the file to be downloaded

    Raises:
        IOError: The file or directory specified was not found on the given server.
"""
def downloadPY2(server, path, file_name):
    print("Python 2")
    import urllib

    
    download_url = "ftp://" + server + path + file_name

    try:
        print("Attempting to download: '" + file_name + "' from 'ftp://" + server + path)
        urllib.urlretrieve(download_url, file_name)
        print("Download Complete!")
    except IOError as e:
        print("Error: No such file or directory")
        raise e

""" Downloads the .dly file for processing using the FTP protocol.
    Args:
        server = hostname of the FTP server to download from
        path   = path from the root of the server to the directory that contains the desired file
        file_name = name of the file to be downloaded

    Raises:
        Exeption: If an error prevents the file from being downloaded
"""
def downloadPY3(server, path, file_name):
    print("Python 3")
    from ftplib import FTP
    
    # Define FTP variables
    ftp_url = server
    file_path = path
    file_name = file_name
    error = False

    while True:
        # Attempt to connect to the FTP server
        try:
            ftp = FTP(ftp_url)  
            print('FTP Connection Successful: ' + ftp_url )
        except: 
            print("FTP Connection Unsuccessful: " + ftp_url)
            error = True
            break

        # Attempt to login to the FTP server
        try:
            ftp.login()
            print('Login Successful')
        except:
            print("Login Unsuccessful | Access Denied")
            error = True
            break

        # Attempt to navigate to the data directory
        try:
            ftp.cwd(file_path)
        except:
            print("Directory: '" + file_path + "' Not Found!")
            error = True
            break

        # Determine if the data file exists in our current directory
        try:
            ftp.sendcmd('MLST ' + file_name)
            # this will return a string if the file exists, but throw an error when the file does not
        except:
            print(file_name + " Not Found!")
            error = True
            break
        
        # Attempt to retrive station data and write it to a temp file
        try:
            print("Downloading " + file_name + "...")
            with open(file_name, 'wb') as fp:
                ftp.retrbinary('RETR ' + file_name, fp.write)
            print("Download Complete!")
            break
        except:
            print("Error loading " + file_name + " from " + ftp_url + '/' + file_path)
            error = True
            break
    
    # Close the FTP connection
    print("Disconnecting...")
    try:
        ftp.quit()          
    except:
        True == True # Do nothing
    print("FTP Disconnected")

    # Check if the file was successfully downloaded, raise exception if not
    if(error):
        raise Exception

def processData(file_name):
    # TODO: implement process data funciton

    p = Parser()

    # Parse CSV into months objects with daily data
    with open(file_name) as fin:
        for line in fin:
            p.addDate(line)

    # Parse the month objects into a readable CSV
    with open('out.csv', 'w') as fout:
        header = "Date,TMAX,TMAX_m,TMAX_q,TMAX_s,TMIN,TMIN_m,TMIN_q,TMIN_s,TOBS,TOBS_m,TOBS_q,TOBS_s,PRCP,PRCP_m,PRCP_q,PRCP_s,SNOW,SNOW_m,SNOW_q,SNOW_s,SNWD,SNWD_m,SNWD_q,SNWD_s\n"
        fout.write(header)
        fout.write(p.toCSV())
    
######################################## Helper Function Definitions ######################################################################################################################################
""" Checks the version of the Python interpreter running the script
    Returns:
        The major revision of the current Python interpreter
"""
def checkVersion():
    return sys.version_info[0]

""" Deletes all temporary files created in the process of the script """
def cleanup():
    ## Delete any and all temporary files
    print("Cleaning Up...")

    # Define files to cleanup
    files = ["USC00479190.dly", "temp.csv"]

    # Clean up the files
    for file_name in files:
        if(os.path.exists(file_name)):
            os.remove(file_name)

    print("Cleanup Complete!")

""" Converts the downloaded file to a CSV for proocessing
    Args:
        file_name = the name of the downloaded file

    Raises:
        IOError: File with the name <file_name> not found.
"""
def convertToCSV(file_name):
    with open(file_name) as fin, open('temp.csv', 'w') as fout:
        for line in fin:
            outStr = line[:21] + ','
            for i in range(22, len(line)-1, 8):
                value = line[i:i+4].strip()
                mFlag = line[i+4].strip()
                qFlag = line[i+5].strip()
                sFlag = line[i+6].strip()
                outStr += '{}|{}|{}|{},'.format(value, mFlag, qFlag, sFlag)
            fout.write(outStr[:len(outStr)-1] + "\n") # cut off the final comma
    

######################################## Call Main Function ###############################################################################################################################################

main()

######################################## End of File ######################################################################################################################################################