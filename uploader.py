import sys
import subprocess
import utilities
from ftplib import FTP


def store_zipped_file(filename, dirname, connection):
    """
    Zips and uploads the file filename into the subdirectory dirname, then cd
    back out to parent directory.
    Exits on error and raises RuntimeError
    """
    filezip = filename + '.zip'
    try:
        subprocess.call("zip " + filezip + ' ' + filename, shell=True)
        connection.cwd(dirname)               # change into subdirectory
        connection.storbinary("STOR " + filezip, open(filezip))
        connection.cwd('..')               # back out
    except:
        utilities.do_RuntimeError(
            'Store of',
            filename,
            '.zip unsuccessful')


def get_zipped_file(filename, dirname, connection):
    """
    Downloads the file filename+zip from the subdirectory dirname, reads into
    tempfile.zip, cds back out to parent directory and unzips
    Exits on error and raises RuntimeError
    """
    fbin = open('tempfile.zip', 'wb')
    try:
        connection.cwd(dirname)               # change into subdirectory
        connection.retrbinary("RETR " + filename + '.zip', fbin.write)
        connection.cwd('..')               # back
        utilities.logger.info(
            'Successfully retrieved ' +
            filename +
            '.zip\n')
    except:
        utilities.do_RuntimeError(
            'Retrieval of',
            filename,
            '.zip unsuccessful')
        return

    fbin.close()
    try:
        # -o: overwrite without prompting
        subprocess.call("unzip -o tempfile.zip", shell=True)
        subprocess.call("rm tempfile.zip", shell=True)  # clean up
    except:
        utilities.do_RuntimeError(
            'Downloaded file',
            filename,
            'could not be decompressed')


def main(datestr, server_info, file_info):
    """
    When something goes amiss, various routines will and pass through a
    RuntimeError(explanation) rather than trying to recover, since this
    probably means something is either wrong with the ftp connection or the
    file structure got corrupted. This error is logged but needs to be caught
    in the calling program.
    """
    global ftp

    # log into the server
    try:
        ftp = FTP(server_info.serv_name)     # connect to host, default port
        ftp.login(server_info.username, server_info.password)
        # change into PHOX directory
        ftp.cwd(server_info.server_dir)
        print 'Logged into: {}/{}'.format(server_info.serv_name,
                                          server_info.server_dir)
    except Exception as e:
        utilities.do_RuntimeError('Login to {} unsuccessful.'.format(server_info.serv_name))

    # upload the daily event and duplicate index files
    try:
        eventfilename = '{}{}.txt'.format(file_info.eventfile_stem, datestr)
        store_zipped_file(eventfilename, 'Daily', ftp)
    except:
        utilities.do_RuntimeError('Transfer of ', eventfilename,
                                       'unsuccessful')

    try:
        dupfilename = '{}{}.txt'.format(file_info.dupfile_stem, datestr)
        store_zipped_file(dupfilename, 'Daily/Duplicates', ftp)
        ftp.cwd('..')               # back out one more level
    except:
        utilities.do_RuntimeError('Transfer of', dupfilename,
                                       'unsuccessful')

    # update the monthly and yearly files
    monthfilename = file_info.outputfile_stem + \
        datestr[:2] + '-' + datestr[2:4] + '.txt'
    yearfilename = file_info.outputfile_stem + datestr[:2] + '.txt'

    curyear = True
    if datestr[2:] == '0101':  # initialize a new year
        subprocess.call("cp " + eventfilename + ' ' + yearfilename, shell=True)
        curyear = False
    else:
        get_zipped_file(yearfilename, 'Annual')
        try:
            # this actually becomes a simple write rather than append when a
            # new month
            fyr = open(yearfilename, 'a')
        except:
            utilities.do_RuntimeError(
                'Could not open yearly file',
                yearfilename)

    curmonth = True
    # just make a copy of the existing file with the DOC lines
    if datestr[4:] == '01':
        subprocess.call("cp " + eventfilename + ' ' + monthfilename,
                        shell=True)
        curmonth = False
    else:  # download existing files and append to it
        get_zipped_file(monthfilename, 'Monthly')
        try:
            # this actually becomes a simple write rather than append when a
            # new month
            fmon = open(monthfilename, 'a')
        except:
            utilities.do_RuntimeError(
                'Could not open monthly file',
                monthfilename)

    if curyear or curmonth:
        try:
            fin = open(eventfilename, 'r')
        except:
            utilities.do_RuntimeError(
                'Could not open the daily event file',
                eventfilename)

        line = fin.readline()
        while len(line) > 0:  # loop through the file
            # copy the new lines, skipping the documentation lines
            if 'DOC\tDOC\t999' not in line:
                if curmonth:
                    fmon.write(line)
                if curyear:
                    fyr.write(line)
            line = fin.readline()

        fin.close()
        if curmonth:
            fmon.close()
        if curyear:
            fyr.close()

    store_zipped_file(monthfilename, 'Monthly')
    store_zipped_file(yearfilename, 'Annual')

    ftp.quit()
    print "Finished"

if __name__ == '__main__':
    if len(sys.argv) > 1:
        datestr = sys.argv[1]
    else:
        print 'Error: No date provided to PHOX_uploader.py'
        sys.exit()

    try:
        main(datestr)
    except RuntimeError as why:
        print 'phox_uploader.main() raised the RuntimeError:', why
