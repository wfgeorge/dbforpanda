import sys
import sqlite3 as sql

from doneJobs import done_job_list

class MyDB:

    def __init__(self,name):
        ''' Create a connection to the DB'''
        self.connection = sql.connect(name)
        self.cursor = self.connection.cursor()

    def parsePbookString(self,line):
        ''' Parse lines from pbook output returning data types for DB '''

        # Split line and remove blank entries
        split_line = line.split(' ')
        item_list = [x for x in split_line if x != '']

        # Get variables of interest
        task_id = int(item_list[0])
        status = item_list[2]
        progress = float(item_list[3].strip('%'))
        taskname = item_list[4]

        return task_id,status,progress,taskname


    def initialise(self):
        ''' Create table to store grid job info, need to be run when DB is first created. '''
        with self.connection:
            self.connection.execute("""
                CREATE TABLE JOB (
                    id INTEGER NOT NULL PRIMARY KEY,
                    status TEXT,
                    progress REAL,
                    taskname TEXT,
                    obsoleted INTEGER,
                    retries INTEGER,
                    process TEXT,
                    note TEXT
                );
            """)


    def removeTable(self):
        ''' Drop (delete) the table '''
        self.cursor.execute('''DROP TABLE IF EXISTS JOB''')
        self.connection.commit()


    def insertJob(self,task_id,status,progress,taskname):
        ''' Insert job into table (for first time) '''
        data = (task_id,status,progress,taskname,0,0,"-","None")
        command = 'INSERT INTO JOB VALUES (?, ?, ?, ?, ?, ?, ?, ?)'
        self.cursor.execute(command,data)
        self.connection.commit()


    def updateJob(self,task_id,status,progress):
        ''' Update job info saved in DB '''
        data = (status,progress,task_id)
        print("Updating {}".format(data))
        command = '''UPDATE JOB
                    SET status = ? ,
                        progress = ?
                    WHERE id == ? '''
        self.cursor.execute(command,data)
        self.connection.commit()


    def checkStatus(self,task_id):
        ''' Query the status of a job '''
        self.cursor.execute("SELECT * FROM JOB WHERE id = ?",(task_id,))
        data = self.cursor.fetchall()
        return data


    def statusAll(self):
        ''' Return status of all jobs '''
        self.cursor.execute("SELECT * FROM JOB")
        data = self.cursor.fetchall()
        for item in data:
            print(item)


    def statusNonObsoleted(self):
        ''' Return status of all non-obsoleted jobs '''
        self.cursor.execute("SELECT * FROM JOB WHERE obsoleted = 0")
        data = self.cursor.fetchall()
        for item in data:
            print(item)


    def readJobFile(self,filename,identifier):
        ''' Read in file with pbook output and inserts/updates jobs '''

        f = open(filename,'r')
        for line in f.readlines():
            task_id,status,progress,taskname = self.parsePbookString(line)

            # Avoid including any grid jobs (e.g. related to MC production) in DB
            if identifier not in taskname:
                continue

            # Check whether job is already in DB
            self.cursor.execute("SELECT * FROM JOB WHERE id = ?", (task_id,))
            data=self.cursor.fetchone()
            if data is None:
                # Job is not already in DB
                self.insertJob(task_id,status,progress,taskname)
            elif data[1] != 'done':
                # Job is alread in DB
                self.updateJob(task_id,status,progress)


    def getIdsByStatus(self,status,progress=None):
        ''' Get list of task ids for a given status 
            Note: ignores obsoleted files as these shouldn't be retried 
        '''
        self.cursor.execute("SELECT id,progress FROM JOB WHERE status = '{}' AND obsoleted = 0".format(status))
        data = self.cursor.fetchall()
        task_id_list = []
        for row in data:
            if progress is not None:
                if '<' in progress:
                    threshold = progress.split('<')[1]
                    if row[1] < float(threshold):
                        task_id_list.append(row[0])
                elif '>' in progress:
                    threshold = progress.split('>')[1]
                    if row[1] >= float(threshold):
                        task_id_list.append(row[0])
            else:
                task_id_list.append(row[0])

        return task_id_list


    def getDetailsByInvStatus(self,status):
        ''' Get list of task ids not satisfying a given status 
            Note: ignores obsoleted files as these shouldn't be retried 
        '''
        self.cursor.execute("SELECT * FROM JOB WHERE status != '{}' AND obsoleted = 0".format(status))
        return self.cursor.fetchall()

    
    def processRetryString(self,rs):
        ''' Read retry string to extract task ids and retry options '''
        # retry([32639261, 32639477, 32639767, 32642889],newOpts={'nFilesPerJob':2,'memory':4000}))
        task_id_str = rs[rs.find('[')+1:rs.find(']')]
        task_ids = [int(x.strip()) for x in task_id_str.split(",")] 
        if "newOpts" in rs:
            note = rs[rs.find('{')+1:rs.find('}')]
        else:
            note = "-"

        return task_ids,note


    def markRetry(self,retry_string):
        ''' Register in the DB that a job has been retried and add a note with retry options '''

        task_ids,note = self.processRetryString(retry_string)

        for task_id in task_ids:
            # Get current number of retries
            self.cursor.execute("SELECT retries FROM JOB WHERE id = '{}'".format(task_id))
            retries = self.cursor.fetchall()[0][0]

            data = (retries+1,note,"retried",task_id)
            print("Updating {}".format(data))
            command = '''UPDATE JOB
                        SET retries = ? ,
                            note = ?,
                            status = ?
                        WHERE id == ? '''
            self.cursor.execute(command,data)
            self.connection.commit()


    def listRetries(self):
        ''' Print lists of jobs which should be retried '''

        print("")
        print("Status of jobs which are not done:")
        l_not_done = self.getDetailsByInvStatus("done")
        l_running  = [x for x in l_not_done if x[1] == u'running' or x[1] == u'pending' or x[1] == u'scouting' or x[1] == u'retried']
        l_not_run  = [x for x in l_not_done if x not in l_running]
        print("  - Still running: ")
        for job in l_running:
            print(job)
        print("")
        print("  - Not running: ")
        for job in l_not_run:
            print(job)

        print("")
        print("SUGGESTIONS:")
        print("")
 
        finished_low_list = self.getIdsByStatus("finished","progress<60")
        if len(finished_low_list) > 0:
            print("{} finished with < 60%:".format(len(finished_low_list)))
            print("retry({},{})".format(finished_low_list,r"newOpts={'nFilesPerJob':2,'memory':4000}"))

        finished_hi_list = self.getIdsByStatus("finished","progress>60")
        if len(finished_hi_list) > 0:
            print("")
            print("{} finished with >= 60%:".format(len(finished_hi_list)))
            print("retry({})".format(finished_hi_list))
    
        failed_list = self.getIdsByStatus("failed")
        if len(failed_list) > 0:
            print("")
            print("{} failed:".format(len(failed_list)))
            print("retry({},{})".format(failed_list,r"newOpts={'nFilesPerJob':2,'memory':4000}"))

        ex_list = self.getIdsByStatus("exhausted")
        if len(ex_list) > 0:
            print("")
            print("{} exhausted:".format(len(ex_list)))
            print("retry({},{})".format(ex_list,r"newOpts={'nFilesPerJob':2,'memory':4000}"))

        broken_list = self.getIdsByStatus("broken")
        if len(broken_list) > 0:
            print("")
            print("{} broken and need resubmitting:".format(len(broken_list)))
        for broken_id in broken_list:
            data = self.checkStatus(broken_id)
            print("  {}".format(data[0][3]))


    def markObsolete(self,task_id):
        ''' Update entry for job to indicate that it is obsoleted '''
        data = (1,task_id)
        print("Marking {} as obsoleted.".format(task_id))
        command = '''UPDATE JOB
                    SET obsoleted = ?
                    WHERE id == ? '''
        self.cursor.execute(command,data)
        self.connection.commit()


    def markMultiObsolete(self,in_string):
        ''' Update entries for multiple jobs to indicate that they are obsoleted '''
        s = in_string.strip("<").strip(">")
        ids = [i.strip() for i in s.split(",")]
        for this_id in ids:
            self.markObsolete(this_id)


    def outputDoneJobs(self,filename):
        ''' Output list of names of done tasks  
            Note: ignores obsoleted files 
        '''
        self.cursor.execute("SELECT * FROM JOB WHERE status = 'done' AND obsoleted = 0")
        data=self.cursor.fetchall()

        f = open(filename,'w')
        print(len(data))
        for thisjob in data:
            if thisjob[0] not in done_job_list:
                print(thisjob[3])
                f.write(thisjob[3])
        f.close()


    def addProcessColumn(self):
        ''' Single use function to add process to table '''
        self.cursor.execute("SELECT * FROM JOB")
        jobs = self.cursor.fetchall()


        f = open("/afs/cern.ch/user/w/wgeorge/MuTau_cLFV/Ntuples/recoFiles/gridfilenames/dsid_categories.txt","r")

        dsid_dict = {}
        for l in f.readlines():
            delim = '\t' if '\t' in l else ' '
            x = l.split(delim)
            x = [y.strip() for y in x if y != '']
            dsid_dict[x[0]] = x[1]

        for job in jobs:
            task_id = job[0]
            dsid = job[3].split(".")[2]
            if dsid in dsid_dict.keys():
                data = (dsid_dict[dsid],task_id)
                print("Marking {} as {}.".format(task_id,dsid_dict[dsid]))
                command = '''UPDATE JOB
                             SET process = ?
                             WHERE id == ? '''
                self.cursor.execute(command,data)
                self.connection.commit()
        f.close()


if __name__ == '__main__':

    if len(sys.argv) == 1:
        print("Please provide arguments to run script. For help run \'python db.py -h\'.")
        sys.exit(1)

    # Open a connection to the DB
    db_name = 'new_db.db'
    db = MyDB(db_name)

    # tag in names to filter out unwanted jobs
    identifier = ""

    if sys.argv[1] == '-h':
        print("Run as \'python db.py <args>\'. Possible arguments include:")
        print("   -i                 -- initialise a DB for the first time.")
        print("   -u <filename>      -- provide file with job status from pbook to update status of files in DB. Filename should end in .txt.")
        print("   -u <retry_string>  -- provide retry string (same as given to pbook) to update DB to register retries.")
        print("   -r                 -- query DB to find files needed rerunning.")
        print("   -o <taskid>        -- mark a given task as obsoleted (indicates that a resubmitted job takes its place).")
        print("   -a                 -- print the status of all jobs in DB (including obsoleted).")
        print("   -no                -- print the status of all jobs in DB (excluding obsoleted).")
        print("   -p                 -- update the process name of jobs using dsid_categories file.")
        print("   -done <filename>   -- output list of done jobs to <filename>.")
        print("   -x                 -- delete the table.")
    elif sys.argv[1] == '-i' and len(sys.argv) == 2:
        try:
            db.initialise()
        except sql.OperationalError as e:
            print('sqlite3.OperationalError: {}'.format(e))
            sys.exit(1)
    elif sys.argv[1] == '-u' and len(sys.argv) == 3:
        if ".txt" in sys.argv[2]:
            db.readJobFile(sys.argv[2],identifier)
        elif "retry" in sys.argv[2]:
            db.markRetry(sys.argv[2])
        else:
            print("Unrecognised arguments: {}".format(sys.argv))
    elif sys.argv[1] == '-r' and len(sys.argv) == 2:
        db.listRetries()
    elif sys.argv[1] == '-o' and len(sys.argv) == 3:
        if sys.argv[2][0] == "<":
            db.markMultiObsolete(sys.argv[2])
        else:
            db.markObsolete(sys.argv[2])
    elif sys.argv[1] == '-a' and len(sys.argv) == 2:
        db.statusAll()
    elif sys.argv[1] == '-no' and len(sys.argv) == 2:
        db.statusNonObsoleted()
    elif sys.argv[1] == '-p' and len(sys.argv) == 2:
        db.addProcessColumn()
    elif sys.argv[1] == '-done' and len(sys.argv) == 3:
        db.outputDoneJobs(sys.argv[2])
    elif sys.argv[1] == '-x' and len(sys.argv) == 2:
        db.removeTable()
    else:
        print("Unrecognised arguments: {}".format(sys.argv))

