# PanDA job monitoring

Provides simple framework for monitoring the status of jobs running on the [PanDA](https://panda-wms.readthedocs.io/en/latest/introduction/introduction.html) workload management system.
Useful for bookkeeping when jobs repeatedly fail and need retrying or resubmitting.
Uses the python library `sqlite3` to create a local database to keep track of jobs.

## Interacting with the database

Interactions with the database are handled by the script `db.py`:
~~~
python db.py <args>
~~~

Available options include:
~~~
  -i                 -- initialise a DB for the first time.
  -u <filename>      -- provide file with job status from pbook to update status of files in DB. Filename should end in .txt.
  -u <retry_string>  -- provide retry string (same as given to pbook) to update DB to register retries.
  -r                 -- query DB to find files needed rerunning.
  -o <taskid>        -- mark a given task as obsoleted (indicates that a resubmitted job takes its place).
  -a                 -- print the status of all jobs in DB (including obsoleted).
  -no                -- print the status of all jobs in DB (excluding obsoleted).
  -p                 -- update the process name of jobs using dsid_categories file.
  -done <filename>   -- output list of done jobs to <filename>.
  -x                 -- delete the table.
~~~

