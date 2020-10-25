# DB
Build a database in two intensive days, in full self-production\
Differing relational functionalities within the same generic API and pre-written test cases using Python. \
Involving run-time and I/O bound optimizations.

## Description
organized collection of data supported : CRUD operation, queries, join and index\
by effective access to disk, optimization by key-index, and implement actions by abstract way
## Getting Started
### Installing
* "pip install dataclasses" (downloand dataclasses library )
### Executing program
import our code to your project by the folloing lines:
```
from db import DataBase
from db_api import DBField, SelectionCriteria, DB_ROOT, DBTable
```
## Test
If you change the code
We recommend that you run the attached tests
to make sure you have not violated the existing code
### Executing test
-pip install pytest\
-run "create_db_backup.py"\
for run spesific test command "py.test -k {test_name}"\
for run all tests command: "py.test"
