# creating database

organized collection of data supported basic operation : read, write, delete, update and queries, join, index

## Description

implement realation database, by effective access to disk, optimization by key-index, and implement actions by abstract way

## Getting Started

### Installing

* "pip install dataclasses" (downloand dataclasses library )

### Executing program

import our code to your project by the folloing the lines:
```
from db import DataBase
from db_api import DBField, SelectionCriteria, DB_ROOT, DBTable
```

## Test

If you change the code
We recommend that you run the attached tests 
to make sure you have not violated the existing code

### Executing test

-pip install pytest
-run "create_db_backup.py"
for run spesific test command "py.test -d {test_name}"
run all tests command: "py.test"
