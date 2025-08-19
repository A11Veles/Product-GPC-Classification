from teradataml import *
from config.settings import TD_HOST, TD_USER, TD_PASS, TD_DB
import pandas as pd


DB = "TD_DB"

# 1) Connect
create_context(host=TD_HOST, username=TD_USER, password=TD_PASS, logmech="TD2")


qry = "SEL Tablename FROM DBC.TablesV WHERE DatabaseName='your_db' AND TableKind='T';"
print(DataFrame.from_query(qry).to_pandas())

