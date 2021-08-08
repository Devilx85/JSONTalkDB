
import jtdb
import time
import json

"""Create DataBase object"""
with jtdb.JTDB() as db:
    #db.CreateDB("my_db2")
    #with open("generated.json","r") as f:
    #    db.Import(json.load(f))
    db.LoadDB("my_db2")
    #db.AddSIndex("name",["name"])
    start = time.time()
    res = db.Query(jtdb.JTDB_AND(name__EQ = "Johnson Rosario"),ix_search=True)
    end = time.time()
    print("Search took:" , end - start)
    res.PrintRecords()
    db.SaveDB()