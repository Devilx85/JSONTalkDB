
import jtdb

"""Create DataBase object"""
with jtdb.JTDB() as db:
    """load from database"""
    db.LoadDB("my_db")
