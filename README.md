# JSONTalk DB 

JSONTalk is a tiny JSON database tool written in Python. It allows to create, save amd modiy data strucutres fast and easy, 
using a file as storage. The data can be dumped (saved) in a textual form in json format after modifications. It is convinient
for small apps and utilities.

Features:
- Simple operations Create/Update/Delete
- Easy query interface to select data
- LUW
- Simple Locks
- Hash indexes (indexes currently are not saved, and should be built on the fly after openning the file)

The full documentation will be provided in the nearest future.

Main class is jtdb.JTDB():

1. Use "WITH" statement to keep DB file not locked in caasde of cruches or unexpected exits. Whenever daabase file is open , ".lck" file is created to notify other users that the data now is used.

```
import jtdb

"""Create DataBase object"""
with jtdb.JTDB() as db:
    ....
```

2. Create/Load and Save DB are simple methods with filename argument:
    
    db.CreateDB("my_db")

3. Method ADD creates an entry in the database assigning the index "__idx__"

```
    db.AddRecord({ "name" : "John", "age" : 15 , "good" : True , "City" : "London"})
    db.AddRecord({ "name" : "Paul", "age" : 30 , "good" : True , "City" : "NY", "hobby" : ["music","food"]})
    db.AddRecord({ "name" : "Tony", "age" : 45 , "good" : False , "City" : "Paris"})
    db.AddRecord({ "name" : "Paul", "age" : 55 , "good" : True , "byear" : 1976, "contacts" : { "tel" : "+765556999" , "email" : "paul@Gmail.com"} })
    db.AddRecord({ "name" : "Parisman", "age" : 24 , "City" : "Paris"})
```

4. Method Query to select the data. As parameter is accepts JTDB_AND & JTDB_OR class to form the conditions for the selection
The classes use arguments as selection filters using following syntax:
<field>__<COND> or <field>__<sub-field>__....<COND> = <value>

The list of supported conditions:
* EQ - field is equal the parameter 
* NE - field is NOT equal the parameter 
* GT - field is greater as the parameter 
* GE - greater or equal 
* LT - less
* LE - fless or equal
* CO - contains (works as standard python "in" operator)
* RE - search for the regex
* NC - does not contain 
* HS - field has attribute
* NH - does not have attribute

```
    print("Search for the person with hobby = nusic")
    db.Query(jtdb.JTDB_AND(hobby__CO = "music")).PrintRecords()
    print("Deep search for the phone in the contacts:")
    db.Query(jtdb.JTDB_AND(contacts__tel__EQ = "+777889999")).PrintRecords()
```

As well functions can be passed intead of values. where the current record parameter will be passed:

```
    db.Query(jtdb.JTDB_OR( name__EQ = lambda x:  x["City"] + "man" if "City" in x  else "" )).PrintRecords()
```

The Result provided is a instance of the class JTDBQuerySet

it has several valuable methods to work with the result data set:

* ObjList - Get the items as a list of JTDBObject (read further)
* PrintRecords - print out the records
* List - get a list of json objects
* Update/Modify to udpate the dataset (modify pusches new keys , where update skips the new keys), result is JTDBQuerySet
* Delete - to delete the data, result is JTDBQuerySet
* Aggregate - to sum up/count hte recieved values (just in case =) )
and further methods....

```
    print("Update contacts adding the birth year via lambda function where there is no byear element")
    db.Query(jtdb.JTDB_AND( City__EQ = "London")).ModifyItems(Land = "GB").PrintRecords()
```

Modify and Udpate uses as well field names as parameters , additionally you can use builtin functions ADD, SUB ,DIV ,and MUL
they allow to update the data adding (dividing etc) argument to inital field value

```
    db.Query(jtdb.JTDB_AND( City__EQ = "London")).ModifyItems(age__ADD = 5).PrintRecords()   
```

or just use function:
```    
    db.Query(jtdb.JTDB_AND( byear__NH = None)).ModifyItems(byear = lambda x: 2021 - x["age"]).PrintRecords()
```

5. JTDBObject is used to have easy interface with each record:

```
    print("Iterate by object")
    res = db.Query(jtdb.JTDB_AND( City__EQ = "London"))
    for item in res.ObjList():
        print("Name:",item.name)
        if hasattr(item, "hobby"):
            print("Hobbies",item.hobby)

        #Change to different Name

        item.name = "Salomon"
        item.Save()

    db.Query(jtdb.JTDB_AND( City__EQ = "London")).PrintRecords()

    print("Adding a new element")

    """Adding the new element"""

    person = jtdb.JTDBObject(db)
    person.CreateFromDict({ "name" : "Helen" , "City" : "Amsterdam" , "age" : 33})
    print("New person is there with ID",person.MyId())
    
    db.Query(jtdb.JTDB_AND( name__EQ = "Helen")).PrintRecords()
```

6. Indexes are experemental and are hashing the fields  so that binary search then used via search rather than loop:

```
    print("Build index and search by index:")
    db.AddSIndex("age_and_city",["age","City"])
    db.AddSIndex("byear",["byear"])

    print("Search for the birth year Index:")
    res = db.SearchByIndex("byear",[ 1990 ])
    res.PrintRecords()
```

7. LUWs are used with commits and rollbacks. The idea behind is to save temporary previous state and restore in case of problem. 
QuerySet uses the LuwCreate/LuwCommit/LuwRollback to mark the records as a part of the LUW. The changes will affect the data abse, but in case of needed can be rolled back. Succesfull result shoudl realease the locks via LuwCommit. 

Both luw and lock indicators are added to the record, engine allows to modify the records only with the provided lock id. And LUW oepration is saving previous state of the records and colelcts the new and deleted records to rollback everything in case of "LuwRollback" command. The changes in LUW go as normal udpate/modify/delete/create transactions applied directly to the records, but can be reversed.

Lock concept is quite simpel here , __lock__ in record is checked against the parameter passed to modify the record and modification will be not allowed if they are not equal. After releasing the lock all modifications are allowed again.


[to be documented]


Do not forget to close and save your DB:

```
    """Save to file"""
    db.SaveDB()
    """Remove lock file"""
    db.Close()
```

More examples in demo1.py
