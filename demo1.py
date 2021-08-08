
import jtdb
import time

"""Create DataBase object"""
with jtdb.JTDB() as db:
    """Assign a file name"""
    db.CreateDB("my_db")

    print("Build index and search by index:")
    db.AddSIndex("age_and_city",["age","City"])
    db.AddSIndex("byear",["byear"])


    """Add records"""
    db.AddRecord({ "name" : "Alex", "age" : 35 , "good" : True , "City" : "NY" , "hobby" : ["music","art"] , "contacts" : { "tel" : "+343423234" , "email" : "noemail@mail.com"} })
    db.AddRecord({ "name" : "John", "age" : 15 , "good" : True , "City" : "London"})
    db.AddRecord({ "name" : "Paul", "age" : 30 , "good" : True , "City" : "NY", "hobby" : ["music","food"]})
    db.AddRecord({ "name" : "Tony", "age" : 45 , "good" : False , "City" : "Paris"})
    db.AddRecord({ "name" : "Kit", "age" : 33 , "good" : True , "City" : "London", "hobby" : ["food","bike"], "contacts" : { "tel" : "+777565644" , "email" : "xxxxx@mail.com"} })
    db.AddRecord({ "name" : "Lara", "age" : 31 , "good" : True , "City" : "NY"})
    db.AddRecord({ "name" : "Michael", "age" : 14 , "good" : True , "City" : "NY "})
    db.AddRecord({ "name" : "Rex", "age" : 69 , "good" : True , "City" : "NY", "hobby" : ["books","photo"]})
    db.AddRecord({ "name" : "Tom", "age" : 34 , "City" : "NY "})
    db.AddRecord({ "name" : "Serg", "age" : 38 , "good" : True , "City" : "London"})
    db.AddRecord({ "name" : "Leo", "age" : 43 , "good" : False , "City" : "Paris", "contacts" : { "tel" : "+6554322334" , "email" : "funnyfunny@Xmail.com"} })
    db.AddRecord({ "name" : "Don", "age" : 36 , "good" : True , "byear" : 1985, "contacts" : { "tel" : "+777889999" , "email" : "myemail@Gmail.com"} })
    db.AddRecord({ "name" : "Roger", "age" : 22 , "good" : True , "City" : "NY" , "hobby" : ["music","art"] , "contacts" : { "tel" : "+943423234" , "email" : "rrick@mail.com"} })
    db.AddRecord({ "name" : "Mary", "age" : 19 , "good" : True , "City" : "London"})
    db.AddRecord({ "name" : "Connor", "age" : 37 , "good" : True , "City" : "NY", "hobby" : ["music","food"]})
    db.AddRecord({ "name" : "Karl", "age" : 31 , "good" : False , "City" : "Paris"})
    db.AddRecord({ "name" : "Arnold", "age" : 21 , "good" : True , "City" : "London", "hobby" : ["food","bike"], "contacts" : { "tel" : "+0067565644" , "email" : "arny99@mail.com"} })
    db.AddRecord({ "name" : "Ken", "age" : 61 , "good" : True , "City" : "NY"})
    db.AddRecord({ "name" : "Michael", "age" : 43 , "good" : True , "City" : "Paris"})
    db.AddRecord({ "name" : "Alexander", "age" : 43 , "good" : True , "City" : "NY", "hobby" : ["books","photo"]})
    db.AddRecord({ "name" : "Tom", "age" : 24 , "City" : "Paris "})
    db.AddRecord({ "name" : "Fabian", "age" : 29 , "good" : True , "City" : "London"})
    db.AddRecord({ "name" : "Jan", "age" : 19 , "good" : False , "City" : "Hamburg", "contacts" : { "tel" : "+5444322334" , "email" : "jan@Xmail.com"} })
    db.AddRecord({ "name" : "Paul", "age" : 55 , "good" : True , "byear" : 1976, "contacts" : { "tel" : "+765556999" , "email" : "paul@Gmail.com"} })
    db.AddRecord({ "name" : "Parisman", "age" : 24 , "City" : "Paris"})

    print("\n\n")
    print("Index 1:")
    print(db.indexes[0].index)


    print("\n\n")
    print("Search for the person with hobby = nusic")
    db.Query(jtdb.JTDB_AND(hobby__CO = "music")).PrintRecords()

    print("\n\n")
    print("Deep search for the phone in the contacts:")
    db.Query(jtdb.JTDB_AND(contacts__tel__EQ = "+777889999")).PrintRecords()

    print("\n\n")
    print("The same query with array of parameters")
    db.Query(jtdb.JTDB_AND(__q__= [[ "contacts.tel" , "EQ", "+777889999" ]])).PrintRecords()

    print("\n\n")
    print("Update contacts adding the Land")
    db.Query(jtdb.JTDB_AND( City__EQ = "London")).ModifyItems(Land = "GB").PrintRecords()

    print("\n\n")
    print("Update contacts adding the birth year via lambda function where there is no byear element")
    db.Query(jtdb.JTDB_AND( byear__NH = None)).ModifyItems(byear = lambda x: 2021 - x["age"]).PrintRecords()

    print("\n\n")
    print("Index 2 is now updated:")
    print(db.indexes[1].index)

    print("\n\n")
    print("Search 'Who has a contact?':")
    db.Query(jtdb.JTDB_AND(contacts__HS = None)).PrintRecords()

    print("\n\n")
    print("Search 'Who has a contacts or at least lives in London?':")
    db.Query(jtdb.JTDB_OR( contacts__HS = None ,City__EQ = "London")).PrintRecords()

    print("\n\n")
    print("Search with lambda:")
    db.Query(jtdb.JTDB_OR( name__EQ = lambda x:  x["City"] + "man" if "City" in x  else "" )).PrintRecords()

    print("\n\n")
    print("Search for the age and city precise:")
    start = time.time()
    res = db.Query(jtdb.JTDB_AND(age__EQ = 43 ,City__EQ="Paris"))
    end = time.time()
    res.PrintRecords()
    print("Search took:" , end - start)

    print("\n\n")
    print("Search for the age and city by Index:")
    start = time.time()
    res = db.SearchByIndex("age_and_city",[ 43, "Paris"])
    end = time.time()
    res.PrintRecords()
    print("Search took:" , end - start)

    print("\n\n")
    print("Search for the birth year Index:")
    start = time.time()
    res = db.SearchByIndex("byear",[ 1990 ])
    end = time.time()
    res.PrintRecords()
    print("Search took:" , end - start)

    print("\n\n")
    print("Fetch data from QSet by Id:")
    person = res.GetById(16)
    print(person)

    """Change an attribute , this will not change data in the db object!"""
    person["age"] = 42

    print("Fetch data from DB by Id:")
    db.GetById(16).PrintRecords()

    print("Mow update and try again:")
    res.UpdateData([person])
    db.GetById(16).PrintRecords()


    print("\n\n")
    print("Iterate by object")
    res = db.Query(jtdb.JTDB_AND( City__EQ = "London"))
    for item in res.ObjList():
        print("Name:",item.name)
        if hasattr(item, "hobby"):
            print("Hobbies",item.hobby)
        #Change to different Name
        item.name = "Salomon"
        item.Save()

    res = db.Query(jtdb.JTDB_AND( City__EQ = "London")).PrintRecords()

    print("\n\n")
    print("Adding a new element")
    """Adding the new element"""
    person = jtdb.JTDBObject(db)
    person.CreateFromDict({ "name" : "Helen" , "City" : "Amsterdam" , "age" : 33})
    print("New person is there with ID",person.MyId())
    db.Query(jtdb.JTDB_AND( name__EQ = "Helen")).PrintRecords()

    """Save to file"""
    #db.SaveDB()
    """Close (in this case not really needed)"""
    db.Close()
