import datetime, os, sys, re, subprocess , traceback , json
import uuid
import time
import types
import threading
from copy import deepcopy




"""Binary search procedure"""
def BinSearch(list, keysearch , value):
    low = 0
    high = len(list) - 1
    while low <= high:
        mid = low + (high - low) // 2
        key = list[mid][keysearch]
        if value == key:
        	return list[mid]
        elif key  > value:
        	high = mid - 1
        else:
        	low = mid + 1

    return None

class JTDBWrongFile(Exception):
    """Base class for other exceptions"""
    pass
class JTDBFileLocked(Exception):
    """Base class for other exceptions"""
    pass
class JTDBFileLockError(Exception):
    """Base class for other exceptions"""
    pass
class JTDBWrongRecord(Exception):
    """Base class for other exceptions"""
    pass
class JTDBNotInit(Exception):
    """Base class for other exceptions"""
    pass
class JTDBIncorrectQuery(Exception):
    """Base class for other exceptions"""
    pass

class JTDBNoValidCondition(Exception):
    """Base class for other exceptions"""
    pass

class JTDBUpdateError(Exception):
    """Base class for other exceptions"""
    pass
class JTDBLUWError(Exception):
    """Base class for other exceptions"""
    pass

class JTDBIndexError(Exception):
    """Base class for other exceptions"""
    pass
class JTDBSIndexError(Exception):
    """Base class for other exceptions"""
    pass
class JTDBQsetLinkError(Exception):
    """Base class for other exceptions"""
    pass
class JTDBLockError(Exception):
    """Base class for other exceptions"""
    pass

"""Base query class"""
class JTDBQuery:
    def __init__(self,**kwargs):
        if "__q__" in kwargs:
            q_ar = kwargs["__q__"]
            if not q_ar:
                raise JTDBIncorrectQuery
            params = {}
            for cond in q_ar:
                if len(cond) != 3:
                    raise JTDBIncorrectQuery
                params[cond[0]+"."+cond[1]] = cond[2]
 

            self.parameters = self.ParseParams(params,".")
            return

        self.parameters = self.ParseParams(kwargs)

    def ParseParams(self,fparam,delim = "__"):
        operators = ["EQ","GT","LE","NE","GE","LT","CO","IN","RE","HS","NH","NC"]
        #             0    1    2    3    4     5   6    7     8    9   10   11
        params = []
        for k,v in fparam.items():
            if isinstance(v, JTDB_AND) or isinstance(v, JTDB_OR):
                params.append({ "f" : k, "c" : -1, "v" : v })
                continue
            name = k.split(delim)
            if len(name) < 2:
                raise JTDBIncorrectQuery
            if name[-1] not in operators:
                raise JTDBIncorrectQuery
            params.append({ "f" : name[:-1], "c" : operators.index(name[-1]) , "v" : v })

        return params

"""AND and OR class"""
class JTDB_AND(JTDBQuery):
    operation = 1
    def __init__(self,**kwargs):
        super(JTDB_AND,self).__init__(**kwargs)

class JTDB_OR(JTDBQuery):
    operation = 2
    def __init__(self,**kwargs):
        super(JTDB_OR,self).__init__(**kwargs)

"""Index class"""
class JTDBIndex():

    def __init__(self,db, name, keys , ix_type):
        self.db_link = db
        self.index = []
        self.name = name
        self.keys = keys
        self.ix_type = ix_type

    def Build(self):
        
        if self.ix_type == 0:
            self.index.clear()
            lst = []
            for record in self.db_link.lpointer:
                vals = []
                for key in self.keys:
                    if key in record:
                        vals.append(record[key])
                l_hash = hash(frozenset(vals))
                lst.append({"hash" : l_hash,"idx":record["__idx__"]})
            
            self.index =  sorted(lst, key=lambda k: k['hash']) 


    def Search(self,vals):
        l_hash = hash(frozenset(vals))
        res = BinSearch(self.index, "hash" , l_hash)
        if res != None:
            qset = JTDBQuerySet(self.db_link)
            qset.AddFromQset(self.db_link.GetById(res["idx"]))
            ixs1 = ixs2 = self.index.index(res)
            ixs1 = ixs1 + 1
            ixs2 = ixs2 - 1
            total = len(self.index)
            while ixs1 < total and self.index[ixs1]["hash"] == l_hash:
                qset.AddFromQset(self.db_link.GetById(self.index[ixs1]["idx"]))
                ixs1 = ixs1 + 1
            while ixs2 > -1 and self.index[ixs2]["hash"] == l_hash:
                qset.AddFromQset(self.db_link.GetById(self.index[ixs2]["idx"]))
                ixs2 = ixs2 - 1
            return qset
        return JTDBQuerySet(self.db_link)

    def Update(self,operation,record):
        
        rec_ix = record["__idx__"]
        if operation == "C":
            vals = []
            for key in self.keys:
                if key in record:
                    vals.append(record[key])
            
            if len(vals) == 0:
                return

            l_hash = hash(frozenset(vals))
            self.index.append({"hash" : l_hash,"idx": rec_ix})
            
        elif operation == "D":
            item = next((item for item in self.index if item["idx"] == rec_ix), None)
            if item is None:
                return
            
            self.index.remove(item)
        
        elif operation == "U":
            item = next((item for item in self.index if item["idx"] == rec_ix), None)

            vals = []

            for key in self.keys:
                if key in record:
                    vals.append(record[key])

            if len(vals) == 0:
                return

            l_hash = hash(frozenset(vals))

            if item is None:
                self.index.append({"hash" : l_hash ,"idx": rec_ix})
            else:
                item["hash"] = l_hash

        self.index =  sorted(self.index, key=lambda k: k['hash']) 


class JTDBObject:
    
    def __init__(self,db,row = None):
        self.__db_link = db
        self.__data = None
        self.__new = True
        if row != None:
            self.Load(row)


    def Load(self,row):
        self.__data = row
        self.__dict__.update(self.__data)
        self.__new = False

    def Refresh(self):
        self.__dict__.update(self.__data)
    
    def isLinked(self):
        if self.__data == None:
            return False

        return True

    def GetDict(self):
        data = {}
        data.update(vars(self))
        del data["_JTDBObject__db_link"]
        del data["_JTDBObject__data"] 
        del data["_JTDBObject__new"] 
        xdata = deepcopy(data)
        return xdata

    def Save(self):
        if self.isLinked():
            u_data = self.GetDict()
            return self.__db_link.ModifyData("U",self.__data,u_data)
        return False

    def Delete(self):
        if self.isLinked():
            if self.__db_link.ModifyData("D",self.__data,None):
                self.__data = None
                return True

        return False
    
    def MyId(self):
        if self.isLinked() and self.__new == False:
            return self.__data["__idx__"]
            

    def CreateFromDict(self,ddict):
        if type(ddict) is not dict:
            raise JTDBWrongRecord
        if self.__new == True:
            if self.__db_link.ModifyData("C",None,ddict):
                self.Load(ddict)
                return True
        
        return False

"""Query set class"""
class JTDBQuerySet:

    def __init__(self,db):
        self.__data = []
        self.__db_link = db
        self.luw = False
        self.lock = False
    
    def ObjList(self):
        objs = []
        for item in self.__data:
            objs.append(JTDBObject(self.__db_link ,item))
        
        return objs

    def GetData(self):
        return self.__data

    def GetById(self,id):
        res = BinSearch(self.__data,"__idx__",id)
        if res == None:
            return None
        return deepcopy(res)
    
    def UpdateData(self,ldata):
        upd_list = deepcopy(self.__data)
        for item in upd_list:
            for u_item in ldata:
                if item["__idx__"] == u_item["__idx__"]:
                    item.update(u_item)

        return self.__db_link.UpdateRecordsBulk(self,upd_list,self.lock)
        
    def AddResult(self,item):
        self.__data.append(item)

    def SetData(self, data):
        self.__data.clear()
        for item in data:
            self.AddResult(item)

    def DB(self):
        return self.__db_link

    def Count(self):
        return len(self.__data)

    def List(self):
        return deepcopy(self.__data)

    def Aggregate(self,groupby,sums = [],counts = [], avgs = [] ):
        result = []

        for item in self.__data:
            rec = {}
            for group in groupby:
                rec[group] = item[group]
            l_hash = hash(frozenset(rec.items()))
            found_value = next((item for item in result if item["__hash__"] == l_hash), None)
            if found_value != None:
                rec = found_value
            else:
                rec["__hash__"] = l_hash
                for sum in sums:
                    rec["sum_"+sum] = 0
                for count in counts:
                    rec["count_"+count] = 0
                for avg in avgs:
                    rec["avg_"+avg] = 0
                result.append(rec)


            for sum in sums:
                rec["sum_"+sum]  = rec["sum_"+sum]  + item[sum]
            for count in counts:
                rec["count_"+count]  = rec["count_"+count]  + 1
            for avg in avgs:
                rec["avg_"+avg] = ( rec["avg_"+avg] +  rec["sum_"+sum] ) /  rec["count_"+count]

        return result

    def PrintRecords(self):
        for item in self.__data:
            print(item)

    def AddFromQset(self,qset):
        self.__data.extend(qset.__data)
        return self

    def AddItems(self,items):
        qset = self.__db_link.AddRecords(items,self.luw,self.lock)
        return self.AddFromQset(qset)

    def UpdateItems(self,**kwargs):
        return self.__db_link.UpdateRecords(self,self.lock,**kwargs)

    def ModifyItems(self,**kwargs):
        return self.__db_link.ModifyRecords(self,self.lock,**kwargs)

    def DeleteItems(self):
        return self.__db_link.DeleteRecords(self,self.lock)

    def LockItems(self):
        if self.lock!= False:
            raise JTDBLockError
        self.lock = str(uuid.uuid4())
        return self.__db_link.LockEntries(self,self.lock )

    def UnLockItems(self):
        if self.lock== False:
            raise JTDBLockError
        self.__db_link.UnLockEntries(self,self.lock )
        self.lock = False
        return self

    def LuwCreate(self):
        if self.luw != False:
            raise JTDBLUWError
        if self.lock != False:
            raise JTDBLUWError

        self.lock = str(uuid.uuid4())
        self.luw = self.lock
        self.__db_link.CreateLuw(self,self.luw)
        return self

    def LuwCommit(self):
        if self.luw == False:
            raise JTDBLUWError
        self.__db_link.CommitLuw(self.luw)
        self.lock = False
        self.luw = False
        return self

    def LuwRollback(self):
        if self.luw == False:
            raise JTDBLUWError
        self.__db_link.RollbackLuw(self.luw)
        self.lock = False
        self.luw = False
        return self

"""Main database class"""
class JTDBLock:
    def __init__(self):
        self.lck_id = str(uuid.uuid4())
        self.fname = None

    def SetFName(self,fname):
        self.fname = self.LockFile(fname)

    def LockFile(self,fname):
        return fname+".lck"

    def IsLocked(self):
        lock_file = self.fname
        if os.path.isfile(lock_file):
            with open(lock_file, 'r') as f:
                info = f.read()
            return True,info
        return False, "OK"

    def WaitLockFile(self,maxwait = 10):
        lock_file = self.fname
        waited = 0
        while waited <= maxwait:
            waited = waited + 1
            isLcked , new_info = self.IsLocked()
            if isLcked == False:
                with open(lock_file, 'w') as f:
                    f.write(self.lck_id)
                return True, "Success"
            time.sleep(1)
        return False, new_info


    def UnlockFile(self):
        lock_file = self.fname
        lock,info = self.IsLocked()
        if lock == False:
            raise JTDBFileLockError
        if info != self.lck_id:
            raise JTDBFileLockError
        try:
            os.unlink(lock_file)
        except:
            raise JTDBFileLockError



"""Main database class"""
class JTDB:

    def __init__(self):
        self.data = {}
        self.lpointer = None
        self.fname = ''
        self.luws = []
        self.indexes = []
        self.withlock = False
        self.LockObj = JTDBLock()
        self.th_lock = threading.Lock()

    def IsInit(self):
        if not self.data:
            raise JTDBNotInit
        return True

    def SearchByIndex(self,name,vals):
        self.IsInit()

        idx_obj = next((item for item in self.indexes if item.name == name), None)
        if idx_obj==None:
            raise JTDBSIndexError
        
        return idx_obj.Search(vals)
        

    def AddSIndex(self,name,keys):
        self.IsInit()

        idx = JTDBIndex(self,name,keys,0) # 0 - hashed / 
        idx.Build()
        self.indexes.append(idx)
    
    def GetIndexedData(self,keysin):
        k_found = 0
        idx_found = None
        keys = keysin.keys()
        keys_c = len(keys)
        for idx in self.indexes:
            ix_key_c = len(idx.keys)
            if ix_key_c > keys_c:
                continue
            flist = list(set(keys).intersection(idx.keys))
            k_count = len(flist)
            if k_count > k_found and k_count == ix_key_c:
                k_found = k_count
                idx_found = idx
        
        if idx_found != None:
            vlist = []
            for k in idx_found.keys:
                vlist.append(keysin[k])
            return self.SearchByIndex(idx_found.name,vlist).data
        
        return None


    def UpdateSIndexes(self,operation,keys,record):

        for idx in self.indexes:
            if not any(e in idx.keys for e in keys):
                continue
            idx.Update(operation,record)

    def ReCreateIndex(self):
        self.IsInit()

        idx = 0
        for record in self.lpointer:
            record["__idx__"] = idx
            idx = idx + 1
        self.data["index"] = idx

    def GetNextIndex(self):
        self.IsInit()

        cur_idx = self.data["JTDBInfo"]["index"]
        self.data["JTDBInfo"]["index"] = cur_idx + 1
        return cur_idx + 1

    def __del__(self):
        if self.withlock:
            raise JTDBFileLocked

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.withlock:
            self.LockObj.UnlockFile()
            self.withlock = False

    def __enter__(self):
        return self

    def Close(self):
        self.IsInit()
        if self.withlock:
            self.LockObj.UnlockFile()
            self.withlock = False

    def LoadDB(self,fname,withlock = True):
        with open(fname, 'r') as f:
            self.data = json.load(f)
            if "JTDBInfo" not in self.data:
                raise JTDBWrongFile
            if "JTDBData" not in self.data:
                raise JTDBWrongFile
            if "index" not in self.data["JTDBInfo"]:
                raise JTDBIndexError
            if self.data["JTDBInfo"]["index"] < len(self.data["JTDBData"]):
                raise JTDBIndexError
            self.fname = fname
            self.lpointer = self.data["JTDBData"]
            self.withlock = withlock
            if withlock == True:
                self.LockObj.SetFName(fname)
                res, info = self.LockObj.WaitLockFile(10)
                if res == False:
                    raise JTDBFileLocked


    def CreateDB(self,fname):
        self.fname = fname
        self.data = { "JTDBInfo" : {
                "version" : "1.0",
                "name" : fname,
                "index" : 0
                },
                "JTDBData" : [ ]
                }
        self.lpointer = self.data["JTDBData"]

    def Import(self, arrayData):
        self.IsInit()

        self.AddRecords(arrayData)
        return len(self.lpointer)

    def AddRecord(self,record, lock_id = False ,luw_id = False):
        self.IsInit()

        if type(record) is not dict:
            raise JTDBWrongRecord

        q_set = JTDBQuerySet(self)

        if self.ModifyData("C",None,record,lock_id,luw_id):
            q_set.AddResult(record)

        return q_set

    def AddRecords(self,records, lockid = False ,luwid = False):
        q_set = JTDBQuerySet(self)
        for record in records:
            q_set.AddFromQset(self.AddRecord(record,lockid,luwid))

        return q_set

    def DeleteRecord(self,record):
        self.IsInit()
        return self.ModifyData(self,"D",record,None)

    def DeleteRecords(self,qset,lockid = ""):
        self.FilterLocks(qset,lockid)
        for record in qset.GetData():
            if self.DeleteRecord(record):
                qset.GetData().remove(record)
  
        return qset

    def PrintRecords(self):
        self.IsInit()
        for record in self.lpointer:
            print(record)

    def SaveDB(self):
        self.IsInit()

        for record in self.lpointer:
            self.ClearControlInd(record)

        with open(self.fname, 'w',encoding='utf-8') as outfile:
            json.dump(self.data, outfile,ensure_ascii=False)

    def QueryCompare(self,data1,operation,data2):
        # ["EQ" 0 ,"GT" 1,"LE" 2,"NE" 3,"GE" 4,"LT" 5,"CO" 6,"IN" 7,"RE" 8,"HS" 9, "NH" 10,"NC" 11]
        if operation == 0:
            return data1 == data2
        if operation == 3:
            return data1 != data2
        if operation == 1:
            return data1 > data2
        if operation == 4:
            return data1 >= data2
        if operation == 5:
            return data1 < data2
        if operation == 2:
            return data1 <= data2
        if operation == 6:
            return data2 in data1
        if operation == 11:
            return  not data2 in data1
        if operation == 8:
            if re.search(data2,data1):
                return True
            else:
                return False
        if operation == 7:
            for v in data2:
                if data1 == v:
                    return True
            return False


    def Filter(self,**kwargs):

        self.IsInit()
        
        qry = JTDBQuery(kwargs)
        params = qry.parameters

        res_list = []

        for record in self.lpointer:
            for param in params:
                key = param["f"]
                if key in record:
                    if self.QueryCompare(record[key],param["c"],param["v"]):
                        res_list.append(record)

        return res_list


    def EvalQuery(self,QObj,record):

        params = QObj.parameters

        if QObj.operation == 2:
            c_result = False
        else:
            c_result = True


        for param in params:


            keys = param["f"]
            comp = param["c"]
            val = param["v"]

            if comp == -1:
                if self.EvalQuery(val,record):
                    if QObj.operation == 2:
                        c_result = True
                        break
                else:
                    if QObj.operation == 1:
                        c_result = False
                        break
                continue
            
            if comp == 9 or comp == 10:
                key= keys[-1]
                keys = keys[:-1]

            rec_pointer = record

            for key in keys:
                if key in rec_pointer:
                    rec_pointer = rec_pointer[key]
                else:
                    rec_pointer = None
                    break

            if rec_pointer != None:

                if comp == 9 or comp == 10:
                    c_res = key in record
                    if comp == 10:
                        c_res = not c_res

                    if c_res:
                        if QObj.operation == 2:
                            c_result = True
                            break
                    else:
                        if QObj.operation == 1:
                            c_result = False
                            break
                else:
                    if callable(val):
                        val = val(deepcopy(record))
                    if self.QueryCompare(rec_pointer,comp,val):
                        if QObj.operation == 2:
                            c_result = True
                            break
                    else:
                        if QObj.operation == 1:
                            c_result = False
                            break
            else:
                c_result = False
                break

        return c_result
    
    def GetIndexedSearch(self,QObj,ixs):
        for param in QObj.parameters:
            keys = param["f"]
            comp = param["c"]
            if comp == 0 and  keys[0] not in ixs:
                ixs[keys[0]] = param["v"]
            elif comp == -1:
                self.GetIndexedSearch(param["v"],ixs)


    def Query(self,QObj,ix_search = False):

        self.IsInit()


        q_set = JTDBQuerySet(self)

        data_loop = self.lpointer

        if ix_search:
            ix_flds = {}
            self.GetIndexedSearch(QObj,ix_flds)
            ixs_data = self.GetIndexedData(ix_flds)

            if ixs_data != None:
                data_loop = ixs_data

        for record in data_loop:
            if self.EvalQuery(QObj,record):
                q_set.AddResult(record)

        return q_set


    def GetByIdRaw(self,id):
 
        res = BinSearch(self.lpointer,"__idx__",id)

        return res

    def GetById(self,id):
        if not self.data:
            raise JTDBNotInit

        q_set = JTDBQuerySet(self)
        res = self.GetByIdRaw(id) 

        if res!=None:
            q_set.AddResult(res)

        return q_set

    """ Everything should land here!!!"""
    def ModifyData(self,operation,item,u_data,lock_id = False, luw_id = False):
        
        """ Do we still have this?"""
        if operation != "C":
            item = self.GetByIdRaw(item["__idx__"])
            if item == None:
                return False

            if  "__lock__" in item:
                if item["__lock__"] != False and item["__lock__"] != lock_id:
                    return False

        trigger_idx_u = True

        if u_data is not None and "__idx__" in u_data:
            del u_data["__idx__"]
        
    
        """Lock"""
        self.th_lock.acquire()

        if u_data is not None:
            self.ClearControlInd(u_data)

        
        if operation == "U":
            """ UPDATE """
            if "__luw__" in item:
                if item["__luw__"] != False:
                    trigger_idx_u = False

            item.update(u_data)

        
        elif operation == "C":
            """ CREATE """
            #item = {}
            self.ClearControlInd(u_data)
            u_data["__idx__"] =  self.GetNextIndex()
            u_data["__luw__"] =  luw_id
            u_data["__lock__"] =  lock_id

            #item.update(u_data)

            if luw_id != False:
                u_data["__new__"] = True
                luw_old = next((item for item in self.luws if item["luw"] == luw_id), None)
                if luw_old != None:
                    #raise JTDBLUWError
                    luw_old["data"].append(u_data)
                    trigger_idx_u = False
            item = u_data
            self.lpointer.append(item)
            
        elif operation == "D":
            """ DELETE """
            allow_rem = True
            if "__luw__" in item:
                if item["__luw__"] != False:
                    item["__del__"] = True
                    trigger_idx_u = False

            if trigger_idx_u:
                self.data.remove(item)
                u_data = None

        """Trigger updates"""
        if trigger_idx_u:
            self.RecordUpdateTrigger(operation,u_data.keys(),item)
        
        """UnLock"""
        self.th_lock.release()

        return True


    def EvalUpdate(self,p_item,ignorekey,key,value,lockid = None):
        
        parts = key.split("__")
        item = {}
        real_update = True
        
        if callable(value):
            value = value(deepcopy(p_item))

        if "__luw__" in item:
            if item["__luw__"] != False:
                real_update = False

        if len(parts) == 1 :
            if key in item or ignorekey == True:
                item[key] = value

        elif len(parts) == 2 :
            key = parts[0]
            if key not in item and ignorekey == False:
                return
            if parts[1] == "ADD":
                item[key] = p_item[key] + value
            if parts[1] == "SUB":
                item[key] = p_item[key] - value
            if parts[1] == "DIV":
                item[key] = p_item[key] / value
            if parts[1] == "MUL":
                item[key] = p_item[key] * value
        else:
            raise JTDBNoValidCondition
        
        return self.ModifyData("U",p_item,item,lockid)



    def FilterLocks(self,qset,lockid):
        for item in qset.GetData()[:]:
            if "__lock__" in item:
                if item["__lock__"] != False and item["__lock__"] != lockid:
                    qset.GetData().remove(item)
        return qset


    def UpdateRecords(self,qset,lockid = "",**kwargs):
        
        self.IsInit()

        self.FilterLocks(qset,lockid)

        for item in qset.GetData():
            for k,v in kwargs.items():
                    self.EvalUpdate(item,False,k,v,lockid)

        return qset

    def ClearControlInd(self,item):
        if "__luw__" in item:
            del item["__luw__"]
        if "__new__" in item:
            del item["__new__"]
        if "__del__" in item:
            del item["__del__"]
        if "__lock__" in item:
            del item["__lock__"]

    def UpdateRecordsBulk(self,qset, bdata,lock_id = False):
        self.IsInit()

        self.FilterLocks(qset,lock_id)

        ix = 0
        for item in qset.GetData():
            u_item = bdata[ix]
            ix = ix + 1
            self.ModifyData("U",item,u_item,lock_id)

        return qset

    def ModifyRecords(self,qset,lockid = "",**kwargs):
        self.IsInit()

        self.FilterLocks(qset,lockid)

        for item in qset.GetData():
            for k,v in kwargs.items():
                    self.EvalUpdate(item,True,k,v)
        return qset


    def LockEntries(self,qset,lockid):
        self.IsInit()

        for item in qset.GetData():
            if "__lock__" in item:
                if item["__lock__"] != False:
                    raise JTDBLockError

        for item in qset.GetData():
            item["__lock__"] = lockid
            
        return qset

    def UnLockEntries(self,qset,lockid):
        self.IsInit()

        for item in qset.GetData():
            if "__lock__" in item:
                if item["__lock__"] != lockid:
                    raise JTDBLockError

        for item in qset.GetData():
            item["__lock__"] = False

        return qset

    def CreateLuw(self,qset,luw_id):
        self.IsInit()

        luw_old = next((item for item in self.luws if item["luw"] == luw_id), None)
        if luw_old != None:
            raise JTDBLUWError
        res = self.LockEntries(qset,luw_id)
        if res == None:
            raise JTDBLUWError
        for item in qset.GetData():
            item["__luw__"] = luw_id
        self.luws.append({ "luw": luw_id , "data" : deepcopy(qset.GetData()) })
        return True

    def RollbackLuw(self,luw_id):
        self.IsInit()

        luw = next((item for item in self.luws if item["luw"] == luw_id), None)
        if luw == None:
            raise JTDBLUWError
        luw_list = luw["data"]
        for item in luw_list:
            data_item = self.GetById(item["__idx__"]).List()
            if len(data_item) != 1:
                raise JTDBLUWError
            xitem = data_item[0]
            if "__new__" in item:
                if item["__new__"] == True:
                    self.lpointer.remove(xitem)
            xitem.update(item)
            xitem["__lock__"] = False
            xitem["__luw__"] = False
            xitem["__new__"] = False
            xitem["__del__"] = False

        luw_list.clear()
        self.luws.remove(luw)

        return True

    def CommitLuw(self,luw_id):
        self.IsInit()

        luw = next((item for item in self.luws if item["luw"] == luw_id), None)
        if luw == None:
            raise JTDBLUWError
        luw_list = luw["data"]
        for item in luw_list:
            data_item = self.GetById(item["__idx__"]).List()
            if len(data_item) != 1:
                raise JTDBLUWError
            xitem = data_item[0]
            xitem["__lock__"] = False
            xitem["__luw__"] = False
            xitem["__new__"] = False
            
            if "__del__" in xitem and xitem["__del__"] == True:
                self.lpointer.remove(xitem)
                self.RecordUpdateTrigger("D",None,item)
            elif "__new__" in xitem and xitem["__new__"] == True:
                self.RecordUpdateTrigger("C",None,item)
            else:
                self.RecordUpdateTrigger("U",None,item)

        luw_list.clear()
        self.luws.remove(luw)

        return True

    def RecordUpdateTrigger(self,operation,key,item):
        self.UpdateSIndexes(operation,key,item)

    def GetAll(self):
        self.IsInit()


        q_set = JTDBQuerySet(self)
        q_set.SetData(self.lpointer)
        return q_set
