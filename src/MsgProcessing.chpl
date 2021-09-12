
module MsgProcessing
{
    use ServerConfig;

    use Time;
    use Math only;
    use Reflection;
    use Errors;
    use Logging;
    use Message;
    
    use MultiTypeSymbolTable;
    use MultiTypeSymEntry;
    use ServerErrorStrings;

    use AryUtil;
    use ArraySetops;

    public use OperatorMsg;
    public use RandMsg;
    public use IndexingMsg;
    public use UniqueMsg;
    public use In1dMsg;
    public use HistogramMsg;
    public use ArgSortMsg;
    public use SortMsg;
    public use ReductionMsg;
    public use FindSegmentsMsg;
    public use EfuncMsg;
    public use ConcatenateMsg;
    public use SegmentedMsg;
    public use JoinEqWithDTMsg;
    public use RegistrationMsg;
    public use ArraySetopsMsg;
    public use KExtremeMsg;
    public use CastMsg;
    public use BroadcastMsg;
    public use FlattenMsg;
    use LinearAlgebra.Sparse;
    use DateTime;
    use BlockDist;
    use BitOps;
    use AryUtil;
    use CommAggregation;
    use IO;
    use CPtr;
    use Reflection;
    use Logging;
    use Unique;
    use ServerConfig;
    use arkouda_server;
    use ArraySetops;
    use LinearAlgebra;
    
    config const RSLSD_vv = false;
    const vv = RSLSD_vv; // these need to be const for comms/performance reasons

    config const RSLSD_numTasks = here.maxTaskPar; // tasks per locale based on locale0
    const numTasks = RSLSD_numTasks; // tasks per locale
    const Tasks = {0..#numTasks}; // these need to be const for comms/performance reasons

    config param RSLSD_bitsPerDigit = 16;
    private param bitsPerDigit = RSLSD_bitsPerDigit; // these need to be const for comms/performance reasons
    
    private config const logLevel = ServerConfig.logLevel;
    const mpLogger = new Logger(logLevel);

    const rsLogger = new Logger(logLevel);

    // calculate sub-domain for task
    inline proc calcBlock(task: int, low: int, high: int) {
        var totalsize = high - low + 1;
        var div = totalsize / numTasks;
        var rem = totalsize % numTasks;
        var rlow: int;
        var rhigh: int;
        if (task < rem) {
            rlow = task * (div+1) + low;
            rhigh = rlow + div;
        }
        else {
            rlow = task * div + rem + low;
            rhigh = rlow + div - 1;
        }
        return {rlow .. rhigh};
    }

    inline proc calcGlobalIndex(bucket: int, loc: int, task: int): int {
            return ((bucket * numLocales * numTasks) + (loc * numTasks) + task);
    }
    /* 
    Parse, execute, and respond to a create message 

    :arg : payload
    :type string: containing (dtype,size)

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: (MsgTuple) response message
    */
    proc createMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        var repMsg: string; // response message
        // split request into fields
        var (dtypestr, sizestr) = payload.splitMsgToTuple(2);
        var dtype = str2dtype(dtypestr);
        var size = try! sizestr:int;
        if (dtype == DType.UInt8) || (dtype == DType.Bool) {
          overMemLimit(size);
        } else {
          overMemLimit(8*size);
        }
        // get next symbol name
        var rname = st.nextName();
        
        // if verbose print action
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(), 
            "cmd: %s dtype: %s size: %i new pdarray name: %s".format(
                                                     cmd,dtype2str(dtype),size,rname));
        // create and add entry to symbol table
        st.addEntry(rname, size, dtype);
        // if verbose print result
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(), 
                                    "created the pdarray %s".format(st.attrib(rname)));

        repMsg = "created " + st.attrib(rname);
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(), repMsg);                                 
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

    proc zerosStoreMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        var repMsg: string; // response message
        // split request into fields
        var (dtypestr, sizestr, store) = payload.splitMsgToTuple(3);
        var dtype = str2dtype(dtypestr);
        var size = try! sizestr:int;
        var res: borrowed GenSymEntry = st.lookup(store);
        var s = toSymEntry(res,int);
        s.a = 0;
        s.hasMin = false;
        s.hasMax = false;
        // if verbose print action
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
            "cmd: %s dtype: %s size: %i updated pdarray name: %s".format(
                                                     cmd,dtype2str(dtype),size,store));
        // if verbose print result
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                    "created the pdarray %s".format(st.attrib(store)));

        repMsg = "created " + st.attrib(store);
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(), repMsg);
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }


    /* 
    Parse, execute, and respond to a delete message 

    :arg reqMsg: request containing (cmd,name)
    :type reqMsg: string 

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: MsgTuple
    */
    proc deleteMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        var repMsg: string; // response message
        // split request into fields
        var (name) = payload.splitMsgToTuple(1);
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(), 
                                     "cmd: %s array: %s".format(cmd,st.attrib(name)));
        // delete entry from symbol table
        if st.deleteEntry(name) {
            repMsg = "deleted %s".format(name);
        }
        else {
            repMsg = "registered symbol, %s, not deleted".format(name);
        }
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);       
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

    /* 
    Clear all unregistered symbols and associated data from sym table
    
    :arg reqMsg: request containing (cmd)
    :type reqMsg: string 

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: MsgTuple
     */
    proc clearMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        var repMsg: string; // response message
        var (_) = payload.splitMsgToTuple(1); // split request into fields
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(), "cmd: %s".format(cmd));
        st.clear();

        repMsg = "success";
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(), repMsg);
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

    /* 
    Takes the name of data referenced in a msg and searches for the name in the provided sym table.
    Returns a string of info for the sym entry that is mapped to the provided name.

    :arg reqMsg: request containing (cmd,name)
    :type reqMsg: string 

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: MsgTuple
     */
    proc infoMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        var repMsg: string; // response message
        // split request into fields
        var (name) = payload.splitMsgToTuple(1);
 
        // if name == "__AllSymbols__" passes back info on all symbols       
        repMsg = st.info(name);
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(), repMsg);
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }
    
    /* 
    query server configuration...
    
    :arg reqMsg: request containing (cmd)
    :type reqMsg: string 

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: MsgTuple
     */
    proc getconfigMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        var repMsg: string; // response message
        var (_) = payload.splitMsgToTuple(1); // split request into fields
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),"cmd: %s".format(cmd));
        return new MsgTuple(getConfig(), MsgType.NORMAL);
    }

    /* 
    query server total memory allocated or symbol table data memory
    
    :arg reqMsg: request containing (cmd)
    :type reqMsg: string 

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: MsgTuple
     */
    proc getmemusedMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        var repMsg: string; // response message
        var (_) = payload.splitMsgToTuple(1); // split request into fields
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),"cmd: %s".format(cmd));
        if (memTrack) {
            return new MsgTuple((getMemUsed():uint * numLocales:uint):string, MsgType.NORMAL);
        }
        else {
            return new MsgTuple(st.memUsed():string, MsgType.NORMAL);
        }
    }
    
    /* 
    Response to __str__ method in python str convert array data to string 

    :arg reqMsg: request containing (cmd,name)
    :type reqMsg: string 

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: (string,MsgType)
   */
    proc strMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        var repMsg: string; // response message
        // split request into fields
        var (name, ptstr) = payload.splitMsgToTuple(2);
        var printThresh = try! ptstr:int;
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(), 
                                              "cmd: %s name: %s threshold: %i".format(
                                               cmd,name,printThresh));  
                                               
        repMsg  = st.datastr(name,printThresh);        
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);  
        return new MsgTuple(repMsg,MsgType.NORMAL);
    }

    /* Response to __repr__ method in python.
       Repr convert array data to string 
       
       :arg reqMsg: request containing (cmd,name)
       :type reqMsg: string 

       :arg st: SymTab to act on
       :type st: borrowed SymTab 

       :returns: MsgTuple
      */ 
    proc reprMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        var repMsg: string; // response message
        // split request into fields
        var (name, ptstr) = payload.splitMsgToTuple(2);
        var printThresh = try! ptstr:int;

        repMsg = st.datarepr(name,printThresh);
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg); 
        return new MsgTuple(repMsg,MsgType.NORMAL);
    }


    /*
    Creates a sym entry with distributed array adhering to the Msg parameters (start, stop, stride)

    :arg reqMsg: request containing (cmd,start,stop,stride)
    :type reqMsg: string 

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: MsgTuple
    */
    proc arangeMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        var repMsg: string; // response message
        var (startstr, stopstr, stridestr) = payload.splitMsgToTuple(3);
        var start = try! startstr:int;
        var stop = try! stopstr:int;
        var stride = try! stridestr:int;
        // compute length
        var len = (stop - start + stride - 1) / stride;
        overMemLimit(8*len);
        // get next symbol name
        var rname = st.nextName();

        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(), 
                       "cmd: %s start: %i stop: %i stride: %i : len: %i rname: %s".format(
                        cmd, start, stop, stride, len, rname));
        
        var t1 = Time.getCurrentTime();
        var e = st.addEntry(rname, len, int);
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                      "alloc time = %i sec".format(Time.getCurrentTime() - t1));

        t1 = Time.getCurrentTime();
        ref ea = e.a;
        ref ead = e.aD;
        forall (ei, i) in zip(ea,ead) {
            ei = start + (i * stride);
        }

        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                      "compute time = %i sec".format(Time.getCurrentTime() - t1));

        repMsg = "created " + st.attrib(rname);
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }            

    proc arangeStoreMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        var repMsg: string; // response message
        var (startstr, stopstr, stridestr, store) = payload.splitMsgToTuple(4);
        var start = try! startstr:int;
        var stop = try! stopstr:int;
        var stride = try! stridestr:int;
        var len = (stop - start + stride - 1) / stride;
        // get store name
        var res: borrowed GenSymEntry = st.lookup(store);

       mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                       "cmd: %s start: %i stop: %i stride: %i store: %s len: %i".format(
                        cmd, start, stop, stride, len));

        var s = toSymEntry(res,int);
        s.hasMin=false;
        s.hasMax=false;
        var t1 = Time.getCurrentTime();
        ref sa = s.a;
        ref sad = s.aD;
        forall (si, i) in zip(sa,sad) {
            si = start + (i * stride);
        }

        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                      "compute time = %i sec".format(Time.getCurrentTime() - t1));

        repMsg = "updated " + st.attrib(store);
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

    /* 
    Creates a sym entry with distributed array adhering to the Msg parameters (start, stop, len)

    :arg reqMsg: request containing (cmd,start,stop,len)
    :type reqMsg: string 

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: MsgTuple
    */
    proc linspaceMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        var repMsg: string; // response message
        var (startstr, stopstr, lenstr) = payload.splitMsgToTuple(3);
        var start = try! startstr:real;
        var stop = try! stopstr:real;
        var len = try! lenstr:int;
        // compute stride
        var stride = (stop - start) / (len-1);
        overMemLimit(8*len);
        // get next symbol name
        var rname = st.nextName();
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                        "cmd: %s start: %r stop: %r len: %i stride: %r rname: %s".format(
                         cmd, start, stop, len, stride, rname));

        var t1 = Time.getCurrentTime();
        var e = st.addEntry(rname, len, real);
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                      "alloc time = %i".format(Time.getCurrentTime() - t1));

        t1 = Time.getCurrentTime();
        ref ea = e.a;
        ref ead = e.aD;
        forall (ei, i) in zip(ea,ead) {
            ei = start + (i * stride);
        }
        ea[0] = start;
        ea[len-1] = stop;
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                   "compute time = %i".format(Time.getCurrentTime() - t1));

        repMsg = "created " + st.attrib(rname);
        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);       
        return new MsgTuple(repMsg,MsgType.NORMAL);
    }

    /* 
    Sets all elements in array to a value (broadcast) 

    :arg reqMsg: request containing (cmd,name,dtype,value)
    :type reqMsg: string 

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: MsgTuple
    :throws: `UndefinedSymbolError(name)`
    */
    proc setMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        param pn = Reflection.getRoutineName();
        var repMsg: string; // response message
        var (name, dtypestr, value) = payload.splitMsgToTuple(3);
        var dtype = str2dtype(dtypestr);

        var gEnt: borrowed GenSymEntry = st.lookup(name);

        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                            "cmd: %s value: %s in pdarray %s".format(cmd,name,st.attrib(name)));

        select (gEnt.dtype, dtype) {
            when (DType.Int64, DType.Int64) {
                var e = toSymEntry(gEnt,int);
                var val: int = try! value:int;
                e.a = val;
                repMsg = "set %s to %t".format(name, val);
            }
            when (DType.Int64, DType.Float64) {
                var e = toSymEntry(gEnt,int);
                var val: real = try! value:real;
                mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                        "cmd: %s name: %s to val: %t".format(cmd,name,val:int));
                e.a = val:int;
                repMsg = "set %s to %t".format(name, val:int);
            }
            when (DType.Int64, DType.Bool) {
                var e = toSymEntry(gEnt,int);
                value = value.replace("True","true");
                value = value.replace("False","false");
                var val: bool = try! value:bool;
                mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                        "cmd: %s name: %s to val: %t".format(cmd,name,val:int));
                e.a = val:int;
                repMsg = "set %s to %t".format(name, val:int);
            }
            when (DType.Float64, DType.Int64) {
                var e = toSymEntry(gEnt,real);
                var val: int = try! value:int;
                mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                      "cmd: %s name: %s to value: %t".format(cmd,name,val:real));
                e.a = val:real;
                repMsg = "set %s to %t".format(name, val:real);
            }
            when (DType.Float64, DType.Float64) {
                var e = toSymEntry(gEnt,real);
                var val: real = try! value:real;
                mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                           "cmd: %s name; %s to value: %t".format(cmd,name,val));
                e.a = val;
                repMsg = "set %s to %t".format(name, val);
            }
            when (DType.Float64, DType.Bool) {
                var e = toSymEntry(gEnt,real);
                value = value.replace("True","true");
                value = value.replace("False","false");                
                var val: bool = try! value:bool;
                mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                       "cmd: %s name: %s to value: %t".format(cmd,name,val:real));
                e.a = val:real;
                repMsg = "set %s to %t".format(name, val:real);
            }
            when (DType.Bool, DType.Int64) {
                var e = toSymEntry(gEnt,bool);
                var val: int = try! value:int;
                mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                       "cmd: %s name: %s to value: %t".format(cmd,name,val:bool));
                e.a = val:bool;
                repMsg = "set %s to %t".format(name, val:bool);
            }
            when (DType.Bool, DType.Float64) {
                var e = toSymEntry(gEnt,int);
                var val: real = try! value:real;
                mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                      "cmd: %s name: %s to  value: %t".format(cmd,name,val:bool));
                e.a = val:bool;
                repMsg = "set %s to %t".format(name, val:bool);
            }
            when (DType.Bool, DType.Bool) {
                var e = toSymEntry(gEnt,bool);
                value = value.replace("True","true");
                value = value.replace("False","false");
                var val: bool = try! value:bool;
                mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                            "cmd: %s name: %s to value: %t".format(cmd,name,val));
                e.a = val;
                repMsg = "set %s to %t".format(name, val);
            }
            otherwise {
                mpLogger.error(getModuleName(),getRoutineName(),
                                               getLineNumber(),"dtype: %s".format(dtypestr));
                return new MsgTuple(unrecognizedTypeError(pn,dtypestr), MsgType.ERROR);
            }
        }

        mpLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

    /* 
    Counts frequencies of occurences in an array 

    :arg reqMsg: request containing (name of first array, name of second array, n, list of arguments)
    :type reqMsg: string 

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: MsgTuple
    :throws: `UndefinedSymbolError(name)`
    */

    proc countFrequenciesMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        param pn = Reflection.getRoutineName();
        var repMsg: string; // response message
        var (name1, name2, nStr, listOfArgsString) = payload.splitMsgToTuple(4);
        var n = try! nStr:int;
        var name1E: borrowed GenSymEntry = st.lookup(name1);
        var name2E: borrowed GenSymEntry = st.lookup(name2);
        listOfArgsString = listOfArgsString.strip("[]");
        var split: [0..n-1] string = listOfArgsString.split(",");
        var listOfArgs: [0..n-1] int;
        var k: int = 0;
        for s in split do {
            listOfArgs[k] = try! s:int;
            k = k+1;
        }
        var a = toSymEntry(name1E, int);
        var b = toSymEntry(name2E, int);
        b.hasMax = false;
        b.hasMin = false;
        var f2 = lambda(b: int, arg3: int) {
            return (b + arg3);
        };
        var kr0: [a.aD] (a.etype,int) = [(key,rank) in zip(a.a,a.aD)] (key,rank);
        var lock : sync bool;
        var l1: sync bool;
        try
        {
            coforall loc in Locales {
                    on loc {
                        coforall task in Tasks {
                            // bucket domain
                            var bD = {0..#b.a.size};
                            // allocate counts
                            var taskBucketCounts: [bD] int;
                            // get local domain's indices
                            var lD = kr0.localSubdomain();
                            // calc task's indices from local domain's indices
                            var tD = calcBlock(task, lD.low, lD.high);
                            if (tD.low<a.a.size && tD.high<a.a.size && tD.low<=tD.high)
                            { 
                                try! rsLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                        "locid: %t task: %t tD: %t".format(loc.id,task,tD));
                                // count digits in this task's part of the array
                                var f1: func(int,int,int,int,int) = lambda(a: int, arg1: int, arg2: int, arg3: int) {
                                    return (((a - arg1) / arg2) % arg3);
                                };
                                for i in tD {
                                    var bucket = f1(a.a[i], listOfArgs[0], listOfArgs[1], listOfArgs[2]); // calc bucket from key
                                    taskBucketCounts[bucket] += listOfArgs[3];
                                }
                                // write counts in to global counts in transposed order
                                for bucket in bD {
                                    lock.writeEF(true);
                                    b.a[bucket] = b.a[bucket] + taskBucketCounts[bucket];
                                    var unlock = lock.readFE();
                                }
                            }
                    }//coforall task
                }//on loc
            }//coforall loc
        }
        catch
        {
            var errorMsg = unknownError("Wrong index");
            omLogger.error(getModuleName(),getRoutineName(),getLineNumber(),errorMsg);
            return new MsgTuple(errorMsg, MsgType.ERROR);
        }
        repMsg = "updated %s".format(st.attrib(name1));
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

    /* 
    Moves records from two arrays to the third

    :arg reqMsg: request containing (name of first array, name of second array, name of third array, n, list of arguments)
    :type reqMsg: string 

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: MsgTuple
    :throws: `UndefinedSymbolError(name)`
    */

    proc moveRecordsMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws
    {
        param pn = Reflection.getRoutineName();
        var repMsg: string; // response message
        var (name1, name2, name3, nStr, listOfArgsString) = payload.splitMsgToTuple(5);
        var ret: int = 0;
        var n = try! nStr:int;
        var name1E: borrowed GenSymEntry = st.lookup(name1);
        var name2E: borrowed GenSymEntry = st.lookup(name2);
        var name3E: borrowed GenSymEntry = st.lookup(name3);
        listOfArgsString = listOfArgsString.strip("[]");
        listOfArgsString = listOfArgsString.replace(",","");
        var split: [1..n] string = listOfArgsString.split(' ');
        var listOfArgs: [1..n] int;
        var k: int = 0;
        for s in split do {
            listOfArgs[k] = try! s:int;
            k = k+1;
        }
        var f1 = lambda(a: int, arg1: int, arg2: int, arg3: int) {
             return (((a - arg1) / arg2) % arg3);
        };
        var f2 = lambda(b: int, arg3: int) {
            return (b + arg3);
        };
        var a = toSymEntry(name1E, int);
        var b = toSymEntry(name2E, int);
        var c = toSymEntry(name3E, int);
        a.a.reverse();
        var lock: sync bool;
        for k in a.a {
            var ret: int = f1(k, listOfArgs[0], listOfArgs[1], listOfArgs[2]);
            b.a[ret] = f2(b.a[ret], listOfArgs[3]);
            c.a[b.a[ret]] = k;
        }
        a.a.reverse();
        repMsg = "updated %s".format(st.attrib(name1));
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

    /* 
    Counts cumulative sum of an array

    :arg reqMsg: request containing (name of array)
    :type reqMsg: string 

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: MsgTuple
    :throws: `UndefinedSymbolError(name)`
    */

    proc cumSumMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        param pn = Reflection.getRoutineName();
        var repMsg: string; // response message
        var (name1) = payload.splitMsgToTuple(1);
        var name1E: borrowed GenSymEntry = st.lookup(name1);
        var a = toSymEntry(name1E, int);
        a.a = + scan a.a;
        a.hasMin = false;
        a.hasMax = false;
        repMsg = "updated %s".format(name1);
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

    /* 
    Remove duplicates from an array

    :arg reqMsg: request containing (name of array)
    :type reqMsg: string 

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: MsgTuple
    :throws: `UndefinedSymbolError(name)`
    */

    proc removeDuplicatesMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        param pn = Reflection.getRoutineName();
        var (name1) = payload.splitMsgToTuple(1);
        var name1E: borrowed GenSymEntry = st.lookup(name1);
        var a = toSymEntry(name1E, int);
        var rname = st.nextName();
        var helper = uniqueFromSorted(a.a, false);
        var e = st.addEntry(rname, helper.size, int);
        e.a=helper;
        var repMsg: string = "created %s".format(st.attrib(rname));
        omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

    /* 
    Makes a pd array from csv

    :arg reqMsg: request containing (full csv file)
    :type reqMsg: string 

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: MsgTuple
    :throws: `UndefinedSymbolError(name)`
    */

    proc getFromCsvMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        param pn = Reflection.getRoutineName();
        var splitted = payload.split('\n');
        var n : int = try! splitted[splitted.size-1] : int;
        var typesList = splitted[splitted.size-2];
        var typesListList = typesList.strip("[]").split(',').strip(' ').strip('\'');
        var rname = st.nextName();
        if (typesListList[n]=="int64") then {
            var e = st.addEntry(rname, splitted.size - 4, int); 
            forall i in 1..splitted.size - 4 do {
                var elems = splitted[i].split(",");
                if (elems[n]!="") then e.a[i-1] = elems[n] : int;
            }
        }
        else if (typesListList[n]=="float64") then {
            var e = st.addEntry(rname, splitted.size - 4, real);
            forall i in 1..splitted.size - 4 do {
                var elems = splitted[i].split(",");
                if (elems[n]!="") then e.a[i-1] = elems[n] : real;
            }
        }
        else if (typesListList[n]=="bool") then {
            var e = st.addEntry(rname, splitted.size - 4, bool);
            forall i in 1..splitted.size - 4 do {
                var elems = splitted[i].split(",");
                if (elems[n]!="") then e.a[i-1] = elems[n] : bool;
            }
        }   
        else if (typesListList[n]=="date") then {
            var e = st.addEntry(rname, splitted.size - 4, int);
            forall i in 1..splitted.size - 4 do {
                var elems = splitted[i].split(",");
                var date = elems[n];
                var first = date.split(" ");
                var second = first[0].split("-");
                var year=1970;
                var month=1;
                var day = 1;
                if (second[0]!="") then year = try! second[0] : int;
                if (second[1]!="") then month = try! second[1] : int;
                if (second[2]!="") then day = try! second[2] : int;
                var third = first[1].split(":");
                var hours = 0;
                var minutes = 0;
                var seconds = 0;
                if (third[0]!="") then hours = try! third[0] : int;
                if (third[1]!="") then minutes = try! third[1] : int;
                if (third[2]!="") then seconds = try! third[2] : int;
                var realDate: datetime; 
                realDate.init(year, month, day, hours, minutes, seconds);
                var basic_date : datetime;
                basic_date.init(1970,1,1);
                var diff : int = (realDate - basic_date).total_seconds() : int;
                e.a[i-1] = diff;
            }
        }
        var repMsg: string = "created %s".format(st.attrib(rname));
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

    /* 
    Transposes a list of arrays

    :arg reqMsg: request containing (number of arrays, [list of arrays])
    :type reqMsg: string 

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: MsgTuple
    :throws: `UndefinedSymbolError(name)`
    */

    proc transposeMsg(cmd, args, st): MsgTuple throws{
        param pn = Reflection.getRoutineName();
        var list_first = args.split(" ");
        var n : int = list_first[0] : int;
        var input_list_names : [0..n-1] string;
        var res_list_names : [0..n-1] string;
        forall i in 0..n-1 do {
            input_list_names[i] = list_first[i+1];
        }
        forall i in 0..n-1 do {
            res_list_names[i] = st.nextName();
            st.addEntry(res_list_names[i], n, int);
        }
        forall i in 0..n-1 do {
            var l =  st.lookup(res_list_names[i]);
            var l1 = toSymEntry(l,int);
            forall j in 0..n-1 do { 
                var r = st.lookup(input_list_names[j]);
                var r1 = toSymEntry(r, int);
                l1.a[j]=r1.a[i];
            }
        }
        var repMsg: string = "created";
        for i in 0..n-1 do {
            repMsg+=" "+res_list_names[i];
        }
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

    /* 
    Does triangle count on dense matrices

    :arg reqMsg: request containing (number of arrays, [list of arrays])
    :type reqMsg: string 

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: MsgTuple
    :throws: `UndefinedSymbolError(name)`
    */

    proc triangleCountMsg(cmd, args, st): MsgTuple throws {
        param pn = Reflection.getRoutineName();
        var list_first = args.split(" ");
        var n : int = list_first[0] : int;
        var mat : [0..(n-1), 0..(n-1)] int;
        for i in 0..n-1 do {
            var l = st.lookup(list_first[i+1]);
            var l1 = toSymEntry(l, int);
            for j in 0..n-1 do {
                mat[i,j] = l1.a[j];
                //writeln("k=", mat[i,j]);
            }
        }
        //writeln(mat);
        //writeln(dot(mat, mat));
        var new_mat = dot(mat, mat)*mat;
        var s: int = + reduce new_mat;
        var repMsg: string = "int64 %i".format(s);
        return new MsgTuple(repMsg, MsgType.NORMAL); 
    }

    proc splice(k, pointers, indexes) 
    {
        var left = pointers[k];
        var right = pointers[k+1]-1;
        return indexes[left..right];
    }

    /* 
    Does triangle count on sparse matrices

    :arg reqMsg: request containing (size of matrix, pointers of base array, indexes of base array, pointers of transposed array, indexes of transposed arrays)
    :type reqMsg: string 

    :arg st: SymTab to act on
    :type st: borrowed SymTab 

    :returns: MsgTuple
    :throws: `UndefinedSymbolError(name)`
    */

  /*  proc sparseTriangleCountMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        param pn = Reflection.getRoutineName();
        var (nStr, name1, name2, name3, name4) = payload.splitMsgToTuple(5);
        var n: int = nStr: int;
        var pointer: borrowed GenSymEntry = st.lookup(name1);
        var pointer2: borrowed GenSymEntry = st.lookup(name3);
        var p = toSymEntry(pointer, int);
        var p2 = toSymEntry(pointer2, int);
        var indexes: borrowed GenSymEntry = st.lookup(name2);
        var indexes2: borrowed GenSymEntry = st.lookup(name4);
        var indexesReal = toSymEntry(indexes, int);
        var indexesReal2 = toSymEntry(indexes2, int);
        var k: int = 0;
        var s: int = 0;
        var D: domain(int, parSafe=false) = {0..(indexesReal.a.size-1)};
        var help2: [D] int = 0;
        forall i in 0..p.a.size-2 do {
            if (p.a[i]<p.a[i+1])
            {
                forall j in p.a[i]..p.a[i+1]-1 do {
                    var first = splice(i, p.a, indexesReal.a);
                    var second = splice(indexesReal.a[j], p2.a, indexesReal2.a);
                    help2[j] = mergeArraysCount(first, second, p.a[i],p2.a[indexesReal.a[j]]);
                    //help2[j] = mergeArraysCount(splice(i, p.a, indexesReal.a),splice(indexesReal.a[j],p2.a, indexesReal2.a),p.a[i], p2.a[indexesReal.a[j]]);
                }
            }
            //writeln("i=",i)
        }
        s = + reduce help2;
        var repMsg: string = "int64 %i".format(s);
        return new MsgTuple(repMsg, MsgType.NORMAL); 
    }*/

    proc sparseTriangleCountMsg(cmd: string, payload: string, st:borrowed SymTab) : MsgTuple throws {
        param pn = Reflection.getRoutineName();
        var (nStr, name1, name2, name3, name4) = payload.splitMsgToTuple(5);
        var n: int = nStr: int;
        var pointer: borrowed GenSymEntry = st.lookup(name1);
        var pointer2: borrowed GenSymEntry = st.lookup(name3);
        var p = toSymEntry(pointer, int);
        var p2 = toSymEntry(pointer2, int);
        var indexes: borrowed GenSymEntry = st.lookup(name2);
        var indexes2: borrowed GenSymEntry = st.lookup(name4);
        var indexesReal = toSymEntry(indexes, int);
        var indexesReal2 = toSymEntry(indexes2, int);
        var s: int = 0;
        var l1: sync bool;
        var kr0: [p.aD] (p.etype,int) = [(key,rank) in zip(p.a,p.aD)] (key,rank);
        coforall loc in Locales with (+ reduce s) {
            on loc {
                var locS: int = 0;
                coforall task in Tasks with (+ reduce locS) {
                    // get local domain's indices
                    var lD = kr0.localSubdomain();
                    // calc task's indices from local domain's indices
                    var tD = calcBlock(task, lD.low, lD.high);
                    if (tD.low<p.a.size && tD.high<p.a.size && tD.low<=tD.high)
                    { 
                        for i in tD {
                            for j in p.a[i]..p.a[i+1]-1 do {
                                var first = splice(i, p.a, indexesReal.a);
                                var second = splice(indexesReal.a[j], p2.a, indexesReal2.a);
                                locS += mergeArraysCount(first, second, p.a[i],p2.a[indexesReal.a[j]]);
                            }
                        }
                    }
                }//coforall task
                s+=locS;
            }//on loc
        }//coforall loc
        //s = + reduce help2;
        var repMsg: string = "int64 %i".format(s);
        return new MsgTuple(repMsg, MsgType.NORMAL); 
    }

    proc vectorTimesMatrixMsg(cmd: string, payload: string, st:borrowed SymTab) : MsgTuple throws {
        param pn = Reflection.getRoutineName();
        var list_first = payload.split(" ");
        var n : int = list_first[0] : int;
        var myType1: string = list_first[1] : string;
        var myType2: string = list_first[2] : string;
        var input_list_names : [0..n-1] string;
        forall i in 0..n-1 do {
            input_list_names[i] = list_first[i+4];
        }
        var v = st.lookup(list_first[3]);
        if (myType1=="int64" && myType2=="int64") then {
            var v_real = toSymEntry(v, int);
            var res_name: string = st.nextName();
            st.addEntry(res_name, n, int);
            var res1 =  st.lookup(res_name);
            var res = toSymEntry(res1,int);
            var l_list: [0..n-1] [0..n-1] int;
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,int);
                forall j in 0..n-1 do {
                    l_list[j][i]=l1.a[j];
                }
            }
            forall i in 0..n-1 do {
                res.a[i] = + reduce (l_list[i] * v_real.a);
            }
            var repMsg: string = "created %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        else if (myType1=="float64" && myType2=="int64") then {
            var v_real = toSymEntry(v, real);
            var res_name: string = st.nextName();
            st.addEntry(res_name, n, real);
            var res1 =  st.lookup(res_name);
            var res = toSymEntry(res1,real);
            var l_list: [0..n-1] [0..n-1] int;
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,int);
                forall j in 0..n-1 do {
                    l_list[j][i]=l1.a[j];
                }
            }
            forall i in 0..n-1 do {
                res.a[i] = + reduce (l_list[i] * v_real.a);
            }
            var repMsg: string = "created %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        else if (myType1=="int64" && myType2=="float64") then {
            var v_real = toSymEntry(v, int);
            var res_name: string = st.nextName();
            st.addEntry(res_name, n, int);
            var res1 =  st.lookup(res_name);
            var res = toSymEntry(res1,real);
            var l_list: [0..n-1] [0..n-1] real;
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,real);
                forall j in 0..n-1 do {
                    l_list[j][i]=l1.a[j];
                }
            }
            forall i in 0..n-1 do {
                res.a[i] = + reduce (l_list[i] * v_real.a);
            }
            var repMsg: string = "created %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        else if (myType1=="float64" && myType2=="float64") then {
            var v_real = toSymEntry(v, real);
            var res_name: string = st.nextName();
            st.addEntry(res_name, n, real);
            var res1 =  st.lookup(res_name);
            var res = toSymEntry(res1,real);
            var l_list: [0..n-1] [0..n-1] real;
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,real);
                forall j in 0..n-1 do {
                    l_list[j][i]=l1.a[j];
                }
            }
            forall i in 0..n-1 do {
                res.a[i] = + reduce (l_list[i] * v_real.a);
            }
            var repMsg: string = "created %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        else return new MsgTuple("", MsgType.ERROR);
    }

    proc vectorTimesMatrixStoreMsg(cmd: string, payload: string, st:borrowed SymTab) : MsgTuple throws {
        param pn = Reflection.getRoutineName();
        var list_first = payload.split(" ");
        var n : int = list_first[0] : int;
        var myType1: string = list_first[1] : string;
        var myType2: string = list_first[2] : string;
        var myType3: string = list_first[3] : string;
        var input_list_names : [0..n-1] string;
        var v = st.lookup(list_first[4]);
        var res1 = st.lookup(list_first[5]);
        var res_name = list_first[5];
        forall i in 0..n-1 do {
            input_list_names[i] = list_first[i+6];
        }
        if (myType1=="int64" && myType2=="int64") then {
            var v_real = toSymEntry(v, int);
            var res = toSymEntry(res1,int);
            var l_list: [0..n-1] [0..n-1] int;
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,int);
                forall j in 0..n-1 do {
                    l_list[j][i]=l1.a[j];
                }
            }
            forall i in 0..n-1 do {
                res.a[i] = + reduce (l_list[i] * v_real.a);
            }
            var repMsg: string = "updated %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        else if (myType1=="float64" && myType2=="int64") then {
            var v_real = toSymEntry(v, real);
            var res = toSymEntry(res1,real);
            var l_list: [0..n-1] [0..n-1] int;
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,int);
                forall j in 0..n-1 do {
                    l_list[j][i]=l1.a[j];
                }
            }
            forall i in 0..n-1 do {
                res.a[i] = + reduce (l_list[i] * v_real.a);
            }
            var repMsg: string = "updated %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        else if (myType1=="int64" && myType2=="float64") then {
            var v_real = toSymEntry(v, int);
            var res = toSymEntry(res1,real);
            var l_list: [0..n-1] [0..n-1] real;
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,real);
                forall j in 0..n-1 do {
                    l_list[j][i]=l1.a[j];
                }
            }
            forall i in 0..n-1 do {
                res.a[i] = + reduce (l_list[i] * v_real.a);
            }
            var repMsg: string = "updated %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        else if (myType1=="float64" && myType2=="float64") then {
            var v_real = toSymEntry(v, real);
            var res = toSymEntry(res1,real);
            var l_list: [0..n-1] [0..n-1] real;
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,real);
                forall j in 0..n-1 do {
                    l_list[j][i]=l1.a[j];
                }
            }
            forall i in 0..n-1 do {
                res.a[i] = + reduce (l_list[i] * v_real.a);
            }
            var repMsg: string = "updated %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        else return new MsgTuple("", MsgType.ERROR);
    }

    proc matrixTimesVectorMsg(cmd: string, payload: string, st:borrowed SymTab): MsgTuple throws {
        param pn = Reflection.getRoutineName();
        var list_first = payload.split(" ");
        var n : int = list_first[0] : int;
        var myType1: string = list_first[1] : string;
        var myType2: string = list_first[2] : string;
        var input_list_names : [0..n-1] string;
        var v = st.lookup(list_first[3]);
        forall i in 0..n-1 do {
            input_list_names[i] = list_first[i+4];
        }
        if (myType1=="int64" && myType2=="int64") then {
            var v_real = toSymEntry(v, int);
            var res_name: string = st.nextName();
            st.addEntry(res_name, n, int);
            var res1 =  st.lookup(res_name);
            var res = toSymEntry(res1,int);
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,int);
                res.a[i] = + reduce (l1.a * v_real.a);
            }
            var repMsg: string = "created %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        else if (myType1=="float64" && myType2=="int64") then {
            var v_real = toSymEntry(v, real);
            var res_name: string = st.nextName();
            st.addEntry(res_name, n, real);
            var res1 =  st.lookup(res_name);
            var res = toSymEntry(res1,real);
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,int);
                res.a[i] = + reduce (l1.a * v_real.a);
            }
            var repMsg: string = "created %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        else if (myType1=="int64" && myType2=="float64") then {
            var v_real = toSymEntry(v, int);
            var res_name: string = st.nextName();
            st.addEntry(res_name, n, real);
            var res1 =  st.lookup(res_name);
            var res = toSymEntry(res1,real);
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,real);
                res.a[i] = + reduce (l1.a * v_real.a);
            }
            var repMsg: string = "created %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        else if (myType1=="float64" && myType2=="float64") then {
            var v_real = toSymEntry(v, real);
            var res_name: string = st.nextName();
            st.addEntry(res_name, n, real);
            var res1 =  st.lookup(res_name);
            var res = toSymEntry(res1,real);
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,real);
                res.a[i] = + reduce (l1.a * v_real.a);
            }
            var repMsg: string = "created %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        else return new MsgTuple("", MsgType.ERROR);
    }

    proc matrixTimesVectorStoreMsg(cmd: string, payload: string, st:borrowed SymTab): MsgTuple throws {
        param pn = Reflection.getRoutineName();
        var list_first = payload.split(" ");
        var n : int = list_first[0] : int;
        var myType1: string = list_first[1] : string;
        var myType2: string = list_first[2] : string;
        var myType3: string = list_first[3] : string;
        var input_list_names : [0..n-1] string;
        var v = st.lookup(list_first[4]);
        var res1 = st.lookup(list_first[5]);
        var res_name = list_first[5];
        forall i in 0..n-1 do {
            input_list_names[i] = list_first[i+6];
        }
        if (myType1=="int64" && myType2=="int64") then {
            var v_real = toSymEntry(v, int);
            var res1 =  st.lookup(res_name);
            var res = toSymEntry(res1,int);
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,int);
                res.a[i] = + reduce (l1.a * v_real.a);
            }
            var repMsg: string = "updated %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        else if (myType1=="float64" && myType2=="int64") then {
            var v_real = toSymEntry(v, real);
            var res1 =  st.lookup(res_name);
            var res = toSymEntry(res1,real);
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,int);
                res.a[i] = + reduce (l1.a * v_real.a);
            }
            var repMsg: string = "updated %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        else if (myType1=="int64" && myType2=="float64") then {
            var v_real = toSymEntry(v, int);
            var res1 =  st.lookup(res_name);
            var res = toSymEntry(res1,real);
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,real);
                res.a[i] = + reduce (l1.a * v_real.a);
            }
            var repMsg: string = "updated %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        else if (myType1=="float64" && myType2=="float64") then {
            var v_real = toSymEntry(v, real);
            var res1 =  st.lookup(res_name);
            var res = toSymEntry(res1,real);
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,real);
                res.a[i] = + reduce (l1.a * v_real.a);
            }
            var repMsg: string = "updated %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        else return new MsgTuple("", MsgType.ERROR);
    }

    proc inverseVectorMsg(cmd: string, payload: string, st:borrowed SymTab) : MsgTuple throws {
        var list = payload.split(' ');
        var name = list[1];
        var myType = list[0];
        var res_name = "";
        if (myType=="int64")
        {
            var input1 =  st.lookup(name);
            var input = toSymEntry(input1, int);
            var res_name: string = st.nextName();
            st.addEntry(res_name, input.size, int);
            var res1 =  st.lookup(res_name);
            var res = toSymEntry(res1,int);
            forall i in 0..input.a.size do {
                if (input.a[i]!=0) then res.a[i]=0;
                else res.a[i]=1;
            }
            var repMsg: string = "created %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        if (myType=="float64")
        {
            var input1 =  st.lookup(name);
            var input = toSymEntry(input1, real);
            var res_name: string = st.nextName();
            st.addEntry(res_name, input.size, real);
            var res1 =  st.lookup(res_name);
            var res = toSymEntry(res1,real);
            forall i in 0..input.a.size do {
                if (input.a[i]!=0) then res.a[i]=0;
                else res.a[i]=1;
            }
            var repMsg: string = "created %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        var repMsg: string = "created %s".format(st.attrib(res_name));
        omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

    proc inverseVectorMsgStore(cmd: string, payload: string, st:borrowed SymTab) : MsgTuple throws {
        var list = payload.split(' ');
        var name = list[1];
        var myType = list[0];
        var res_name = list[2];
        if (myType=="int64")
        {
            var input1 =  st.lookup(name);
            var input = toSymEntry(input1, int);
            var res1 =  st.lookup(res_name);
            var res = toSymEntry(res1,int);
            forall i in 0..input.a.size do {
                if (input.a[i]!=0) then res.a[i]=0;
                else res.a[i]=1;
            }
            var repMsg: string = "updated %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        if (myType=="float64")
        {
            var input1 =  st.lookup(name);
            var input = toSymEntry(input1, real);
            var res1 =  st.lookup(res_name);
            var res = toSymEntry(res1,real);
            forall i in 0..input.a.size do {
                if (input.a[i]!=0) then res.a[i]=0;
                else res.a[i]=1;
            }
            var repMsg: string = "updated %s".format(st.attrib(res_name));
            omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
            return new MsgTuple(repMsg, MsgType.NORMAL);
        }
        var repMsg: string = "updated %s".format(st.attrib(res_name));
        omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

    proc vector_times_matrix(st, n, myType1, myType2, input_list_names, v_real) throws {
        var res : [0..n-1] real;
        if (myType1=="int64" && myType2=="int64") then {
            var l_list: [0..n-1] [0..n-1] int;
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,int);
                forall j in 0..n-1 do {
                    l_list[j][i]=l1.a[j];
                }
            }
            forall i in 0..n-1 do {
                res[i] = + reduce (l_list[i] * v_real);
            }
        }
        else if (myType1=="float64" && myType2=="int64") then {
            var l_list: [0..n-1] [0..n-1] int;
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,int);
                forall j in 0..n-1 do {
                    l_list[j][i]=l1.a[j];
                }
            }
            forall i in 0..n-1 do {
                res[i] = + reduce (l_list[i] * v_real);
            }
        }
        else if (myType1=="int64" && myType2=="float64") then {
            var l_list: [0..n-1] [0..n-1] real;
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,real);
                forall j in 0..n-1 do {
                    l_list[j][i]=l1.a[j];
                }
            }
            forall i in 0..n-1 do {
                res[i] = + reduce (l_list[i] * v_real);
            }
        }
        else if (myType1=="float64" && myType2=="float64") then {
            var l_list: [0..n-1] [0..n-1] real;
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,real);
                forall j in 0..n-1 do {
                    l_list[j][i]=l1.a[j];
                }
            }
            forall i in 0..n-1 do {
                res[i] = + reduce (l_list[i] * v_real);
            }
        }
        return res;
    }

    proc matrix_times_vector(st, n, myType1, myType2, input_list_names, v_real) : [0..n-1] real throws {
        var res : [0..n-1] real;
        if (myType1=="int64" && myType2=="int64") then {
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,int);
                res[i] = + reduce (l1.a * v_real);
            }
        }
        else if (myType1=="float64" && myType2=="int64") then {
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,int);
                res[i] = + reduce (l1.a * v_real);
            }
        }
        else if (myType1=="int64" && myType2=="float64") then {
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,int);
                res[i] = + reduce (l1.a * v_real);
            }
        }
        else if (myType1=="float64" && myType2=="float64") then {
            forall i in 0..n-1 do {
                var l =  st.lookup(input_list_names[i]);
                var l1 = toSymEntry(l,int);
                res[i] = + reduce (l1.a * v_real);
            }
        }
        return res;
    }  

    proc inverse(v) {
        var inv: [0..v.size-1] int;
        forall i in 0..v.size-1 do {
            inv[i] = if (v[i]==0) then 1 else 0;
        }
        return inv;
    }

    proc intersection_div(q,v) {
        var k: [0..v.size-1] real;
        forall i in 0..v.size-1 do {
            k[i] = if (v[i]==0) then 0 else q[i]/v[i];
        }
        return k;
    }

    proc betweennessCentralityMsg(cmd: string, payload: string, st: borrowed SymTab) : MsgTuple throws {
        param pn = Reflection.getRoutineName();
        var list_first = payload.split(" ");
        var n : int = list_first[0] : int;
        var source : int = list_first[1] : int;
        var input_names : [0..(n-1)] string;
        var q: [0..n-1] real = 0;
        var p = q;
        q[source] = 1;
        var res_name: string = st.nextName();
        st.addEntry(res_name, n, real);
        var delta1 =  st.lookup(res_name);
        var delta = toSymEntry(delta1,real);
        delta.a=0;
        forall i in 0..n-1 do {
            input_names[i] = list_first[i+2];
        }
        var sigma : [0..(n-1)][0..(n-1)] real;
        var d: int = 0;
        var sum: real = 0;
        while (true) {
            sigma[d] = q;
            p = p + q;
            var s: string;
            q = vector_times_matrix(st, n, "float64","int64",input_names,q);
            writeln("q=",q);
            q = q*inverse(p);
            sum = + reduce q;
            d+=1;
            if (sum==0) then break;
        }
        d-=1;
        while (d>0) do {
            var t1 : [0..n-1] real = 1;
            t1 = t1 + delta.a;
            var t2 = sigma[d];
            t2 = intersection_div(t1,t2);
            var t3 = matrix_times_vector(st, n, "float64","int64",input_names,t2);
            var t4 = sigma[(d - 1)];
            t4 = t4 * t3;
            delta.a = delta.a + t4;
            d-=1;
        }

        var repMsg: string = "created %s".format(st.attrib(res_name));
        omLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

}
