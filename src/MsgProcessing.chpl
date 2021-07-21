
module MsgProcessing
{
    use ServerConfig;

    use Time only;
    use Math only;
    use Reflection;
    use Errors;
    use Logging;
    use Message;
    
    use MultiTypeSymbolTable;
    use MultiTypeSymEntry;
    use ServerErrorStrings;

    use AryUtil;
    
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
    use BlockDist;
    use BitOps;
    use AryUtil;
    use CommAggregation;
    use IO;
    use CPtr;
    use Reflection;
    use Logging;
    use ServerConfig;
    config const RSLSD_vv = false;
    const vv = RSLSD_vv; // these need to be const for comms/performance reasons

    config const RSLSD_numTasks = here.maxTaskPar; // tasks per locale based on locale0
    const numTasks = RSLSD_numTasks; // tasks per locale
    const Tasks = {0..#numTasks}; // these need to be const for comms/performance reasons

    config param RSLSD_bitsPerDigit = 16;
    private param bitsPerDigit = RSLSD_bitsPerDigit; // these need to be const for comms/performance reasons
    private param numBuckets = 1 << bitsPerDigit; // these need to be const for comms/performance reasons
    private param maskDigit = numBuckets-1; // these need to be const for comms/performance reasons
    
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
        coforall a in s.a do
            a = 0;
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

    /**
    *
        Count frequencies
    *
    */

    class MyScanOp: ReduceScanOp {
        type eltType;
        var arg1: int;
        var arg2: int;
        var arg3: int;
        var value = _prod_id(eltType);

        proc identity return _prod_id(eltType);
        proc accumulate(x) {
            var f1: func(int,int,int,int,int) = lambda(a: int, arg1: int, arg2: int, arg3: int) {
                 return (((a - arg1) / arg2) % arg3);
            };
          value *= x;
        }
        proc accumulateOntoState(ref state, x) {
          state *= x;
        }
        proc combine(x) {
          value *= x.value;
        }
        proc generate() return value;
        proc clone() return new unmanaged ProductReduceScanOp(eltType=eltType);
    }

    proc countFrequenciesMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        param pn = Reflection.getRoutineName();
        var repMsg: string; // response message
        var (name1, name2, nStr, listOfArgsString) = payload.splitMsgToTuple(4);
        var n = try! nStr:int;
        var name1E: borrowed GenSymEntry = st.lookup(name1);
        var name2E: borrowed GenSymEntry = st.lookup(name2);
        listOfArgsString = listOfArgsString.strip("[]");
        listOfArgsString = listOfArgsString.replace(",","");
        var split: [1..n] string = listOfArgsString.split(' ');
        var listOfArgs: [1..n] int;
        var k: int = 0;
        for s in split do {
            listOfArgs[k] = try! s:int;
            k = k+1;
        }
        var a = toSymEntry(name1E, int);
        var b = toSymEntry(name2E, int);

        var f1: func(int,int,int,int,int) = lambda(a: int, arg1: int, arg2: int, arg3: int) {
             return (((a - arg1) / arg2) % arg3);
        };
        var f2 = lambda(b: int, arg3: int) {
            return (b + arg3);
        };
        var kr0: [a.aD] (a.etype,int) = [(key,rank) in zip(a.a,a.aD)] (key,rank);
        forall loc in Locales {
                on loc {
                    forall task in Tasks {
                        // bucket domain
                        var bD = {0..#b.a.size};
                        // allocate counts
                        var taskBucketCounts: [bD] int;
                        // get local domain's indices
                        var lD = kr0.localSubdomain();
                        // calc task's indices from local domain's indices
                        var tD = calcBlock(task, lD.low, lD.high);
                        try! rsLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                   "locid: %t task: %t tD: %t".format(loc.id,task,tD));
                        // count digits in this task's part of the array
                        for i in tD {
                            const (key,_) = kr0[i];
                            var bucket = f1(key, listOfArgs[0], listOfArgs[1], listOfArgs[2]); // calc bucket from key
                            taskBucketCounts[bucket] += listOfArgs[3];
                        }
                        // write counts in to global counts in transposed order
                        var aggregator = newDstAggregator(int);
                        for bucket in bD {
                            aggregator.copy(b.a[calcGlobalIndex(bucket, loc.id, task)],
                                                         taskBucketCounts[bucket]);
                    }
                    aggregator.flush();
                }//coforall task
            }//on loc
        }//coforall loc
        repMsg = "updated %s".format(st.attrib(name1));
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

    /**

    * Move records

    **/
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
            // lock.writeEF(true);
            b.a[ret] = f2(b.a[ret], listOfArgs[3]);
            c.a[b.a[ret]] = k;
            // lock.reset();
        }
        a.a.reverse();
        repMsg = "updated %s".format(st.attrib(name1));
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

    proc cumSumMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        param pn = Reflection.getRoutineName();
        var repMsg: string; // response message
        var (name1) = payload.splitMsgToTuple(1);
        var name1E: borrowed GenSymEntry = st.lookup(name1);
        var a = toSymEntry(name1E, int);
        a.a = + scan a.a;
        repMsg = "updated %s".format(name1);
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }
}
