module RandMsg
{
    use ServerConfig;
    
    use Time only;
    use Math only;
    use Reflection;
    use Errors;
    use Logging;
    use Message;
    use RandArray;
    
    use MultiTypeSymbolTable;
    use MultiTypeSymEntry;
    use ServerErrorStrings;

    private config const logLevel = ServerConfig.logLevel;
    const randLogger = new Logger(logLevel);

    /*
    parse, execute, and respond to randint message
    uniform int in half-open interval [min,max)

    :arg reqMsg: message to process (contains cmd,aMin,aMax,len,dtype)
    */
    proc randintMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        param pn = Reflection.getRoutineName();
        var repMsg: string; // response message
        // split request into fields
        var (lenStr,dtypeStr,aMinStr,aMaxStr,seed) = payload.splitMsgToTuple(5);
        var len = lenStr:int;
        var dtype = str2dtype(dtypeStr);

        // get next symbol name
        var rname = st.nextName();
        
        // if verbose print action
        randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
               "cmd: %s len: %i dtype: %s rname: %s aMin: %s: aMax: %s".format(
                                           cmd,len,dtype2str(dtype),rname,aMinStr,aMaxStr)); 
        select (dtype) {
            when (DType.Int64) {
                overMemLimit(8*len);
                var aMin = aMinStr:int;
                var aMax = aMaxStr:int;
                var t1 = Time.getCurrentTime();
                var e = st.addEntry(rname, len, int);
                randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                   "alloc time = %i sec".format(Time.getCurrentTime() - t1));
                
                t1 = Time.getCurrentTime();
                fillInt(e.a, aMin, aMax, seed);
                randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                  "compute time = %i sec".format(Time.getCurrentTime() - t1));
            }
            when (DType.UInt8) {
                overMemLimit(len);
                var aMin = aMinStr:int;
                var aMax = aMaxStr:int;
                var t1 = Time.getCurrentTime();
                var e = st.addEntry(rname, len, uint(8));
                randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                     "alloc time = %i sec".format(Time.getCurrentTime() - t1));
                
                t1 = Time.getCurrentTime();
                fillUInt(e.a, aMin, aMax, seed);
                randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                        "compute time = %i".format(Time.getCurrentTime() - t1));
            }
            when (DType.Float64) {
                overMemLimit(8*len);
                var aMin = aMinStr:real;
                var aMax = aMaxStr:real;
                var t1 = Time.getCurrentTime();
                var e = st.addEntry(rname, len, real);
                randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                         "alloc time = %i sec".format(Time.getCurrentTime() - t1));
                
                t1 = Time.getCurrentTime();
                fillReal(e.a, aMin, aMax, seed);
                randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                          "compute time = %i sec".format(Time.getCurrentTime() - t1));
            }
            when (DType.Bool) {
                overMemLimit(len);
                var t1 = Time.getCurrentTime();
                var e = st.addEntry(rname, len, bool);
                randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                  "alloc time = %i sec".format(Time.getCurrentTime() - t1));
                
                t1 = Time.getCurrentTime();
                fillBool(e.a, seed);
                randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                "compute time = %i sec".format(Time.getCurrentTime() - t1));
            }            
            otherwise {
                var errorMsg = notImplementedError(pn,dtype);
                randLogger.error(getModuleName(),getRoutineName(),getLineNumber(),errorMsg);
                return new MsgTuple(errorMsg, MsgType.ERROR);
            }
        }

        repMsg = "created " + st.attrib(rname);
        randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

    proc randintStoreMsg(cmd: string, payload: string, st: borrowed SymTab) throws
    {
        param pn = Reflection.getRoutineName();
        var repMsg: string; // response message
        // split request into fields
        var (lenStr,dtypeStr,aMinStr,aMaxStr,seed, store) = payload.splitMsgToTuple(6);
        var len = lenStr:int;
        var dtype = str2dtype(dtypeStr);

        // get store name
        var res: borrowed GenSymEntry = st.lookup(store);

        // if verbose print action
        randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
               "cmd: %s len: %i dtype: %s rname: %s aMin: %s: aMax: %s".format(
                                           cmd,len,dtype2str(dtype),store,aMinStr,aMaxStr));

        select (dtype) {
            when (DType.Int64) {
                var aMin = aMinStr:int;
                var aMax = aMaxStr:int;
                var t1 = Time.getCurrentTime();
                var s = toSymEntry(res,int);
                s.hasMin=false;
                s.hasMax=false;
                randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                   "alloc time = %i sec".format(Time.getCurrentTime() - t1));

                t1 = Time.getCurrentTime();
                fillInt(s.a, aMin, aMax, seed);
                randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                  "compute time = %i sec".format(Time.getCurrentTime() - t1));
            }
            when (DType.UInt8) {
                var aMin = aMinStr:int;
                var aMax = aMaxStr:int;
                var t1 = Time.getCurrentTime();
                var s = toSymEntry(res, uint(8));
                s.hasMin=false;
                s.hasMax=false;
                randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                     "alloc time = %i sec".format(Time.getCurrentTime() - t1));

                t1 = Time.getCurrentTime();
                fillUInt(s.a, aMin, aMax, seed);
                randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                        "compute time = %i".format(Time.getCurrentTime() - t1));
            }
            when (DType.Float64) {
                var aMin = aMinStr:real;
                var aMax = aMaxStr:real;
                var t1 = Time.getCurrentTime();
                var s = toSymEntry(res,real);
                s.hasMin=false;
                s.hasMax=false;
                randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                         "alloc time = %i sec".format(Time.getCurrentTime() - t1));

                t1 = Time.getCurrentTime();
                fillReal(s.a, aMin, aMax, seed);
                randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                          "compute time = %i sec".format(Time.getCurrentTime() - t1));
            }
            when (DType.Bool) {
                var t1 = Time.getCurrentTime();
                var s = toSymEntry(res,bool);
                s.hasMin=false;
                s.hasMax=false;
                randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                  "alloc time = %i sec".format(Time.getCurrentTime() - t1));

                t1 = Time.getCurrentTime();
                fillBool(s.a, seed);
                randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),
                                "compute time = %i sec".format(Time.getCurrentTime() - t1));
            }
            otherwise {
                var errorMsg = notImplementedError(pn,dtype);
                randLogger.error(getModuleName(),getRoutineName(),getLineNumber(),errorMsg);
                return new MsgTuple(errorMsg, MsgType.ERROR);
            }
        }

        repMsg = "updated " + st.attrib(store);
        randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }

    proc randomNormalMsg(cmd: string, payload: string, st: borrowed SymTab): MsgTuple throws {
        var pn = Reflection.getRoutineName();
        var (lenStr, seed) = payload.splitMsgToTuple(2);
        var len = lenStr:int;
        // Result + 2 scratch arrays
        overMemLimit(3*8*len);
        var rname = st.nextName();
        var entry = new shared SymEntry(len, real);
        fillNormal(entry.a, seed);
        st.addEntry(rname, entry);

        var repMsg = "created " + st.attrib(rname);
        randLogger.debug(getModuleName(),getRoutineName(),getLineNumber(),repMsg);
        return new MsgTuple(repMsg, MsgType.NORMAL);
    }
}
