import json, os
from typing import cast, Mapping, Optional, Tuple, Union
import warnings, pkg_resources
import zmq # type: ignore
import pyfiglet # type: ignore
from arkouda import security, io_util, __version__
from arkouda.logger import getArkoudaLogger
from arkouda.message import RequestMessage, MessageFormat, ReplyMessage, \
    MessageType
from queue import Queue
import weakref
from collections import defaultdict
from arkouda.dtypes import int64 as akint64, float64 as akfloat64

__all__ = ["connect", "disconnect", "shutdown", "get_config", "get_mem_used", "ruok", "generic_msg", "client_to_server_names"]

# stuff for zmq connection
pspStr = ''
context = zmq.Context()
socket = context.socket(zmq.REQ)
connected = False
# username and token for when basic authentication is enabled
username = ''
token = ''
# verbose flag for arkouda module
verboseDefVal = False
verbose = verboseDefVal
# threshold for __iter__() to limit comms to arkouda_server
pdarrayIterThreshDefVal = 100
pdarrayIterThresh = pdarrayIterThreshDefVal
maxTransferBytesDefVal = 2 ** 30
maxTransferBytes = maxTransferBytesDefVal

logger = getArkoudaLogger(name='Arkouda Client')
clientLogger = getArkoudaLogger(name='Arkouda User Logger', logFormat='%(message)s')

# Print splash message
print('{}'.format(pyfiglet.figlet_format('Arkouda')))
print('Client Version: {}'.format(__version__)) # type: ignore

queue_size: int = 2

q = Queue(queue_size)
client_to_server_names = {}
id_to_args = {}
args_to_id = {}
names_to_number_of_live_references = {}

# Default dictionary so you can access cached pdarrays as
# cache[type of stored value][size of pdarray]
cache = dict()
cache[akint64] = defaultdict(set)
cache[akfloat64] = defaultdict(set)

# reset settings to default values
def set_defaults() -> None:
    """
    Sets client variables including verbose, maxTransferBytes and
    pdarrayIterThresh to default values.

    Returns
    -------
    None
    """
    global verbose, maxTransferBytes, pdarrayIterThresh
    verbose = verboseDefVal
    pdarrayIterThresh = pdarrayIterThreshDefVal
    maxTransferBytes = maxTransferBytesDefVal


# create context, request end of socket, and connect to it
def connect(server: str = "localhost", port: int = 5555, timeout: int = 0,
            access_token: str = None, connect_url=None) -> None:
    """
    Connect to a running arkouda server.

    Parameters
    ----------
    server : str, optional
        The hostname of the server (must be visible to the current
        machine). Defaults to `localhost`.
    port : int, optional
        The port of the server. Defaults to 5555.
    timeout : int, optional
        The timeout in seconds for client send and receive operations.
        Defaults to 0 seconds, whicn is interpreted as no timeout.
    access_token : str, optional
        The token used to connect to an existing socket to enable access to
        an Arkouda server where authentication is enabled. Defaults to None.
    connect_url : str, optional
        The complete url in the format of tcp://server:port?token=<token_value>
        where the token is optional

    Returns
    -------
    None

    Raises
    ------
    ConnectionError
        Raised if there's an error in connecting to the Arkouda server
    ValueError
        Raised if there's an error in parsing the connect_url parameter
    RuntimeError
        Raised if there is a server-side error

    Notes
    -----
    On success, prints the connected address, as seen by the server. If called
    with an existing connection, the socket will be re-initialized.
    """
    global context, socket, pspStr, connected, verbose, username, token

    logger.debug("ZMQ version: {}".format(zmq.zmq_version()))

    if connect_url:
        url_values = _parse_url(connect_url)
        server = url_values[0]
        port = url_values[1]
        if len(url_values) == 3:
            access_token = url_values[2]

    # "protocol://server:port"
    pspStr = "tcp://{}:{}".format(server, port)

    # check to see if tunnelled connection is desired. If so, start tunnel
    tunnel_server = os.getenv('ARKOUDA_TUNNEL_SERVER')
    if tunnel_server:
        (pspStr, _) = _start_tunnel(addr=pspStr, tunnel_server=tunnel_server)

    logger.debug("psp = {}".format(pspStr))

    # create and configure socket for connections to arkouda server
    socket = context.socket(zmq.REQ)  # request end of the zmq connection

    # if timeout is specified, set send and receive timeout params
    if timeout > 0:
        socket.setsockopt(zmq.SNDTIMEO, timeout * 1000)
        socket.setsockopt(zmq.RCVTIMEO, timeout * 1000)

    # set token and username global variables
    username = security.get_username()
    token = cast(str, _set_access_token(access_token=access_token,
                                        connect_string=pspStr))

    # connect to arkouda server
    try:
        socket.connect(pspStr)
    except Exception as e:
        raise ConnectionError(e)

    # send the connect message
    cmd = "connect"
    logger.debug("[Python] Sending request: {}".format(cmd))

    # send connect request to server and get the response confirming if
    # the connect request succeeded and, if not not, the error message
    return_message = _send_string_message(cmd=cmd)
    logger.debug("[Python] Received response: {}".format(str(return_message)))
    connected = True

    conf = get_config()
    if conf['arkoudaVersion'] != __version__:
        warnings.warn(('Version mismatch between client ({}) and server ({}); ' +
                       'this may cause some commands to fail or behave ' +
                       'incorrectly! Updating arkouda is strongly recommended.'). \
                      format(__version__, conf['arkoudaVersion']), RuntimeWarning)
    clientLogger.info(return_message)


def _parse_url(url: str) -> Tuple[str, int, Optional[str]]:
    """
    Parses the url in the following format if authentication enabled:

    tcp://<hostname/url>:<port>?token=<token>

    If authentication is not enabled, the url is expected to be in the format:

    tcp://<hostname/url>:<port>

    Parameters
    ----------
    url : str
        The url string

    Returns
    -------
    Tuple[str,int,Optional[str]]
        A tuple containing the host, port, and token, the latter of which is None
        if authentication is not enabled for the Arkouda server being accessed

    Raises
    ------
    ValueError
        if the url does not match one of the above formats, if the port is not an
        integer, or if there's a general string parse error raised in the parsing
        of the url parameter
    """
    try:
        # split on tcp:// and if missing or malformmed, raise ValueError
        no_protocol_stub = url.split('tcp://')
        if len(no_protocol_stub) < 2:
            raise ValueError(('url must be in form tcp://<hostname/url>:<port>' +
                              ' or tcp://<hostname/url>:<port>?token=<token>'))

        # split on : to separate host from port or port?token=<token>
        host_stub = no_protocol_stub[1].split(':')
        if len(host_stub) < 2:
            raise ValueError(('url must be in form tcp://<hostname/url>:<port>' +
                              ' or tcp://<hostname/url>:<port>?token=<token>'))
        host = host_stub[0]
        port_stub = host_stub[1]

        if '?token=' in port_stub:
            port_token_stub = port_stub.split('?token=')
            return (host, int(port_token_stub[0]), port_token_stub[1])
        else:
            return (host, int(port_stub), None)
    except Exception as e:
        raise ValueError(e)


def _set_access_token(access_token: Optional[str],
                      connect_string: str = 'localhost:5555') -> Optional[str]:
    """
    Sets the access_token for the connect request by doing the following:

    1. retrieves the token configured for the connect_string from the
       .arkouda/tokens.txt file, if any
    2. if access_token is None, returns the retrieved token
    3. if access_token is not None, replaces retrieved token with the access_token
       to account for situations where the token can change for a url (for example,
       the arkouda_server is restarted and a corresponding new token is generated).

    Parameters
    ----------
    username : str
        The username retrieved from the user's home directory
    access_token : str
        The access_token supplied by the user, which is required if authentication
        is enabled, defaults to None
    connect_string : str
        The arkouda_server host:port connect string, defaults to localhost:5555

    Returns
    -------my_pdarray
    str
        The access token configured for the host:port, None if there is no
        token configured for the host:port

    Raises
    ------
    IOError
        If there's an error writing host:port -> access_token mapping to
        the user's tokens.txt file or retrieving the user's tokens
    """
    path = '{}/tokens.txt'.format(security.get_arkouda_client_directory())
    try:
        tokens = io_util.delimited_file_to_dict(path)
    except Exception as e:
        raise IOError(e)

    if cast(str, access_token) and cast(str, access_token) not in {'', 'None'}:
        saved_token = tokens.get(connect_string)
        if saved_token is None or saved_token != access_token:
            tokens[connect_string] = cast(str, access_token)
            try:
                io_util.dict_to_delimited_file(values=tokens, path=path,
                                               delimiter=',')
            except Exception as e:
                raise IOError(e)
        return access_token
    else:
        try:
            tokens = io_util.delimited_file_to_dict(path)
        except Exception as e:
            raise IOError(e)
        return tokens.get(connect_string)


def _start_tunnel(addr: str, tunnel_server: str) -> Tuple[str, object]:
    """
    Starts ssh tunnel

    Parameters
    ----------
    tunnel_server : str
        The ssh server url

    Returns
    -------
    str
        The new tunneled-version of connect string
    object
        The ssh tunnel object

    Raises
    ------
    ConnectionError
        If the ssh tunnel could not be created given the tunnel_server
        url and credentials (either password or key file)
    """
    from zmq import ssh
    kwargs = {'addr': addr,
              'server': tunnel_server}
    keyfile = os.getenv('ARKOUDA_KEY_FILE')
    password = os.getenv('ARKOUDA_PASSWORD')

    if keyfile:
        kwargs['keyfile'] = keyfile
    if password:
        kwargs['password'] = password

    try:
        return ssh.tunnel.open_tunnel(**kwargs)
    except Exception as e:
        raise ConnectionError(e)


def _send_string_message(cmd: str, recv_bytes: bool = False,
                         args: str = None) -> Union[str, bytes]:
    """
    Generates a RequestMessage encapsulating command and requesting
    user information, sends it to the Arkouda server, and returns
    either a string or binary depending upon the message format.

    Parameters
    ----------
    cmd : str
        The name of the command to be executed by the Arkouda server
    recv_bytes : bool, defaults to False
        A boolean indicating whether the return message will be in bytes
        as opposed to a string
    args : str
        A delimited string containing 1..n command arguments

    Returns
    -------
    Union[str,bytes]
        The response string or byte array sent back from the Arkouda server

    Raises
    ------
    RuntimeError
        Raised if the return message contains the word "Error", indicating
        a server-side error was thrown
    ValueError
        Raised if the return message is malformed JSON or is missing 1..n
        expected fields
    """
    message = RequestMessage(user=username, token=token, cmd=cmd,
                             format=MessageFormat.STRING, args=cast(str, args))

    logger.debug('sending message {}'.format(message))

    socket.send_string(json.dumps(message.asdict()))

    if recv_bytes:
        return_message = socket.recv()

        # raise errors or warnings sent back from the server
        if return_message.startswith(b"Error:"):
            raise RuntimeError(return_message.decode())
        elif return_message.startswith(b"Warning:"):
            warnings.warn(return_message.decode())
        return return_message
    else:
        raw_message = socket.recv_string()
        try:
            return_message = ReplyMessage.fromdict(json.loads(raw_message))

            # raise errors or warnings sent back from the server
            if return_message.msgType == MessageType.ERROR:
                raise RuntimeError(return_message.msg)
            elif return_message.msgType == MessageType.WARNING:
                warnings.warn(return_message.msg)
            return return_message.msg
        except KeyError as ke:
            raise ValueError('Return message is missing the {} field'.format(ke))
        except json.decoder.JSONDecodeError:
            raise ValueError('Return message is not valid JSON: {}'. \
                             format(raw_message))


def _send_binary_message(cmd: str, payload: bytes, recv_bytes: bool = False,
                         args: str = None) -> Union[str, bytes]:
    """
    Generates a RequestMessage encapsulating command and requesting user information,
    information prepends the binary payload, sends the binary request to the Arkouda
    server, and returns either a string or binary depending upon the message format.

    Parameters
    ----------
    cmd : str
        The name of the command to be executed by the Arkouda server
    payload : bytes
        The bytes to be converted to a pdarray, Strings, or Categorical object
        on the Arkouda server
    recv_bytes : bool, defaults to False
        A boolean indicating whether the return message will be in bytes
        as opposed to a string
    args : str
        A delimited string containing 1..n command arguments

    Returns
    -------
    Union[str,bytes]
        The response string or byte array sent back from the Arkouda server

    Raises
    ------
    RuntimeError
        Raised if the return message contains the word "Error", indicating
        a server-side error was thrown
    ValueError
        Raised if the return message is malformed JSON or is missing 1..n
        expected fields
    """
    message = RequestMessage(user=username, token=token, cmd=cmd,
                             format=MessageFormat.BINARY, args=cast(str, args))

    logger.debug('sending message {}'.format(message))

    socket.send('{}BINARY_PAYLOAD'. \
                format(json.dumps(message.asdict())).encode() + payload)

    if recv_bytes:
        binary_return_message = cast(bytes, socket.recv())
        # raise errors or warnings sent back from the server
        if binary_return_message.startswith(b"Error:"): \
                raise RuntimeError(binary_return_message.decode())
        elif binary_return_message.startswith(b"Warning:"): \
                warnings.warn(binary_return_message.decode())
        return binary_return_message
    else:
        raw_message = socket.recv_string()
        try:
            return_message = ReplyMessage.fromdict(json.loads(raw_message))

            # raise errors or warnings sent back from the server
            if return_message.msgType == MessageType.ERROR:
                raise RuntimeError(return_message.msg)
            elif return_message.msgType == MessageType.WARNING:
                warnings.warn(return_message.msg)
            return return_message.msg
        except KeyError as ke:
            raise ValueError('Return message is missing the {} field'.format(ke))
        except json.decoder.JSONDecodeError:
            raise ValueError('{} is not valid JSON, may be server-side error'. \
                             format(raw_message))


# message arkouda server the client is disconnecting from the server
def disconnect() -> None:
    """
    Disconnects the client from the Arkouda server

    Returns
    -------
    None

    Raises
    ------
    ConnectionError
        Raised if there's an error disconnecting from the Arkouda server
    """
    global socket, pspStr, connected, verbose, token

    if connected:
        # send disconnect message to server
        message = "disconnect"
        logger.debug("[Python] Sending request: {}".format(message))
        return_message = cast(str, _send_string_message(message))
        logger.debug("[Python] Received response: {}".format(return_message))
        try:
            socket.disconnect(pspStr)
        except Exception as e:
            raise ConnectionError(e)
        connected = False
        clientLogger.info(return_message)
    else:
        clientLogger.info("not connected; cannot disconnect")


def shutdown() -> None:
    """
    Sends a shutdown message to the Arkouda server that does the
    following:

    1. Delete all objects in the SymTable
    2. Shuts down the Arkouda server
    3. Disconnects the client from the stopped Arkouda Server

    Returns
    -------
    None

    Raises
    ------
    RuntimeError
        Raised if the client is not connected to the Arkouda server or
        there is an error in disconnecting from the server
    """
    global socket, pspStr, id_to_args, connected, verbose

    if not connected:
        raise RuntimeError('not connected, cannot shutdown server')
    # send shutdown message to server
    message = "shutdown"

    logger.debug("[Python] Sending request: {}".format(message))
    return_message = cast(str, _send_string_message(message))
    logger.debug("[Python] Received response: {}".format(return_message))

    try:
        socket.disconnect(pspStr)
    except Exception as e:
        raise RuntimeError(e)
    connected = False

maxNumServerVariables = 0

def generic_msg(cmd: str, args: Union[str, bytes] = None, send_bytes: bool = False,
                recv_bytes: bool = False, return_value_needed: bool = False,
                create_pdarray: bool = False, buff_emptying: bool = False, arr_id: str = None, my_pdarray = None) -> Union[str, bytes]:
    """
    Sends a binary or string message composed of a command and corresponding 
    arguments to the arkouda_server, returning the response sent by the server.

    Parameters
    ----------
    cmd : str
        The server-side command to be executed
    args : Union[str,bytes]
        A space-delimited list of command arguments or a byte array, the latter
        of which is for creating an Arkouda array
    send_bytes : bool
        Indicates if the message to be sent is binary, defaults to False
    recv_bytes : bool
        Indicates if the return message will be binary, default to False

    Returns
    -------
    Union[str, bytes]
        The string or binary return message

    Raises
    ------
    KeyboardInterrupt
        Raised if the user interrupts during command execution
    RuntimeError
        Raised if the client is not connected to the server or if
        there is a server-side error thrown
        
    Notes
    -----
    If the server response is a string, the string corresponds to a success  
    confirmation, warn message, or error message. A response of type bytes 
    corresponds to an Arkouda array output as a numpy array.
    """
    global socket, pspStr, connected, verbose

    if not connected:
        raise RuntimeError("client is not connected to a server")

    if send_bytes:
        buff_item = BufferItem(cmd=cmd,
                               args=cast(bytes, args),
                               send_bytes=send_bytes,
                               recv_bytes=recv_bytes,
                               create_pdarray=create_pdarray,
                               pdarray_id=arr_id,
                               my_pd_array=my_pdarray)
    else:
        buff_item = BufferItem(cmd=cmd,
                                args=cast(str, args),
                                send_bytes=send_bytes,
                                recv_bytes=recv_bytes,
                                create_pdarray=create_pdarray,
                                pdarray_id=arr_id,
                                my_pd_array=my_pdarray)

    if return_value_needed and not buff_emptying:
        ret = buff_push(buff_item)
        if (q.empty()):
            return ret
        return execute_with_dependencies(buff_item)

    # print("----MAP----")
    # for (key, value) in client_to_server_names.items():
    #    print("key=", key, "value=", value)

    if buff_emptying or return_value_needed:
        try:
            # Transform the args with client to server names
            args = transform_args(args)
            # print("cmd=", cmd)
            # print("args=",args)
            # print(client_to_server_names)
            # Send the message
            if send_bytes:
                repMsg = _send_binary_message(cmd=cmd,
                                              payload=cast(bytes, args),
                                              recv_bytes=recv_bytes)
            else:
                repMsg = _send_string_message(cmd=cmd,
                                              args=cast(str, args),
                                              recv_bytes=recv_bytes)

            if create_pdarray:
                fields = repMsg.split()
                name = fields[1]
                mydtype = fields[2]
                size = int(fields[3])
                ndim = int(fields[4])
                shape = [int(el) for el in fields[5][1:-1].split(',')]
                itemsize = int(fields[6])
                logger.debug(("created Chapel array with name: {} dtype: {} size: {} ndim: {} shape: {} " +
                              "itemsize: {}").format(name, mydtype, size, ndim, shape, itemsize))
                client_to_server_names[arr_id] = name
                num = int(name[3:])
                global maxNumServerVariables
                if (num > maxNumServerVariables):
                    maxNumServerVariables = num
                    #print("max_num=", maxNumServerVariables)
            # print("repmsg=",repMsg)
            return repMsg

        except KeyboardInterrupt as e:
            # if the user interrupts during command execution, the socket gets out
            # of sync reset the socket before raising the interrupt exception
            socket = context.socket(zmq.REQ)
            socket.connect(pspStr)
            raise e
    else:
        buff_push(buff_item)
    return


def get_config() -> Mapping[str, Union[str, int, float]]:
    """
    Get runtime information about the server.

    Returns
    -------
    Mapping[str, Union[str, int, float]]
        serverHostname
        serverPort
        numLocales
        numPUs (number of processor units per locale)
        maxTaskPar (maximum number of tasks per locale)
        physicalMemory

    Raises
    ------
    RuntimeError
        Raised if there is a server-side error in getting memory used
    ValueError
        Raised if there's an error in parsing the JSON-formatted server config
    """
    try:
        raw_message = cast(str, generic_msg(cmd="getconfig", return_value_needed=True))
        return json.loads(raw_message)
    except json.decoder.JSONDecodeError:
        raise ValueError('Returned config is not valid JSON: {}'.format(raw_message))
    except Exception as e:
        raise RuntimeError('{} in retrieving Arkouda server config'.format(e))


def get_mem_used() -> int:
    """
    Compute the amount of memory used by objects in the server's symbol table.

    Returns
    -------
    int
        Indicates the amount of memory allocated to symbol table objects.

    Raises
    ------
    RuntimeError
        Raised if there is a server-side error in getting memory used
    ValueError
        Raised if the returned value is not an int-formatted string
    """
    mem_used_message = cast(str, generic_msg(cmd="getmemused"))
    return int(mem_used_message)


def _no_op() -> str:
    """
    Send a no-op message just to gather round trip time

    Returns
    -------
    str
        The noop command result

    Raises
    ------
    RuntimeError
        Raised if there is a server-side error in executing noop request
    """
    return cast(str,generic_msg(cmd="noop"))
  
def ruok() -> str:
    """
    Simply sends an "ruok" message to the server and, if the return message is
    "imok", this means the arkouda_server is up and operating normally. A return
    message of "imnotok" indicates an error occurred or the connection timed out.

    This method is basically a way to do a quick healthcheck in a way that does
    not require error handling.

    Returns
    -------
    str
        A string indicating if the server is operating normally (imok), if there's
        an error server-side, or if ruok did not return a response (imnotok) in
        both of the latter cases
    """
    try:
        res = cast(str,generic_msg(cmd='ruok'))
        if res == 'imok':
            return 'imok'
        else:
            return 'imnotok because: {}'.format(res)
    except Exception as e:
        return 'ruok did not return response: {}'.format(str(e))


class BufferItem:
    def __init__(self, cmd: str, args: Union[str, bytes] = None, send_bytes: bool = False,
                 recv_bytes: bool = False, create_pdarray: bool = False, pdarray_id: str = None, executed: bool = False, my_pd_array = None):
        self.cmd = cmd
        self.args = args
        self.send_bytes = send_bytes
        self.recv_bytes = recv_bytes
        self.create_pdarray = create_pdarray
        self.pdarray_id = pdarray_id
        self.dependencies = []
        self.executed = executed
        self.my_pd_array = []


    def __str__(self):
        return "Buffer Item, Cmd={0}, Args={1}, Pdarray_id={2}".format(self.cmd, self.args, self.pdarray_id)

    def execute(self):
        # print("executing",self)
        self.executed = True
        retMsg = generic_msg(self.cmd, self.args, self.send_bytes, self.recv_bytes,
                    return_value_needed=True, create_pdarray=self.create_pdarray,
                    buff_emptying=True, arr_id=self.pdarray_id)
        for info in self.my_pd_array:
            ret = delete_from_args_map(info[0])
            if ret:
                cache_array(info[0], info[1], info[2])
        return retMsg

def buff_push(item: BufferItem):
    #item.args=transform_args(item.args)
    q.put(item)
    make_dependencies(item)
    if q.full():
        return buff_empty_partial(q.maxsize - 1)
    return None

def is_temporary(arg: str):
    if (arg[:2]=="id"):
        return True
    else:
        return False

def make_dependencies(item: BufferItem):
    # print('starting length of dependencies:',len(item.dependencies))
    if item.args is None:
        return
    args_list = item.args.split(" ")
    # args_list = args_list[1:]
    args_list = list(filter(is_temporary, args_list))
    # print("args_list=",args_list)
    for q_elem in reversed(list(q.queue)):
        if (q_elem is item):
            continue
        args_list_q_elem=list()
        for arg in q_elem.args.split(" "):
            args_list_q_elem.append(arg)
        if (q_elem.pdarray_id!=None):
            args_list_q_elem.append(q_elem.pdarray_id)
        # args_list_q_elem=list(filter(is_temporary, args_list_q_elem))
        for arg_q_elem in args_list_q_elem:
            if arg_q_elem in args_list and not(q_elem in item.dependencies):
                item.dependencies.append(weakref.ref(q_elem))

def remove_from_queue(item: BufferItem):
    top = None
    helper_queue = Queue(queue_size)
    while (not q.empty() and top!=item):
        top=q.get()
        if (top!=item):
            helper_queue.put(top)
    while (not helper_queue.empty()):
        q.put(helper_queue.get())

def transform_args(args: str):
    if (args==None):
        return None
    args_list = list()
    args_list = args.split(" ")
    for i in range(len(args_list)):
        if (args_list[i] in client_to_server_names.keys()):
            args_list[i]=client_to_server_names[args_list[i]]
    s=""
    for i in range(len(args_list)):
        if (i<len(args_list)-1):
            s+=args_list[i]+" "
        else:
            s+=args_list[i]
    return s

def execute_with_dependencies(item: BufferItem):
    #print("[Executing with depedencies]:",item)
    if (item.executed):
        return
    for dependency in item.dependencies:
        if (dependency() is not None):
            execute_with_dependencies(dependency())
    remove_from_queue(item)
    return item.execute()


def buff_empty():
    q.get().execute()
    if not q.empty():
        buff_empty()
        # print("New Queue Size is:", q.qsize())


def buff_empty_partial(size):
    while q.qsize() > size:
        return q.get().execute()

def find_last(arr):
    for q_elem in reversed(list(q.queue)):
        if (arr.name in q_elem.args.split(" ")):
            q_elem.my_pd_array.append((arr.name, arr.dtype, arr.size))
            if (arr.name not in names_to_number_of_live_references.keys()):
                names_to_number_of_live_references[arr.name] = 1
            else:
                names_to_number_of_live_references[arr.name] = names_to_number_of_live_references[arr.name] + 1
            return True
    return False

def delete_from_args_map(arrName: str):
    # try:
    logger.debug('deleting pdarray with name {}'.format(arrName))
    # delete all things that are in same arguments
    if (arrName in names_to_number_of_live_references.keys()):
        names_to_number_of_live_references[arrName] = names_to_number_of_live_references[arrName] - 1
    if (arrName in names_to_number_of_live_references.keys() and names_to_number_of_live_references[arrName]!=0):
        return False
    all_deletions = []
    keys = []
    for key in args_to_id.keys():
        bris = key.split(":")
        if (arrName in bris):
            if (args_to_id[key]() is not None):
                all_deletions.append(args_to_id[key]())
                keys.append(key)

    # keys = []

    if (arrName in id_to_args.keys()):
        for nes in id_to_args[arrName]:
            if nes in args_to_id.keys():
                del args_to_id[nes]
        del id_to_args[arrName]

    for dels in all_deletions:
        for key in args_to_id.keys():
            bris = key.split(":")
            if (dels.name in bris):
                all_deletions.append(args_to_id[key]())
                keys.append(key)

    for key in keys:
        if (key in args_to_id.keys()):
            #pass
            del args_to_id[key]
    return True
    

def cache_array(arrName: str, arrType, arrSize):
    # print("name=",arr.name)
    if arrName not in client_to_server_names.keys():
        return;
    #print("----MAP----")
    #for (key, value) in client_to_server_names.items():
    #    print("key=", key, "value=", value)
    # print("Caching ", client_to_server_names[arr.name], arr.size    )
    cache[arrType][arrSize].add(client_to_server_names[arrName])
    print('caching',client_to_server_names[arrName])
    client_to_server_names.pop(arrName)