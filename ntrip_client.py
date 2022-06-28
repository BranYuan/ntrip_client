import sys
import socket
import threading
import logging
import collections
import time
import select
import base64
from pyrtcm import RTCMReader

try:
    # Use monotonic clock if available
    time_func = time.monotonic
except AttributeError:
    time_func = time.time

if sys.version_info[0] >= 3:
    # define some alias for python2 compatibility
    unicode = str
    basestring = str

# Message types
CONNECT = 0x10
CONNACK = 0x20
PUBLISH = 0x30
DISCONNECT = 0xE0

# Log levels
NTRIP_LOG_INFO = 0x01
NTRIP_LOG_NOTICE = 0x02
NTRIP_LOG_WARNING = 0x04
NTRIP_LOG_ERR = 0x08
NTRIP_LOG_DEBUG = 0x10
LOGGING_LEVEL = {
    NTRIP_LOG_DEBUG: logging.DEBUG,
    NTRIP_LOG_INFO: logging.INFO,
    NTRIP_LOG_NOTICE: logging.INFO,  # This has no direct equivalent level
    NTRIP_LOG_WARNING: logging.WARNING,
    NTRIP_LOG_ERR: logging.ERROR,
}


# Error values
NTRIP_ERR_AGAIN = -1
NTRIP_ERR_SUCCESS = 0
NTRIP_ERR_INVAL = 3
NTRIP_ERR_NO_CONN = 4
NTRIP_ERR_CONN_REFUSED = 5
NTRIP_ERR_CONN_LOST = 7
NTRIP_ERR_UNKNOWN = 13
NTRIP_ERR_KEEPALIVE = 16


# Connection state
ntrip_cs_new = 0
ntrip_cs_connected = 1
ntrip_cs_disconnecting = 2
ntrip_cs_connect_async = 3

# Message state
ntrip_ms_queued = 9


sockpair_data = b"0"


def _socketpair_compat():
    """TCP/IP socketpair including Windows support"""
    listensock = socket.socket(
        socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_IP)
    listensock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    listensock.bind(("127.0.0.1", 0))
    listensock.listen(1)

    iface, port = listensock.getsockname()
    sock1 = socket.socket(
        socket.AF_INET, socket.SOCK_STREAM, socket.IPPROTO_IP)
    sock1.setblocking(0)
    try:
        sock1.connect(("127.0.0.1", port))
    except BlockingIOError:
        pass
    sock2, address = listensock.accept()
    sock2.setblocking(0)
    listensock.close()
    return (sock1, sock2)


class Client:

    def __init__(self, reconnect_on_failure=True):

        self._sock = None
        self._rtcm_reader = None
        self._sockpairR, self._sockpairW = (None, None,)
        self._keepalive = 60
        self._connect_timeout = 5.0

        self._username = ''
        self._password = ''
        self._in_packet = {
            "command": 0,
            "packet": bytearray(b""),
            "to_process": 0,
            "pos": 0}
        self._out_packet = collections.deque()
        self._last_msg_in = time_func()
        self._last_msg_out = time_func()
        self._reconnect_min_delay = 1
        self._reconnect_max_delay = 120
        self._reconnect_delay = None
        self._reconnect_on_failure = reconnect_on_failure
        self._last_mid = 0
        self._state = ntrip_cs_new
        self._out_messages = collections.OrderedDict()
        self._in_messages = collections.OrderedDict()
        self._inflight_messages = 0

        self._host = ""
        self._port = 9993
        self._bind_address = ""
        self._bind_port = 0
        self._in_callback_mutex = threading.Lock()
        self._callback_mutex = threading.RLock()
        self._msgtime_mutex = threading.Lock()
        self._out_message_mutex = threading.RLock()
        self._in_message_mutex = threading.Lock()
        self._reconnect_delay_mutex = threading.Lock()
        self._mid_generate_mutex = threading.Lock()
        self._thread = None
        self._thread_terminate = False

        self._logger = None
        self._registered_write = False
        # No default callbacks
        self._on_log = None
        self._on_connect = None
        self._on_connect_fail = None
        self._on_message = None
        self._on_disconnect = None
        self._on_socket_open = None
        self._on_socket_close = None
        self._on_socket_register_write = None
        self._on_socket_unregister_write = None
        self.suppress_exceptions = False  # For callbacks

    def __del__(self):
        self._reset_sockets()

    def _sock_send(self, buf):
        '''
        通过socket来发送数据
        :param buf:
        :return:
        '''
        try:
            return self._sock.send(buf)
        except BlockingIOError:
            self._call_socket_register_write()
            raise BlockingIOError

    def _sock_recv(self, bufsize):
        '''
        通过socket来接收指定字符串的数据
        :param bufsize:
        :return:
        '''
        try:
            return self._sock.recv(bufsize)
        except Exception:
            raise BlockingIOError

    def _sock_recv_rtcm(self):
        '''
        通过socket来接收指定字符串的数据
        :param bufsize:
        :return:
        '''
        try:
            return self._rtcm_reader.read()
        except Exception:
            raise BlockingIOError

    def _sock_close(self):
        '''
        关闭socket连接
        :return:
        '''
        if not self._sock:
            return

        try:
            sock = self._sock
            self._sock = None
            self._call_socket_unregister_write(sock)
            self._call_socket_close(sock)
        finally:
            # In case a callback fails, still close the socket to avoid leaking the file descriptor.
            sock.close()

    def _reset_sockets(self, sockpair_only=False):
        '''
        释放所有读写sockets
        :param sockpair_only:
        :return:
        '''
        if sockpair_only == False:
            self._sock_close()

        if self._sockpairR:
            self._sockpairR.close()
            self._sockpairR = None
        if self._sockpairW:
            self._sockpairW.close()
            self._sockpairW = None


    # ============================================================
    # callback functions
    # ============================================================
    @property
    def on_log(self):
        '''
        日志回调
        :return:
        '''
        return self._on_log

    @on_log.setter
    def on_log(self, func):
        self._on_log = func

    def log_callback(self):
        def decorator(func):
            self.on_log = func
            return func

        return decorator

    @property
    def on_connect(self):
        """If implemented, called when the broker responds to our connection
        request."""
        return self._on_connect

    @on_connect.setter
    def on_connect(self, func):
        with self._callback_mutex:
            self._on_connect = func

    def connect_callback(self):
        def decorator(func):
            self.on_connect = func
            return func

        return decorator

    @property
    def on_connect_fail(self):
        return self._on_connect_fail

    @property
    def on_connect_fail(self):
        return self._on_connect_fail

    @on_connect_fail.setter
    def on_connect_fail(self, func):
        with self._callback_mutex:
            self._on_connect_fail = func

    def connect_fail_callback(self):
        def decorator(func):
            self.on_connect_fail = func
            return func
        return decorator

    @property
    def on_disconnect(self):
        '''
        断开连接
        :return:
        '''
        return self._on_disconnect

    @on_disconnect.setter
    def on_disconnect(self, func):
        with self._callback_mutex:
            self._on_disconnect = func

    def disconnect_callback(self):
        def decorator(func):
            self.on_disconnect = func
            return func

        return decorator

    @property
    def on_socket_open(self):
        return self._on_socket_open

    @on_socket_open.setter
    def on_socket_open(self, func):
        with self._callback_mutex:
            self._on_socket_open = func

    def socket_open_callback(self):
        def decorator(func):
            self.on_socket_open = func
            return func
        return decorator

    def _call_socket_open(self):
        """Call the socket_open callback with the just-opened socket"""
        with self._callback_mutex:
            on_socket_open = self.on_socket_open

        if on_socket_open:
            with self._in_callback_mutex:
                try:
                    on_socket_open(self, self._username, self._sock)
                except Exception as err:
                    self._easy_log(
                        NTRIP_LOG_ERR, 'Caught exception in on_socket_open: %s', err)
                    if not self.suppress_exceptions:
                        raise


    @property
    def on_socket_close(self):
        """If implemented, called just before the socket is closed."""
        return self._on_socket_close

    @on_socket_close.setter
    def on_socket_close(self, func):
        with self._callback_mutex:
            self._on_socket_close = func

    def socket_close_callback(self):
        def decorator(func):
            self.on_socket_close = func
            return func

        return decorator

    def _call_socket_close(self, sock):
        """Call the socket_close callback with the about-to-be-closed socket"""
        with self._callback_mutex:
            on_socket_close = self.on_socket_close

        if on_socket_close:
            with self._in_callback_mutex:
                try:
                    on_socket_close(self, self._username, sock)
                except Exception as err:
                    self._easy_log(
                        NTRIP_LOG_ERR, 'Caught exception in on_socket_close: %s', err)
                    if not self.suppress_exceptions:
                        raise


    @property
    def on_socket_register_write(self):
        """If implemented, called when the socket needs writing but can't."""
        return self._on_socket_register_write

    @on_socket_register_write.setter
    def on_socket_register_write(self, func):
        with self._callback_mutex:
            self._on_socket_register_write = func

    def socket_register_write_callback(self):
        def decorator(func):
            self._on_socket_register_write = func
            return func

        return decorator

    def _call_socket_register_write(self):
        """Call the socket_register_write callback with the unwritable socket"""
        if not self._sock or self._registered_write:
            return
        self._registered_write = True
        with self._callback_mutex:
            on_socket_register_write = self.on_socket_register_write

        if on_socket_register_write:
            try:
                on_socket_register_write(
                    self, self._username, self._sock)
            except Exception as err:
                self._easy_log(
                    NTRIP_LOG_ERR, 'Caught exception in on_socket_register_write: %s', err)
                if not self.suppress_exceptions:
                    raise


    @property
    def on_socket_unregister_write(self):
        """If implemented, called when the socket doesn't need writing anymore."""
        return self._on_socket_unregister_write

    @on_socket_unregister_write.setter
    def on_socket_unregister_write(self, func):

        with self._callback_mutex:
            self._on_socket_unregister_write = func

    def socket_unregister_write_callback(self):
        def decorator(func):
            self._on_socket_unregister_write = func
            return func

        return decorator

    def _call_socket_unregister_write(self, sock=None):
        """Call the socket_unregister_write callback with the writable socket"""
        sock = sock or self._sock
        if not sock or not self._registered_write:
            return
        self._registered_write = False

        with self._callback_mutex:
            on_socket_unregister_write = self.on_socket_unregister_write

        if on_socket_unregister_write:
            try:
                on_socket_unregister_write(self, self._username, sock)
            except Exception as err:
                self._easy_log(
                    NTRIP_LOG_ERR, 'Caught exception in on_socket_unregister_write: %s', err)
                if not self.suppress_exceptions:
                    raise


    @property
    def on_message(self):
        return self._on_message

    @on_message.setter
    def on_message(self, func):
        with self._callback_mutex:
            self._on_message = func

    def message_callback(self):
        def decorator(func):
            self.on_message = func
            return func

        return decorator

    # ============================================================
    # users functions
    # ============================================================
    def reinitialise(self, client_id="", clean_session=True, userdata=None):
        self._reset_sockets()
        self.__init__(client_id, clean_session, userdata)

    def enable_logger(self, level=NTRIP_LOG_DEBUG, logger=None):
        """ Enables a logger to send log messages to """
        if logger is None:
            if self._logger is not None:
                # Do not replace existing logger
                return
            logger = logging.getLogger(__name__)
            logger.setLevel(level)
        self._logger = logger

    def disable_logger(self):
        self._logger = None

    # ============================================================
    # connect functions
    # ============================================================
    def connect(self, host, port=9993, mount_point='', keepalive=60, bind_address="", bind_port=0):
        self.connect_async(host, port, mount_point, keepalive, bind_address, bind_port)
        return self.reconnect()

    def connect_async(self, host, port=9993, mount_point='', keepalive=60, bind_address="", bind_port=0,
                       ):
        if host is None or len(host) == 0:
            raise ValueError('Invalid host.')
        if port <= 0:
            raise ValueError('Invalid port number.')
        if keepalive < 0:
            raise ValueError('Keepalive must be >=0.')
        if bind_address != "" and bind_address is not None:
            if sys.version_info < (2, 7) or (3, 0) < sys.version_info < (3, 2):
                raise ValueError('bind_address requires Python 2.7 or 3.2.')
        if bind_port < 0:
            raise ValueError('Invalid bind port number.')

        self._host = host
        self._port = port
        self._mout_point = mount_point
        self._keepalive = keepalive
        self._bind_address = bind_address
        self._bind_port = bind_port
        self._state = ntrip_cs_connect_async

    def reconnect(self):
        """Reconnect the client after a disconnect. Can only be called after
        connect()/connect_async()."""
        if len(self._host) == 0:
            raise ValueError('Invalid host.')
        if self._port <= 0:
            raise ValueError('Invalid port number.')

        self._in_packet = {
            "command": 0,
            "packet": bytearray(b""),
            "to_process": 0,
            "pos": 0
        }

        self._out_packet = collections.deque()

        with self._msgtime_mutex:
            self._last_msg_in = time_func()
            self._last_msg_out = time_func()
        self._state = ntrip_cs_new
        self._sock_close()

        # Put messages in progress in a valid state.
        self._messages_reconnect_reset()

        sock = self._create_socket_connection()
        self._sock = sock
        self._rtcm_reader = RTCMReader(self._sock)
        self._sock.setblocking(0)
        self._registered_write = False
        self._call_socket_open()
        return self._send_connect(self._keepalive)

    def _create_socket_connection(self):
        addr = (self._host, self._port)
        source = (self._bind_address, self._bind_port)
        if sys.version_info < (2, 7) or (3, 0) < sys.version_info < (3, 2):
            # Have to short-circuit here because of unsupported source_address
            # param in earlier Python versions.
            return socket.create_connection(addr, timeout=self._connect_timeout)

        return socket.create_connection(addr, timeout=self._connect_timeout, source_address=source)

    def _send_connect(self, keepalive):
        connect_flags = 0
        command = CONNECT
        packet = bytearray()
        username = self._username
        password = self._password
        mout_point = self._mout_point

        pwd = base64.b64encode("{}:{}".format(username, password).encode('ascii')).decode('ascii')

        HEADER = \
            "GET /%s HTTP/1.1\r\n" % mout_point + \
            "User-Agent: NTRIP client.py/0.1\r\n" + \
            "Authorization: Basic {}\r\n\r\n".format(pwd)
        packet.extend(HEADER.encode('ascii'))

        self._keepalive = keepalive
        self._easy_log(
            NTRIP_LOG_DEBUG,
            "Sending CONNECT (u%d, p%d, wr%d, wq%d, wf%d, c%d, k%d) username=%s",
            (connect_flags & 0x80) >> 7,
            (connect_flags & 0x40) >> 6,
            (connect_flags & 0x20) >> 5,
            (connect_flags & 0x18) >> 3,
            (connect_flags & 0x4) >> 2,
            (connect_flags & 0x2) >> 1,
            keepalive,
            self._username
        )
        return self._packet_queue(command, packet, 0, 0)

    def _messages_reconnect_reset_out(self):
        with self._out_message_mutex:
            self._inflight_messages = 0
            self._out_messages = collections.OrderedDict()

    def _messages_reconnect_reset_in(self):
        with self._in_message_mutex:
            self._in_messages = collections.OrderedDict()

    def _messages_reconnect_reset(self):
        self._messages_reconnect_reset_out()
        self._messages_reconnect_reset_in()

    def disconnect(self):
        self._state = ntrip_cs_disconnecting

        if self._sock is None:
            return NTRIP_ERR_NO_CONN

        return self._send_disconnect()

    def _send_disconnect(self):
        self._easy_log(NTRIP_LOG_DEBUG, "Sending DISCONNECT")
        command = DISCONNECT
        packet = bytearray()
        packet.append(command)
        self._in_packet['command'] = DISCONNECT
        self._packet_handle()
        return self._packet_queue(command, packet, 0, 0)

    def _packet_queue(self, command, packet, mid, pos=0, info=None):
        """
        消息打包
        :param command:
        :param packet:
        :param mid:
        :return:
        """
        mpkt = {
            'command': command,
            'mid': mid,
            'to_process': len(packet),
            'packet': packet,
            'pos': pos,
            'info': info
            }
        self._out_packet.append(mpkt)

        # Write a single byte to sockpairW (connected to sockpairR) to break
        # out of select() if in threaded mode.
        if self._sockpairW is not None:
            try:
                self._sockpairW.send(sockpair_data)
            except BlockingIOError:
                pass

        # If we have an external event loop registered, use that instead
        # of calling loop_write() directly.
        if self._thread is None and self._on_socket_register_write is None:
            if self._in_callback_mutex.acquire(False):
                self._in_callback_mutex.release()
                return self.loop_write()

        self._call_socket_register_write()

        return NTRIP_ERR_SUCCESS

    def loop_read(self):
        """
        socket读取一次循环
        :return:
        """
        if self._sock is None:
            return NTRIP_ERR_NO_CONN

        max_packets = len(self._out_messages) + len(self._in_messages)
        if max_packets < 1:
            max_packets = 1

        for _ in range(0, max_packets):
            if self._sock is None:
                return NTRIP_ERR_NO_CONN
            rc = self._packet_read()
            if rc > 0:
                return self._loop_rc_handle(rc)
            elif rc == NTRIP_ERR_AGAIN:
                return NTRIP_ERR_SUCCESS
        return NTRIP_ERR_SUCCESS

    def loop_write(self):
        """
        socket 发送数据循环一次
        :return:
        """
        if self._sock is None:
            return NTRIP_ERR_NO_CONN
        try:
            rc = self._packet_write()
            if rc == NTRIP_ERR_AGAIN:
                return NTRIP_ERR_SUCCESS
            elif rc > 0:
                return self._loop_rc_handle(rc)
            else:
                return NTRIP_ERR_SUCCESS
        finally:
            if self.want_write():
                self._call_socket_register_write()
            else:
                self._call_socket_unregister_write()

    def want_write(self):
        """Call to determine if there is network data waiting to be written.
        Useful if you are calling select() yourself rather than using loop().
        """
        try:
            packet = self._out_packet.popleft()
            self._out_packet.appendleft(packet)
            return True
        except IndexError:
            return False

    def loop_start(self):
        """
        开启一个循环新进程
        :return:
        """
        if self._thread is not None:
            return NTRIP_ERR_INVAL

        self._sockpairR, self._sockpairW = _socketpair_compat()
        self._thread_terminate = False
        self._thread = threading.Thread(target=self._thread_main)
        self._thread.daemon = True
        self._thread.start()

    def loop_stop(self):
        """
        结束循环
        :return:
        """
        if self._thread is None:
            return NTRIP_ERR_INVAL

        self._thread_terminate = True
        if threading.current_thread() != self._thread:
            self._thread.join()
            self._thread = None

    def _thread_main(self):
        """
        启动进程
        :return:
        """
        self.loop_forever(retry_first_connection=True)

    def _reconnect_wait(self):
        # See reconnect_delay_set for details
        now = time_func()
        with self._reconnect_delay_mutex:
            if self._reconnect_delay is None:
                self._reconnect_delay = self._reconnect_min_delay
            else:
                self._reconnect_delay = min(
                    self._reconnect_delay * 2,
                    self._reconnect_max_delay,
                )

            target_time = now + self._reconnect_delay

        remaining = target_time - now
        while (self._state != ntrip_cs_disconnecting
               and not self._thread_terminate
               and remaining > 0):

            time.sleep(min(remaining, 1))
            remaining = target_time - time_func()

    def loop_forever(self, timeout=1.0, retry_first_connection=False):
        """
        循环处理网络数据主程序
        :param timeout:
        :param retry_first_connection:
        :return:
        """
        run = True
        while run:
            if self._thread_terminate is True:
                break
            if self._state == ntrip_cs_connect_async:
                try:
                    self.reconnect()
                except OSError:
                    self._handle_on_connect_fail()
                    if not retry_first_connection:
                        raise
                    self._easy_log(
                        NTRIP_LOG_DEBUG, "Connection failed, retrying")
                    self._reconnect_wait()
            else:
                break

        while run:
            rc = NTRIP_ERR_SUCCESS

            while rc == NTRIP_ERR_SUCCESS:
                rc = self._loop(timeout)
                # We don't need to worry about locking here, because we've
                # either called loop_forever() when in single threaded mode, or
                # in multi threaded mode when loop_stop() has been called and
                # so no other threads can access _out_packet or _messages.
                if (self._thread_terminate is True
                    and len(self._out_packet) == 0
                        and len(self._out_messages) == 0):
                    rc = 1
                    run = False

            def should_exit():
                return self._state == ntrip_cs_disconnecting or run is False or self._thread_terminate is True

            if should_exit() or not self._reconnect_on_failure:
                run = False
            else:
                self._reconnect_wait()

                if should_exit():
                    run = False
                else:
                    try:
                        self.reconnect()
                    except OSError:
                        self._handle_on_connect_fail()
                        self._easy_log(
                            NTRIP_LOG_DEBUG, "Connection failed, retrying")

        return rc

    def loop(self, timeout=1.0):
        if self._sockpairR is None or self._sockpairW is None:
            self._reset_sockets(sockpair_only=True)
            self._sockpairR, self._sockpairW = _socketpair_compat()
        return self._loop(timeout)

    def publish_gga(self, payload=None):
        if self._state != ntrip_cs_connected:
            return
        if isinstance(payload, unicode):
            local_payload = payload.encode('utf-8')
        elif isinstance(payload, (bytes, bytearray)):
            local_payload = payload
        elif isinstance(payload, (int, float)):
            local_payload = str(payload).encode('ascii')
        elif payload is None:
            local_payload = b''
        else:
            raise TypeError(
                'payload must be a string, bytearray, int, float or None.')

        if len(local_payload) > 268435455:
            raise ValueError('Payload too large.')

        local_mid = self._mid_generate()

        rc = self._send_publish(local_mid, local_payload)

    def username_pw_set(self, username='', password=''):
        self._username = username
        self._password = password

    # ============================================================
    # Private functions
    # ============================================================
    def _loop_rc_handle(self, rc):
        if rc:
            self._sock_close()
            if self._state == ntrip_cs_disconnecting:
                rc = NTRIP_ERR_SUCCESS
            self._do_on_disconnect(rc)
        return rc

    def _loop(self, timeout=1.0):
        """
        循环处理数据
        :param timeout:
        :return:
        """
        if timeout < 0.0:
            raise ValueError('Invalid timeout.')

        try:
            packet = self._out_packet.popleft()
            self._out_packet.appendleft(packet)
            wlist = [self._sock]
        except IndexError:
            wlist = []

        # used to check if there are any bytes left in the (SSL) socket
        pending_bytes = 0
        if hasattr(self._sock, 'pending'):
            pending_bytes = self._sock.pending()

        # if bytes are pending do not wait in select
        if pending_bytes > 0:
            timeout = 0.0

        # sockpairR is used to break out of select() before the timeout, on a
        # call to publish() etc.
        if self._sockpairR is None:
            rlist = [self._sock]
        else:
            rlist = [self._sock, self._sockpairR]

        try:
            socklist = select.select(rlist, wlist, [], timeout)

        except TypeError as e:
            # Socket isn't correct type, in likelihood connection is lost
            self._easy_log(NTRIP_LOG_ERR, 'connection lost: %s', e)
            return NTRIP_ERR_CONN_LOST
        except ValueError:
            # Can occur if we just reconnected but rlist/wlist contain a -1 for
            # some reason.
            return NTRIP_ERR_CONN_LOST
        except Exception:
            # Note that KeyboardInterrupt, etc. can still terminate since they
            # are not derived from Exception
            return NTRIP_ERR_UNKNOWN

        if self._sock in socklist[0] or pending_bytes > 0:
            rc = self.loop_read()
            if rc or self._sock is None:
                return rc

        if self._sockpairR and self._sockpairR in socklist[0]:
            # Stimulate output write even though we didn't ask for it, because
            # at that point the publish or other command wasn't present.
            socklist[1].insert(0, self._sock)
            # Clear sockpairR - only ever a single byte written.
            try:
                # Read many bytes at once - this allows up to 10000 calls to
                # publish() inbetween calls to loop().
                self._sockpairR.recv(10000)
            except BlockingIOError:
                pass

        if self._sock in socklist[1]:
            rc = self.loop_write()
            if rc or self._sock is None:
                return rc

        return self.loop_misc()

    def loop_misc(self):
        """Process miscellaneous network events. Use in place of calling loop() if you
        wish to call select() or equivalent on.

        Do not use if you are using the threaded interface loop_start()."""
        if self._sock is None:
            return NTRIP_ERR_NO_CONN

        self._check_keepalive()

        return NTRIP_ERR_SUCCESS

    def _send_publish(self, mid, payload=b''):
        # we assume that topic and payload are already properly encoded
        assert not isinstance(
            payload, unicode) and payload is not None

        if self._sock is None:
            return NTRIP_ERR_NO_CONN

        command = PUBLISH
        packet = bytearray()
        payloadlen = len(payload)

        self._easy_log(
            NTRIP_LOG_DEBUG,
            "Sending PUBLISH m%d, ... (%d bytes)",
            mid, payloadlen
        )

        packet.extend(payload)

        return self._packet_queue(command, packet, mid)

    def _check_keepalive(self):
        if self._keepalive == 0:
            return NTRIP_ERR_SUCCESS

        now = time_func()

        with self._msgtime_mutex:
            last_msg_out = self._last_msg_out
            last_msg_in = self._last_msg_in

        if self._sock is not None and (now - last_msg_out >= self._keepalive or now - last_msg_in >= self._keepalive):
            if self._state == ntrip_cs_connected:
                self._sock_close()
                if self._state == ntrip_cs_disconnecting:
                    rc = NTRIP_ERR_SUCCESS
                else:
                    rc = NTRIP_ERR_KEEPALIVE
                self._do_on_disconnect(rc)

    def _mid_generate(self):
        """
        生成消息号
        :return:
        """
        with self._mid_generate_mutex:
            self._last_mid += 1
            if self._last_mid == 65536:
                self._last_mid = 1
            return self._last_mid

    def _packet_read(self):
        if self._in_packet['command'] == 0:
            try:
                if self._state == ntrip_cs_new:
                    data = self._sock_recv(4096)
                    self._easy_log(
                        NTRIP_LOG_DEBUG, 'received on socket: %s', data)
                    if data.find(b"ENDSOURCETABLE") >= 0:  # end of sourcetable
                        data = data.split(b'\r\n')
                        for line in data:
                            if line.find(b';') >= 0:
                                self._easy_log(
                                    NTRIP_LOG_DEBUG, 'receive moutpoint on socket: %s', line)
                        self.loop_stop()
                        return NTRIP_ERR_CONN_LOST
                    elif (
                            data.find(b"401 Unauthorized") >= 0
                            or data.find(b"403 Forbidden") >= 0
                            or data.find(b"404 Not Found") >= 0
                    ):
                        self._easy_log(
                            NTRIP_LOG_ERR, 'connecting failed: %s', data)
                        return NTRIP_ERR_CONN_LOST
                    elif data.find(b"ICY 200 OK") >= 0:
                        self._in_packet['command'] = CONNACK
                        self._in_packet['packet'] = data
                    else:
                        self._easy_log(
                            NTRIP_LOG_ERR, 'connecting failed: %s', data)
                        return NTRIP_ERR_CONN_LOST
                else:
                    data, parsed_data = self._sock_recv_rtcm()
                    # data = self._sock_recv(2048)
                    # parsed_data = data
                    if data:
                        if not type(parsed_data) is str:
                            header = parsed_data.identity
                            self._easy_log(NTRIP_LOG_DEBUG, 'rtcm received: %s', parsed_data.identity)
                        else:
                            header = 0
                            self._easy_log(NTRIP_LOG_DEBUG, 'rtcm received: %s', data)
                        self._in_packet['command'] = PUBLISH
                        self._in_packet['packet'] = (header, data)
                        with self._msgtime_mutex:
                            self._last_msg_in = time_func()

            except BlockingIOError:
                return NTRIP_ERR_AGAIN
            except ConnectionError as err:
                self._easy_log(
                    NTRIP_LOG_ERR, 'failed to receive on socket: %s', err)
                return NTRIP_ERR_CONN_LOST

        # All data for this packet is read.
        self._in_packet['pos'] = 0
        rc = self._packet_handle()

        # Free data and reset values
        self._in_packet = {
            'command': 0,
            'packet': bytearray(b""),
            'to_process': 0,
            'pos': 0}

        return rc

    def _packet_handle(self):
        cmd = self._in_packet['command'] & 0xF0
        if cmd == PUBLISH:
            return self._handle_publish()
        elif cmd == CONNACK:
            return self._handle_connack()
        elif cmd == DISCONNECT:
            return self._handle_disconnect()
        else:
            return NTRIP_ERR_SUCCESS

    def _packet_write(self):
        while True:
            try:
                packet = self._out_packet.popleft()
            except IndexError:
                return NTRIP_ERR_SUCCESS

            try:
                if (packet['command'] & 0xF0) == DISCONNECT:
                    with self._msgtime_mutex:
                        self._last_msg_out = time_func()
                    self._do_on_disconnect(NTRIP_ERR_SUCCESS)
                    self._sock_close()
                    return NTRIP_ERR_SUCCESS

                write_length = self._sock_send(
                    packet['packet'][packet['pos']:])
                self._easy_log(NTRIP_LOG_DEBUG, 'sock sended %d', write_length)
            except (AttributeError, ValueError):
                self._out_packet.appendleft(packet)
                return NTRIP_ERR_SUCCESS
            except BlockingIOError:
                self._out_packet.appendleft(packet)
                return NTRIP_ERR_AGAIN
            except ConnectionError as err:
                self._out_packet.appendleft(packet)
                self._easy_log(
                    NTRIP_LOG_ERR, 'failed to receive on socket: %s', err)
                return NTRIP_ERR_CONN_LOST
            if write_length > 0:
                packet['to_process'] -= write_length
                packet['pos'] += write_length
                if packet['to_process'] == 0:
                    pass
                else:
                    # We haven't finished with this packet
                    self._out_packet.appendleft(packet)
            else:
                break
        with self._msgtime_mutex:
            self._last_msg_out = time_func()

        return NTRIP_ERR_SUCCESS

    def _easy_log(self, level, fmt, *args):
        if self.on_log is not None:
            buf = fmt % args
            try:
                self.on_log(self, self._username, level, buf)
            except Exception:
                # Can't _easy_log this, as we'll recurse until we break
                pass  # self._logger will pick this up, so we're fine
        if self._logger is not None:
            level_std = LOGGING_LEVEL[level]
            self._logger.log(level_std, fmt, *args)

    def _do_on_disconnect(self, rc):
        with self._callback_mutex:
            on_disconnect = self.on_disconnect

        if on_disconnect:
            with self._in_callback_mutex:
                try:
                    on_disconnect(self, self._username, rc)
                except Exception as err:
                    self._easy_log(
                        NTRIP_LOG_ERR, 'Caught exception in on_disconnect: %s', err)
                    if not self.suppress_exceptions:
                        raise

    # ============================================================
    # handle functions
    # ============================================================
    def _handle_connack(self):
        self._state = ntrip_cs_connected
        self._reconnect_delay = None
        self._easy_log(
            NTRIP_LOG_DEBUG, "Received CONNACK (%s, %s)", self._reconnect_delay, ntrip_cs_connected)

        with self._callback_mutex:
            on_connect = self.on_connect

        if on_connect:
            with self._in_callback_mutex:
                try:
                    on_connect(self, self._username)
                except Exception as err:
                    self._easy_log(
                        NTRIP_LOG_ERR, 'Caught exception in on_connect: %s', err)
                    if not self.suppress_exceptions:
                        raise
        rc = 0
        with self._out_message_mutex:
            for m in self._out_messages.values():
                m.timestamp = time_func()
                if m.state == ntrip_ms_queued:
                    self.loop_write()  # Process outgoing messages that have just been queued up
                    return NTRIP_ERR_SUCCESS

                if m.qos == 0:
                    with self._in_callback_mutex:  # Don't call loop_write after _send_publish()
                        rc = self._send_publish(
                            m.mid,
                            m.topic.encode('utf-8'),
                            m.payload,
                        )
                    if rc != 0:
                        return rc
                self.loop_write()  # Process outgoing messages that have just been queued up

        return rc

    def _handle_publish(self):
        rc = 0
        header = self._in_packet['command']
        packet = self._in_packet['packet']
        self._in_packet = {
            "command": 0,
            "packet": bytearray(b""),
            "to_process": 0,
            "pos": 0}
        self._handle_on_message(packet)
        return NTRIP_ERR_SUCCESS

    def _handle_on_message(self, message):
        matched = False
        on_message_callbacks = []
        with self._callback_mutex:
            if len(on_message_callbacks) == 0:
                on_message = self.on_message
            else:
                on_message = None

        if on_message:
            with self._in_callback_mutex:
                try:
                    on_message(self, self._username, message)
                except Exception as err:
                    self._easy_log(
                        NTRIP_LOG_ERR, 'Caught exception in on_message: %s', err)
                    if not self.suppress_exceptions:
                        raise

    def _handle_on_connect_fail(self):
        with self._callback_mutex:
            on_connect_fail = self.on_connect_fail

        if on_connect_fail:
            with self._in_callback_mutex:
                try:
                    on_connect_fail(self, self._username)
                except Exception as err:
                    self._easy_log(
                        NTRIP_LOG_ERR, 'Caught exception in on_connect_fail: %s', err)

    def _handle_disconnect(self):
        self._easy_log(NTRIP_LOG_DEBUG, "Received DISCONNECT")
        self._sock_close()
        self._do_on_disconnect(NTRIP_ERR_SUCCESS)
        self._state = ntrip_cs_disconnecting
        return NTRIP_ERR_SUCCESS
