# 使用方法(usage)

```commandline
    def generate_gga():
        import datetime
        import operator
        from functools import reduce
        tst=datetime.datetime.utcnow().strftime("%H%M%S.00,")
        gga_str = "GPGGA," + tst + "3352.51932,N,14214.80758,E,1,00,1.0,3.428,M,-3.428,M,0.0,"
        gga_str += '*%02X' % reduce(operator.xor, map(ord, gga_str), 0)
        gga_str = '$' + gga_str + '\r\n\r\n'
        return gga_str

    def ntrip_on_log(client, userdata, level, buf):
        # 日志打印
        print('[NTRIP {}]: {}'.format(level, buf))
        pass
    
    def on_connect(ntripc, userdata):
        print("connect success: " + str(userdata))
    
    def on_connect_fail(ntripc, userdata):
        print("connect failed: " + str(userdata))
    
    def on_message(ntripc, userdata, msg):
        print(msg[0])
        pass
    
    def on_disconnect(ntripc, userdata, rc):
        print('disconnect')
        pass
    
    # create client
    ntripc = Client()
    # set ntrip account username and password
    ntripc.username_pw_set('username', 'password')
    # set call backs
    ntripc.on_log = ntrip_on_log
    ntripc.on_message = on_message
    ntripc.on_connect = on_connect
    ntripc.on_disconnect = on_disconnect
    ntripc.on_connect_fail = on_connect_fail
    # connect to ntrip caster, if mount_point='', then client will get a mount tables from
    # ntrip caster, users could get the list from on_log() call back function
    ntripc.connect('ntrip.com', mount_point='84RTCM3X', keepalive=60)
    # run a loop in new thread,  use loop_stop() to stop the thread
    # also you can use loop_forever() to run loop method in this thread
    # use loop() for loop onece
    ntripc.loop_start()
    ntripc.enable_logger()
    
    data = generate_gga()
    while True:
        ntripc.publish_gga(data)
        time.sleep(5)
        # ntripc.publish_gga(data)
        # time.sleep(15)
        # ntripc.disconnect()
```