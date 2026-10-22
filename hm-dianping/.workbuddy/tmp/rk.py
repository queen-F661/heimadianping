import socket,time
s=socket.create_connection(("192.168.45.128",6379),5)
def enc(a):
    if isinstance(a,str): a=a.encode()
    return b"$%d\r\n%s\r\n"%(len(a),a)
def cmd(*args):
    s.sendall(b"*%d\r\n"%len(args)+b"".join(enc(a) for a in args))
    time.sleep(0.2)
    return s.recv(65536)
cmd("AUTH","123456")
print("seckill keys:", cmd("KEYS","seckill*"))
print("stock 55:", cmd("GET","seckill:stock:55"))
