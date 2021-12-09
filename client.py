import sys
import socket
import numpy as np
import time
from threading import Thread

client_name = sys.argv[1]
config_file = sys.argv[2]
#print(client_name)
#print(config_file)
MAX_BUFFER_SIZE = 500

yesCount = 0
abort = 0

with open(config_file) as f:
    lines = f.readlines()

servers = np.array([])
servers_ip = {}
for line in lines:
    info = line.split()
    server =  info[0]
    ip =      info[1]
    port =    info[2]
    servers_ip[server] = np.array([ip, port])
    servers = np.append(servers, [server])

#print(servers)
#print(servers_ip)
server_sock = {}
#client_soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
#client_soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
#client_soc.bind(())

for server in servers_ip:
    #print(servers_ip[server][0])
    ip = servers_ip[server][0]
    port = servers_ip[server][1]
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((ip,int(port)))
    server_sock[server] = sock
    print('Connected to ' + str(server))

def decode_transaction():
    global abort
    global yesCount

    while True:
        try:
            message = sys.stdin.readline()
            txn = message.split()
            if txn[0] == "BEGIN":
                print("OK")
                for serv in server_sock:
                    server = server_sock[serv]
                    msg = "BEGIN"
                    server.sendall(msg.encode('utf8'))
            elif txn[0] == "DEPOSIT" or txn[0] == "WITHDRAW" or txn[0] == "BALANCE":
                acc = txn[1]
                accn = acc.split(".")
                serv = accn[0]
                server = server_sock[serv]
                server.sendall(message.encode('utf8'))
            elif txn[0] == "COMMIT":
                for serv in server_sock:
                    server = server_sock[serv]
                    msg = "CAN_COMMIT"
                    server.sendall(msg.encode('utf8'))
                while yesCount != len(servers):
                    if abort: 
                        break
                msg = ""
                if abort != 1:
                    msg = "DO_COMMIT"
                    print("COMMITTED")
                else:
                    msg = "ABORT"
                    print("ABORTED")
                yesCount = 0
                abort = 0
                for serv in server_sock:
                    server = server_sock[serv]
                    server.sendall(msg.encode('utf8'))
            elif txn[0] == "ABORT":
                for serv in server_sock:
                    server = server_sock[serv]
                    msg = "ABORT"
                    server.sendall(msg.encode('utf8'))
                print("ABORTED")
        except:
            print("Error")
            time.sleep(5)

def server_reply(serv):
    global abort
    global yesCount
    server = server_sock[serv]
    
    while True:
        reply = server.recv(MAX_BUFFER_SIZE).decode('utf8').strip()
        line = reply.split()
        if reply == "NOT FOUND, ABORTED":
            abort = 1

        if line[0] == "OK":
            print(reply)
        elif line[0] == "YES_COMMIT":
            yesCount += 1
        elif line[0] == "ABORTED":
            abort = 1
        else:
            print(reply)


for serv in server_sock:
    Thread(target=server_reply,args=(serv)).start()
decode_transaction()
