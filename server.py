#!/usr/bin/env python3
import sys
import socket
import time
from threading import Thread

server_name = sys.argv[1]
config_file = sys.argv[2]

server_ip = ""
server_port = 0
MAX_BUFFER_SIZE = 500

txn_count = 0
accounts = {}
wts = {}
rts = {}
txn_list = {}

with open(config_file) as f:
    lines = f.readlines()

for line in lines:
    info = line.split()
    if info[0] == server_name:
        server_ip = info[1]
        server_port = int(info[2])

def accept_incoming_connections(ip,port):
    soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # soc.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    try:
        soc.bind((ip, port))
    except:
        print('Bind failed. Error : ' + str(sys.exc_info()))
        sys.exit()

    soc.listen(100)
    while(True):
        connection, address = soc.accept()
        ip, port = str(address[0]), str(address[1])
        print('Connected with ' + ip + ':' + port)
        #setup_msg = connection.recv(MAX_BUFFER_SIZE, socket.MSG_WAITALL).decode('utf8').strip()
        #setup = setup_msg.split()
        #ip, port = setup[0], int(setup[1])

        try:
            #print("Starting service thread")
            Thread(target=node_receive,args=(connection, ip, port)).start()
        except:
            print('Thread did not start.')
            traceback.print_exc()

def node_receive(connection,ip,port):
    #client_soc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    #client_soc.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    #client_soc.connect((ip,int(port)))

    global txn_count
    txn_num = 0
    while True:
        client_msg = connection.recv(MAX_BUFFER_SIZE).decode('utf8').strip()
        print(client_msg)
        msg = client_msg.split()
        if(len(msg)) == 0:
            break
        msg_typ = msg[0]
        acc_found = 0
        if msg_typ == "DEPOSIT" or msg_typ == "WITHDRAW" or msg_typ == "BALANCE":
            accn = msg[1].split(".")
            acc = accn[1]
            txn = txn_list[txn_num]
            if acc in txn:
                acc_found = 1
            else:
                if acc in accounts:
                    acc_found = 2

        if msg_typ == "BEGIN":
            txn_count += 1
            txn_num = txn_count
            new_txn = {}
            txn_list[txn_count] = new_txn
        elif msg_typ == "DEPOSIT":
            txn = txn_list[txn_num]
            if acc_found == 0:
                txn[acc] = int(msg[2])
            elif acc_found == 1:
                txn[acc] += int(msg[2])
            elif acc_found == 2:
                bal = accounts[acc]
                txn[acc] = bal + int(msg[2])
            txn_list[txn_num] = txn
            print(txn)
            m = "OK"
            connection.sendall(m.encode('utf8'))
        elif msg_typ == "WITHDRAW":
            txn = txn_list[txn_num]
            if acc_found == 0:
                m = "NOT FOUND, ABORTED"
                connection.sendall(m.encode('utf8'))
            elif acc_found == 1:
                txn[acc] -= int(msg[2])
            elif acc_found == 2:
                bal = accounts[acc]
                txn[acc] = bal - int(msg[2])
            txn_list[txn_num] = txn
            print(txn)
            m = "OK"
            connection.sendall(m.encode('utf8'))
        elif msg_typ == "BALANCE":
            txn = txn_list[txn_num]
            print(txn)
            bal = 0
            if acc_found == 0:
                m = "NOT FOUND, ABORTED"
                connection.sendall(m.encode('utf8'))
            elif acc_found == 1:
                bal = txn[acc]
            elif acc_found == 2:
                bal = accounts[acc]
            m = msg[1] + " = " + str(bal)
            print(m)
            connection.sendall(m.encode('utf8'))
        elif msg_typ == "CAN_COMMIT":
            txn = txn_list[txn_num]
            abort = 0
            for a in txn:
                if txn[a] < 0:
                    abort = 1
            if abort:
                m = "ABORTED"
            else:
                m = "YES_COMMIT"
            print(m)
            connection.sendall(m.encode('utf8'))
        elif msg_typ == "DO_COMMIT":
            txn = txn_list[txn_num]
            for a in txn:
                accounts[a] = txn[a]
            print(accounts)
        elif msg_typ == "ABORT":
            txn_list.pop(txn_num,f)


Thread(target=accept_incoming_connections,args=(server_ip,server_port)).start()
time.sleep(10)
