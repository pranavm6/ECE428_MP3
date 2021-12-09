# ECE428_MP3
Getting started
Clone files into a directory
Ensure server and client files are in the directory
Make server and client executable
  	chmod +x server client

Launch Server first:
  	./server <server name> <config_file_name>

   5. Launch clients

./client <client_name> <config_file_name>
 
Implementation:

Timestamp ordering used to implement Distributed Transactions. No deadlocks seen but transaction aborts if TS rules are violated
All Transactions that depend on an aborted transaction are also aborted
No Deadlocks
