# ECE428_MP3
Getting started
  1. Clone files into a directory
  2. Ensure server and client files are in the directory
  3. Make server and client executable
  	   chmod +x server client
  4. Launch Server first:
  	  ./server <server name> <config_file_name>
  5. Launch clients
      ./client <client_name> <config_file_name>
 
Implementation:
  1. Timestamp ordering used to implement Distributed Transactions. No deadlocks seen but transaction aborts if TS rules are violated
  2. All Transactions that depend on an aborted transaction are also aborted
  3. No Deadlocks
