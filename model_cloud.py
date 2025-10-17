# ================== EX1 - IPC ====================
# data_object.py:
class DataObject:
    def __init__(self, values):
        self.values = values

# server.py:
import socket
import pickle
from data_object import DataObject

HOST = '127.0.0.1'
PORT = 8044

# Create server socket
server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
server_socket.bind((HOST, PORT))
server_socket.listen()

print(f"<SERVER> Listening on {HOST}:{PORT}")

conn, addr = server_socket.accept()
print(f"<SERVER> Connected by {addr}")

while True:
    raw_data = conn.recv(1024) # receive raw data

    data = pickle.loads(raw_data)

    if isinstance(data, str):
        conn.send("Connection Terminated..".encode())
        conn.close()
        print("<SERVER> Connection Terminated...")
        break

    print(f"<SERVER> Received: {data.values}")

    total = str(sum(data.values))

    conn.send(total.encode())


server_socket.close()

# client.py:
import socket
import pickle
from data_object import DataObject

HOST = '127.0.0.1'
PORT = 8044

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client_socket.connect((HOST, PORT))

print("<CLIENT> Connected to Server.")

values = [1, 2, 3, 4, 5, 6, 7, 8]
obj = DataObject(values=values)

# Send the serialized object
raw_data = pickle.dumps(obj)
client_socket.send(raw_data)

# Receive Response
total_value = client_socket.recv(1024).decode()
print(f"<SERVER> Response from server: {total_value}")

exit_msg = "exit"
raw_exit = pickle.dumps(exit_msg)
response = client_socket.send(raw_exit)
print(f"<SERVER> {response}")

client_socket.close()
# ================== EX2 - RMI ====================
# calc.py:
import Pyro4

@Pyro4.expose
class Calculator:
    def add(self, num1, num2):
        return num1 + num2

    def sub(self, a, b):
        return a - b

    def mul(self, a, b):
        return a * b
    
    def div(self, a, b):
        try:
            return a / b
        except:
            return "Can't divide by Zero.."
        
# server.py:
import Pyro4
from calc import Calculator

daemon = Pyro4.Daemon()
calc_uri = daemon.register(Calculator)

name_server = Pyro4.locateNS()
name_server.register("ex2.calculator", calc_uri)

print(f"<SERVER> Calculator for RMI Accesible via 'ex2.calculator' or {calc_uri}")

daemon.requestLoop()

# client.py:
import Pyro4

name_server = Pyro4.locateNS()

calc_uri = name_server.lookup("ex2.calculator")

# Get the RMI Obj
calc_rmi = Pyro4.Proxy(calc_uri)

a = 10
b = 12

print(f"Two numbers are: {a}, {b}")

print(f"Add: {calc_rmi.add(a, b)}")
print(f"Sub: {calc_rmi.sub(a, b)}")
print(f"Mul: {calc_rmi.mul(a, b)}")
print(f"Div: {calc_rmi.div(a, b)}")

# ================== Message Passing ====================
# SERVER
import socket
import threading

clients = []  # Track connected clients

def handle_client(c):
    while True:
        try:
            msg = c.recv(1024).decode()
            if not msg:
                break
            print(f"[SERVER] Received: {msg}")
            # Broadcast to all other clients
            for client in clients:
                if client != c:
                    client.send(msg.encode())
        except:
            break
    c.close()
    clients.remove(c)
    print("[SERVER] A client disconnected.")

def start_server():
    s = socket.socket()
    s.bind(('localhost', 1234))
    s.listen()
    print("[SERVER] Running on localhost:1234 ... Waiting for clients.")
    while True:
        c, addr = s.accept()
        print(f"[SERVER] Client connected from {addr}")
        clients.append(c)
        threading.Thread(target=handle_client, args=(c,), daemon=True).start()

if __name__ == "__main__":
    start_server()

# CLIENT
import socket
import threading
import sys

def listen(s):
    while True:
        try:
            msg = s.recv(1024).decode()
            if not msg:
                print("\n[CLIENT] Server closed connection.")
                break
            # Print incoming message on a new line and re-show prompt
            sys.stdout.write(f"\n{msg}\nEnter message: ")
            sys.stdout.flush()
        except:
            break

def start_client():
    s = socket.socket()
    s.connect(('localhost', 1234))
    print("[CLIENT] Connected to server at localhost:1234")
    threading.Thread(target=listen, args=(s,), daemon=True).start()

    # Input loop with clear prompt
    while True:
        try:
            msg = input("Enter message: ")
            s.send(msg.encode())
        except (KeyboardInterrupt, EOFError):
            print("\n[CLIENT] Exiting...")
            s.close()
            break

if __name__ == "__main__":
    start_client()

# =========================== Coordinator ==========================

import threading, queue, time, random
from collections import deque

N = 4
lock = threading.Lock()

def log(tag, msg):
    with lock:
        print(f"[{time.strftime('%H:%M:%S')}] {tag}: {msg}")

# Coordinator thread
class Coordinator(threading.Thread):
    def __init__(self):
        super().__init__(daemon=True)
        self.requests = deque()
        self.servers = {}  # pid -> queue to send grant
        self._stop = False

    def register(self, pid, q):
        self.servers[pid] = q

    def run(self):
        while True:
            # if there are pending requests and coordinator is free, grant the next
            if self.requests:
                pid = self.requests.popleft()
                log("COORD", f"Granting CS to P{pid}")
                self.servers[pid].put(("GRANT",))
                
                time.sleep(0.1)
            else:
                time.sleep(0.05)

    # API for processes to call (simulated message passing)
    def request_cs(self, pid):
        log("COORD", f"Received REQUEST from P{pid}")
        self.requests.append(pid)

# Process thread
class Process(threading.Thread):
    def __init__(self, pid, coord: Coordinator):
        super().__init__(daemon=True)
        self.pid = pid
        self.coord = coord
        self.inbox = queue.Queue()
        self.coord.register(pid, self.inbox)

    def run(self):
        while True:
            time.sleep(random.uniform(2, 6))
            log(f"P{self.pid}", "REQUEST -> coordinator")
            self.coord.request_cs(self.pid)
            # wait for GRANT
            while True:
                msg = self.inbox.get()
                if msg[0] == "GRANT":
                    log(f"P{self.pid}", "*** ENTER CS *** (granted by coordinator)")
                    time.sleep(1)  # in critical section
                    log(f"P{self.pid}", "*** EXIT  CS *** (releasing)")
                    # Notify coordinator by simply sleeping briefly — coordinator handles queue order
                    break

def main():
    coord = Coordinator()
    coord.start()
    procs = [Process(i, coord) for i in range(N)]
    for p in procs:
        p.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopped.")

if __name__ == "__main__":
    main()
# =========================== Ricarta ==========================
import threading, queue, time, random

N = 3  # number of processes
network = [queue.Queue() for _ in range(N)]
lock = threading.Lock()

def log(pid, msg):
    with lock:
        print(f"[{time.strftime('%H:%M:%S')}] P{pid}: {msg}")

class Process(threading.Thread):
    def __init__(self, pid):
        super().__init__(daemon=True)
        self.pid = pid
        self.clock = 0
        self.req_time = None
        self.requesting = False
        self.deferred = []
        self.replies = 0

    def send(self, dst, msg):
        network[dst].put(msg)

    def broadcast_request(self):
        self.requesting = True
        self.req_time = self.clock = self.clock + 1
        self.replies = 0
        log(self.pid, f"REQUEST(ts={self.req_time})")
        for j in range(N):
            if j != self.pid:
                self.send(j, ("REQ", self.req_time, self.pid))

    def handle(self, msg):
        t, ts, src = msg
        self.clock = max(self.clock, ts) + 1
        if t == "REQ":
            if (not self.requesting or
                (self.req_time, self.pid) > (ts, src)):
                log(self.pid, f"REPLY → P{src} (they requested earlier)")
                self.send(src, ("REP", self.clock, self.pid))
            else:
                log(self.pid, f"DEFER reply to P{src} (my request earlier)")
                self.deferred.append(src)
        elif t == "REP":
            self.replies += 1
            log(self.pid, f"got REPLY from P{src} ({self.replies}/{N-1})")

    def enter_cs(self):
        while self.replies < N - 1:
            try:
                msg = network[self.pid].get(timeout=0.5)
                self.handle(msg)
            except queue.Empty:
                pass
        log(self.pid, f"*** ENTER CS (ts={self.req_time}) ***")
        time.sleep(1)
        log(self.pid, f"*** EXIT  CS (ts={self.req_time}) ***")
        self.requesting = False
        for p in self.deferred:
            log(self.pid, f"Sending deferred REPLY to P{p}")
            self.send(p, ("REP", self.clock, self.pid))
        self.deferred.clear()

    def run(self):
        while True:
            time.sleep(random.uniform(2, 5))
            self.broadcast_request()
            self.enter_cs()

# Start processes
procs = [Process(i) for i in range(N)]
for p in procs: p.start()

try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    print("\nStopped.")

# =========================== Token Passing ==========================
import threading, queue, time, random

N = 4  # number of processes in the ring
lock = threading.Lock()

def log(pid, msg):
    with lock:
        print(f"[{time.strftime('%H:%M:%S')}] P{pid}: {msg}")

class Process(threading.Thread):
    def __init__(self, pid, inbox, outbox):
        super().__init__(daemon=True)
        self.pid = pid
        self.inbox = inbox   # queue to receive token (or messages)
        self.outbox = outbox # queue of next process
        self.want_cs = False

    def run(self):
        while True:
            # random time before wanting CS
            time.sleep(random.uniform(2, 6))
            self.want_cs = True
            log(self.pid, "Wants CS (will wait for token)")

            # wait for token/message
            while True:
                try:
                    token = self.inbox.get(timeout=0.5)
                except queue.Empty:
                    continue
                # token is a dict; token['holder'] is pid that holds it now
                if token.get("type") == "TOKEN":
                    # we hold token
                    log(self.pid, "Received TOKEN")
                    if self.want_cs:
                        log(self.pid, "*** ENTER CS ***")
                        time.sleep(1)  # in critical section
                        log(self.pid, "*** EXIT  CS ***")
                        self.want_cs = False
                    # pass token to next
                    self.outbox.put({"type": "TOKEN"})
                    log(self.pid, "Passed TOKEN to next")
                    break
                else:
                    # ignore unknown messages
                    pass

def main():
    # create ring of queues
    queues = [queue.Queue() for _ in range(N)]
    procs = []
    for i in range(N):
        out = queues[(i+1) % N]
        p = Process(i, queues[i], out)
        procs.append(p)
    for p in procs:
        p.start()

    # inject initial token into process 0
    queues[0].put({"type": "TOKEN"})
    log("MAIN", "Initial TOKEN injected to P0")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopped.")

if __name__ == "__main__":
    main()

# ================================== Ex 4 ==================================
1. sudo apt update (in vm 0)
2. sudo apt install nginx
3. cd /etc
4. cd nginx
5. sudo apt install gedit
6. sudo gedit nginx.conf

open nginx.conf{
    add 
        http{
            upstream backend{
                server ip a;(vm 1)
                server ip a;(vm 2)
            }
        }
}

7. cd sites-available
8. sudo gedit default

open default{
    add
        location{
            proxy_pass http://backend;
            proxy_next_upstream error timeout invalid_header http_500 http_502 http_503 http_504;
        }    
}

9. go to vm 1
10. sudo apt update
11. sudo apt install apache2 -y
12. go to vm 2
13. repat 10 11
14. browser enter ip
15. go to main
16. systemctl status apache2
17. vm 1
18. cd /var
19. cd www
20. cd html
21. sudo apt install
22. sudo gedit index.html
23. add h1 tag
24. main
25. systemctl start nginx

# ================================== Ex 5 ==================================
1. sudo apt update
2. systemctl restart cron
3. sudo apt install gedit
4. sudo gedit ~/task.sh

open task.sh{
    add{
        #!/bin/bash
        echo " Hi from hari $(date)" >> ~/output.txt
    }
}

5. chmod +x ~/task.sh
6. crontab -e

open{
    add{
        * * * * * ~/task.sh
    }
}

7. cat output.txt
8. watch cat output.txt

# ==================================== EOF ==================================
# ---------- TIME SERVER ----------------- #
import socket
from datetime import datetime, timezone, timedelta

IST = timezone(timedelta(hours=5, minutes=30))

HOST = '127.0.0.1' 
PORT = 12345

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.bind((HOST, PORT))    
    s.listen()                   
    print(f"Time Server (IST) listening on {HOST}:{PORT}...")

    while True:
        conn, addr = s.accept()
        with conn:
            print(f"Connected by {addr}")

            data = conn.recv(1024)
            if not data:
                break

            server_time = datetime.now(IST).timestamp()

            conn.sendall(str(server_time).encode('utf-8'))

            formatted = datetime.fromtimestamp(server_time, IST).strftime("%d-%m-%Y %H:%M:%S %p %Z")
            print(f"Sent server IST time: {formatted}")

# ---------- TIME CLIENT ----------------- #
import socket
from datetime import datetime, timezone, timedelta

# Define IST timezone
IST = timezone(timedelta(hours=5, minutes=30))

HOST = '127.0.0.1'
PORT = 12345

# Record t1: client send time (IST)
t1 = datetime.now(IST).timestamp()

with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
    s.connect((HOST, PORT))
    s.sendall(b"Requesting time")
    data = s.recv(1024)

# Record t4: client receive time (IST)
t4 = datetime.now(IST).timestamp()

server_time = float(data.decode('utf-8'))

# Cristian’s algorithm for clock offset
offset = ((server_time + (t4 - t1) / 2) - t4)

print(f"Client send time (t1): {t1}")
print(f"Server time received : {server_time}")
print(f"Client recv time (t4): {t4}")
print(f"Estimated clock offset: {offset:.6f} seconds")

# Adjusted synchronized time in IST
adjusted_time = datetime.fromtimestamp(t4 + offset, IST)
formatted_time = adjusted_time.strftime("%d-%m-%Y %I:%M:%S %p %Z")

print(f"Synchronized client time (IST): {formatted_time}")
