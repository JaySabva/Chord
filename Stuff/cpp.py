import socket
import random
import time
import threading

M = 48  # Number of bits for hashing
R = 10  # Size of the successor list


class SocketAndPort:
    def __init__(self):
        self.port_no_server = None
        self.sock = None
        self.current = None
        self.specify_port_server()

    def specify_port_server(self):
        random.seed(time.time())
        self.port_no_server = random.randint(1024, 65535)
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.current = ('127.0.0.1', self.port_no_server)
        self.sock.bind(self.current)
        print(f"Server running on {self.current[0]}:{self.current[1]}")

    def port_in_use(self, port_no):
        try:
            temp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            temp_sock.bind(('127.0.0.1', port_no))
            temp_sock.close()
            return False
        except OSError:
            return True

    def get_ip_address(self):
        return self.current[0]

    def get_port_number(self):
        return self.port_no_server

    def get_socket_fd(self):
        return self.sock.fileno()

    def close_socket(self):
        self.sock.close()


class NodeInformation:
    def __init__(self):
        self.id = None
        self.predecessor = (None, -1)
        self.successor = (None, -1)
        self.finger_table = [(None, -1)] * (M + 1)
        self.dictionary = {}
        self.successor_list = [None] * R
        self.is_in_ring = False
        self.sp = SocketAndPort()

    def set_id(self, node_id):
        self.id = node_id

    def set_successor(self, ip, port):
        self.successor = (ip, port)

    def set_predecessor(self, ip, port):
        self.predecessor = (ip, port)

    def store_key(self, key, value):
        self.dictionary[key] = value

    def get_value(self, key):
        return self.dictionary.get(key)

    def find_successor(self, node_id):
        if self.successor[1] == -1:
            return self.sp.get_ip_address(), self.sp.get_port_number()

        if self.id < node_id <= self.successor[1]:
            return self.successor
        elif self.id == node_id or self.successor[1] == node_id:
            return self.sp.get_ip_address(), self.sp.get_port_number()
        else:
            node = self.closest_preceding_node(node_id)
            if node is None:
                return self.sp.get_ip_address(), self.sp.get_port_number()

            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            try:
                sock.sendto(str(node_id).encode(), node)
                response, _ = sock.recvfrom(1024)
                ip, port = response.decode().split(':')
                return ip, int(port)
            except socket.timeout:
                return self.sp.get_ip_address(), self.sp.get_port_number()
            finally:
                sock.close()

    def closest_preceding_node(self, node_id):
        for i in range(M, 0, -1):
            if self.finger_table[i][1] != -1 and self.id < self.finger_table[i][1] < node_id:
                return self.finger_table[i]
        return None

    def create_ring(self):
        self.is_in_ring = True
        self.successor = (self.sp.get_ip_address(), self.sp.get_port_number())
        self.predecessor = (self.sp.get_ip_address(), self.sp.get_port_number())
        print("DHT Ring created.")

    def join_ring(self, ip, port):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.sendto(f'JOIN {self.sp.get_ip_address()}:{self.sp.get_port_number()}'.encode(), (ip, port))
            response, _ = sock.recvfrom(1024)
            self.successor = response.decode().split(':')
            print(f"Joined the ring with successor {self.successor}")
        except socket.timeout:
            print("Could not join the ring.")
        finally:
            sock.close()

    def stabilize(self):
        if self.successor[1] == -1:
            return

        pred_node = self.get_predecessor(self.successor)
        if pred_node and (pred_node[1] == -1 or pred_node[1] < self.id):
            self.successor = pred_node

    def get_predecessor(self, node):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.sendto(b'GET_PREDECESSOR', node)
            pred_info, _ = sock.recvfrom(1024)
            ip, port = pred_info.decode().split(':')
            return ip, int(port)
        except socket.timeout:
            return None
        finally:
            sock.close()

    def check_predecessor(self):
        if self.predecessor[1] == -1:
            return

        if not self.is_alive(self.predecessor):
            self.predecessor = (None, -1)

    def check_successor(self):
        if not self.is_alive(self.successor):
            self.successor = self.successor_list[0]  # Replace with appropriate successor

    def is_alive(self, node):
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            sock.sendto(b'PING', node)
            sock.settimeout(1)
            sock.recvfrom(1024)
            return True
        except socket.timeout:
            return False
        finally:
            sock.close()

    def print_state(self):
        print(f"ID: {self.id}")
        print(f"Predecessor: {self.predecessor}")
        print(f"Successor: {self.successor}")
        print(f"Finger Table: {self.finger_table}")
        print(f"Successor List: {self.successor_list}")

    def print_keys(self):
        for key, value in self.dictionary.items():
            print(f"{key}: {value}")

    def run_server(self):
        while True:
            data, addr = self.sp.sock.recvfrom(1024)
            command = data.decode()
            if command.startswith('JOIN'):
                _, successor_info = command.split()
                self.successor = (successor_info.split(':')[0], int(successor_info.split(':')[1]))
                self.stabilize()
                self.sp.sock.sendto(f"{self.sp.get_ip_address()}:{self.sp.get_port_number()}".encode(), addr)
            elif command == 'GET_PREDECESSOR':
                self.sp.sock.sendto(f"{self.predecessor[0]}:{self.predecessor[1]}".encode(), addr)
            elif command == 'PING':
                self.sp.sock.sendto(b'PONG', addr)
            else:
                node_id = int(command)
                response = self.find_successor(node_id)
                self.sp.sock.sendto(f"{response[0]}:{response[1]}".encode(), addr)

    def start(self):
        threading.Thread(target=self.run_server, daemon=True).start()
        print(f"Server running on {self.sp.get_ip_address()}:{self.sp.get_port_number()}")


if __name__ == "__main__":
    node_info = NodeInformation()
    node_info.set_id(random.randint(0, 2 ** M))
    node_info.start()

    while True:
        command = input("Enter command (create / join <ip> <port> / printstate / print / port [number] / put <key> <value> / get <key> / exit): ")
        if command == "create":
            node_info.create_ring()
        elif command.startswith("join"):
            _, ip, port = command.split()
            node_info.join_ring(ip, int(port))
        elif command == "printstate":
            node_info.print_state()
        elif command == "print":
            node_info.print_keys()
        elif command.startswith("port"):
            _, *new_port = command.split()
            if new_port:
                new_port_number = int(new_port[0])
                if node_info.sp.port_in_use(new_port_number):
                    print(f"Port {new_port_number} is already in use.")
                else:
                    node_info.sp.close_socket()
                    node_info.sp.port_no_server = new_port_number
                    node_info.sp.specify_port_server()
                    print(f"Port changed to {new_port_number}.")
            else:
                print(f"Listening on port: {node_info.sp.get_port_number()}")
        elif command.startswith("put"):
            _, key, value = command.split()
            node_info.store_key(key, value)
            print(f"Stored key: {key}, value: {value}")
        elif command.startswith("get"):
            _, key = command.split()
            value = node_info.get_value(key)
            if value is not None:
                print(f"Value for key '{key}': {value}")
            else:
                print(f"Key '{key}' not found.")
        elif command == "exit":
            break

    node_info.sp.close_socket()
