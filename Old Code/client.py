import xmlrpc.client

class ChordClient:
    def __init__(self, node_port):
        self.node = xmlrpc.client.ServerProxy(f"http://localhost:{node_port}")

    def store_data(self, key, value):
        successor_port = self.node.find_successor(key)
        successor_node = xmlrpc.client.ServerProxy(f"http://localhost:{successor_port}")
        successor_node.store(key, value)
        print(f"Stored {key}:{value} at node {successor_port}")

    def retrieve_data(self, key):
        successor_port = self.node.find_successor(key)
        successor_node = xmlrpc.client.ServerProxy(f"http://localhost:{successor_port}")
        value = successor_node.retrieve(key)
        if value:
            print(f"Retrieved {key}:{value} from node {successor_port}")
        else:
            print(f"Key {key} not found in the network")

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 4:
        print("Usage: python chord_client.py <node_port> <operation> <key> [<value>]")
        sys.exit(1)

    client = ChordClient(int(sys.argv[1]))
    operation = sys.argv[2]
    key = int(sys.argv[3])

    if operation == "store":
        value = sys.argv[4]
        client.store_data(key, value)
    elif operation == "retrieve":
        client.retrieve_data(key)
    else:
        print("Invalid operation. Use 'store' or 'retrieve'")
