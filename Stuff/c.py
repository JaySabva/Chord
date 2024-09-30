import xmlrpc.client

def get_node_info(port):
    try:
        # Connect to the node
        node = xmlrpc.client.ServerProxy(f'http://127.0.0.1:{port}')
        
        # Get successor and predecessor
        successor = node.get_successor()
        predecessor = node.get_predecessor()
        
        print(f'Node running on port {port}:')
        print(f'Successor: {successor}')
        print(f'Predecessor: {predecessor}')
        
    except Exception as e:
        print(f'Error connecting to node on port {port}: {e}')

if __name__ == "__main__":
    port = int(input('Enter the port number of the node to query: '))
    get_node_info(port)
