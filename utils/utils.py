import socket
import requests

def find_free_port():
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))
            return s.getsockname()[1]

def get_local_ip():
    """Get the local IP address of the machine."""
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.connect(("8.8.8.8", 80))
    ip = s.getsockname()[0]
    s.close()
    return ip

def get_public_ip():
    """Get public IP address of the machine."""
    try:
        response = requests.get('https://api.ipify.org')
        if response.status_code == 200:
            return response.text
        else:
            print("Could not determine public IP, falling back to local IP")
            return get_local_ip()
    except Exception as e:
        print(f"Error determining public IP: {e}, falling back to local IP")
        return get_local_ip()
    
    
def check_port_externally_accessible(port):
        """Check if port is accessible from external machines"""
        public_ip = get_public_ip()
        print(f"[🔍] Checking if port {port} on {public_ip} is accessible externally...")
        
        try:
            import requests
            response = requests.get(f'https://portchecker.co/check?port={port}&host={public_ip}', timeout=5)
            if "Port is open" in response.text:
                print(f"[✅] Port {port} is externally accessible")
                return True
            else:
                print(f"[⚠️] Port {port} appears to be blocked by firewall")
                print(f"[ℹ️] You may need to run: sudo ufw allow {port}/tcp")
                return False
        except Exception as e:
            print(f"[⚠️] Could not verify external port accessibility: {e}")
            return False