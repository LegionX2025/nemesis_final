import os
import subprocess
import sys
import time
import threading
import re
from dotenv import load_dotenv
load_dotenv()

def update_wrangler_toml(tunnel_url):
    print(f"[DEPLOYER] Updating wrangler.toml with TUNNEL_URL: {tunnel_url}")
    try:
        with open("wrangler.toml", "r") as f:
            content = f.read()
        
        # Replace existing TUNNEL_URL if it exists
        if "TUNNEL_URL" in content:
            content = re.sub(r'TUNNEL_URL\s*=\s*".*"', f'TUNNEL_URL = "{tunnel_url}"', content)
        else:
            # Append it to [vars] or create [vars]
            if "[vars]" in content:
                content = content.replace("[vars]", f"[vars]\nTUNNEL_URL = \"{tunnel_url}\"")
            else:
                content += f'\n\n[vars]\nTUNNEL_URL = "{tunnel_url}"\n'
                
        with open("wrangler.toml", "w") as f:
            f.write(content)
    except Exception as e:
        print(f"[DEPLOYER] Failed to update wrangler.toml: {e}")

def main():
    print("==================================================")
    print("  NEMESIS AUTO-CLOUDFLARE BUILDER & DEPLOYER")
    print("  (Quick Tunnel Auto-Routing Mode)")
    print("==================================================")

    # 1. Start Backend & Quick Tunnel FIRST to get the URL
    print("\n>>> 1. Starting Local Backend and Cloudflare Quick Tunnel...")
    
    backend_cmd = f"{sys.executable} nemesis_core.py"
    
    cloudflared_path = os.path.abspath(os.path.join(os.getcwd(), "..", "cloudflared.exe"))
    if not os.path.exists(cloudflared_path):
        cloudflared_path = "cloudflared"
        
    tunnel_cmd = f'"{cloudflared_path}" tunnel --url http://127.0.0.1:8000'

    # Start Backend
    print(f"[BACKEND] Starting: {backend_cmd}")
    backend_proc = subprocess.Popen(backend_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    def stream_backend():
        for line in backend_proc.stdout:
            print(f"[BACKEND] {line}", end='')
    threading.Thread(target=stream_backend, daemon=True).start()

    time.sleep(2) # Give backend a moment to start

    # Start Tunnel
    print(f"[TUNNEL] Starting: {tunnel_cmd}")
    # Cloudflared outputs to stderr usually
    tunnel_proc = subprocess.Popen(tunnel_cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True)
    
    tunnel_url = None
    url_found = threading.Event()
    
    def stream_tunnel():
        nonlocal tunnel_url
        for line in tunnel_proc.stdout:
            print(f"[TUNNEL] {line}", end='')
            # Look for the trycloudflare URL
            match = re.search(r'(https://[a-zA-Z0-9-]+\.trycloudflare\.com)', line)
            if match and not tunnel_url:
                tunnel_url = match.group(1)
                url_found.set()
                
    threading.Thread(target=stream_tunnel, daemon=True).start()

    print("[DEPLOYER] Waiting for Cloudflare to assign a public URL...")
    # Wait up to 20 seconds for the URL
    if url_found.wait(timeout=20) and tunnel_url:
        print(f"\n[DEPLOYER] => Successfully acquired public URL: {tunnel_url}")
        
        # 2. Update wrangler.toml
        update_wrangler_toml(tunnel_url)
        
        # 3. Build the frontend
        print("\n>>> 2. Building Frontend...")
        result = subprocess.run([sys.executable, "build_frontend.py"], capture_output=True, text=True)
        if result.returncode == 0:
            print("Frontend built successfully into dist/ folder.")
        else:
            print("Error building frontend:")
            print(result.stdout)
            print(result.stderr)

        # 4. Deploy Frontend
        print("\n>>> 3. Deploying Frontend to Cloudflare Pages (via Wrangler)...")
        deploy_cmd = "npx wrangler pages deploy dist --project-name nemesis-frontend"
        deploy_result = subprocess.run(deploy_cmd, shell=True)
        if deploy_result.returncode != 0:
            print("Warning: Wrangler deploy failed.")
            
    else:
        print("\n[DEPLOYER] => Failed to acquire a public URL from Cloudflare Tunnel within 20 seconds.")
        print("[DEPLOYER] Check the tunnel logs above for errors.")

    print("\n==================================================")
    print(" SYSTEM IS ONLINE AND READY")
    print(" - Backend running locally on port 8000")
    if tunnel_url:
        print(f" - Cloudflare Tunnel active at: {tunnel_url}")
    print(" - Frontend Deployed to Cloudflare Pages")
    print(" Press Ctrl+C to stop all services.")
    print("==================================================\n")

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down services...")
        backend_proc.terminate()
        tunnel_proc.terminate()
        print("Shutdown complete.")

if __name__ == "__main__":
    main()
