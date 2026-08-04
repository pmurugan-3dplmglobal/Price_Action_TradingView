# Oracle Cloud Free Tier Deployment Guide
## Price Action Unified Strategy System (Prod Code v02)

This document provides a comprehensive, step-by-step guide to deploying the Price Action Trading Strategy system on **Oracle Cloud Always Free Tier** (Ubuntu 22.04 / 24.04 LTS).

---

## Overview of Components To Deploy
1. **Options Dashboard**: `Trade_Option/app_option_Trade.py` (Port `6060`)
2. **Stock Dashboard**: `Trade_Stock/app_Sock_Trade.py` (Port `6061`)
3. **Automated Export Scheduler Daemon**: `Trade_Option/run_export_scheduler_daemon.py` (Runs at 10:30 AM, 1:00 PM, 3:15 PM)

---

## Phase 1: Oracle Cloud Instance & Security Setup

### Step 1: Create Oracle Cloud VM Instance
1. Log into your **Oracle Cloud Console**.
2. Go to **Compute** $\rightarrow$ **Instances** $\rightarrow$ **Create Instance**.
3. **Name**: `trading-strategy-server`
4. **Image**: `Ubuntu 24.04` or `Ubuntu 22.04 Minimal`
5. **Shape**: Select **Ampere (ARM)** — 2 to 4 OCPUs, 12 to 24 GB RAM (Always Free Eligible), or AMD `VM.Standard.E2.1.Micro`.
6. **SSH Keys**: Download/save your private SSH key (`.key` or `.pem` file).
7. Click **Create**. Note down your instance's **Public IP Address** (e.g., `129.x.x.x`).

---

### Step 2: Open Ports in Oracle VCN Security List
*Oracle Cloud blocks external web ports by default. You must allow incoming traffic on ports `5050` and `5051`.*

1. In Oracle Console, go to **Networking** $\rightarrow$ **Virtual Cloud Networks (VCN)**.
2. Click your VCN $\rightarrow$ **Security Lists** $\rightarrow$ Click **Default Security List**.
3. Click **Add Ingress Rules**:
   - **Source CIDR**: `0.0.0.0/0`
   - **IP Protocol**: `TCP`
   - **Destination Port Range**: `5050,5051`
   - **Description**: `Trading Dashboards (Options & Stock)`
4. Click **Add Ingress Rules**.

---

## Phase 2: Server Preparation & OS Firewall Config

### Step 3: SSH into your Instance
Open your terminal (PowerShell or Git Bash) on your computer and SSH into Oracle Cloud:
```bash
ssh -i /path/to/your-private-key.key ubuntu@<YOUR_SERVER_IP>
```

### Step 4: Update System Packages & Install Python Environment
Once logged into the server, run:
```bash
sudo apt update && sudo apt upgrade -y
sudo apt install -y python3 python3-pip python3-venv git curl iptables-persistent
```

### Step 5: Open Ports on Ubuntu OS Firewall
Oracle Ubuntu instances use `iptables`. Open ports `5050` & `5051` by running:
```bash
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 5050 -j ACCEPT
sudo iptables -I INPUT 6 -m state --state NEW -p tcp --dport 5051 -j ACCEPT
sudo netfilter-persistent save
```

---

## Phase 3: Code Deployment & Virtual Environment Setup

### Step 6: Clone Project Code to Server
Clone your Git repository into the home directory:
```bash
cd /home/ubuntu
git clone <YOUR_GIT_REPOSITORY_URL> Price_Action_Strategy
cd Price_Action_Strategy
```

### Step 7: Create & Activate Python Virtual Environment
```bash
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
pip install kiteconnect pandas numpy flask openpyxl requests schedule
```

---

## Phase 4: Setup Systemd Auto-Start Services

We will configure systemd so the web dashboards and background export daemon run automatically 24/7 and auto-restart if the server reboots.

### Step 8: Create Options Dashboard Service (Port 5050)
Run:
```bash
sudo nano /etc/systemd/system/trading-options.service
```
Paste the following content:
```ini
[Unit]
Description=Price Action Options Trading Dashboard (Port 5050)
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/Price_Action_Strategy/Trade_Option
ExecStart=/home/ubuntu/Price_Action_Strategy/venv/bin/python app_option_Trade.py
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```
*Press `Ctrl + O`, `Enter`, then `Ctrl + X` to save and exit.*

---

### Step 9: Create Stock Dashboard Service (Port 5051)
Run:
```bash
sudo nano /etc/systemd/system/trading-stock.service
```
Paste the following content:
```ini
[Unit]
Description=Price Action Stock Trading Dashboard (Port 5051)
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/Price_Action_Strategy/Trade_Stock
ExecStart=/home/ubuntu/Price_Action_Strategy/venv/bin/python app_Sock_Trade.py
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```
*Press `Ctrl + O`, `Enter`, then `Ctrl + X` to save and exit.*

---

### Step 10: Create Background Export Scheduler Service
Run:
```bash
sudo nano /etc/systemd/system/trading-export.service
```
Paste the following content:
```ini
[Unit]
Description=Price Action Automated Strategy Export Scheduler Daemon
After=network.target

[Service]
User=ubuntu
WorkingDirectory=/home/ubuntu/Price_Action_Strategy
ExecStart=/home/ubuntu/Price_Action_Strategy/venv/bin/python Trade_Option/run_export_scheduler_daemon.py
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
```
*Press `Ctrl + O`, `Enter`, then `Ctrl + X` to save and exit.*

---

### Step 11: Enable and Start All Services
Run the following commands to reload systemd and start all 3 services:
```bash
sudo systemctl daemon-reload
sudo systemctl enable trading-options trading-stock trading-export
sudo systemctl start trading-options trading-stock trading-export
```

---

## Phase 5: Verification & Maintenance

### Step 12: Check Service Status & Logs
Verify all 3 services are active and running:
```bash
sudo systemctl status trading-options
sudo systemctl status trading-stock
sudo systemctl status trading-export
```

To view live application logs:
```bash
journalctl -u trading-options -f
journalctl -u trading-stock -f
journalctl -u trading-export -f
```

### Step 13: Test Browser Access
Open your browser and verify accessibility:
- **Options Dashboard**: `http://<YOUR_SERVER_IP>:5050`
- **Stock Dashboard**: `http://<YOUR_SERVER_IP>:5051`
