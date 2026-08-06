#!/bin/bash
# ==============================================================================
# Price Action TradingView Open-Source Edition — Multi-OS Cloud Deployment Script
# Supports: Oracle Linux (opc), Ubuntu (ubuntu), Debian, RHEL, CentOS
# Services: tradingview-options.service (Port 6060), tradingview-stock.service (Port 6061)
# ==============================================================================
set -e

echo "🚀 Starting Price Action TradingView Open-Source Deployment..."

# 1. Detect Package Manager & Install Dependencies
if command -v apt &> /dev/null; then
    echo "📦 Detected Ubuntu/Debian OS (apt)..."
    sudo apt update && sudo apt upgrade -y
    sudo apt install -y python3 python3-pip python3-venv git curl iptables-persistent
elif command -v dnf &> /dev/null; then
    echo "📦 Detected Oracle Linux / RHEL OS (dnf)..."
    sudo dnf update -y
    sudo dnf install -y python3 python3-pip git curl || sudo dnf install -y python3 git curl
elif command -v yum &> /dev/null; then
    echo "📦 Detected CentOS / RHEL OS (yum)..."
    sudo yum update -y
    sudo yum install -y python3 python3-pip git curl
else
    echo "⚠️ Unknown package manager. Proceeding with existing python3 installation..."
fi

# 2. Open OS Firewall Ports 6060 & 6061
echo "🔥 Configuring OS Firewall (Opening ports 6060 & 6061)..."
if command -v firewall-cmd &> /dev/null && sudo systemctl is-active --quiet firewalld; then
    sudo firewall-cmd --permanent --add-port=6060/tcp || true
    sudo firewall-cmd --permanent --add-port=6061/tcp || true
    sudo firewall-cmd --reload || true
fi

sudo iptables -I INPUT 1 -p tcp --dport 6060 -j ACCEPT || true
sudo iptables -I INPUT 1 -p tcp --dport 6061 -j ACCEPT || true

if command -v netfilter-persistent &> /dev/null; then
    sudo netfilter-persistent save || true
elif command -v service &> /dev/null; then
    sudo service iptables save || true
fi

# 3. Setup Virtual Environment
APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
echo "🐍 Setting up Python Virtual Environment in $APP_DIR/venv ..."
cd "$APP_DIR"
if [ ! -d "venv" ]; then
    python3 -m venv venv || virtualenv venv
fi
source venv/bin/activate
pip install --upgrade pip
pip install pandas numpy flask openpyxl requests schedule yfinance

# 4. Fix SELinux Context for Oracle Linux / RHEL Systemd Execution
if command -v getenforce &> /dev/null && [ "$(getenforce)" != "Disabled" ]; then
    echo "🛡️ Setting SELinux permissions for virtual environment..."
    sudo chcon -R -t bin_t "$APP_DIR/venv/bin/" || true
fi

# 5. Create Systemd Service: Options Dashboard (Port 6060)
echo "⚙️ Creating Systemd Service: tradingview-options.service (Port 6060)..."
sudo bash -c "cat <<EOF > /etc/systemd/system/tradingview-options.service
[Unit]
Description=Price Action TradingView Options Dashboard (Port 6060)
After=network.target

[Service]
User=$USER
WorkingDirectory=$APP_DIR/Trade_Option
ExecStart=$APP_DIR/venv/bin/python $APP_DIR/Trade_Option/app_option_Trade.py
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF"

# 6. Create Systemd Service: Stock Dashboard (Port 6061)
echo "⚙️ Creating Systemd Service: tradingview-stock.service (Port 6061)..."
sudo bash -c "cat <<EOF > /etc/systemd/system/tradingview-stock.service
[Unit]
Description=Price Action TradingView Stock Dashboard (Port 6061)
After=network.target

[Service]
User=$USER
WorkingDirectory=$APP_DIR/Trade_Stock
ExecStart=$APP_DIR/venv/bin/python $APP_DIR/Trade_Stock/app_Sock_Trade.py
Restart=always
RestartSec=5
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF"

# 7. Create Systemd Service: Export Scheduler Daemon
echo "⚙️ Creating Systemd Service: tradingview-export.service..."
sudo bash -c "cat <<EOF > /etc/systemd/system/tradingview-export.service
[Unit]
Description=Price Action TradingView Export Scheduler Daemon
After=network.target

[Service]
User=$USER
WorkingDirectory=$APP_DIR
ExecStart=$APP_DIR/venv/bin/python $APP_DIR/Trade_Option/run_export_scheduler_daemon.py
Restart=always
RestartSec=10
Environment=PYTHONUNBUFFERED=1

[Install]
WantedBy=multi-user.target
EOF"

# 8. Enable and Start Services
echo "🔄 Reloading systemd daemon & starting services..."
sudo systemctl daemon-reload
sudo systemctl enable tradingview-options tradingview-stock tradingview-export
sudo systemctl restart tradingview-options tradingview-stock tradingview-export

echo ""
echo "=============================================================================="
echo "✅ TRADINGVIEW OPEN-SOURCE DEPLOYMENT COMPLETE!"
echo "=============================================================================="
echo "📊 Services Status:"
sudo systemctl status tradingview-options --no-pager | head -n 5 || true
echo "---"
sudo systemctl status tradingview-stock --no-pager | head -n 5 || true
echo "---"
sudo systemctl status tradingview-export --no-pager | head -n 5 || true
echo ""
echo "🌐 Options Dashboard: http://<YOUR_SERVER_IP>:6060"
echo "🌐 Stock Dashboard:   http://<YOUR_SERVER_IP>:6061"
echo "=============================================================================="
