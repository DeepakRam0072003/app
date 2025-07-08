class InventoryWebSocket {
    constructor() {
        this.clientId = 'inventory_' + Math.random().toString(36).substr(2, 9);
        this.socket = null;
        this.reconnectAttempts = 0;
        this.maxAttempts = 5;
        this.baseDelay = 1000;
        this.connect();
    }

    connect() {
        const protocol = window.location.protocol === 'https:' ? 'wss:' : 'ws:';
        this.socket = new WebSocket(`${protocol}//${window.location.hostname}:8000/ws/${this.clientId}`);

        this.socket.onopen = () => {
            this.reconnectAttempts = 0;
            this.updateStatus('connected', '#4CAF50');
            this.send({ type: 'register', page: 'inventory' });
            this.send({ type: 'subscribe', channel: 'inventory' });
        };

        this.socket.onmessage = (event) => {
            try {
                const data = JSON.parse(event.data);
                this.handleMessage(data);
            } catch (e) {
                console.error('WebSocket message error:', e);
            }
        };

        this.socket.onerror = (error) => {
            console.error('WebSocket error:', error);
        };

        this.socket.onclose = () => {
            if (this.reconnectAttempts < this.maxAttempts) {
                const delay = this.baseDelay * Math.pow(2, this.reconnectAttempts);
                this.updateStatus('reconnecting', '#FF9800');
                setTimeout(() => {
                    this.reconnectAttempts++;
                    this.connect();
                }, delay);
            } else {
                this.updateStatus('disconnected', '#F44336');
            }
        };
    }

    send(message) {
        if (this.socket && this.socket.readyState === WebSocket.OPEN) {
            this.socket.send(JSON.stringify({
                ...message,
                clientId: this.clientId,
                timestamp: new Date().toISOString()
            }));
        }
    }

    updateStatus(status, color) {
        const el = document.getElementById('ws-status');
        if (el) {
            el.innerHTML = `<small>Connection: <span style="color: ${color}">${status}</span></small>`;
        }
    }

    handleMessage(data) {
        switch (data.type) {
            case 'notification':
                this.showNotification(data.message, data.category);
                break;
                
            case 'refresh':
                if (data.channel === 'inventory' || data.channel === 'all') {
                    this.handleRefresh(data);
                }
                break;
                
            case 'data_update':
                if (data.target === 'inventory-table') {
                    this.updateTable(data.data);
                }
                break;
                
            default:
                console.log('Received unhandled message type:', data.type);
        }
    }

    showNotification(message, category = 'info') {
        const colors = {
            info: '#2196F3',
            success: '#4CAF50',
            warning: '#FF9800',
            error: '#F44336'
        };

        const notification = document.createElement('div');
        notification.className = 'ws-notification';
        notification.style.backgroundColor = colors[category] || colors.info;
        notification.innerHTML = `
            <span style="margin-right: 8px">${message}</span>
            <small style="opacity: 0.8; margin-left: 10px">
                ${new Date().toLocaleTimeString()}
            </small>
        `;

        document.body.appendChild(notification);
        
        setTimeout(() => {
            notification.style.animation = 'fadeOut 0.3s';
            setTimeout(() => notification.remove(), 300);
        }, 5000);
    }

    handleRefresh(data) {
        if (data.force || confirm('New data available. Refresh page?')) {
            window.location.reload();
        }
    }

    updateTable(data) {
        // Implement dynamic table updates here
        console.log('Updating table with:', data);
    }
}

// Initialize when Streamlit is ready
if (window.streamlitReady) {
    window.inventoryWS = new InventoryWebSocket();
} else {
    window.addEventListener('streamlit:ready', () => {
        window.inventoryWS = new InventoryWebSocket();
    });
}