class StreamlitWS {
    constructor() {
        this.socket = new WebSocket(`ws://${window.location.hostname}:8000/ws`);
        this.setupHandlers();
    }

    setupHandlers() {
        this.socket.onmessage = (event) => {
            const data = JSON.parse(event.data);
            
            if (data.action === "redirect") {
                window.location.href = data.url;
            }
            
            if (data.update) {
                this.showNotification(data.update);
            }
        };

        this.socket.onclose = () => {
            setTimeout(() => new StreamlitWS(), 1000);
        };
    }

    showNotification(message) {
        const notification = document.createElement("div");
        notification.className = "ws-notification";
        notification.innerHTML = message;
        document.body.appendChild(notification);
        setTimeout(() => notification.remove(), 3000);
    }
}

// Initialize on page load
new StreamlitWS();