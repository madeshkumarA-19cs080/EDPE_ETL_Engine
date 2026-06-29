document.addEventListener("DOMContentLoaded", () => {
    const testConnectionButton = document.getElementById("test-connection");
    const resetButton = document.getElementById("reset-form");
    const statusPanel = document.getElementById("connection-status");
    const refreshButton = document.getElementById("refresh-logs");
    const logOutput = document.getElementById("log-output");

    if (testConnectionButton) {
        testConnectionButton.addEventListener("click", async () => {
            const form = document.getElementById("connection-form");
            const formData = new FormData(form);
            const payload = {};
            formData.forEach((value, key) => {
                payload[key] = value;
            });

            statusPanel.textContent = "Testing connection...";
            try {
                const response = await fetch("/test_connection", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(payload),
                });
                const result = await response.json();
                if (response.ok && result.success) {
                    statusPanel.textContent = result.message;
                } else {
                    statusPanel.textContent = result.message || "Connection failed.";
                }
            } catch (error) {
                statusPanel.textContent = "Unable to reach the server.";
            }
        });
    }

    if (resetButton) {
        resetButton.addEventListener("click", () => {
            const form = document.getElementById("connection-form");
            form.reset();
            statusPanel.textContent = "";
        });
    }

    if (refreshButton && logOutput) {
        const fetchLogs = async () => {
            try {
                const response = await fetch("/logs_data");
                if (response.ok) {
                    logOutput.textContent = await response.text();
                }
            } catch (error) {
                logOutput.textContent = "Unable to load logs.";
            }
        };

        refreshButton.addEventListener("click", fetchLogs);
        fetchLogs();
        setInterval(fetchLogs, 5000);
    }
});
