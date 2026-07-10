# HVLP - Hilscher Variable Length Protocol

HVLP is a simple protocol for sending messages between clients and a broker. The protocol is 
based on the MQTT protocol and is implemented in Python. The main focus of the project is to
demonstrate the use of OOP, SOLID principles, sockets and threading in Python. 

## Features

- Connect to a broker
- Subscribe to a topic
- Publish to a topic
- Disconnect from a broker
- Support for multiple clients

## Requirements

- Python 3.7 or newer
- The dependencies in `requirements.txt` (currently just `six`)

Install them, ideally into a virtual environment:

```bash
python -m venv .venv

# Windows (PowerShell):
.venv\Scripts\Activate.ps1
# Linux / macOS:
source .venv/bin/activate

pip install -r requirements.txt
```

## Project Setup

### 1. Clone the HVLP project

Use a git client to clone the project from https://github.com/braboj/demo-hvlp
to a folder of your choice.

### 2. Go to the project folder

Type `cd <path to project folder>` and install the dependencies as shown under
[Requirements](#requirements).

### 3. Open a new command terminal for the broker

From the project root, start the broker:

```bash
python -m hvlp.broker <IP ADDR> <PORT>
```

If the arguments are omitted, the default IP is `127.0.0.1` and the default
port is `65432`.

### 4. Open a new command terminal for a client

From the project root, start a client:

```bash
python -m hvlp.client <SERVER ADDR> <PORT>
```

If the arguments are omitted, the default server address is `127.0.0.1` and the
default port is `65432`.

### 5. Connect the client to the broker

Type `connect` in the client console to connect to the broker

![img.png](assets/images/connect.png)

### 6. Subscribe the client to a topic

Type `subscribe test` in the client console to subscribe to a topic

![img.png](assets/images/subscribe.png)

### 7. Repeat steps 4-6 to add a second client

![img.png](assets/images/two_clients.png)

### 8. Publish to a topic

Type publish test 1 in one of the client consoles.

![img.png](assets/images/publish.png)

### 9. Get help how to use the application

Type `help` in the client console for more commands

![img.png](assets/images/help.png)

### 10. Exit the applications

Type `quit` or press CTRL + C in a client console to exit the client. To exit the broker press 
CTRL + C. There might be a slight delay (1-2 seconds) until the application ends.

 

