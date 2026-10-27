# Distributed Systems Midterm — Environment Setup

**Course:** 2601_1462_Distributed Systems

## 1. Required Technologies

- Python and a virtual environment
- Git and GitHub
- Docker and Docker Compose
- Kubernetes
- Prometheus
- Grafana
- Apache Kafka
- RabbitMQ

## 2. Installed and Verified

| Technology | Current status |
|---|---|
| Python 3.11.9 | Installed |
| Git 2.53.0 | Installed |
| Docker 29.1.3 | Working |
| Docker Compose v5.0.0 | Available |
| Kubernetes client v1.34.1 | Installed |
| Kubernetes cluster | Running; node Ready |
| WSL 3.0.3 | Installed |
| GitHub repository | Connected; push succeeded |

**Docker resources:** 12 CPUs and approximately 7.6 GiB RAM available.

## 3. Project Location

```text
Desktop/
└── DS-Midterm/
    ├── .git/
    ├── .gitignore
    ├── .venv/
    ├── main.py
    └── MIDTERM-SETUP.md
```

The `.venv/` and `.git/` directories should not be uploaded as ordinary project files. The virtual environment is excluded using `.gitignore`; Git manages its own `.git/` directory.

## 4. Activate Python Environment

Open PowerShell and run:

```powershell
cd "$HOME\Desktop\DS-Midterm"
.\.venv\Scripts\Activate.ps1
```

Verify Python:

```powershell
python --version
python -m pip --version
python main.py
```

The terminal should display `(.venv)` when the environment is active.

## 5. Git Commands to Remember

Check changes:

```powershell
git status
```

Stage project files:

```powershell
git add .
```

Save a commit:

```powershell
git commit -m "Describe your changes"
```

Upload commits to GitHub:

```powershell
git push
```

Download remote changes:

```powershell
git pull
```

Check the configured remote:

```powershell
git remote -v
```

## 6. Docker Commands to Remember

Check Docker:

```powershell
docker --version
docker info
docker ps
```

Run a test container:

```powershell
docker run --rm hello-world
```

Check Docker Compose:

```powershell
docker compose version
```

Useful container commands:

```powershell
docker ps -a
docker logs CONTAINER_NAME
docker stop CONTAINER_NAME
docker images
```

Replace `CONTAINER_NAME` with the actual container name.

## 7. Kubernetes Commands to Remember

Check the client:

```powershell
kubectl version --client
```

Check the active context:

```powershell
kubectl config current-context
```

List cluster nodes:

```powershell
kubectl get nodes
```

List pods:

```powershell
kubectl get pods -A
```

List deployments and services in the default namespace:

```powershell
kubectl get deployments
kubectl get services
```

## 8. Distributed Systems Technologies — Purpose

- **Docker:** Packages applications and dependencies into containers.
- **Docker Compose:** Defines and runs multiple related containers.
- **Kubernetes:** Orchestrates containers and manages workloads across a cluster.
- **Prometheus:** Collects and stores monitoring metrics.
- **Grafana:** Visualizes metrics in dashboards.
- **Apache Kafka:** Distributed event-streaming platform for publishing, storing, and consuming event streams.
- **RabbitMQ:** Message broker that routes messages between producers and consumers.

## 9. Remaining Preparation

- Create and test a Docker Compose configuration.
- Start and verify Kafka and RabbitMQ.
- Start and verify Prometheus and Grafana.
- Review basic Python, Git, Docker, and Kubernetes commands.
- Commit configuration files to GitHub.

## 10. Important Notes

- The exact quiz format is not yet known.
- Installing a tool does not guarantee that its service or cluster is running.
- Check `docker ps` before starting more services.
- Do not commit passwords, API keys, access tokens, or `.env` files.
- Keep the virtual environment out of Git; dependencies can later be recorded in `requirements.txt`.
