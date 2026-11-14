FROM python:3.12-alpine
WORKDIR /app
COPY watcher.py config.json ./
COPY web ./web
EXPOSE 8080
CMD ["python", "watcher.py"]
