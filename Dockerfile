FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY bot.py .
COPY static/ static/
EXPOSE 8000
CMD ["sh", "-c", "python3 bot.py"]
