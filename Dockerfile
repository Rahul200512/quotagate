# Only used to run several copies side by side on a laptop; Vercel builds the
# deployment from source and never sees this file.
FROM python:3.13-slim

WORKDIR /srv
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt uvicorn==0.34.0
COPY app.py ./
COPY quotagate ./quotagate

ENV PORT=8000
CMD ["sh", "-c", "uvicorn app:app --host 0.0.0.0 --port ${PORT} --log-level warning"]
