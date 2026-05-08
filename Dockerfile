FROM python:3.13-slim

# Set working directory
WORKDIR /app

# Install dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .

# Railway sets PORT dynamically; default to 8000 for local builds
ENV PORT=8000

# Expose the port (informational — Railway uses $PORT)
EXPOSE $PORT

# Run the app — use shell to expand $PORT at runtime
CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8000}"]
