import os
import uvicorn

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8000))
    # reload=True is for local dev only; Railway/production should use the
    # Procfile or Dockerfile CMD which omit --reload.
    uvicorn.run("app.main:app", host="0.0.0.0", port=port, reload=True)
