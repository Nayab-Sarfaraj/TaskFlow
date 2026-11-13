from fastapi import FastAPI
from src.routers.jobs_router import router as job_router
from src.logging_config import configure_uvicorn_logging

configure_uvicorn_logging()

app = FastAPI()


@app.get("/")
def read_root():
    return {"status": "running"}


app.include_router(router=job_router, prefix="/api/v1/jobs")
