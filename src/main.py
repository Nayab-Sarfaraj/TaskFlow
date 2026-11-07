from fastapi import FastAPI
from src.routers.jobs_router import router as job_router


app = FastAPI()

@app.get("/")
def read_root():
    return {"status": "running"}

app.include_router(router=job_router,prefix="/api/v1/jobs")

