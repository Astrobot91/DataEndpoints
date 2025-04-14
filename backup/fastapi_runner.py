from fastapi import FastAPI
from endpoints import router
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(title="Trading API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router, prefix="/api/v1/upstox", tags=["upstox"])


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)