import asyncio
from datetime import datetime, timedelta

from fastapi import Depends, FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.security import OAuth2PasswordRequestForm
from jose import jwt
from passlib.context import CryptContext
from starlette.datastructures import State

from api.v1 import handle
from api.v1 import models
from config import redis, CONFIG

# to get a string like this run:
# openssl rand -hex 32
USERNAME = CONFIG['api']['username']
PASSWORD = CONFIG['api']['password']
SECRET_KEY = CONFIG['api']['secret_key']
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = CONFIG['api']['access_to_token_expiry_minutes']  # 10000 hours

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")

app = FastAPI(
    title="API endpoints Binance Price",
    description=f"A prototype of mounting the main API app under /api",
    version="1.0"
)
app.include_router(handle.router, prefix='/api')
origins = ["*"]

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def authenticate_token(username: str, password: str):
    if username in USERNAME and password in PASSWORD:
        return True
    return False


def create_access_token(data: dict, expires_delta: timedelta | None = None):
    to_encode = data.copy()
    if expires_delta:
        expire = datetime.utcnow() + expires_delta
    else:
        expire = datetime.utcnow() + timedelta(minutes=15)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt


@app.post("/token", response_model=models.Token)
async def login_for_access_token(form_data: OAuth2PasswordRequestForm = Depends()):
    res = authenticate_token(form_data.username, form_data.password)
    if not res:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password",
            headers={"WWW-Authenticate": "Bearer"},
        )
    access_token_expires = timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    access_token = create_access_token(
        data={"sub": form_data.username}, expires_delta=access_token_expires
    )
    return {"access_token": access_token, "token_type": "bearer"}


@app.on_event("startup")
async def startup() -> None:
    State.redis = redis
    State.secret = SECRET_KEY
    State.username = USERNAME
    State.task_run_klines = asyncio.create_task(handle.run_klines(State.q))


@app.on_event("shutdown")
async def shutdown() -> None:
    _redis = State.redis
    await _redis.close()
    task_run_klines = State.task_run_klines
    task_run_klines.cancel()
    try:
        await task_run_klines
    except asyncio.CancelledError:
        print("Task run_klines() is cancelled now")
