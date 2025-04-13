import logging
import sys
import asyncio

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from app.api.routes import message_router, conversation_router
from app.controllers.message_controller import MessageController
from app.controllers.conversation_controller import ConversationController
from app.db.cassandra_client import get_cassandra_client


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="FB Messenger API",
    description="Backend API for FB Messenger implementation using Cassandra",
    version="1.0.0"
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"], 
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

def get_message_controller():
    return MessageController()

def get_conversation_controller():
    return ConversationController()

app.dependency_overrides[MessageController] = get_message_controller
app.dependency_overrides[ConversationController] = get_conversation_controller

app.include_router(message_router)
app.include_router(conversation_router)

@app.get("/")
async def root():
    return {"message": "FB Messenger API is running with Cassandra backend"}

@app.on_event("startup")
async def startup_event():
    logger.info("Initializing application...")
    max_retries = 20

    for i in range(max_retries):
        try:
            cassandra = get_cassandra_client()
            cassandra.get_session()
            logger.info("Cassandra connection established")
            return
        except Exception as e:
            logger.warning(f"[{i+1}/{max_retries}] Cassandra not ready yet: {str(e)}")
            await asyncio.sleep(3)

    logger.error("Failed to connect to Cassandra after multiple retries. Exiting.")
    sys.exit(1)


@app.on_event("shutdown")
async def shutdown_event():
    logger.info("Shutting down application...")
    get_cassandra_client().close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("app.main:app", host="0.0.0.0", port=8000, reload=True)
