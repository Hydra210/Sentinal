# FastAPI Application

from fastapi import FastAPI
from pydantic import BaseModel
import databases
import sqlalchemy

DATABASE_URL = "sqlite:///./test.db"
database = databases.Database(DATABASE_URL)
metadata = sqlalchemy.MetaData()

app = FastAPI()

class Item(BaseModel):
    name: str
    description: str = None

@app.on_event("startup")
async def startup():
    await database.connect()

@app.on_event("shutdown")
async def shutdown():
    await database.disconnect()

@app.post("/items/")
async def create_item(item: Item):
    query = "INSERT INTO items(name, description) VALUES (:name, :description)"
    await database.execute(query, item.dict())
    return item

@app.get("/items/{item_id}")
async def read_item(item_id: int):
    query = "SELECT * FROM items WHERE id = :id"
    return await database.fetch_one(query, values={"id": item_id})

# Monitoring Loop
import time

async def monitor():
    while True:
        # Monitoring code here
        time.sleep(60)  # Sleep for a minute

if __name__ == "__main__":
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(monitor())