from http.client import HTTPException
from fastapi import FastAPI, Query
import uvicorn
from services.sports_api.sports_api import SportsAPIConfig, TheOddsAPI
from http_client.requests_http_client import RequestsHTTPClient
import os
import dotenv
from fastapi.middleware.cors import CORSMiddleware
from services.event_ingestor.event_ingestor import EventIngestor
from services.props_ingestor.prop_ingestor import PropIngestor
import asyncio
from db.PostgresConnection import PostgresConnection
from repositories.prop_snapshots_repository import PropSnapshotsRepository

dotenv.load_dotenv()

app = FastAPI()
sports_api = TheOddsAPI(config=SportsAPIConfig(api_key=os.getenv("SPORTS_API_KEY"), base_url="https://api.the-odds-api.com/v4"), http_client=RequestsHTTPClient()) 


app.add_middleware(
      CORSMiddleware,
      allow_origins=["*"],
      allow_credentials=True,
      allow_methods=["*"],
      allow_headers=["*"],
)


# @app.get("/events")
# async def get_events(sport: str = "basketball_nba"):
#     event_ingestor = EventIngestor(BetAPI=sports_api)
#     events = await event_ingestor.ingest_events(sport=sport, hours=24)
#     return {"events": events}

@app.get("/test")
async def get_props():
    data = await ingest_events()
    return data


#Ingest Events
async def ingest_events():
    event_ingestor = EventIngestor(BetAPI=sports_api)
    events = await event_ingestor.ingest_events(sport="basketball_nba", hours=24)
    

    prop_ingestor = PropIngestor(BetAPI=sports_api, prop_snapshots_repository=props_snapshots_repo)
    props = await prop_ingestor.ingest_props(events=events, markets=["player_points", "player_points_q1", "player_triple_double"], region="us", sport="basketball_nba")
    print(props)
    return props




if __name__ == "__main__":
    db = PostgresConnection(connection_string=os.getenv("DATABASE_URL"), minconn=1, maxconn=5)
    global props_snapshots_repo
    props_snapshots_repo = PropSnapshotsRepository(db=db)
    db.initiate_connection()
    uvicorn.run(app, host="0.0.0.0", port=8000)
    #asyncio.run(ingest_events())









# if __name__ == "__main__":
#     uvicorn.run(app, host="0.0.0.0", port=8000)

    