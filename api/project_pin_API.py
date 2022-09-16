from email import message
from fastapi import FastAPI
from pydantic import BaseModel
import uvicorn
from json import dumps
from kafka import KafkaProducer

#Create an instance of FastAPI
app = FastAPI()

class Data(BaseModel):
    """
    Pydantic model to cast parameters to the correct type

    Attributes:
        category (str): The category of the entry
        index (int): The index of the entry
        unique_id (str): The unique id of the entry
        title (str): The title of the entry
        description (str): The title of the entry
        poster_name (str): The title of the entry
        follower_count (str): The number of followers of the entry
        tag_list (str): The tags of the entry
        image_or_video (str): The lable indicating whether entry is image or video
        image_src (str): The image urls of the entry
        downloaded (int): The number containing 0(not downloaded) or 1(downloaded)
        save_location (str): The save location of where the entry saved

    """
    category: str
    index: int
    unique_id: str
    title: str
    description: str
    poster_name: str
    follower_count: str
    tag_list: str
    is_image_or_video: str
    image_src: str
    downloaded: int
    save_location: str

# Create Producer to send message to a kafka topic
td_producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    client_id="Pinterest data producer",
    value_serializer=lambda mlmessage: dumps(mlmessage).encode("ascii")
) 

# An API execute some actions when a post request to localhost:9092/pin/
@app.post("/pin/")
def get_db_row(item: Data):
    data = dict(item)
    print(data)
    td_producer.send(topic="Pinterest_data", value=data)
    return item


if __name__ == '__main__':
    uvicorn.run("project_pin_API:app", host="localhost", port=8000)
