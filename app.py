import os
import pandas as pd
from typing import List
from fastapi import FastAPI
from datetime import datetime
from pydantic import BaseModel
from sqlalchemy import create_engine



def batch_load_sql(query: str) -> pd.DataFrame:
    CHUNKSIZE = 200000
    engine = create_engine(
        "<your_connection_url>"
    )
    conn = engine.connect().execution_options(stream_results=True)
    chunks = []
    for chunk_dataframe in pd.read_sql(query, conn, chunksize=CHUNKSIZE):
        chunks.append(chunk_dataframe)
    conn.close()
    return pd.concat(chunks, ignore_index=True)

def load_features(query):
    df = batch_load_sql(query)
    return df

from catboost import CatBoostClassifier
def get_model_path(path: str) -> str:
    if os.environ.get("IS_LMS") == "1":  # проверяем где выполняется код в лмс, или локально. Немного магии
        MODEL_PATH = '/workdir/user_input/model'
    else:
        MODEL_PATH = path
    return MODEL_PATH

def load_models():
    model_path = get_model_path("<model_path>")
    from_file = CatBoostClassifier()
    model = from_file.load_model(model_path)
    return model
app = FastAPI()

class PostGet(BaseModel):
    id: int
    text: str
    topic: str

    class Config:
        orm_mode = True

model = load_models()
dataframe = load_features('select * from karapetian30_features_lesson_22_main')
posts = load_features('select * from karapetian30_features_lesson_22_posts limit 150')
@app.get("/post/recommendations/", response_model=List[PostGet])
def recommended_posts(
		id: int, 
		time: datetime, 
		limit: int = 10) -> List[PostGet]:
    df = dataframe[dataframe['user_id'] == id].copy()
    df['timestamp'] = time
    df['prob'] = model.predict_proba(df[model.feature_names_])[:, 1]
    mapping = {}
    for val in df['topic']:
        mapping[val] = df[df['topic'] == val].prob.values[0]
    posts['prob'] = posts['topic'].map(mapping)
    posts.sort_values(by = 'prob',ascending=False, inplace=True)
    lst = posts.head()[['post_id', 'text', 'topic']].values.tolist()
    result_list = [PostGet(id=item[0], text=item[1], topic=item[2]) for item in lst]
    return result_list
if __name__ == '__main__':
    app.run()