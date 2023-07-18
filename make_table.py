import pandas as pd
from sqlalchemy import create_engine


engine = create_engine(
    "<your_connection_url")


table_name_main= "karapetian30_features_lesson_22_main"
table_name_posts = "karapetian30_features_lesson_22_posts"
conn_url = "your_connection_url"

main = pd.read_csv('dataframe.csv')
posts = pd.read_sql(
    "select * from public.post_text_df;",
    conn_url)

main.to_sql(table_name_main, engine, if_exists='replace', index=False, chunksize=5000)
posts.to_sql(table_name_posts, engine, if_exists='replace', index=False, chunksize=5000)
