В этом проекте я сделал рекомендательную систему на основе CatBoostClassifier, которая работает на сервере и выдает 5 предложенных постов по адресу {server_address}/post/recommendations?id=<user_id>&time=<your_time>&limit=5

Таблицы пользователей и постов хранятся в базе данных, они скачиваются из бд в app.py, а создаются на бд в make_table.py

Модель и данные для обучения(dataframe.csv в .gitignore) создаются и сохраняются в main.ipynb