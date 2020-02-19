Описание проекта:

Аналитика для интернет-магазина с целью увеличить его выручку. С помощью фреймворков ICE и RICE выявлени приоритетные гипотезы, затем проведена чистка результатов A/B тестов от выбросов и искажений, результаты интерпретированы.

Необходимые модули:

- pandas
- numpy
- matplotlib
- scipy

Описание данных:

*Файл /datasets/hypothesis.csv:*

`Hypothesis` — краткое описание гипотезы;
`Reach` — охват пользователей по 10-балльной шкале;
`Impact` — влияние на пользователей по 10-балльной шкале;
`Confidence` — уверенность в гипотезе по 10-балльной шкале;
`Efforts` — затраты ресурсов на проверку гипотезы по 10-балльной шкале. Чем больше значение Efforts, тем дороже проверка гипотезы.

*Файл /datasets/orders.csv:*

`transactionId` — идентификатор заказа;
`visitorId` — идентификатор пользователя, совершившего заказ;
`date` — дата, когда был совершён заказ;
`revenue` — выручка заказа;
`group` — группа A/B-теста, в которую попал заказ.

*Файл /datasets/visitors.csv*

`date` — дата;
`group` — группа A/B-теста;
`visitors` — количество пользователей в указанную дату в указанной группе A/B-теста