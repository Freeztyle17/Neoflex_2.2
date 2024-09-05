# Задание №2  

В предыдущем задании вы занимались восстановлением потерянных данных и сломавшегося функционала расчёта витрины 101-й формы в реляционной БД (Oracle / PostgreSQL).  
Во время общения с архитектором вы узнаёте, что банк решил мигрировать свой DWH в Hadoop, а ETL-процессы перевести на Spark.  
В виду этого необходимо настроить среду для исследования возможностей новых инструментов и того как в них можно переложить существующий ETL-функционал.  

## Задача 2.1

В помощь, архитектор поделился с вами скриптами по настройке среды разработки со Spark.  
По мимо этого он отправил вам файл «Athletes.csv» и попросил выполнить несколько запросов для проверки работоспособности Spark-приложений.  
Настройте виртуальную машину с Ubuntu, установите на неё Spark (PySpark по желанию). 
Так же вы можете установить среду разработки - например Jupyter. 
Команды по установке и первичной настройке находятся в файле "ubuntu_commands.txt", рядом с ним ещё должен быть прикреплён файл «PySpark_Simple_example.txt».


### Проверочные задачи:

1) Сгенерировать DataFrame из трёх колонок (row_id, discipline, season) - олимпийские дисциплины по сезонам.  

row_id - число порядкового номера строки;  
discipline - наименование олимпиский дисциплины на английском (полностью маленькими буквами);  
season - сезон дисциплины (summer / winter);  
> *Укажите не мнее чем по 5 дисциплин для каждого сезона.  

Сохраните DataFrame в csv-файл, разделитель колонок табуляция, первая строка должна содержать название колонок.  
Данные должны быть сохранены в виде 1 csv-файла а не множества маленьких.  

---------------------------------------------------------------------------------

2) Прочитайте исходный файл "Athletes.csv".  

Посчитайте в разрезе дисциплин сколько всего спортсменов в каждой из дисциплин принимало участие.  
Результат сохраните в формате parquet.  

---------------------------------------------------------------------------------

3) Прочитайте исходный файл "Athletes.csv".

Посчитайте в разрезе дисциплин сколько всего спортсменов в каждой из дисциплин принимало участие.

Получившийся результат нужно объединить с сгенерированным вами DataFrame из 1-го задания и в итоге вывести количество участников, только по тем дисциплинам, что есть в вашем сгенерированном DataFrame.

Результат сохраните в формате parquet.


## Задача 2.2

Вы проверили работоспособность нового ETL-инструмента (Spark) и теперь с его помощью необходимо разработать один из типовых процессов для банковского хранилища - построение зеркал.  

Зеркало - это таблица в сыром или детальном слое, в которой содержится последнее (актуальное) состояние каждой еë сущности (записи/строки).
То есть, если 3 раза запускался процесс загрузки, который выделял дельту и в каждой такой дельте была одна и так же сущность (какой-то один счëт), у которой менялись вторичные параметры - то в зеркале должна остаться одна строка с последним состоянием, каждого из вторичных параметров.  

Для того что бы такой процесс был возможным - у таблицы обязательно должен быть уникальный ключ (id или номер счëта, например).  

Данный механизм необходимо сделать универсальным - в него должны передаваться:  путь где хранятся дельты, наименование таблицы и список полей, являющимся уникальным ключём. Согласовано, что дельты внутри указанного пути (основной директории) всегда хранятся в виде вложенных директорий с наименованием в порядке возрастания (например: 1000, 1001…..) – это тоже нужно заложить в универсальный механизм.  

Для демонстрации работоспособности вашего приложения нужно будет обработать 4 дельты с изменениями некоторых параметров в справочнике счетов. Каждая дельта - это csv- файл внутри директории с наименованием id- дельты (id сессии загрузки).  

Итоговый результат должен быть сохранëн в csv- файл в директории «mirr_md_account_d».  

Важно добавить в этот механизм процесс логирования, что бы не обрабатывать повторно, ранее загруженные дельты - это будет экономить время и ресурсы сервера.  
