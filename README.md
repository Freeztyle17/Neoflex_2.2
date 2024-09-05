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
