# big_data_poly

Настройка проекта после клонирования
1. Install plugin как говорит IDEA
2. Сбилдить проект sbt

Дополнительно на Windows
1. git clone https://github.com/steveloughran/winutils.git
2. Создать папку winutil в любом месте, e.g. C:\winutil
3. Скопировать папку bin из hadoop-2.8.3 в C:\winutil
4. Добить env variable HADOOP_HOME, указывающую на C:\winutil
5. Добавить %HADOOP_HOME%\bin в PATH
6. Перезагруить компьютер

Распредление ID для скачивания
1. 1 - 250000000 - Юра
2. 250000001 - 500000000 - Женя
3. 500000001 - 750000000 - Сеня
4. 750000001 - last - Никита

# Собираем docker образ с hadoop
Предлагаю ставить докер отсюда https://www.docker.com/products/docker-desktop/
там можно будет выставить все квоты, СPU и memory выставляем на максимум минимум 16gb и 8 CPUs.
Если у вас 8Gb все парметры в файле `img-hdp-zeppelin/yarn-site.xml` разделите на два
и запускайте максиму с двумя токенами для приложение и 2 экзекьюторами.
Если все с ресурсами то с 4 токенами и 4 экзекьюторами
Запуск докер образа с контейнером hadoop где мы будем выполнять нашу задачу,
лучше unix, под  винду тоже есть инструкция потом добавлю

```bash
cd img-hdp-zeppelin
docker build -t img-hdp-zeppelin .
docker run -it --name zeppelin \
       -p 50090:50090 \
       -p 50075:50075 \
       -p 50070:50070 \
       -p 8042:8042 \
       -p 8088:8088 \
       -p 8888:8888 \
       -p 4040:4040 \
       -p 4044:4044 \
       --hostname localhost \
       img-hdp-zeppelin
```

## Особенность под windows:
1. Согласно инструкции установки Docker для Windows 10 нужно предварительно установить
   WSL. На сайте Microsoft есть инструкция как это сделать. Виртуализация Hyper V тоже
   подойдет, если WSL вам не нравится:
2. В качестве терминала подойдут PowerShell, cmder или MobaXterm
3. В качестве текстового редактора лучше использовать Notepad++, Atom, Brackets, Visual
   Studio Code, IntelliJ IDEA. Разделитель строк: LF(\n)
   4.Команды используем без перевода строк
```bash
docker run -it --name zeppelin -p 50090:50090 -p 50075:50075 -p
50070:50070 -p 8042:8042 -p 8088:8088 -p 4040:4040 -p 4044:4044 -p 8888:8888
--hostname localhost img-hdp-zeppelin
```

Для премещения файлов в docker лучше использовать
```bash
docker cp <path_to_file> zeppelin:/home/hduser/
```
Закончить работу
```bash
exit
```
Возобновить работу
```bash
docker start -i zeppelin
```
Удаление
```bash
docker rm zeppelin
docker rmi img-hdp-zeppelin
docker system prune
```

# Сборка проекта для кластера

```bash
sbt clean && sbt assembly
```

Собранный jar-ник и config.properties нужно перенсти в контейнер
```bash
docker cp target/scala-2.11/big_data_poly-assembly-0.1.0-SNAPSHOT.jar zeppelin:/home/hduser/
docker cp src/conf/config.properties zeppelin:/home/hduser/
```

В файле `config.properties` важными парамтерами являются:
```properties
download.data.users.start.vk.id = 1                                                            # стартовый id
download.data.users.count.vk.id = 1000                                                         # количество id 
download.data.vk.tokens = Bearer <token_1>:Bearer <token_2>:Bearer <token_3>:Bearer <token_4>  # ключи для запросов
download.data.batch.size = 100008                                                              # размер партии для промежуточных сохранений (должен делится на 24)
```

download.data.batch.size выставляйте с учетом того, что он обрабатывается параллельно.
Предлагаю использовать 100008 по умолчанию если 4, использовать 50016 по умолчанию если 2

Команда запуска в контейнере
```bash
spark-submit --class DownloadData --master yarn --deploy-mode cluster --num-executors 4 --executor-cores 1 \
 --conf spark.eventLog.enabled=false \
 --conf "spark.driver.extraJavaOptions=-XX:+UseG1GC -XX:+PrintGCDetails -XX:+PrintGCTimeStamps" \
 --conf "spark.executor.extramemJavaOptions=-XX:+UseG1GC" \
 --files config.properties \
 big_data_poly-assembly-0.1.0-SNAPSHOT.jar
```

Файлы будут ложиться в локальный hdfs по пути:
```
hduser@localhost:~$ hdfs dfs -ls /user/hduser/
Found 5 items
drwxr-xr-x   - hduser supergroup          0 2023-11-30 17:55 /user/hduser/.sparkStaging
drwxr-xr-x   - hduser supergroup          0 2023-11-30 17:45 /user/hduser/batches_1701366274902
drwxr-xr-x   - hduser supergroup          0 2023-11-30 17:45 /user/hduser/batches_1701366320984
drwxr-xr-x   - hduser supergroup          0 2023-11-30 17:54 /user/hduser/batches_1701366830480
drwxr-xr-x   - hduser supergroup          0 2023-11-30 17:55 /user/hduser/data_1_1001_1701366830480
hduser@localhost:~$ hdfs dfs -ls /user/hduser/data*
Found 2 items
-rw-r--r--   1 hduser supergroup          0 2023-11-30 17:55 /user/hduser/data_1_1001_1701366830480/_SUCCESS
-rw-r--r--   1 hduser supergroup      51824 2023-11-30 17:55 /user/hduser/data_1_1001_1701366830480/part-00000-2ec569af-0a7d-45a7-86df-0e4da4ed8531-c000.snappy.parquet

```

# Про количество ключей и экзекьюторов
Для начало надо узнать сколько доступно ядер в контейнере
команда `cat /proc/cpuinfo`  или `less /proc/cpuinfo`
соответственно надо установить параметр `--num-executors 4`
в количество ядер и столько же должно быть ключей это наша степень параллелизма,
возможно стоит сделать и больше, надо проверять.


# Работа с zeppelin 

На http://localhost:8888/#/ находится главная страница, перед началом работы важно выполнить следующие настройки ![img.png](screenshpts%2Fimg.png),
затем можно импортировать существующий блокнот или создать новый.