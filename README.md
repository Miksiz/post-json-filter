# Отправщик POST сообщений из JSON-строк из файла с фильтрацией
Программа считывает строки из файла, фильтрует и отправляет post-запросы
Работа программы проверена с Python 3.10

```
python3 post_json_filter.py [-h] [-f FILTER] [-o OUTFILE] [-b BATCH_SIZE] [-c COUNT] [--dry-run] [--vcpu VCPU] infile url

positional arguments:
  infile                Путь до входного файла с json-строками
  url                   URL сервиса для отправки POST-запросов

options:
  -h, --help            Показать сообщение о ключах и аргументах
  -f FILTER, --filter FILTER
                        Выражение для фильтрации строк, можно несколько значений с этим ключом, строка должна подходить под все указанные выражения
  -o OUTFILE, --outfile OUTFILE
                        Выходной файл, в который будут записаны строки не прошедшие фильтрацию, если такой файл существует, он будет перезаписан
  -b BATCH_SIZE, --batch-size BATCH_SIZE
                        Количество отправляемых в одном запросе к сервису строк, прошедших фильтрацию
  -c COUNT, --count COUNT
                        Колчество отправляемых к серверу запросов, -1 - обработать весь входной файл
  --dry-run             Не отправлять запросы на URL
  --vcpu VCPU           Максимальное количество ядер, используемых программой, по умолчанию - все ядра системы
```

Скорость программы подстраивается под производительность системы или может быть задана в аргументах через количество используемых ядер


## Алгоритм работы программы
Основной поток отвечает за запуск всех зависимых потоков и процессов и ожидает их выполнения
(если присутствует фильтрация)
Потоки в основном процессе:
 - Считывание данных из файла и заполнение очереди для процессов фильтрации
 - Считывание данных из очереди после фильтрации и отправка в сервис
 - Считывание данных из очереди не прошедших фильтрацию и запись в файл (если такой файл указан)
Процессы (по количеству ядер системы):
 - Обработчик json и фильтратор, считывает из очереди на фильтрацию и записывает в две очереди - прошедшие и не прошедшие фильтрацию строки (одну, если не указан файл для не прошедших фильтрацию строк)

(когда фильтрация отсутствует)
Потоки в основном процессе:
 - Считывание данных из файла и заполнение очереди
 - Считывание из очереди и отправка запроса на сервис
