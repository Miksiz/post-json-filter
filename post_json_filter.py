from typing import Union, Optional
from pathlib import Path
import threading
import multiprocessing
import urllib.request
import time
import json
from closable_queue import ClosableQueue
from json_filter import JsonFilter
from arguments import parse_args

def read_input(in_file: Path, out_queue: Union[ClosableQueue, multiprocessing.Queue]) -> None:
  '''Поток для считывания строк из входного файла, записывает считанные строки в очередь'''
  with open(in_file, 'r') as f:
    for line in f:
      if not line.strip(): continue
      try:
        out_queue.put(line)
      except ValueError: # Raised when queue is closed, will raise AssertionError if python <= 3.7
        return

def filter_process(in_queue: multiprocessing.Queue, filters: list[JsonFilter], accepted_queue: multiprocessing.Queue, rejected_queue: Optional[multiprocessing.Queue]=None) -> None:
  '''Процесс для фильтрации строк, забирает строки из входящей очереди и распределяет их по двум очередям - пропущенные и отфильтрованные.
  Завершается при получении пустого значения.'''
  line = in_queue.get()
  while line is not None:
    accepted = True
    try:
      parsed_json = json.loads(line)
    except json.JSONDecodeError:
      accepted = False
    if accepted:
      for json_filter in filters:
        if not json_filter.check(parsed_json):
          accepted = False
          break
    if accepted:
      try:
        accepted_queue.put(line)
      except ValueError: # Raised when queue is closed, will raise AssertionError if python <= 3.7
        return
    else:
      if rejected_queue is not None: rejected_queue.put(line)
    line = in_queue.get()

def write_output(out_file: Path, in_queue: multiprocessing.Queue) -> None:
  '''Поток для записи данных в выходной файл, работает пока в очереди не появится пустое значение'''
  with open(out_file, 'w') as f:
    while True:
      line = in_queue.get()
      if line is None: break
      f.write(line)

def post_data(url: str, data: list) -> None:
  '''Функция отправляет POST-запрос на URL'''
  payload = bytes(f'{{"timestamp": {int(time.time())},\n"data":[{", ".join(data)}]}}', encoding='utf-8')
  urllib.request.urlopen(url=url, data=payload)

class StopProcessing(Exception):
  pass

def post_data_batch(url: str, in_queue: Union[ClosableQueue, multiprocessing.Queue], batch_size: int = 1, dry_run: bool=False) -> None:
  '''Функция собирает данные из очереди на отправку в нужном обеме записей и отправляет.
  Может отправить раньше, если в очереди на отправку окажется пустое значение - сигнал окончания данных.'''
  data = []
  data_item_count = 0
  while data_item_count < batch_size:
    line = in_queue.get()
    if line is None:
      if not dry_run: post_data(url, data)
      raise StopProcessing
    data.append(line)
    data_item_count += 1
    if not dry_run: post_data(url, data)

def post_data_thread(url: str, in_queue: Union[ClosableQueue, multiprocessing.Queue], batch_size: int = 1, count: int = -1, dry_run: bool=False) -> None:
  '''Поток для отправки данных на URL'''
  try:
    if count == -1:
      post_data_batch(url, in_queue, batch_size, dry_run)
    else:
      batch_count = 0
      while batch_count < count:
        post_data_batch(url, in_queue, batch_size, dry_run)
      in_queue.close()
  except StopProcessing:
    pass

def run_unfiltered(infile: Path, url: str, dry_run: bool, batch_size: int=1, count: int=-1) -> None:
  '''Функция обрабатывает входные данные без фильтрации'''
  lines_queue = ClosableQueue(100)
  
  reader_thread = threading.Thread(target=read_input, args=(infile, lines_queue), name=f'Reader from {infile.name}')
  post_request_thread = threading.Thread(target=post_data_thread, args=(url, lines_queue, batch_size, count, dry_run), name=f'Post request sender to {url}')
  
  reader_thread.start()
  post_request_thread.start()
  
  reader_thread.join()
  lines_queue.put(None)
  post_request_thread.join()

def run_filtered(infile: Path, url: str, dry_run: bool, filters: list[JsonFilter], vcpus: int, outfile: Optional[Path]=None, batch_size: int=1, count: int=-1) -> None:
  '''Функция обрабатывает входные данные с фильтрацией'''
  read_lines_queue = multiprocessing.Queue(100)
  accepted_lines_queue = multiprocessing.Queue(100)
  if outfile:
    rejected_lines_queue = multiprocessing.Queue(100)
  else:
    rejected_lines_queue = None
  
  reader_thread = threading.Thread(target=read_input, args=(infile, read_lines_queue), name=f'Reader from {infile.name}')
  json_filter_processes = [multiprocessing.Process(target=filter_process, args=(read_lines_queue, filters, accepted_lines_queue, rejected_lines_queue), name=f'Json filter process {i}') for i in range(vcpus)]
  post_request_thread = threading.Thread(target=post_data_thread, args=(url, accepted_lines_queue, batch_size, count, dry_run), name=f'Post request sender to {url}')
  if outfile:
    outfile_writer_thread = threading.Thread(target=write_output, args=(outfile, rejected_lines_queue), name=f'Rejected lines writer to {outfile.name}')

  reader_thread.start()
  for p in json_filter_processes:
    p.start()
  post_request_thread.start()
  if outfile: outfile_writer_thread.start()
  
  reader_thread.join()
  for _ in range(len(json_filter_processes)):
    read_lines_queue.put(None)
  for p in json_filter_processes:
    p.join()
  accepted_lines_queue.put(None)
  if outfile: rejected_lines_queue.put(None)
  post_request_thread.join()
  if outfile: outfile_writer_thread.join()

def main() -> None:
  '''Главная функиця, считывает аргументы командной строки и запускает процесс с фильтрацией или без'''
  args = parse_args()
  if not args.filter:
    run_unfiltered(args.infile, args.url, args.dry_run, args.batch_size, args.count)
  else:
    run_filtered(args.infile, args.url, args.dry_run, args.filter, args.vcpu, args.outfile, args.batch_size, args.count)

if __name__ == '__main__':
  main()