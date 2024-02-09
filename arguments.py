import argparse
from pathlib import Path
from json_filter import JsonFilter, FilterParseError
import os

def parse_args():
  parser = argparse.ArgumentParser(
    prog='python3 post_json_filter.py',
    description='Программа считывает строки из файла, фильтрует и отправляет post-запросы',
    epilog='Скорость программы подстраивается под производительность системы или может быть задана в аргументах через количество используемых ядер'
  )
  parser.add_argument('infile', type=check_infile, help='Путь до входного файла с json-строками')
  parser.add_argument('url', help='URL сервиса для отправки POST-запросов')
  parser.add_argument('-f', '--filter', type=create_filter, action='append', help='Выражение для фильтрации строк, можно несколько значений с этим ключом, строка должна подходить под все указанные выражения')
  parser.add_argument('-o', '--outfile', type=Path, help='Выходной файл, в который будут записаны строки не прошедшие фильтрацию, если такой файл существует, он будет перезаписан')
  parser.add_argument('-b', '--batch-size', default=1, type=int, help='Количество отправляемых в одном запросе к сервису строк, прошедших фильтрацию')
  parser.add_argument('-c', '--count', default=-1, type=int, help='Колчество отправляемых к серверу запросов, -1 - обработать весь входной файл')
  parser.add_argument('--dry-run', action='store_true', help='Не отправлять запросы на URL')
  parser.add_argument('--vcpu', default=os.cpu_count(), type=check_vcpus, help='Максимальное количество ядер, используемых программой, по умолчанию - все ядра системы')
  # parser.add_argument('--ram', help='Максимальный объем памяти, доступный программе, по умолчанию - вся свободная память системы') # Решил не использовать, объем максимально памяти ограничен размером очередей между частями программы - 100

  return parser.parse_args()

def check_infile(in_file: str) -> Path:
  in_file = Path(in_file)
  if not in_file.exists():
    raise argparse.ArgumentTypeError('Такого файла не существует')
  try:
    if not in_file.is_file():
      raise argparse.ArgumentTypeError('Указанный путь не является фалом')
  except PermissionError:
    raise argparse.ArgumentTypeError('Нет доступа к файлу')
  return in_file

def create_filter(filter_str: str) -> JsonFilter:
  try:
    return JsonFilter(filter_str)
  except FilterParseError as e:
    raise argparse.ArgumentTypeError(str(e))

def check_count_arg(count_str: str) -> int:
  try:
    count = int(count_str)
  except ValueError:
    raise argparse.ArgumentTypeError('Значение может быть только целым числом')
  if count < -1:
    raise argparse.ArgumentTypeError('Значение не может меньше -1')
  return count

def check_batch_size(batch_size_str: str) -> int:
  try:
    batch_size = int(batch_size_str)
  except ValueError:
    raise argparse.ArgumentTypeError('Значение может быть только целым числом')
  if batch_size < 1:
    raise argparse.ArgumentTypeError('Значение не может меньше 1')
  return batch_size

def check_vcpus(vcpus_str: str) -> int:
  try:
    vcpus = int(vcpus_str)
  except ValueError:
    raise argparse.ArgumentTypeError('Значение может быть только целым числом')
  if vcpus < 1:
    raise argparse.ArgumentTypeError('Значение не может меньше 1')
  return vcpus