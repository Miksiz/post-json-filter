class FilterParseError(Exception):
  pass

class JsonFilter:
  '''Класс, который принимает выражение фильтра для Json-объекта.
  Позволяет проверить, пороходит ли декодированный json-объект фильтрацию.'''
  def __init__(self, filter_str: str) -> None:
    try:
      object_path, value = filter_str.split('==')
    except ValueError:
      raise FilterParseError('Отсутствует символ двойного равенства в фильтре')
    self.object_path = tuple(object_path.split('.'))
    if not self.object_path or not all(self.object_path):
      raise FilterParseError('Некорректный путь до элемента объекта')
    try:
      if value.startswith('"') and value.endswith('"'):
        self.value = value[1:-1]
      elif '.' in value:
        self.value = float(value)
      else:
        self.value = int(value)
    except:
      raise FilterParseError('Некорректное значение для фильтра')
  
  def check(self, parsed_json: list | dict) -> bool:
    # {"values": {"a": 1, "b": "s", "m": [{"i": "a"}, {"i": "b"}, {"j": {"z": 1, "v": 2}}]}}
    # values.m.j.v==2
    return self._check(parsed_json, self.object_path)
  
  def _check(self, parsed_json: list | dict, left_path: slice) -> bool:
    '''Рекурсивная проверка объекта на соответствия правилу фильтра'''
    if len(left_path) == 1:
      if isinstance(parsed_json, list):
        for o in parsed_json:
          if o.get(left_path[0], None) == self.value: return True
      else:
        return parsed_json.get(left_path[0], None) == self.value
    else:
      if isinstance(parsed_json, list):
        for o in parsed_json:
          if left_path[0] not in o: continue
          if self._check(o, left_path[1:]): return True
      else:
        if left_path[0] not in parsed_json: return False
        return self._check(parsed_json[left_path[0]], left_path[1:])