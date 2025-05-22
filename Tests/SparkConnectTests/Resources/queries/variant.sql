FROM VALUES parse_json('{"a": true, "b": 1, "c": "swift"}') T(v)
|> SELECT v,
          variant_get(v, '$.a', 'boolean') as a,
          variant_get(v, '$.b', 'int') as b,
          variant_get(v, '$.c', 'string') as c
