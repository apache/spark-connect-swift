EXPLAIN EXTENDED select k, sum(v) from values (1, 2), (1, 3) t(k, v) group by k
