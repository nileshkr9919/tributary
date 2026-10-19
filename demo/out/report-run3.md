# Demo report

| | Celery 5 + Redis | Tributary v0 |
|---|---|---|
| 429s | 1,807 | 0 |
| duplicate paid calls | 47 | 0 |
| wasted tokens | 23,500 | 0 |
| billed tokens | 349,100 | 336,000 |
| interactive p50 (s) | 38.3 | 0.3 |
| interactive p95 (s) | 134.3 | 0.8 |
| completed jobs | 308/320 | 320/320 |
| total time (s) | 177.7 | 211.2 |
