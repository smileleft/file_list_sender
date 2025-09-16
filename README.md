# file_list_sender

## Normal Execute

```
# 새로 시작하거나 이전 상태에서 재시작
python file_rabbitmq_app.py --directory /path/to/scan

# 사용자 정의 상태 파일 사용
python file_rabbitmq_app.py \
    --directory /path/to/scan \
    --state-file my_app_state.json
```

## Stop and New Start

```
python file_rabbitmq_app.py \
    --directory /path/to/scan \
    --reset-state
```

## Message Format

```
{
  "batch_number": 15,
  "batch_id": "batch_15_1710502245",
  "file_count": 100,
  "file_paths": [...],
  "timestamp": "2024-03-15T14:30:45.123456"
}
```
