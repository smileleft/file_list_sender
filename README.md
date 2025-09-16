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
