# Protocol v1

## 1. Length-Prefixed Frame 형식

| 필드          | 바이트 | 설명                                     |
| ----------- | --- | -------------------------------------- |
| **Length**  | 4   | 네트워크 바이트 오더<br>(`opcode + payload` 크기) |
| **Opcode**  | 1   | 메시지 타입 식별자                             |
| **Payload** | N   | JSON(UTF-8) 또는 바이너리                    |


Frame 최대 크기: 1 MB (1048576 B). 초과 시 ERROR(0x0F) 로 응답 후 세션 강제 종료.

```
0          4        5                4+N
+----------+--------+-----------------+
| Length   | Opcode |   Payload ...   |
+----------+--------+-----------------+
```

## 2. Opcode 테이블

   | Opcode(hex) | 명령              | 방향  | ACK        | 설명              |
   | ----------- | --------------- | --- | ---------- | --------------- |
   | `0x01`      | **CONNECT**     | C→S | `CONNACK`  | 최초 핸드셰이크        |
   | `0x02`      | **CONNACK**     | S→C | ―          | 연결 승인/거절        |
   | `0x03`      | **DISCONNECT**  | C→S | ―          | 정상 종료 통보        |
   | `0x04`      | **SUBSCRIBE**   | C→S | `SUBACK`   | 토픽 목록 구독        |
   | `0x05`      | **SUBACK**      | S→C | ―          | 토픽별 결과 코드 포함    |
   | `0x06`      | **UNSUBSCRIBE** | C→S | `UNSUBACK` | 구독 해제           |
   | `0x07`      | **UNSUBACK**    | S→C | ―          | 해제 결과           |
   | `0x08`      | **PUBLISH**     | C→S | ―          | QoS 0, 브로드캐스트   |
   | `0x09`      | **EVENT**       | S→C | ―          | 브로커가 보내는 실제 메시지 |
   | `0x0F`      | **ERROR**       | S→C | ―          | 프로토콜/논리 오류 알림   |

## 3. Payload JSON 스키마 예시

3.1 CONNECT (0x01)
```
{
  "clientId": "cli-123",
  "version": "1.0"
}
```

오류 코드 (CONNACK 0x02):
```
{ "status": 0 }   // 0=Success, 1=DuplicateClientId, 2=UnsupportedVersion
```

---

3.2 SUBSCRIBE (0x04)
```
{
  "topics": ["stocks1", "stocks2"]
}
```

SUBACK (0x05) 예시:
```
{
  "results": {
    "stocks1": 0,   // 0=OK
    "stocks2": 1    // 1=Denied
  }
}
```

---

3.3 PUBLISH (0x08)
```
{
  "topic": "stocks",
  "body": { "price": 3012.55, "ts": 1714720000 }
}
```

---

3.4 ERROR (0x0F)
```
{
  "code": 3001,
  "reason": "Invalid topic"
}
```
에러 코드 정의 필요
