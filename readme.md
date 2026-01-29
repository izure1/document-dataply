# 사용 방법

```typescript
const db = new DocumentDataply<{
  name: string
  age: number
  options?: {
    address: string
    tags: string[]
  }
}>('my-db.db', {
  wal: 'my-db.db.wal',
  indices: {
    name: true,
    age: false,
    'options.tags.0': true,
    'options.tags.1': false,
  }
})
```

## DocumentInnerMetadata

내부적으로 document-dataply는 최초 데이터베이스 생성 시,
새로운 overflow행을 삽입함. 이 행의 pk는 1임.
이후 새로운 documentInnerMetadata가 업데이트 된다면, 이 pk가 1인 행을 업데이트함으로써, 내부에 데이터를 저장함.

## 인덱스

인스턴스 생성 시, indices 옵션으로 지정된 값만 인덱스를 생성.
키-값으로 이루어져있으며, 값은 boolean 형태임.

키는 삽입될 문서의 필드에 해당함.
만일 배열이 있다면, 배열의 0번째 요소에 대해선 [필드명.숫자] 형식으로 지정함. 이걸 인덱스필드라고 함.
[필드명A.필드명B.숫자] 이런식으로 깊게 들어갈 수도 있음.

값은 true, false로 지정되며,
true로 지정될 경우, 이전에 삽입되었던 모든 문서에 대해서도 인덱스를 생성함.
false로 지정될 경우, 이전에 삽입되었던 문서에는 인덱스를 생성하지 않으며, 향후 삽입될 문서에 대해서만 인덱스를 생성함.
이를 인덱스 정책이라 하겠음.

이 옵션은 이전에 저장된 옵션 데이터를 기반으로 동작함.
예를 들어 이전에 false로 지정되었으나, 이번에 true로 지정되었다면 이전에 저장된 값이랑 비교해서 해당 동작을 해야함.
이 옵션 데이터 저장은 데이터페이스 pk가 1인 행에 documentInnerMetadata에 삽입됨.

### 인덱스 b+tree

documentInnerMetadata에는 여러 데이터가 저장되어있으며, JSON.parse로 파싱하면 아래와 같은 구조가 있을 것.

```typescript
// documentInnerMetadata
{
  ...
  indices: {
    [인덱스필드명]: [인덱스 b+tree 헤드 pk, 인덱스 정책]
  }
}
// sample
{
  ...
  indices: {
    _id: [2, true],
    name: [3, true],
    'options.tags.1': [5, false]
  }
}
```

새로운 인덱스를 생성해야할 때, 새로운 행을 insertAsOverflow 행으로 삽입하고, 반환된 pk를 인덱스 b+tree 헤드 행으로 삼음.
향후 인덱스의 헤드가 변경되면, documentStrategy.ts에서 writeHead 를 호출하여 저장될 것이고, 이 때, documentInnerMetadata에 있는 해당 필드에 해당하는 헤드 pk 행을 업데이트하는 방식으로 진행됨.

인덱스는 생성하거나, 수정될 때, 트랜잭션 내부에서 시행하여 원자성을 보존해야함.
