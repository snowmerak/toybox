# singleflight

동일한 문자열 키의 **겹치는 호출**을 하나로 병합하는 실험용 패키지입니다.
캐시가 아니므로 완료 후 다시 호출하면 함수를 새로 실행합니다.

## 사용

```go
import "github.com/snowmerak/toybox/singleflight"

var group singleflight.Group // 기본 255개 샤드 + WaitGroup 대기

value, err, shared := group.Do("user:42", func() (any, error) {
    return loadUser(42)
})

// 또는 명시적으로 샤드 수 지정 (양수)
custom := singleflight.New(16) // WaitGroup 기본값

// channel close 신호를 사용하려면 생성 시 선택
channel := singleflight.NewWithOptions(singleflight.DefaultShards, singleflight.Options{
    WaitMode: singleflight.ChannelMode,
})
_ = custom
_ = channel
```

- 하나의 `maphash.Seed`로 `maphash.String(seed, key) % shardCount`를 계산합니다.
- 모든 샤드에 독립적인 mutex와 `map[string]*call`을 만듭니다.
- 기본 `WaitGroupMode`: 실행별 `sync.WaitGroup`에 map 등록 전 `Add(1)`하고,
  생산자의 공통 완료 경로에서 한 번 `Done()`합니다. 여러 대기자가 동시에 또는
  완료 이후 반복해서 `Wait()`할 수 있으며, 실행 객체를 재사용하지 않습니다.
- `ChannelMode`: 실행별 `chan struct{}`를 map 등록 전에 생성하고 생산자만
  결과·오류 확정 후 한 번 닫습니다. 값을 보내지 않습니다.
- `NewWithOptions`의 `Options{}`도 WaitGroup 기본값입니다. 알 수 없는 모드와
  0 이하 샤드 수는 panic합니다. 옵션은 복사되며 생성 후 모드는 바꿀 수 없습니다.
  그룹별로 독립 선택할 수 있지만 동일 Group 안에서는 모드가 고정됩니다.
- 두 방식 모두 함수 실행·대기는 샤드 mutex 밖에서 수행하고, 완료된 대기자는
  mutex를 재획득하지 않습니다. 샤딩·결과·panic·Goexit·Forget 계약은 동일합니다.
- 서로 다른 키가 같은 샤드에 해시되더라도 결과는 병합되지 않습니다.
- `Group`은 재사용하는 것이 전제이며, 사용 후 복사하면 안 됩니다.
- 공개 API는 `golang.org/x/sync/singleflight`의 `Do`, `DoChan`,
  `Forget`, `Result` 형태를 따릅니다. 내부적으로 x/sync를 호출하지 않습니다.
  x/sync는 비교 벤치마크를 위한 테스트 의존성입니다.

## 동작과 주의점

- `shared`는 실제 다른 호출과 병합되었는지 나타내며, 최초 호출자도
  중복 호출이 붙었다면 `true`를 받습니다.
- `DoChan`은 버퍼 크기 1의 채널을 반환합니다. 결과는 한 번 전달되고
  채널은 닫히지 않습니다. 수신을 포기해도 실행은 계속됩니다.
  ChannelMode의 내부 `done` 채널만 닫힙니다. WaitGroupMode도 공개 채널은 동일합니다.
- 개별 대기 취소는 `select`와 context로 처리할 수 있지만, 공유 작업 자체를
  취소하거나 시간·동시 키 수·대기자 수를 제한하는 API는 없습니다.
- `Forget`은 진행 중인 작업을 취소하지 않습니다. 새 호출이 별도의 작업을
  시작할 수 있고, 이전 작업의 완료가 새 작업을 삭제하지 않습니다.
- x/sync와 마찬가지로 `Do`의 panic은 원래 값과 스택을 포함한 error로
  다시 panic하며, 원래 값이 error였다면 `errors.Is/As`로 조회할 수 있습니다.
  `DoChan` 구독자가 있는 작업의 panic은 **프로세스를 종료**합니다.
  `runtime.Goexit`는 `Do` 대기자도 종료하고, 채널 결과는 보내지 않습니다.
- 같은 키는 권한·테넌트 등을 포함해 정말 같은 결과를 뜻해야 합니다.
  반환된 포인터·슬라이스·map은 공유되므로 불변으로 취급하거나 직접 복사하세요.
- 한 프로세스의 중복만 병합합니다. 분산 중복 방지나 외부 변경의
  exactly-once 실행을 보장하지 않습니다.

## 검증과 벤치마크

모듈 루트에서 실행합니다. PowerShell에서는 쉼표가 있는 CPU 인자를 따옴표로
감싸야 합니다.

```powershell
go test ./... -count=30 -shuffle=on '-cpu=1,8' -timeout=120s
go build ./...
go vet ./...
go test -race ./... # race를 지원하는 OS/아키텍처와 툴체인 필요

# 신규 키와 재사용 키가 함께 몰리는 서비스형 혼합 부하
go test ./singleflight -run '^$' -bench '^BenchmarkServiceFlood$' -benchmem -benchtime=500ms -count=5 -cpu=8 -timeout=20m

go test ./singleflight -run '^$' -bench 'Benchmark(Serial|Parallel|DoChan|ColdGroup)$' -benchmem -benchtime=300ms -count=5 '-cpu=1,8'
go test ./singleflight -run '^$' -bench '^BenchmarkWaitModeOverhead$' -benchmem -benchtime=300ms -count=5 '-cpu=1,8'
go test ./singleflight -run '^$' -bench '^BenchmarkShardCount$' -benchmem -benchtime=500ms -count=5 '-cpu=1,8'
```

두 모드에 동일한 테스트를 적용합니다: 다수 대기자, error/값 공유, `Do`/`DoChan`
혼합, 수신 포기, 같은 샤드의 다른 키, `Forget` 후 교체, panic/`Goexit` 정리,
채널 panic의 자식 프로세스 종료. 완료 후 반복·동시 대기, 완료 경계 반복 호출,
샤드 락을 유지해도 대기자가 반환하는지와 기본값·잘못된 옵션도 검증합니다.

벤치마크는 실제 WaitGroup/Channel 옵션 경로와 Cond·x/sync를 같은 실행에서
비교합니다. 옵션 추가 비용은 고정된 이전 WaitGroup 구현과 별도로 비교합니다.
과거 측정과 테스트 전용 `*_baseline_test.go`는 재현·비교용으로 보존합니다.

측정 결과·환경·해석은 [BENCHMARKS.md](BENCHMARKS.md)에 기록합니다.
다중 키·신규/중복 혼합 포화 부하의 설계와 결과는
[BENCHMARKS-SERVICE.md](BENCHMARKS-SERVICE.md)에 따로 기록합니다.
64/1024개 클라이언트가 매번 키를 고르며 Uniform/Zipf/핫셋 분포에 진짜 신규 키를
섞습니다. 같은 그룹에 Do/DoChan도 혼합하며 처리량·p50/p95/p99·실제 백엔드 실행량을
측정합니다. 고정 도착률의 open-loop 테스트나 실제 외부 서비스 테스트는 아닙니다.
panic/`Goexit` 처리 일부는 Go Authors의 BSD 라이선스 코드에서 가져왔으며,
해당 라이선스는 [LICENSE](LICENSE)에 보존되어 있습니다.
