# 선택 가능한 WaitGroup / Channel 완료 신호

다중 키에 신규/중복 요청이 함께 몰리는 서비스형 부하는
[BENCHMARKS-SERVICE.md](BENCHMARKS-SERVICE.md)에 별도로 기록합니다.
아래는 완료 프로토콜별 기존 마이크로벤치마크 비교입니다.

## 현재 구현과 선택

현재 `Group`은 **WaitGroup을 기본값**으로 사용하며, 생성할 때
`NewWithOptions(shardCount, Options{WaitMode: ChannelMode})`로 channel close 방식을
선택할 수 있습니다. 사용 예와 공개 계약은 [README.md](README.md)에 있습니다.

- `Group{}`, `New(shardCount)`, `Options{}`는 모두 `WaitGroupMode`입니다.
  기존 `New`의 `func(int) *Group` 시그니처는 그대로입니다.
- 옵션은 복사되고 그룹의 모드는 생성 후 고정됩니다. 잘못된 모드는 panic합니다.
- 두 방식은 같은 샤딩·레지스트리·예외 완료 경로를 사용합니다.
  기본 255개 샤드, maphash, Do/DoChan/Forget 및 결과 계약을 유지합니다.
- WaitGroup은 map 공개 전에 `Add(1)`, 생산자의 공통 완료 defer에서 한 번
  `Done()`합니다. 여러 호출자가 동시에 또는 완료 이후 반복해서 `Wait()`할 수 있습니다.
- Channel은 map 공개 전에 `chan struct{}`를 만들고, 결과 확정 후 생산자만 한 번
  닫습니다. 값을 보내지 않으며, 완료 전 대기와 완료 후 수신 모두 안전합니다.
- 두 신호 모두 결과 쓰기를 대기자의 읽기와 동기화합니다. 정상 반환·error·panic·Goexit
  모두 신호를 완료하며, 대기자는 샤드 mutex를 놓고 기다리고 완료 후 재잠금하지 않습니다.
- Forget은 신호를 완료하거나 작업을 취소하지 않습니다. 완료 시 자기 실행과 동일한
  map 엔트리만 제거합니다. 실행 객체·신호는 재사용하거나 풀링하지 않습니다.
- **DoChan의 공개 결과 채널은 두 모드 모두 닫지 않습니다.** 내부 완료 신호와 별개입니다.

현재 `call`은 WaitGroup 필드와 선택적 channel 필드를 함께 보유하되 하나만 초기화합니다.
공통 로직을 유지하는 대신 객체 크기가 늘어납니다. 아래에서 이 비용도 별도로 측정했습니다.

테스트 전용 Cond 및 이전 단일 모드 WaitGroup 구현은 `*_baseline_test.go`에 보존합니다.
프로덕션 패키지에는 포함되지 않습니다. 과거 보고서는
[BENCHMARKS-COND.md](BENCHMARKS-COND.md),
[BENCHMARKS-WAITGROUP.md](BENCHMARKS-WAITGROUP.md),
[BENCHMARKS-CHANNEL.md](BENCHMARKS-CHANNEL.md)에 있습니다.
과거 500ms 측정과 아래 현재 소스의 300ms 측정은 별개입니다.

## 환경과 재현

- 측정일: 2026-10-04 (KST)
- Go: `go1.27.1 windows/arm64`, `CGO_ENABLED=0`
- CPU: Snapdragon X Plus X1P42100 (Qualcomm Oryon), 8코어 / 8 논리 프로세서
- 비교 대상: `golang.org/x/sync/singleflight v0.23.0`
- 주 비교: 12개 워크로드 × 4개 구현 × GOMAXPROCS 1/8 = **96개 케이스**,
  각각 300ms × 5회, PASS (226.574s).
- 옵션 비용 비교: 2개 워크로드 × 2개 구현 × GOMAXPROCS 1/8 = **8개 케이스**,
  각각 300ms × 5회, 별도 실행 PASS (19.975s).
- ColdGroup을 제외하면 생성·초기화·워밍업은 타이머 밖입니다.
  반환값·오류를 확인하고 할당 및 병합 비율을 기록합니다.
- 세 샤딩 구현은 워크로드별 같은 maphash seed로 샤드 배치를 맞춥니다.
  seed는 무작위라 재실행에서는 배치가 달라질 수 있습니다.
- WaitGroup/Channel 열은 **현재 프로덕션 옵션 경로**입니다.
  Cond 열은 고정된 이전 구현입니다.
- 원본: [windows-arm64-options.txt](testdata/benchmarks/windows-arm64-options.txt),
  [windows-arm64-options-overhead.txt](testdata/benchmarks/windows-arm64-options-overhead.txt)
- 집계: [summary-options.json](testdata/benchmarks/summary-options.json),
  [summary-options-overhead.json](testdata/benchmarks/summary-options-overhead.json)
- 측정 소스 SHA-256: [source-sha256-options.txt](testdata/benchmarks/source-sha256-options.txt).
  소스는 미커밋 작업 상태이며 파일 해시로 식별합니다.

모듈 루트의 PowerShell에서:

```powershell
go test ./singleflight -run '^$' -bench 'Benchmark(Serial|Parallel|DoChan|ColdGroup)$' -benchmem -benchtime=300ms -count=5 '-cpu=1,8' | Tee-Object -FilePath singleflight/testdata/benchmarks/windows-arm64-options.txt
& ./singleflight/testdata/benchmarks/summarize.ps1

go test ./singleflight -run '^$' -bench '^BenchmarkWaitModeOverhead$' -benchmem -benchtime=300ms -count=5 '-cpu=1,8' | Tee-Object -FilePath singleflight/testdata/benchmarks/windows-arm64-options-overhead.txt
& ./singleflight/testdata/benchmarks/summarize.ps1 -InputPath singleflight/testdata/benchmarks/windows-arm64-options-overhead.txt -OutputPath singleflight/testdata/benchmarks/summary-options-overhead.json -ExpectedCases 8
```

집계 스크립트는 PASS와 예상 케이스 수·각각의 5개 표본을 확인하고 원본을 UTF-8로
정규화합니다. 과거 출력을 집계할 때는 InputPath/OutputPath와 ExpectedCases를 명시하세요.
새 측정으로 파일을 덮어쓸 때는 소스 해시도 다시 기록해야 합니다.

## GOMAXPROCS=8: 5회 중앙값 (ns/op)

작을수록 좋습니다. 마지막 열은 `(Channel / WaitGroup - 1) × 100`입니다.
음수는 비용 감소, 양수는 증가입니다. ns/op는 전체 호출 수 기준 처리 비용이며
개별 요청 지연·p95/p99가 아닙니다. Serial과 ColdGroup은 8에서도 직렬 실행입니다.

| 케이스 | WaitGroup | Channel | Cond | x/sync | Channel의 WG 대비 비용 변화 |
|---|---:|---:|---:|---:|---:|
| Serial / 짧은 키 | 151.2 | 211.2 | 112 | 100.8 | +39.7% |
| Serial / 4096바이트 키 | 530.5 | 566.4 | 530.9 | 244.1 | +6.8% |
| Do / 독립 키, 즉시 반환 | 69.85 | 112 | 56.4 | 704.8 | +60.3% |
| Do / 256개 키 순환, 즉시 반환 | 97.24 | 133.7 | 98.33 | 604.7 | +37.5% |
| Do / 단일 키, 즉시 반환 | 725.5 | 704.6 | 755.6 | 669.7 | -2.9% |
| Do / 단일 키, Gosched | 809.9 | 673.2 | 938.2 | 814.7 | -16.9% |
| Do / 16개 키 순환, Gosched | 566.8 | 537.3 | 487.8 | 1055 | -5.2% |
| Do / 독립 키, CPU 작업 | 297.4 | 354.5 | 280.4 | 1417 | +19.2% |
| Do / 단일 키, CPU 작업 | 860.4 | 558.2 | 1129 | 861.3 | -35.1% |
| DoChan / 독립 키, 즉시 반환 | 272.7 | 446.5 | 260 | 1478 | +63.7% |
| DoChan / 단일 키, Gosched | 834.8 | 852.1 | 786 | 945.6 | +2.1% |
| ColdGroup / 매 호출 새 Group | 12392 | 8781 | 13871 | 237.4 | -29.1% |

## GOMAXPROCS=1: 5회 중앙값 (ns/op)

| 케이스 | WaitGroup | Channel | Cond | x/sync |
|---|---:|---:|---:|---:|
| Serial / 짧은 키 | 165.2 | 151.7 | 117.7 | 137.5 |
| Serial / 4096바이트 키 | 535 | 569.7 | 511.2 | 239.1 |
| Do / 독립 키, 즉시 반환 | 137.2 | 151.5 | 112.6 | 113.7 |
| Do / 256개 키 순환, 즉시 반환 | 134.3 | 174.5 | 116.3 | 97.75 |
| Do / 단일 키, 즉시 반환 | 132.1 | 164.3 | 111.8 | 117.1 |
| Do / 단일 키, Gosched | 249.5 | 296.1 | 235.6 | 248.2 |
| Do / 16개 키 순환, Gosched | 254 | 293.7 | 269.7 | 250.6 |
| Do / 독립 키, CPU 작업 | 1442 | 1444 | 1405 | 1425 |
| Do / 단일 키, CPU 작업 | 1425 | 1481 | 1421 | 1396 |
| DoChan / 독립 키, 즉시 반환 | 698.4 | 850.4 | 566.1 | 544.9 |
| DoChan / 단일 키, Gosched | 728.8 | 764.7 | 653 | 687 |
| ColdGroup / 매 호출 새 Group | 5899 | 6024 | 12086 | 212.3 |

RunParallel의 기본 워커 수는 GOMAXPROCS입니다. 1에서 동기 Do는 워커가 하나라
병합이 없습니다. 독립 키는 워커마다 하나의 다른 키를 반복 사용합니다.
순환 키는 같은 목록을 시작점만 달리하여 순회하며 무작위 분포가 아닙니다.
Gosched는 겹침을 만들기 위한 스케줄러 양보이지 실제 I/O 지연 모사가 아닙니다.
CPU 작업은 데이터 의존적인 1024회 정수 연산입니다.

## 범위·병합량·할당: 주요 케이스 (8)

범위는 실제 5개 표본의 최소–최대이며 신뢰구간이 아닙니다.
backend/op는 호출 하나당 생산자 실행 횟수, shared/op는 공유 결과를 받은 호출 비율입니다.
최초 호출자도 Shared=true일 수 있어 두 비율의 합이 1인 관계는 아닙니다.

| 케이스 | WG 범위 | Channel 범위 | backend/op WG / Ch | shared/op WG / Ch | B/op WG / Ch |
|---|---:|---:|---:|---:|---:|
| Serial / 짧은 키 | 126.2–214.7 | 171.5–232.6 | — / — | — / — | 96 / 208 |
| Do / 독립 키, 즉시 반환 | 67.65–70.63 | 108.6–117.1 | 1 / 1 | 0 / 0 | 96 / 208 |
| Do / 단일 키, 즉시 반환 | 680–789.6 | 661.7–714.2 | 0.6346 / 0.6242 | 0.5626 / 0.572 | 60 / 129 |
| Do / 단일 키, Gosched | 796.7–847.8 | 663.7–709.1 | 0.2308 / 0.2153 | 0.98 / 0.9872 | 22 / 44 |
| Do / 16개 키 순환, Gosched | 544.9–573.8 | 501.6–548.1 | 0.5277 / 0.4975 | 0.7472 / 0.7783 | 50 / 103 |
| Do / 독립 키, CPU 작업 | 271–321.1 | 349.8–390.8 | 1 / 1 | 0 / 0 | 104 / 216 |
| Do / 단일 키, CPU 작업 | 844.6–882.3 | 453.1–853.6 | 0.1855 / 0.1735 | 0.9818 / 0.9842 | 19 / 37 |
| DoChan / 독립 키, 즉시 반환 | 266.2–274 | 342.9–1272 | 1 / 1 | 0 / 0 | 328 / 440 |
| DoChan / 단일 키, Gosched | 817.9–850.2 | 839.2–855.9 | 0.2434 / 0.2401 | 0.9705 / 0.9729 | 216 / 242 |

중복 없는 짧은 Do는 **WG 96 B/op / 1 alloc/op**, **Channel 208 B/op / 2 allocs/op**,
Cond·x/sync는 80 B/op / 1 alloc/op입니다. 같은 현재 구현끼리 비교하면
Channel은 생산자 실행마다 **112 B와 할당 1회**가 추가됩니다.

- 독립 CPU 작업: WG 104 B / 2회, Channel 216 B / 3회, Cond·x/sync 88 B / 2회
  (interface boxing 포함).
- 독립 DoChan: WG 328 B / 5회, Channel 440 B / 6회, Cond 312 B / 5회,
  x/sync 296 B / 5회. Channel은 공개 결과 채널 외에도 내부 완료 채널을 만듭니다.
- ColdGroup: WG 16,704 B / 259회, Channel 16,816 B / 260회,
  Cond 16,672 B / 259회, x/sync 352 B / 4회. Group을 요청마다 만들지 말고 재사용하세요.
- 병합 케이스의 allocs/op 정수 표시는 분수 할당량을 숨깁니다.
  0으로 표시되어도 실제 할당이 없다는 뜻은 아닙니다.

## 옵션 지원 자체의 비용: 고정 WG와 별도 비교

이 표는 주 비교와 다른 실행입니다. FrozenWaitGroup은 보존된 단일 모드 구현이고,
SelectableWaitGroup은 현재 `New(DefaultShards)` 경로입니다.
같은 워크로드별 seed를 사용하며, 숫자는 중앙값 (최소–최대)입니다.

| 케이스 | GOMAXPROCS | 고정 WG ns/op | 현재 선택형 WG ns/op | 비용 변화 | backend/op 고정 / 현재 |
|---|---:|---:|---:|---:|---:|
| IndependentCheap | 1 | 115 (105.6–120.4) | 139.6 (133.3–187) | +21.4% | 1 / 1 |
| IndependentCheap | 8 | 51.21 (45.27–54.92) | 60.34 (58.92–61.28) | +17.8% | 1 / 1 |
| HotYield | 1 | 269.7 (241.4–310.6) | 254.5 (252.1–353.6) | -5.6% | 1 / 1 |
| HotYield | 8 | 876.3 (860.7–901.4) | 808.1 (792.7–821.3) | -7.8% | 0.2288 / 0.2335 |

중복 없는 실행의 할당은 고정 WG 80 B / 1회에서 현재 WG 96 B / 1회로 늘었습니다.
추가 channel 필드와 할당 크기 등급 때문에 **실행당 16 B 증가**, 할당 횟수는 동일합니다.
Channel도 이전 단일 모드의 192 B / 2회에서 현재 208 B / 2회로 늘었습니다.
옵션 생성·대기·완료의 분기도 추가되었습니다. 이번 짧은 독립 키 측정에서는
17.8–21.4%의 비용 증가가 관측됐으므로 옵션 지원을 무비용이라고 할 수 없습니다.
핫 키 결과와 실행 간 수치도 달라서 이를 모든 부하의 고정 패널티로 해석하면 안 됩니다.

## 해석과 한계

1. **WaitGroup을 기본값으로, channel을 선택지로 유지합니다.**
   이번 8워커 독립 키의 짧은 Do는 69.85 vs 112 ns/op로 WG가 유리했습니다.
   낮은 할당량과 많은 키의 짧은 작업을 고려한 기본값이며, 항상 더 빠르다는 뜻은 아닙니다.
2. **일부 핫 키 Do에서는 channel이 유리했습니다.** Gosched 중앙값은 WG보다
   16.9%, CPU 작업은 35.1% 낮았으나 backend/op도 달라집니다. 특히 Channel HotCPU의
   453.1–853.6 ns/op 범위는 넓습니다. 순수한 close 대 Done 깨우기 비용의 차이나
   실제 서비스의 개선율로 단정할 수 없습니다.
3. DoChan은 공개 결과 채널로 기다리므로 내부 완료 채널 수신의 이점을 직접 받지
   않습니다. Channel의 추가 생성·close 비용이 있으며, 이번 독립 DoChan의
   Channel 범위도 342.9–1272 ns/op로 넓어 중앙값만으로 고정 개선율을 말할 수 없습니다.
4. **옵션 지원 자체도 크기·분기 비용을 지불합니다.** WG 실행당 16 B 증가를
   기록했습니다. 고정 단일 모드 WG와 현재 WG를 동일한 구현으로 간주하지 마세요.
5. WG와 channel 모두 런타임 내부 동기화를 사용합니다. lock-free 구현이나
   fake lock/busy wait으로 바꾼 것이 아닙니다.
6. 개발 환경에서 순차 측정했고 전력 모드·주파수·백그라운드 부하를 고정하지
   않았습니다. 작은 차이·겹치는 범위·실행 순서·해시 배치의 영향을 고려해야 합니다.
   ColdGroup의 역전도 큰 변동을 동반합니다. benchstat 미설치로 통계적 유의성
   검정은 하지 않았으며, 별도 실행인 옵션 비용 표와 주 비교 표를 섞어 비율을 내면 안 됩니다.
7. 마이크로벤치마크로 실제 I/O·지연 분포·고카디널리티·장시간 부하를 검증하지
   않았습니다. 취소·타임아웃 API를 새로 추가한 것도 아닙니다.

두 방식의 계약은 같지만 비용은 작업 길이·키 분포·병합량에 따라 달라집니다.
그룹별로 생성 시 선택한 뒤 실제 부하를 기준으로 비교하는 것이 적절합니다.

## 정확성 검증

- gofmt 검사, go mod tidy, go build ./..., go vet ./...: 통과.
- `go test ./... -count=30 -shuffle=on '-cpu=1,8' -timeout=120s`: 통과.
- 두 모드에 같은 계약 테스트 적용: 32개 중복 대기자, Do/DoChan 혼합, 다른 키,
  수신 포기, Forget 교체, panic/Goexit, 공개 결과 채널의 비닫힘.
- 완료 후 64개 동시·반복 대기, 샤드 락을 잡아도 대기자 반환,
  16개 워커의 완료 경계 반복 호출과 정상/error/panic/Goexit 완료 신호 검사.
- 기본값·기존 New 시그니처·선택 신호·샤드 수·옵션 복사·잘못된 설정 검사.
- 문장 커버리지 97.4%. 이는 정확성 증명이 아닙니다.
- golangci-lint 2.14.0 run --no-config: 0 issues.
  활성 검사: errcheck, govet, ineffassign, staticcheck, unused.
- go test -race: Windows ARM64 미지원으로 미검증.
- govulncheck 미설치로 미실행. 지원 환경의 race 및 취약점 검사는 남아 있습니다.
