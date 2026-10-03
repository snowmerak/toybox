# WaitGroup 전환 벤치마크 (과거 기록)

이 문서는 채널 전환 전 측정을 보존한 기록입니다. 현재 구현과 비교 결과는
[BENCHMARKS.md](BENCHMARKS.md)를 참고하세요. 아래 명령은 당시 소스 기준이며,
현재 기본 벤치마크와 집계 스크립트는 네 구현을 대상으로 합니다.

## 변경과 비교 방식

프로덕션 구현은 `sync.Cond` 대신 각 실행에 내장된 `sync.WaitGroup`을 사용합니다.
키 등록·삭제는 기존 샤드 mutex로 보호하되, 중복 호출자는 락을 놓고 기다리며
완료 후 샤드 mutex를 재획득하지 않습니다.

- `Add(1)`은 map에 공개하기 전에 수행합니다.
- 정상 반환·panic·`runtime.Goexit` 모두 완료 defer에서 `Done()`을 수행합니다.
- 완료된 실행 객체를 재사용하지 않으므로 다수·반복 `Wait()`가 안전합니다.
- 기본 255개 샤드, maphash, 공개 API, `Forget`의 교체 실행 보호는 유지합니다.
- 이전 Cond 구현은 `cond_baseline_test.go`에 이름만 구분하여 보존했습니다.
  테스트 파일이므로 프로덕션 빌드에는 포함되지 않습니다.

과거 결과와 새 결과를 단순 비교하지 않고 **현재 WaitGroup / 이전 Cond /
x/sync를 같은 실행에서 다시 측정**했습니다. 최초 Cond 측정은
[BENCHMARKS-COND.md](BENCHMARKS-COND.md)에 별도로 보존합니다.

## 환경과 재현

- 측정일: 2026-10-04 (KST)
- Go: `go1.27.1 windows/arm64`, `CGO_ENABLED=0`
- CPU: Snapdragon X Plus X1P42100 (Qualcomm Oryon), 8코어 / 8 논리 프로세서
- 기준 라이브러리: `golang.org/x/sync/singleflight v0.23.0`
- 12개 워크로드 × 3개 구현 × `GOMAXPROCS=1,8` = 72개 케이스
- 각 케이스 500ms × 5회. 전체 실행 PASS (259.551s).
- ColdGroup을 제외하고 생성·초기화·워밍업은 타이머 밖에 둡니다.
  반환값과 오류를 검사하고 할당량을 보고합니다.
- 두 샤딩 구현은 워크로드별 **같은 maphash seed**를 사용해 샤드 배치를 맞춥니다.
  seed는 실행마다 무작위이므로 재실행 시 배치가 달라질 수 있습니다.
- 측정 코드의 SHA-256:
  [source-sha256-waitgroup.txt](testdata/benchmarks/source-sha256-waitgroup.txt)
- 원본: [windows-arm64-waitgroup.txt](testdata/benchmarks/windows-arm64-waitgroup.txt)
- 중앙값·최소·최대·할당·병합 비율:
  [summary-waitgroup.json](testdata/benchmarks/summary-waitgroup.json)

모듈 루트의 PowerShell에서:

```powershell
go test ./singleflight -run '^$' -bench 'Benchmark(Serial|Parallel|DoChan|ColdGroup)$' -benchmem -benchtime=500ms -count=5 '-cpu=1,8' | Tee-Object -FilePath singleflight/testdata/benchmarks/windows-arm64-waitgroup.txt
& ./singleflight/testdata/benchmarks/summarize.ps1
```

집계 스크립트는 PASS, 72개 케이스, 각각 5개 표본을 확인하며 원본을 UTF-8로
정규화합니다. 원본을 새 실행으로 덮어쓸 때 소스 해시도 다시 기록해야 합니다.

## GOMAXPROCS=8: 5회 중앙값 (ns/op)

작을수록 좋습니다. 마지막 열은 `(WaitGroup / Cond - 1) × 100`이며,
음수는 처리 비용 감소, 양수는 증가입니다. ns/op는 전체 호출 수 기준 처리 비용으로,
개별 요청 지연이나 p95/p99가 아닙니다. Serial과 ColdGroup은 8에서도 직렬 실행입니다.

| 케이스 | Cond | WaitGroup | x/sync | Cond 대비 비용 변화 |
|---|---:|---:|---:|---:|
| Serial / 짧은 키 | 92.33 | 119.3 | 105.3 | +29.2% |
| Serial / 4096바이트 키 | 523.6 | 534.6 | 253.7 | +2.1% |
| Do / 워커별 독립 키, 즉시 반환 | 48.49 | 49.67 | 676.4 | +2.4% |
| Do / 256개 키 순환, 즉시 반환 | 86.86 | 89.38 | 707.8 | +2.9% |
| Do / 단일 키, 즉시 반환 | 715.0 | 668.4 | 655.2 | -6.5% |
| Do / 단일 키, Gosched | 872.0 | 751.3 | 764.9 | -13.8% |
| Do / 16개 키 순환, Gosched | 534.7 | 540.4 | 958.9 | +1.1% |
| Do / 워커별 독립 키, CPU 작업 | 226.1 | 326.3 | 1256 | +44.3% |
| Do / 단일 키, CPU 작업 | 1122 | 776.1 | 1143 | -30.8% |
| DoChan / 독립 키, 즉시 반환 | 262.8 | 262.6 | 1435 | -0.1% |
| DoChan / 단일 키, Gosched | 752.4 | 781.9 | 905.2 | +3.9% |
| ColdGroup / 매 호출 새 Group | 10355 | 9761 | 283.2 | -5.7% |

## GOMAXPROCS=1: 5회 중앙값 (ns/op)

| 케이스 | Cond | WaitGroup | x/sync |
|---|---:|---:|---:|
| Serial / 짧은 키 | 129.0 | 118.7 | 126.4 |
| Serial / 4096바이트 키 | 546.7 | 545.1 | 240.1 |
| Do / 워커별 독립 키, 즉시 반환 | 112.4 | 135.3 | 127.8 |
| Do / 256개 키 순환, 즉시 반환 | 117.8 | 133.0 | 120.0 |
| Do / 단일 키, 즉시 반환 | 126.0 | 127.5 | 151.8 |
| Do / 단일 키, Gosched | 250.8 | 257.0 | 241.3 |
| Do / 16개 키 순환, Gosched | 242.2 | 251.8 | 250.0 |
| Do / 워커별 독립 키, CPU 작업 | 1435 | 1550 | 1490 |
| Do / 단일 키, CPU 작업 | 1475 | 1374 | 1703 |
| DoChan / 독립 키, 즉시 반환 | 597.8 | 681.0 | 564.8 |
| DoChan / 단일 키, Gosched | 709.4 | 745.9 | 657.0 |
| ColdGroup / 매 호출 새 Group | 6211 | 7373 | 190.1 |

`RunParallel`의 기본 워커 수는 GOMAXPROCS입니다. 1에서 동기 Do는 워커가
하나라 병합이 없습니다. 독립 키는 워커마다 하나의 다른 키를 반복 사용하고,
순환 키는 같은 키 목록을 시작점만 달리하여 순회합니다.
`Gosched`는 겹침을 만들기 위한 스케줄러 양보이지 I/O 지연 모사가 아닙니다.
CPU 작업은 데이터 의존적인 1024회 정수 연산입니다.

## 범위와 병합량: 주요 케이스 (8)

각 범위는 5개 실제 표본의 최소–최대이며 신뢰구간이 아닙니다.
`backend/op`는 호출 하나당 생산자 실행 횟수, `shared/op`는 공유 결과 비율입니다.
최초 호출자도 Shared=true일 수 있으므로 두 비율의 합이 1인 관계는 아닙니다.

| 케이스 | Cond 범위 | WG 범위 | backend/op Cond / WG | shared/op Cond / WG | B/op Cond / WG |
|---|---:|---:|---:|---:|---:|
| Serial / 짧은 키 | 88.35–101.8 | 112.5–123.6 | — | — | 80 / 80 |
| Do / 독립 키, 즉시 반환 | 47.97–54.30 | 48.59–54.29 | 1 / 1 | 0 / 0 | 80 / 80 |
| Do / 단일 키, 즉시 반환 | 652.3–795.5 | 644.9–678.0 | 0.6997 / 0.6424 | 0.4168 / 0.5391 | 63 / 51 |
| Do / 단일 키, Gosched | 860.6–881.4 | 721.6–752.6 | 0.2481 / 0.2335 | 0.9668 / 0.9806 | 33 / 18 |
| Do / 16개 키, Gosched | 526.6–542.3 | 518.5–546.8 | 0.5075 / 0.5240 | 0.7680 / 0.7528 | 58 / 41 |
| Do / 독립 키, CPU 작업 | 196.5–285.0 | 248.1–344.1 | 1 / 1 | 0 / 0 | 88 / 88 |
| Do / 단일 키, CPU 작업 | 1106–1158 | 627.9–905.9 | 0.2695 / 0.2045 | 0.9521 / 0.9703 | 37 / 18 |

중복 없는 Do는 세 구현 모두 80 B/op / 1 alloc/op이며, CPU 작업의 interface
boxing을 포함하면 88 B/op / 2 allocs/op입니다. 중복 없는 DoChan은
Cond/WG 312 B/op, x/sync 296 B/op이며 모두 5 allocs/op입니다.
ColdGroup은 Cond/WG 모두 16,672 B/op / 259 allocs/op, x/sync는
352 B/op / 4 allocs/op입니다. Group은 재사용하는 것이 전제입니다.
Go의 allocs/op 정수 표시는 병합 케이스의 분수 할당량을 숨길 수 있습니다.

## 해석과 한계

- **완료 후 샤드 락 재획득은 제거됐습니다.** 샤드 mutex를 의도적으로 잡아 둔
  상태에서 완료된 Do 대기자가 반환하는 회귀 테스트로 이 차이를 검증합니다.
- **겹치는 핫 키의 동기 Do에는 이점이 관측됐습니다.** Gosched 케이스의
  중앙값은 약 14%, CPU 케이스는 약 31% 감소했습니다. 다만 CPU 케이스의
  backend/op도 낮아져, 이를 순수한 대기·깨우기 비용 감소로만 해석하면 안 됩니다.
- **항상 빠른 변경은 아닙니다.** 중복 없는 경우 Cond는 대기 객체를 만들지 않지만
  WG는 생산자마다 Add/Done을 수행합니다. 짧은 직렬 호출에서 비용 증가가
  관측됐으며, 독립 CPU 작업은 이번 중앙값이 약 44% 나빠졌습니다.
  그 증가 전부가 WG 때문이라고 분리 입증한 것은 아닙니다.
- 독립 키 Do는 WG 49.67 vs x/sync 676.4 ns/op로 약 13.6배 차이가 났고,
  독립 DoChan은 약 5.5배였습니다. 이는 해당 워크로드의 샤딩 이점이지
  모든 키 분포에서의 보장이 아닙니다. 핫 키와 긴 키에는 전역 락 절감 효과가
  작거나 없으며, 긴 키의 추가 해시 비용도 그대로 남습니다.
- DoChan은 채널로 기다리므로 Cond의 mutex 재획득 제거 효과를 직접 받지 않습니다.
- 일반 개발 환경에서 순차 측정했으며 전력 모드·주파수·백그라운드 부하를
  고정하지 않았습니다. 작은 차이, 겹치는 범위, 실행 순서와 샤드 배치의 영향을
  고려해야 합니다. benchstat는 미설치라 유의성 검정은 수행하지 않았습니다.
- 마이크로벤치마크이며 지연 분포·실제 I/O·고카디널리티·장시간 부하나
  255개가 최적이라는 주장까지 검증하지 않습니다.
- 따라서 Cond가 유일한 병목이었다는 결론이 아니라, **대기자 재잠금 제거와
  일부 핫 키 개선을 확인했고 다른 경로의 비용도 함께 확인했다**는 결과입니다.

## 정확성 검증

- `go mod tidy`, `go build ./...`, `go vet ./...`: 통과.
- `go test ./... -count=30 -shuffle=on '-cpu=1,8' -timeout=120s`: 통과.
- 기존 32개 대기자 병합, Do/DoChan 혼합, Forget 교체, panic/Goexit 테스트 유지.
- 추가: 완료된 실행의 64개 동시·반복 Wait, 샤드 락 없는 반환,
  16개 워커의 완료 경계 반복 호출.
- 문장 커버리지 97.1%. 커버리지는 정확성 보장이 아닙니다.
- golangci-lint 2.14.0 `run --no-config ./...`: 0 issues.
  활성 검사: errcheck, govet, ineffassign, staticcheck, unused.
- `go test -race ./...`: Windows ARM64 미지원으로 미검증.
- govulncheck 미설치로 미실행. 지원 환경의 race 검사와 취약점 검사는 남아 있습니다.
