#!/usr/bin/env bash
#
# build.sh — Cross-compila el supervisor Go para todas las plataformas/arquitecturas.
#
# Las dependencias (AWS SDK v2, gopkg.in/yaml.v3) son Go puro, así que con
# CGO_ENABLED=0 cada binario es ESTÁTICO y sirve tanto para glibc como para musl
# (no hacen falta variantes "-alpine" a diferencia de los binarios PyInstaller).
#
# Salida: dist/binaries/<nombre> y dist/binaries/<nombre>.zip
set -euo pipefail

cd "$(dirname "$0")"

VERSION="${FAAS_VERSION:-$(git describe --tags --always 2>/dev/null || echo dev)}"
LDFLAGS="-s -w -X main.version=${VERSION}"
OUT=dist/binaries

mkdir -p "$OUT"

# build <goos> <goarch> [goarm] [extra-env...] <name>
build() {
	local os=$1 arch=$2 arm=$3 name=$4 extra=${5:-}
	echo "==> ${os}/${arch}${arm:+ (GOARM=${arm})} ${extra:+ (${extra})}  ->  ${name}"
	env GOOS="$os" GOARCH="$arch" GOARM="$arm" GOMIPS=softfloat GOMIPS64=softfloat \
		CGO_ENABLED=0 $extra \
		go build -trimpath -ldflags "$LDFLAGS" -o "$OUT/$name" ./cmd/faassupervisor
}

# ---- Linux (nombres canónicos del proyecto + resto de arquitecturas) ----
build linux amd64 ""      supervisor
build linux arm64 ""      supervisor-arm64
build linux 386 ""        supervisor-386
build linux arm  7        supervisor-arm
build linux arm  6        supervisor-arm6
build linux riscv64 ""    supervisor-riscv64
build linux ppc64le ""    supervisor-ppc64le
build linux ppc64 ""      supervisor-ppc64
build linux s390x ""      supervisor-s390x
build linux loong64 ""    supervisor-loong64
build linux mips64le ""   supervisor-mips64le
build linux mips64 ""     supervisor-mips64
build linux mipsle ""     supervisor-mipsle
build linux mips ""       supervisor-mips

# ---- No-Linux (bonus: máquinas de desarrollo) ----
build darwin amd64 ""     supervisor-darwin-amd64
build darwin arm64 ""     supervisor-darwin-arm64
build windows amd64 ""    supervisor-windows-amd64.exe
build windows arm64 ""    supervisor-windows-arm64.exe
build windows 386 ""      supervisor-windows-386.exe
build freebsd amd64 ""    supervisor-freebsd-amd64
build freebsd arm64 ""    supervisor-freebsd-arm64
build freebsd 386 ""      supervisor-freebsd-386
build freebsd arm ""      supervisor-freebsd-arm
build freebsd riscv64 ""  supervisor-freebsd-riscv64

# ---- Empaqueta cada binario en un zip (como la distribución original) ----
echo ""
echo "==> Empaquetando zips..."
(
	cd "$OUT"
	shopt -s nullglob
	for f in *; do
		[ -f "$f" ] || continue
		case "$f" in
		*.zip) continue ;;
		esac
		zip -q -9 "${f}.zip" "$f"
	done
)

echo ""
echo "==> Listo. Binarios en: $(pwd)/$OUT"
ls -1 "$OUT"