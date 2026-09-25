#!/usr/bin/env bash
#
# Build a static libsrt (with static OpenSSL libcrypto) into native/srt/deps/libsrt.
#
#   SRT_SRC           libsrt source tree        (default: ~/GithubProject/srt)
#   OPENSSL_ROOT_DIR  OpenSSL prefix containing a static libcrypto.a
#                     (default: macOS builds OpenSSL $OPENSSL_VERSION from source into
#                     deps/openssl with the same deployment target; Linux uses /usr)
#   JOBS              parallel build jobs
#
set -euo pipefail

ADDON_DIR="$(cd "$(dirname "$0")/.." && pwd)"
SRT_SRC="${SRT_SRC:-$HOME/GithubProject/srt}"
PREFIX="$ADDON_DIR/deps/libsrt"
BUILD_DIR="$ADDON_DIR/deps/build-libsrt"
JOBS="${JOBS:-$(getconf _NPROCESSORS_ONLN 2>/dev/null || echo 4)}"

if [ ! -f "$SRT_SRC/CMakeLists.txt" ]; then
  echo "libsrt source not found at $SRT_SRC" >&2
  echo "  git clone https://github.com/Haivision/srt \"$SRT_SRC\"  or set SRT_SRC" >&2
  exit 1
fi

OPENSSL_VERSION="3.5.4"
OPENSSL_SHA256="967311f84955316969bdb1d8d4b983718ef42338639c621ec4c34fddef355e99"

OS="$(uname -s)"
if [ "$OS" = "Darwin" ]; then
  # Keep every static object's deployment target in sync with binding.gyp;
  # a mismatch makes ld warn "built for newer macOS version than being linked".
  TARGET="$(sed -nE 's/.*MACOSX_DEPLOYMENT_TARGET["'"'"'][^0-9]*([0-9.]+).*/\1/p' "$ADDON_DIR/binding.gyp" | head -n1)"
  TARGET="${TARGET:-13.5}"
fi

build_openssl() {
  local prefix="$ADDON_DIR/deps/openssl"
  local work="$ADDON_DIR/deps/build-openssl"
  local tarball="$work/openssl-$OPENSSL_VERSION.tar.gz"
  local arch
  case "$(uname -m)" in
    arm64) arch=darwin64-arm64-cc ;;
    *)     arch=darwin64-x86_64-cc ;;
  esac
  if [ -f "$prefix/lib/libcrypto.a" ] && [ "$(cat "$prefix/.stamp" 2>/dev/null)" = "$OPENSSL_VERSION-$TARGET" ]; then
    OPENSSL_ROOT_DIR="$prefix"
    return
  fi
  echo "openssl: building $OPENSSL_VERSION (macOS $TARGET) -> $prefix"
  rm -rf "$work" "$prefix"
  mkdir -p "$work"
  curl -fsSL -o "$tarball" "https://github.com/openssl/openssl/releases/download/openssl-$OPENSSL_VERSION/openssl-$OPENSSL_VERSION.tar.gz"
  echo "$OPENSSL_SHA256  $tarball" | shasum -a 256 -c - >/dev/null
  tar -xzf "$tarball" -C "$work"
  (
    cd "$work/openssl-$OPENSSL_VERSION"
    MACOSX_DEPLOYMENT_TARGET="$TARGET" ./Configure "$arch" no-shared no-tests no-docs \
      --prefix="$prefix" --libdir=lib "-mmacosx-version-min=$TARGET" >/dev/null
    make -j "$JOBS" build_libs >/dev/null
    make install_dev >/dev/null
  )
  rm -rf "$work"
  echo "$OPENSSL_VERSION-$TARGET" > "$prefix/.stamp"
  OPENSSL_ROOT_DIR="$prefix"
}

if [ -z "${OPENSSL_ROOT_DIR:-}" ]; then
  if [ "$OS" = "Darwin" ]; then
    build_openssl
  else
    OPENSSL_ROOT_DIR="/usr"
  fi
fi
if [ ! -f "$OPENSSL_ROOT_DIR/lib/libcrypto.a" ] && [ ! -f "$OPENSSL_ROOT_DIR/lib64/libcrypto.a" ]; then
  echo "static libcrypto.a not found under $OPENSSL_ROOT_DIR (set OPENSSL_ROOT_DIR)" >&2
  exit 1
fi

CMAKE_ARGS=(
  -DCMAKE_BUILD_TYPE=Release
  -DCMAKE_INSTALL_PREFIX="$PREFIX"
  -DCMAKE_INSTALL_LIBDIR=lib
  -DCMAKE_POSITION_INDEPENDENT_CODE=ON
  -DENABLE_SHARED=OFF
  -DENABLE_STATIC=ON
  -DENABLE_APPS=OFF
  -DENABLE_ENCRYPTION=ON
  -DSRT_USE_OPENSSL_STATIC_LIBS=ON
  -DOPENSSL_ROOT_DIR="$OPENSSL_ROOT_DIR"
)

if [ "$OS" = "Darwin" ]; then
  CMAKE_ARGS+=(-DCMAKE_OSX_DEPLOYMENT_TARGET="$TARGET")
fi

echo "libsrt: $SRT_SRC -> $PREFIX (openssl: $OPENSSL_ROOT_DIR)"
rm -rf "$BUILD_DIR" "$PREFIX"
cmake -S "$SRT_SRC" -B "$BUILD_DIR" "${CMAKE_ARGS[@]}"
cmake --build "$BUILD_DIR" -j "$JOBS"
cmake --install "$BUILD_DIR"
rm -rf "$BUILD_DIR"

# binding.gyp links libcrypto.a from here so the addon has no OpenSSL runtime dependency.
echo "$OPENSSL_ROOT_DIR" > "$PREFIX/openssl_root"
echo "libsrt installed to $PREFIX"
