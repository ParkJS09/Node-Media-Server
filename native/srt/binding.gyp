{
  "targets": [
    {
      "target_name": "srt_addon",
      "sources": ["src/srt_addon.cc"],
      "include_dirs": ["deps/libsrt/include"],
      "cflags!": ["-fno-exceptions"],
      "cflags_cc!": ["-fno-exceptions", "-std=gnu++17"],
      "cflags_cc": ["-std=c++17"],
      "libraries": [
        "<(module_root_dir)/deps/libsrt/lib/libsrt.a",
        "<!(sh -c 'r=$(cat deps/libsrt/openssl_root 2>/dev/null || echo /usr); if [ -f $r/lib/libcrypto.a ]; then echo $r/lib/libcrypto.a; else echo $r/lib64/libcrypto.a; fi')"
      ],
      "conditions": [
        ["OS=='mac'", {
          "xcode_settings": {
            "CLANG_CXX_LANGUAGE_STANDARD": "c++17",
            "GCC_ENABLE_CPP_EXCEPTIONS": "YES",
            "MACOSX_DEPLOYMENT_TARGET": "13.5"
          }
        }],
        ["OS=='linux'", {
          "libraries": ["-lpthread", "-ldl"]
        }]
      ]
    }
  ]
}
