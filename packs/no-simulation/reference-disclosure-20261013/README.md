# Reference disclosure and REA continuation

The installed Fabric and WebAddress changes pass 149 and 61 regression assertions respectively. The site checks use Chrome at desktop and phone widths. Core conformance and ExperienceMap checks pass; the control-plane suite reports 871 passing tests, 259 skipped and no failures. The atlas validates with 504 architectures and 444 interactions.

`installation.json` records binary digests and rollback paths. `corpus-installation.json` records the prior and current installed ExperienceMap releases. Full MCP results are retained in `fabric-mcp.json` and `web-address-mcp.json`. The desktop and phone screenshots show that embedded Markdown remains code.

## Storage

The two retained `bonfyre-space-reclaim*.json` receipts record a combined net recovery of 7,300,743,168 bytes, approximately 6.8 GiB. They identify removed caches and older tool versions. Free space was approximately 5.9 GiB during the subsequent Ghidra investigation, after the native toolchain was installed. These are observations at different times; later processes can consume space.

## Existing REA investigation

The [shared conversation](https://chatgpt.com/share/6ace989d-9330-83ea-a1c0-863d9f41b8e5) identifies Reverse Engineer Anything and the installed `bonfyre-compress` binary as an existing native investigation target. The installed REA 6.4.0 helper was used directly; no replacement installation or MCP configuration change was needed.

- `rea-compress-dependencies.json`: Evidence `ev_c1907d13d0a607585b62e77f3e51a2768a82f66d44e68d9391ccf2262625991b` identifies an arm64 Mach-O importing SQLite and libSystem. Its coverage is complete for the selected artifact graph. System libraries are outside the analyzed root, so actual dependency resolution remains undetermined.
- `rea-compress-native.json`: Evidence `ev_bd53714c76d7c1f802726a98bd6aa0918bf7733703e4855293374f16f3b6c0ec` records a Ghidra 12.1.4 overview with 648 procedures and 1,357 strings. This establishes that the installed native analysis provider works.

Both records bind to installed binary SHA-256 `1456d049e02db34e8c011b468ce0a4f4a82beabc9c0651fb83e9dbf05651de6c`. The checkout binary observed during this investigation has a different SHA-256, `b74b4f5bfb6a0c66d5a832dd7ee5d87fef1780f32d19c21fc703cc9551c5fde8`. Source behavior must therefore be checked separately before attributing it to the installed binary. These static records establish neither a runtime speedup nor functional equivalence.

`rea-compress-savings.json` retains Evidence `ev_4ea360465340edbff1695306027a395f2e435ffe43f62c3e6491aada6f1cfa3f`, Ghidra pseudocode for installed `_cmd_savings` at `0x100000dac`. It shows a tar subprocess and read loop followed by a call to `_run_tar_zstd`. The checkout source separately shows that the first stream counts archive bytes and the second compresses a newly produced archive. Combining these passes is a candidate optimization; no runtime improvement has been measured or installed for Compress in this change. Recovered calling conventions and variadic arguments are incomplete and must not be treated as original source.

The earlier `main` name lookup failed because Ghidra did not resolve that procedure name. Its error remains in `rea-compress-main.json`; the successful follow-up used the exact `_cmd_savings` address reported by the installed binary's symbol table. CLI invocations own separate analysis sessions. `rea-compress.snapshot.json` preserves the resulting evidence snapshot for later work.
