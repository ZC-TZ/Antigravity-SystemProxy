# Antigravity 跟随系统代理

**下载安装包：** [最新 Windows 安装包](https://github.com/ZC-TZ/Antigravity-SystemProxy/releases/latest/download/Antigravity-SystemProxy-Windows.zip)（完整解压后运行 `install.cmd`）。

让 Antigravity 在开启 Windows 系统代理时使用代理，关闭系统代理时直接连接。当前版本：**v5.26**，适用于 **Windows 10/11 x64**。

**首次使用只需：完整解压安装包 → 退出 Antigravity → 在文件资源管理器中双击 `install.cmd` → 从 `Antigravity (System Proxy)` 启动。**

以后都使用明确命名的 **Antigravity (System Proxy)** 入口。普通的 Antigravity、搜索历史或“最近使用”条目可能仍指向官方程序，无法保证接入代理。

## 目录

- [安装前准备](#安装前准备)
- [首次安装](#首次安装)
- [日常使用与自动退出](#日常使用与自动退出)
- [检查是否正常](#检查是否正常)
- [常见问题](#常见问题)
- [更新、移动与换电脑](#更新移动与换电脑)
- [停用与恢复原状](#停用与恢复原状)
- [可选配置与更多文档](#可选配置与更多文档)

## 安装前准备

准备以下内容：

| 需要准备 | 说明 |
| --- | --- |
| Windows 10/11 x64 | 其他平台尚未验证 |
| 已安装的 Antigravity | 本工具不负责安装 Antigravity |
| 已能正常工作的代理软件 | 例如你已有的 Clash Party；节点和网络需要自行准备 |
| 完整的转发器 ZIP 安装包 | 文件名为 `Antigravity-SystemProxy-Windows.zip` |

普通使用不需要安装 Go、Python 或编译工具，通常不需要管理员权限。这里的“代理软件”和“转发器”是两个程序：代理软件提供上游连接，转发器负责把 Antigravity 的请求按系统设置送过去。

本工具读取 **Windows 手动系统代理**，不支持 PAC/WPAD 自动代理脚本。仅开启浏览器代理扩展不能替代系统代理设置。

## 首次安装

### 按顺序操作

1. 保存任务，**完全退出 Antigravity**。不要只最小化窗口。
2. 把 ZIP **完整解压**到普通文件夹；不要直接在压缩包内运行，也不要只取出一个 EXE。
3. 在 **Windows 文件资源管理器**中打开解压目录，双击 **`install.cmd`**。
4. 等待结束。成功时窗口会显示 `Installed for the current Windows user`；如果显示 `Installation failed`，先按报错处理，不要认为已经安装成功。
5. 在桌面或开始菜单搜索中找到 **`Antigravity (System Proxy)`**，从这个入口启动。

建议将这个独立入口固定到开始菜单，避免以后点到普通 Antigravity 的历史条目。开始菜单刷新可能稍有延迟，此时可先使用桌面的同名快捷方式。

**请从文件资源管理器运行安装器。** 某些打包应用或自动化宿主会把开始菜单文件写入私有副本，导致工具内可见、Windows 看不到。安装器检测到重定向会拒绝继续，并提示回到文件资源管理器运行，不要绕过此检查。

### 安装器会做什么

- 检测 Antigravity 的程序位置，并安装当前用户使用的启动器与转发服务。
- 备份已有的受管快捷方式，创建桌面和开始菜单中的独立代理入口。
- 检查本机端口与运行配置，不修改 Antigravity 二进制或安装目标 HTTPS 解密证书。

安装目录通常为 `%LOCALAPPDATA%\AntigravitySystemProxy`；已有安装也可能位于之前宿主的实际数据目录。**以代理快捷方式“属性 → 目标”所在的目录为准**，不要照搬其他电脑的路径。

### 找不到 Antigravity 或端口冲突

默认安装会自动寻找应用。确实找不到时，在文件资源管理器中打开解压目录，再在该目录打开 Windows PowerShell，运行：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\install.ps1 -AntigravityPath "D:\Apps\Antigravity\Antigravity.exe"
```

把示例路径替换为自己的 **Antigravity.exe 文件路径**。中文、空格和单引号路径受支持；复制整行时不要省略路径两边的双引号。

如果只想查看安装计划，在命令末尾加 `-InspectOnly`；这不会执行安装。默认本机入口端口是 `17891`；如果报端口冲突，可在安装命令末尾加 `-Port 17892`，由安装器同步更新快捷方式和配置。

## 日常使用与自动退出

### 每次启动

1. 打开代理软件，确认节点可用。
2. 按需要开启代理软件中的“系统代理”。
3. 从 **`Antigravity (System Proxy)`** 启动 Antigravity。

**Windows 系统代理继续由原代理软件管理，不要把它改成转发器的 `17891` 端口。** 也不用在转发器里手工填写自己的上游代理端口。

| 系统状态 | Antigravity 经本工具启动后的行为 |
| --- | --- |
| 系统代理开启，配置有效 | 使用当前系统代理 |
| 系统代理关闭 | 直接连接 |
| 运行中开关或更换系统代理 | 自动检测并调整路由，通常约每 0.5 秒检查一次 |

本机地址始终直连。系统代理关闭时，如果网络本身无法直连 Google，Antigravity 仍可能无法使用；这不代表转发器应该悄悄改走代理。

路由真正变化或 Wi-Fi 断开，可能使正在生成的回复、上传或命令确认中断。工具不会自动重放业务请求，也不能补造执行结果。遇到“命令可能已经执行但没有确认”时，先检查结果，再决定是否重试。

### 关闭与电脑重启

- 多个 Antigravity 实例可以共用转发服务，关闭一个实例不会立即停止其他实例的服务。
- 所有匹配的应用进程结束后，等待 **30 秒宽限期**；期间应用自重启会取消关闭。
- 宽限期结束且剩余连接自然结束后，转发器与监护一起退出，不强断未完成连接。
- 应用运行期间，监护会尝试恢复异常退出的转发入口；同时终止包括监护在内的全部代理进程后，需要重新运行代理启动器。
- **没有开机或登录自启。** 电脑重启后，再打开 `Antigravity (System Proxy)`，服务会随启动器一起恢复。

因此，没有使用 Antigravity 时，后台没有转发器进程是正常现象。安装刚完成但尚未启动应用时，后台也可能在约 30 秒后自动退出。

## 检查是否正常

先确认应用是从 **`Antigravity (System Proxy)`** 打开的。如果已经从普通入口启动过，保存任务并完全退出，再从代理入口重新打开；再次点击代理入口不一定能改变既有应用进程的环境。

需要查看状态时：

1. 右键桌面的代理快捷方式，打开“属性”，找到“目标”所在的安装目录。
2. 在该目录打开 Windows PowerShell。
3. 保持 Antigravity 正在运行，执行：

```powershell
.\AntigravitySystemProxy.exe --status | Out-Host
```

| 字段 | 正常使用时的含义 |
| --- | --- |
| `ready: true` | 本机转发服务已就绪 |
| `bridge_running: true` | 入口与工作服务可访问 |
| `application_following: true` | 应用生命周期监护正在运行 |
| `application_exists: true` | 配置中的应用程序文件存在 |

**应用关闭后**，前三项可能为 `false`，这是随应用退出后的正常状态。`ready: true` 只能说明本机服务就绪，不代表账号、节点、远端模型或所有应用组件都正常。

## 常见问题

| 现象或提示 | 建议处理 |
| --- | --- |
| 搜索只找到普通 Antigravity | 搜索完整名称 `Antigravity (System Proxy)`；仍没有时先用桌面同名入口，并从资源管理器重新运行 `install.cmd`，检查是否报错 |
| 点击“最近添加 / 最近使用”后不走代理 | 这些可能是官方历史条目。完全退出后改用明确命名的代理入口，不靠图标相同判断 |
| 提示快捷方式所指向的程序已更改或移动 | 等应用更新完成，确认 Antigravity.exe 存在；从完整交付目录重新安装，必要时用 `-AntigravityPath` 指定新位置 |
| 提示 `redirected` 或要求从 File Explorer 运行 | 关闭当前安装窗口，直接在 Windows 文件资源管理器中双击 `install.cmd`，不要从会重定向 AppData 的工具宿主运行 |
| 找不到应用 | 使用前面的自定义程序路径安装命令 |
| 提示端口被占用 | 用安装命令的 `-Port` 选择其他端口；不要随意结束未知占用进程 |
| 状态正常，但登录或模型请求失败 | 检查代理软件、节点、系统代理、账号和远端服务；本机就绪不等于远端请求成功 |
| 应用一直卡在启动画面 | 先排除应用仍在更新，再完全退出并从代理入口重开；无效时收集下面的诊断资料 |
| 应用关闭后后台仍在 | 等待 30 秒及已有连接结束；进程识别不确定时也会保守保留服务 |
| 安装提示存在未完成事务 / `upgrade-pending` | 保留现场，从完整交付目录重新运行安装器恢复；不要先删除配置或备份 |

### 问题仍未解决时提供什么

记录点击的入口名称、故障发生时间、是否刚更新或断网、代理软件及系统代理状态，并在**安装目录**运行：

```powershell
.\AntigravitySystemProxy.exe --status | Out-Host
.\AntigravitySystemProxy.exe --diagnostics | ConvertFrom-Json | ConvertTo-Json -Depth 16
.\AntigravitySystemProxy.exe --diagnostics-history | ConvertFrom-Json | ConvertTo-Json -Depth 16
```

服务已经退出时，实时 `--diagnostics` 可能失败，仍可查看 `--status` 和历史记录。转发器保存有界诊断快照与故障事件，不记录请求正文、token 或代理凭据；应用自身的日志可能包含其他信息，分享前仍应检查并遮盖敏感内容。

维护命令 `--ensure` 可恢复本机服务，`--follow-app` 可启用应用监护，但它们不能把代理环境补进一个已经直接启动的应用进程。日常排错仍优先完全退出后使用正确入口。

## 更新、移动与换电脑

### 更新转发器

最容易确认版本完整生效的方式是：保存任务、完全退出 Antigravity、完整解压新版包，然后从新版解压目录打开 Windows PowerShell，执行：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\install.ps1 -RestartEntry -DrainSeconds 60
```

这会更新入口，连接未能在期限内排空时会取消维护更新并恢复接收，保留现有连接。维护期间新请求可能暂时失败，因此请先退出应用。

普通重复运行 `install.cmd` 可在兼容时在线接管新工作进程，但不保证更新正在运行的入口与监护代码。后台各角色可能暂时显示不同版本；需要完整切换时按上面的维护步骤操作。不要为强行升级而结束正在执行任务的进程。

### 更新 Antigravity、移动程序或换电脑

- Antigravity 更新可能改写官方快捷方式；以后仍使用独立的代理入口。
- 如果更新后程序位置变了，等更新结束后重新安装，必要时指定新的 `-AntigravityPath`。
- 换电脑时复制完整交付 ZIP，在新电脑上重新安装；不要复制旧电脑的安装目录、配置、日志或快捷方式备份。
- 安装完成后，删除下载的 ZIP 或解压目录不影响已安装服务。保留一份最新包便于重装；不要误删快捷方式目标所在的安装目录。

## 停用与恢复原状

如果不再使用本工具：

1. 保存任务并完全退出 Antigravity。
2. 从文件资源管理器打开代理快捷方式目标所在的**安装目录**，再在该目录打开 Windows PowerShell。
3. 先预览恢复计划：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\restore-shortcuts.ps1 -InspectOnly
```

4. 确认计划后执行恢复：

```powershell
powershell.exe -NoProfile -ExecutionPolicy Bypass -File .\restore-shortcuts.ps1
```

恢复会停止转发器与监护、恢复原有受管快捷方式、删除安装器新建的独立入口，不会卸载 Antigravity，也不会关闭你的代理软件。原始快捷方式反映的是备份时的程序位置；应用后来移动过时，可能还需要用应用安装器修复官方入口。

恢复不会删除安装目录。**先完成恢复，再决定是否清理安装文件**；在恢复成功前保留 `backup` 与 `shortcuts.json`，不要只删转发器 EXE。恢复执行中遇到磁盘错误时可能只完成部分操作，应保留备份排查。

## 可选配置与更多文档

新用户一般保持默认即可。本机配置保存在**实际安装目录**的 `config.json`；不要把包含本机路径或凭据的配置直接复制给其他人。

支持 HTTP、HTTPS、SOCKS5/SOCKS5h 上游；HTTP(S) 支持 Basic 认证，SOCKS5 支持用户名密码。无协议的 `host:port` 视为 HTTP；不猜测含糊的 SOCKS 版本。**不支持 PAC/WPAD、NTLM/Kerberos、SOCKS4 或 UDP。** 仅遵守代理环境变量或 Chromium 参数的组件受此工具覆盖。

工具不解密目标 HTTPS，不替代账号登录，也不保证提高模型生成速度。Wi-Fi 断线后的既有远端会话能否恢复，仍取决于网络和应用。

交付包的解压目录还包含以下说明，按需要查看，不必先读完才能安装：

| 文件 | 用途 |
| --- | --- |
| [CHANGELOG.md](CHANGELOG.md) | 版本更新记录，已从本指南移出 |
| [architecture-review.md](architecture-review.md) | 架构、详细配置、连接额度、维护恢复与诊断指标参考 |
| [performance-results.md](performance-results.md) | 性能测量、测试范围和限制 |
| [benchmark/README.md](benchmark/README.md) | 基准工具使用方法；普通安装不需要运行它 |
| `native-source` / `benchmark-source` | 源码与测试；使用预编译安装包无需编译 |

以上扩展文档以完整解压包为准，安装目录中的 README 不代表其他开发文档也全部复制到了安装目录。
