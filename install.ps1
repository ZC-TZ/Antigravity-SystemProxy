[CmdletBinding()]
param(
    [string]$AntigravityPath,
    [ValidateRange(1,65535)][int]$Port = 17891,
    [switch]$InspectOnly,
    [switch]$RestartEntry,
    [ValidateRange(1,3600)][int]$DrainSeconds = 60
)
$ErrorActionPreference = 'Stop'
[Console]::OutputEncoding = New-Object Text.UTF8Encoding
$nativeName = 'AntigravitySystemProxy.exe'
$sourceNative = Join-Path (Split-Path -Parent $MyInvocation.MyCommand.Path) $nativeName
if (-not (Test-Path -LiteralPath $sourceNative -PathType Leaf)) { throw 'Native executable is missing. Extract the complete ZIP package.' }
$sourceRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
function Set-ProxyShortcutIdentity([string]$Path, [string]$AppId, [string]$RelaunchCommand) {
    if (-not ('AntigravityShortcutIdentity' -as [type])) {
        Add-Type -TypeDefinition @'
using System;
using System.Runtime.InteropServices;

public static class AntigravityShortcutIdentity {
    [DllImport("kernel32.dll", CharSet=CharSet.Unicode)]
    static extern int GetCurrentPackageFullName(ref uint length, IntPtr name);
    public static bool IsPackaged() {
        uint length=0;
        int status=GetCurrentPackageFullName(ref length, IntPtr.Zero);
        if (status==15700) return false;
        if (status==0 || status==122) return true;
        throw new InvalidOperationException("Could not verify Windows package context: " + status);
    }
    [StructLayout(LayoutKind.Sequential)]
    public struct Key { public Guid Format; public uint Id;
        public Key(uint id) { Format = new Guid("9F4C2855-9F79-4B39-A8D0-E1D42DE1D5F3"); Id = id; }
    }
    [StructLayout(LayoutKind.Explicit, Size=24)]
    public struct Value { [FieldOffset(0)] public ushort Type; [FieldOffset(8)] public IntPtr Pointer; }
    [ComImport, Guid("886D8EEB-8CF2-4446-8D02-CDBA1DBDCF99"), InterfaceType(ComInterfaceType.InterfaceIsIUnknown)]
    public interface Store {
        [PreserveSig] int GetCount(out uint count);
        [PreserveSig] int GetAt(uint index, out Key key);
        [PreserveSig] int GetValue(ref Key key, out Value value);
        [PreserveSig] int SetValue(ref Key key, ref Value value);
        [PreserveSig] int Commit();
    }
    [DllImport("shell32.dll", CharSet=CharSet.Unicode, PreserveSig=true)]
    static extern int SHGetPropertyStoreFromParsingName(string path, IntPtr context, uint flags, ref Guid iid, out Store store);
    [DllImport("shell32.dll", CharSet=CharSet.Unicode)]
    static extern void SHChangeNotify(uint kind, uint flags, string path, IntPtr unused);
    [DllImport("ole32.dll")]
    static extern int PropVariantClear(ref Value value);
    static void Set(Store store, uint id, string text) {
        Key key = new Key(id);
        Value value = new Value { Type=31, Pointer=Marshal.StringToCoTaskMemUni(text) };
        try { Marshal.ThrowExceptionForHR(store.SetValue(ref key, ref value)); }
        finally { Marshal.FreeCoTaskMem(value.Pointer); }
    }
    public static string Read(string path, uint id) {
        Guid iid = typeof(Store).GUID;
        Store store;
        Marshal.ThrowExceptionForHR(SHGetPropertyStoreFromParsingName(path, IntPtr.Zero, 0, ref iid, out store));
        try {
            Key key = new Key(id); Value value;
            Marshal.ThrowExceptionForHR(store.GetValue(ref key, out value));
            try { return value.Type==31 ? Marshal.PtrToStringUni(value.Pointer) : null; }
            finally { PropVariantClear(ref value); }
        } finally { Marshal.ReleaseComObject(store); }
    }
    public static void Write(string path, string appId, string relaunch) {
        Guid iid = typeof(Store).GUID;
        Store store;
        Marshal.ThrowExceptionForHR(SHGetPropertyStoreFromParsingName(path, IntPtr.Zero, 2, ref iid, out store));
        try {
            // Set relaunch before identity, as required by the Shell contract.
            Set(store, 2, relaunch);
            if (!String.IsNullOrEmpty(appId)) Set(store, 5, appId);
            Marshal.ThrowExceptionForHR(store.Commit());
        } finally { Marshal.ReleaseComObject(store); }
        if (Read(path, 2)!=relaunch || (!String.IsNullOrEmpty(appId) && Read(path, 5)!=appId))
            throw new InvalidOperationException("Shortcut identity did not persist.");
        SHChangeNotify(0x2000, 5, path, IntPtr.Zero);
    }
}
'@
    }
    if (-not $Path) { return }
    [AntigravityShortcutIdentity]::Write($Path, $AppId, $RelaunchCommand)
}
function Assert-NativeConfiguration([hashtable]$Configuration, [string]$Root) {
    $trialRoot = Join-Path ([IO.Path]::GetTempPath()) ('antigravity-config-' + [Guid]::NewGuid().ToString('N'))
    $trialProcess = $null
    New-Item -ItemType Directory -Path $trialRoot | Out-Null
    try {
        $trialConfig = $Configuration.Clone()
        if ($trialConfig.proxy_ca_file -and -not [IO.Path]::IsPathRooted($trialConfig.proxy_ca_file)) {
            $trialConfig.proxy_ca_file = Join-Path $Root $trialConfig.proxy_ca_file
        }
        $trialConfig | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath (Join-Path $trialRoot 'config.json') -Encoding UTF8
        $start = New-Object Diagnostics.ProcessStartInfo
        $start.FileName = $sourceNative
        $start.Arguments = '--check-config'
        $start.UseShellExecute = $false
        $start.CreateNoWindow = $true
        $start.RedirectStandardOutput = $true
        $start.RedirectStandardError = $true
        $start.StandardOutputEncoding = New-Object Text.UTF8Encoding
        $start.StandardErrorEncoding = New-Object Text.UTF8Encoding
        $start.EnvironmentVariables['AG_SYSTEM_PROXY_STATE_DIR'] = $trialRoot
        $start.EnvironmentVariables.Remove('AG_SYSTEM_PROXY_PORT')
        $trialProcess = New-Object Diagnostics.Process
        $trialProcess.StartInfo = $start
        [void]$trialProcess.Start()
        $outputTask = $trialProcess.StandardOutput.ReadToEndAsync()
        $errorTask = $trialProcess.StandardError.ReadToEndAsync()
        if (-not $trialProcess.WaitForExit(15000)) {
            $trialProcess.Kill()
            $trialProcess.WaitForExit()
            throw 'Native configuration check timed out. Existing background was not stopped.'
        }
        $outputText = $outputTask.GetAwaiter().GetResult()
        $errorText = $errorTask.GetAwaiter().GetResult()
        if ($trialProcess.ExitCode -ne 0) { throw ('Native configuration check failed: ' + $errorText.Trim()) }
        $checked = $outputText | ConvertFrom-Json
        if (-not $checked.config_valid) { throw 'Native configuration check returned an invalid result.' }
        $script:candidateRevision = [string]$checked.revision
        $script:candidateFingerprint = [string]$checked.runtime_fingerprint
        if ([version]$script:candidateRevision -lt [version]'5.15') { throw 'Candidate executable is too old for this installer. Extract the complete package.' }
    } finally {
        if ($trialProcess) { $trialProcess.Dispose() }
        Remove-Item -LiteralPath (Join-Path $trialRoot 'config.json') -Force -ErrorAction SilentlyContinue
        Remove-Item -LiteralPath $trialRoot -Force -ErrorAction SilentlyContinue
    }
}
$installRoot = Join-Path $env:LOCALAPPDATA 'AntigravitySystemProxy'
$installedManifest = Join-Path $installRoot 'shortcuts.json'
$installedConfig = Join-Path $installRoot 'config.json'
$shellObject = New-Object -ComObject WScript.Shell
$personalFolders = @([Environment]::GetFolderPath('Desktop'), [Environment]::GetFolderPath('Programs'))
# Package filesystem virtualization can make a shortcut look installed while
# Explorer sees no file. Tests use isolated folders; real personal shortcuts
# must be installed from an ordinary desktop process, never a private overlay.
if (-not $InspectOnly -and ([Environment]::GetFolderPath('Programs') -in $personalFolders)) {
    Set-ProxyShortcutIdentity '' '' ''
    if ([AntigravityShortcutIdentity]::IsPackaged()) {
        throw 'Packaged host redirects personal AppData. Run install.cmd from Windows File Explorer so Start menu shortcuts are installed in the real user profile.'
    }
    # Descendants can retain redirected file access without a package identity.
    # Verify a disposable Shell-created link's physical path as well.
    $probePath = Join-Path ([Environment]::GetFolderPath('Programs')) ('Antigravity-proxy-location-' + [Guid]::NewGuid().ToString('N') + '.lnk')
    try {
        $probeLink = $shellObject.CreateShortcut($probePath)
        $probeLink.TargetPath = $sourceNative
        $probeLink.Save()
        $resolvedProbe = (& $sourceNative --resolve-dir $probePath | Out-String).Trim()
        if ($LASTEXITCODE -ne 0 -or -not $resolvedProbe.Equals([IO.Path]::GetFullPath($probePath),[StringComparison]::OrdinalIgnoreCase)) {
            throw 'Personal shortcut writes are redirected. Run install.cmd from Windows File Explorer so Start menu shortcuts are installed in the real user profile.'
        }
    } finally {
        if (Test-Path -LiteralPath $probePath) { Remove-Item -LiteralPath $probePath -Force }
    }
}
# A Store-packaged host can redirect LocalAppData. Reuse the actual existing
# installation referenced by personal shortcuts when reinstalling outside that host.
foreach ($folder in $personalFolders) {
    foreach ($link in @(Get-ChildItem -LiteralPath $folder -Filter '*Antigravity*.lnk' -Recurse -File -ErrorAction SilentlyContinue)) {
        $existingShortcut = $shellObject.CreateShortcut($link.FullName)
        if ([IO.Path]::GetFileName($existingShortcut.TargetPath) -eq $nativeName) {
            $nativeRoot = Split-Path -Parent $existingShortcut.TargetPath
            $localPrefix = [IO.Path]::GetFullPath($env:LOCALAPPDATA).TrimEnd('\') + '\'
            if ($nativeRoot.StartsWith($localPrefix,[StringComparison]::OrdinalIgnoreCase) -and (Test-Path -LiteralPath (Join-Path $nativeRoot 'shortcuts.json'))) { $installRoot = $nativeRoot }
        }

    }
}
$installedManifest = Join-Path $installRoot 'shortcuts.json'
$installedConfig = Join-Path $installRoot 'config.json'
function Test-PersonalShortcut([string]$Path) {
    try { $full = [IO.Path]::GetFullPath($Path) } catch { throw ('Invalid shortcut path: ' + $Path) }
    if ([IO.Path]::GetExtension($full) -ne '.lnk') { return $false }
    foreach ($folder in $personalFolders) {
        $prefix = [IO.Path]::GetFullPath($folder).TrimEnd('\') + '\'
        if ($full.StartsWith($prefix, [StringComparison]::OrdinalIgnoreCase)) { return $true }
    }
    return $false
}
if (Test-Path -LiteralPath $installRoot) {
    $installRoot = (& $sourceNative --resolve-dir $installRoot | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) { throw 'Could not resolve the installation directory.' }
    $installedManifest = Join-Path $installRoot 'shortcuts.json'
    $installedConfig = Join-Path $installRoot 'config.json'
}
function Get-NativeStatus([string]$Executable) {
    $raw = (& $Executable --status | Out-String)
    if ($LASTEXITCODE -ne 0) { throw 'Could not read native status.' }
    return ($raw | ConvertFrom-Json)
}
function Get-SnapshotHash([string]$Path) {
    $stream = [IO.File]::OpenRead($Path)
    $hash = [Security.Cryptography.SHA256]::Create()
    try { return [BitConverter]::ToString($hash.ComputeHash($stream)).Replace('-','') }
    finally { $hash.Dispose(); $stream.Dispose() }
}
function Resume-Native([string]$Executable) {
    if (Test-Path -LiteralPath $Executable) {
        $status = Get-NativeStatus $Executable
        if ($status.bridge_running -and [version]$status.revision -ge [version]'5.15') {
            & $Executable --resume | Out-Host
            if ($LASTEXITCODE -ne 0) { throw 'Could not resume the installed worker.' }
        }
    }
}
function Write-UpgradeJournal($Journal, [string]$Directory) {
    $target = Join-Path $Directory 'journal.json'
    $temporary = Join-Path $Directory 'journal.tmp'
    $Journal | ConvertTo-Json -Depth 6 | Set-Content -LiteralPath $temporary -Encoding UTF8
    if (Test-Path -LiteralPath $target) { [IO.File]::Replace($temporary,$target,(Join-Path $Directory 'journal.previous')) }
    else { [IO.File]::Move($temporary,$target) }
}
function Clear-UpgradeJournal($Journal, [string]$Directory) {
    foreach ($entry in $Journal.Entries) {
        $snapshot = Join-Path $Directory $entry.Snapshot
        if ($entry.Existed -and (Test-Path -LiteralPath $snapshot -PathType Leaf)) {
            Remove-Item -LiteralPath $snapshot -Force -ErrorAction Stop
        }
    }
    foreach ($name in @('journal.tmp','journal.previous','journal.json')) {
        $path = Join-Path $Directory $name
        if (Test-Path -LiteralPath $path -PathType Leaf) { Remove-Item -LiteralPath $path -Force -ErrorAction Stop }
    }
    # Deliberately non-recursive: never remove unexpected contents.
    Remove-Item -LiteralPath $Directory -Force -ErrorAction Stop
}
function Restore-UpgradeJournal([string]$Directory) {
    $journalFile = Join-Path $Directory 'journal.json'
    if (-not (Test-Path -LiteralPath $journalFile -PathType Leaf)) {
        throw 'Incomplete upgrade snapshot. Inspect upgrade-pending before retrying.'
    }
    $journal = Get-Content -LiteralPath $journalFile -Raw | ConvertFrom-Json
    if ($journal.Version -ne 1 -or $journal.Root -ne $installRoot) { throw 'Upgrade journal identity mismatch.' }
    $allowed = @(@($nativeName,'AntigravitySystemProxy.previous.exe','restore-shortcuts.ps1','README.md','config.json','shortcuts.json','active-release.json') | ForEach-Object { Join-Path $installRoot $_ })
    $backupPrefix = [IO.Path]::GetFullPath((Join-Path $installRoot 'backup')).TrimEnd('\') + '\'
    # Validate every path before stopping a process or restoring files.
    foreach ($entry in $journal.Entries) {
        $target = [IO.Path]::GetFullPath($entry.Path)
        $validBackup = $target.StartsWith($backupPrefix,[StringComparison]::OrdinalIgnoreCase) -and [IO.Path]::GetExtension($target) -eq '.lnk'
        if ($target -notin $allowed -and -not (Test-PersonalShortcut $target) -and -not $validBackup) { throw 'Upgrade rollback target is outside allowed paths.' }
        if ($entry.Snapshot -notmatch '^file-[0-9]+[.]bin$') { throw 'Invalid upgrade snapshot name.' }
    }
    if ($journal.Phase -in @('committed','rolled_back')) {
        Clear-UpgradeJournal $journal $Directory
        return
    }
    foreach ($entry in $journal.Entries) {
        if ($entry.Existed) {
            $snapshot = Join-Path $Directory $entry.Snapshot
            if (-not (Test-Path -LiteralPath $snapshot -PathType Leaf) -or (Get-SnapshotHash $snapshot) -ne $entry.SHA256) { throw 'Upgrade snapshot is missing or corrupt.' }
        }
    }
    $native = Join-Path $installRoot $nativeName
    if ($journal.Phase -eq 'prepared') {
        Resume-Native $native
        if ($journal.WasRunning) {
            & $native --ensure | Out-Host
            if ($LASTEXITCODE -ne 0) { throw 'Could not restore old worker readiness; snapshot retained.' }
        }
        $journal.Phase = 'rolled_back'
        Write-UpgradeJournal $journal $Directory
        Clear-UpgradeJournal $journal $Directory
        return
    }
    if ($journal.Phase -ne 'mutating') { throw 'Invalid upgrade journal phase.' }
    if (-not $journal.Seamless -and (Test-Path -LiteralPath $native)) {
        & $native --stop | Out-Host
        if ($LASTEXITCODE -ne 0) { throw 'Could not stop candidate before rollback; snapshot retained.' }
    }
    foreach ($entry in $journal.Entries) {
        if ($entry.Existed) {
            New-Item -ItemType Directory -Path (Split-Path -Parent $entry.Path) -Force | Out-Null
            Copy-Item -LiteralPath (Join-Path $Directory $entry.Snapshot) -Destination $entry.Path -Force
        } elseif (Test-Path -LiteralPath $entry.Path -PathType Leaf) {
            Remove-Item -LiteralPath $entry.Path -Force
        }
    }
    if ($journal.Seamless) {
        & $native --reconcile-active | Out-Host
        if ($LASTEXITCODE -ne 0) { throw 'Could not reconcile the running release after rollback; inspect diagnostics.' }
    }
    if ($journal.WasRunning) {
        & $native --ensure | Out-Host
        if ($LASTEXITCODE -ne 0) { throw 'Old files restored, but old worker did not become healthy; snapshot retained.' }
    }
    $journal.Phase = 'rolled_back'
    Write-UpgradeJournal $journal $Directory
    Clear-UpgradeJournal $journal $Directory
}
$guardHash = [Security.Cryptography.SHA256]::Create()
try { $guardKey = [BitConverter]::ToString($guardHash.ComputeHash([Text.Encoding]::UTF8.GetBytes($installRoot.ToLowerInvariant()))).Replace('-','') } finally { $guardHash.Dispose() }
$installGuard = New-Object Threading.Mutex($false,('Local\AntigravitySystemProxy-install-' + $guardKey))
$guardHeld = $false
try {
    try { $guardHeld = $installGuard.WaitOne(0) } catch [Threading.AbandonedMutexException] { $guardHeld = $true }
    if (-not $guardHeld) { throw 'Another installer is already working on this installation.' }
    $pendingUpgrade = Join-Path $installRoot 'upgrade-pending'
    if (Test-Path -LiteralPath $pendingUpgrade) {
        if ($InspectOnly) { throw 'Interrupted upgrade detected. Run installation without InspectOnly to recover it first.' }
        Restore-UpgradeJournal $pendingUpgrade
        Write-Host 'Recovered the previous interrupted upgrade before continuing.'
    }
$appCandidates = @()
if ($AntigravityPath) {
    if (-not (Test-Path -LiteralPath $AntigravityPath -PathType Leaf)) { throw 'Specified Antigravity executable does not exist.' }
    $appCandidates += [IO.Path]::GetFullPath($AntigravityPath)
} else {
    if (Test-Path -LiteralPath $installedConfig) {
        $oldConfig = Get-Content -LiteralPath $installedConfig -Raw | ConvertFrom-Json
        $appCandidates += $oldConfig.application
        if (-not $PSBoundParameters.ContainsKey('Port') -and $oldConfig.port) { $Port = [int]$oldConfig.port }
    }
    $appCandidates += @(Get-CimInstance Win32_Process -Filter "Name='Antigravity.exe'" -ErrorAction SilentlyContinue | Select-Object -ExpandProperty ExecutablePath)
    foreach ($folder in @($personalFolders + @([Environment]::GetFolderPath('CommonPrograms'),[Environment]::GetFolderPath('CommonDesktopDirectory')))) {
        if ($folder -and (Test-Path -LiteralPath $folder)) {
            foreach ($link in @(Get-ChildItem -LiteralPath $folder -Filter '*Antigravity*.lnk' -Recurse -File -ErrorAction SilentlyContinue)) { $appCandidates += $shellObject.CreateShortcut($link.FullName).TargetPath }
        }
    }
    $appCandidates += Join-Path $env:LOCALAPPDATA 'Programs\antigravity\Antigravity.exe'
    foreach ($programFolder in @($env:ProgramFiles,${env:ProgramFiles(x86)})) {
        if ($programFolder) { $appCandidates += Join-Path $programFolder 'Antigravity\Antigravity.exe' }
    }
    foreach ($registryRoot in @('HKCU:\Software\Microsoft\Windows\CurrentVersion\Uninstall','HKLM:\Software\Microsoft\Windows\CurrentVersion\Uninstall')) {
        foreach ($key in @(Get-ChildItem -LiteralPath $registryRoot -ErrorAction SilentlyContinue)) {
            $record = Get-ItemProperty -LiteralPath $key.PSPath -ErrorAction SilentlyContinue
            if ($record.DisplayName -match '^(Google )?Antigravity(?:\s+\d[\w.\-+]*)?$') {
                if ($record.InstallLocation) { $appCandidates += Join-Path $record.InstallLocation 'Antigravity.exe' }
                if ($record.DisplayIcon) {
                    $iconPath = ([string]$record.DisplayIcon -replace ',\s*-?\d+\s*$','').Trim().Trim('"')
                    if ([IO.Path]::IsPathRooted($iconPath) -and [IO.Path]::GetFileName($iconPath) -eq 'Antigravity.exe') { $appCandidates += $iconPath }
                }
            }
        }
    }
}
$appExe = $null
foreach ($candidate in $appCandidates) {
    if ($candidate -and [IO.Path]::GetFileName($candidate) -eq 'Antigravity.exe' -and (Test-Path -LiteralPath $candidate -PathType Leaf)) { $appExe = [IO.Path]::GetFullPath($candidate); break }
}
if (-not $appExe) { throw 'Could not locate Antigravity.exe. Pass -AntigravityPath "D:\path\Antigravity.exe".' }
# Never import a delivery-folder manifest: paths and backups belong to one computer.
$originals = @()
if (Test-Path -LiteralPath $installedManifest) {
    $originals = Get-Content -LiteralPath $installedManifest -Raw | ConvertFrom-Json
    $originals = @($originals)
    $manifestPaths = @{}
    foreach ($entry in $originals) {
        if ($entry.PSObject.Properties['Created'] -and $entry.Created -isnot [bool]) { throw 'Invalid Created flag in shortcut manifest.' }
        $manifestPath = [IO.Path]::GetFullPath($entry.Path)
        if ($manifestPaths.ContainsKey($manifestPath)) { throw 'Duplicate shortcut path in installed manifest.' }
        $manifestPaths[$manifestPath] = $true
        if (-not $entry.Created -and [IO.Path]::GetExtension($entry.Backup) -ne '.lnk') { throw 'Shortcut backup must be a .lnk file.' }
        if (-not $entry.Created -and (Test-Path -LiteralPath $entry.Backup)) {
            $entry.Backup = (& $sourceNative --resolve-dir $entry.Backup | Out-String).Trim()
        }
        $backupPrefix = [IO.Path]::GetFullPath((Join-Path $installRoot 'backup')).TrimEnd('\') + '\'
        if (-not (Test-PersonalShortcut $entry.Path)) { throw 'Installed shortcut manifest does not belong to this Windows profile.' }
        if (-not $entry.Created -and (-not ([IO.Path]::GetFullPath($entry.Backup).StartsWith($backupPrefix,[StringComparison]::OrdinalIgnoreCase)) -or -not (Test-Path -LiteralPath $entry.Backup))) { throw 'Shortcut backup is missing or invalid. No shortcuts changed.' }
    }
}
$linkPaths = @($originals | Select-Object -ExpandProperty Path)
foreach ($folder in $personalFolders) {
    if (Test-Path -LiteralPath $folder) {
        foreach ($link in @(Get-ChildItem -LiteralPath $folder -Filter '*.lnk' -Recurse -File -ErrorAction SilentlyContinue)) {
            if ($shellObject.CreateShortcut($link.FullName).TargetPath -eq $appExe) { $linkPaths += $link.FullName }
        }
    }
    $linkPaths += Join-Path $folder 'Antigravity.lnk'
    $linkPaths += Join-Path $folder 'Antigravity (System Proxy).lnk'
}
$linkPaths = @($linkPaths | Select-Object -Unique)
foreach ($path in $linkPaths) { if (-not (Test-PersonalShortcut $path)) { throw 'Shortcut path is outside this Windows profile.' } }
$listener = New-Object Net.Sockets.TcpListener([Net.IPAddress]::Loopback,$Port)
$portAvailable = $false
try { $listener.Start(); $portAvailable = $true } catch { } finally { $listener.Stop() }
if (-not $portAvailable) {
    try {
        $controlRecord = $null
        $workerStatePath = Join-Path $installRoot ('worker-' + $Port + '.json')
        if (Test-Path -LiteralPath $workerStatePath) {
            $record = Get-Content -LiteralPath $workerStatePath -Raw | ConvertFrom-Json
            if ($record.proxy_port -eq $Port -and ($record.script -eq (Join-Path $installRoot $nativeName) -or $record.script -match ('^' + [regex]::Escape($installRoot) + '\\releases\\[0-9a-f]{64}\\AntigravitySystemProxy[.]exe$')) -and $record.version -eq 'Antigravity-SystemProxy-v5' -and $record.health_port -ge 1 -and $record.health_port -le 65535) {
                $healthPort = [int]$record.health_port
                $controlRecord = $record
            }
        }
        if (-not $controlRecord) { throw 'No native worker identity for the occupied port.' }
        $request = [Net.HttpWebRequest]::Create(('http://127.0.0.1:' + $healthPort + '/__antigravity_system_proxy_health__'))
        $request.Proxy = $null; $request.Timeout = 1500; $request.ReadWriteTimeout = 1500
        $response = $request.GetResponse()
        if ($response.Headers['X-Worker-Pid'] -ne [string]$controlRecord.pid -or $response.Headers['X-Worker-Generation'] -ne $controlRecord.generation) { $response.Dispose(); throw 'Worker identity mismatch' }
        $reader = New-Object IO.StreamReader($response.GetResponseStream())
        try { $marker = $reader.ReadToEnd() } finally { $reader.Dispose(); $response.Dispose() }
        if ($marker -ne 'Antigravity-SystemProxy-v5') { throw 'Different service' }
    } catch { throw ('Port ' + $Port + ' is occupied by another service. Pass -Port with an available port.') }
}
$configToWrite = @{application=$appExe;port=$Port}
if (Test-Path -LiteralPath $installedConfig) {
    $previousConfig = Get-Content -LiteralPath $installedConfig -Raw | ConvertFrom-Json
    foreach ($field in @('socket_write_metrics','idle_timeout_seconds','proxy_ca_file','max_connections','max_connections_per_destination','max_pending_connections','admission_wait_seconds','health_failures_before_restart','websocket_write_timeout_seconds')) {
        if ($null -ne $previousConfig.$field) { $configToWrite[$field] = $previousConfig.$field }
    }
}
# Check the candidate binary and configuration before changing backups, shortcuts,
# or the running worker. The check performs no outbound network requests.
Assert-NativeConfiguration $configToWrite $installRoot
$plan = [pscustomobject]@{Application=$appExe;Engine='Go native / no Python required';InstallDirectory=$installRoot;Port=$Port;Shortcuts=$linkPaths;ExistingBackups=$originals.Count;EntryRestartRequested=[bool]$RestartEntry}
if ($InspectOnly) { $plan | ConvertTo-Json -Depth 5; exit 0 }
foreach ($name in @($nativeName,'restore-shortcuts.ps1','README.md')) {
    if (-not (Test-Path -LiteralPath (Join-Path $sourceRoot $name) -PathType Leaf)) { throw ('Package file is missing: ' + $name) }
}
$oldWasRunning = $false
$installedNative = Join-Path $installRoot $nativeName
$oldStatus = $null
if (Test-Path -LiteralPath $installedNative) { $oldStatus = Get-NativeStatus $installedNative; $oldWasRunning = [bool]$oldStatus.bridge_running }
if ($oldWasRunning -and [version]$oldStatus.revision -lt [version]'5.15') { throw 'Unsupported pre-entry installation. Stop it explicitly and restore its original shortcuts before installing this package.' }
$seamless = $false
if (-not $RestartEntry -and $oldWasRunning -and $oldStatus.port -eq $Port -and [version]$oldStatus.revision -ge [version]'5.15') {
    $oldDiagnostics = (& $installedNative --diagnostics | Out-String) | ConvertFrom-Json
    $seamless = [bool]$oldDiagnostics.upgrade_entry.seamless_upgrade -and $oldDiagnostics.runtime_fingerprint -eq $script:candidateFingerprint
}
New-Item -ItemType Directory -Path (Join-Path $installRoot 'backup') -Force | Out-Null
$installRoot = (& $sourceNative --resolve-dir $installRoot | Out-String).Trim()
$installedManifest = Join-Path $installRoot 'shortcuts.json'
$installedConfig = Join-Path $installRoot 'config.json'
$plan.InstallDirectory = $installRoot
$transactionDirectory = Join-Path $installRoot 'upgrade-pending'
New-Item -ItemType Directory -Path $transactionDirectory -ErrorAction Stop | Out-Null
$snapshotTargets = @(@($nativeName,'AntigravitySystemProxy.previous.exe','restore-shortcuts.ps1','README.md','config.json','shortcuts.json','active-release.json') | ForEach-Object { Join-Path $installRoot $_ })
$snapshotTargets += $linkPaths
foreach ($path in $linkPaths) {
    if (@($originals | Where-Object { $_.Path -eq $path }).Count -eq 0 -and (Test-Path -LiteralPath $path)) {
        $hash = [Security.Cryptography.SHA256]::Create()
        try { $digest = [BitConverter]::ToString($hash.ComputeHash([Text.Encoding]::UTF8.GetBytes($path.ToLowerInvariant()))).Replace('-','').Substring(0,16) } finally { $hash.Dispose() }
        $snapshotTargets += Join-Path (Join-Path $installRoot 'backup') ($digest + '.lnk')
    }
}
$entries = @()
foreach ($target in @($snapshotTargets | Select-Object -Unique)) {
    $exists = Test-Path -LiteralPath $target -PathType Leaf
    $snapshotName = 'file-' + $entries.Count + '.bin'
    $digest = $null
    if ($exists) {
        Copy-Item -LiteralPath $target -Destination (Join-Path $transactionDirectory $snapshotName)
        $digest = Get-SnapshotHash (Join-Path $transactionDirectory $snapshotName)
    }
    $entries += [pscustomobject]@{Path=$target;Existed=$exists;Snapshot=$snapshotName;SHA256=$digest}
}
$journal = [pscustomobject]@{Version=1;Root=$installRoot;WasRunning=$oldWasRunning;Phase='prepared';Entries=$entries;Seamless=$seamless}
Write-UpgradeJournal $journal $transactionDirectory
$upgradeCommitted = $false
try {
    if ($oldWasRunning -and -not $seamless) {
        & $installedNative --drain $DrainSeconds | Out-Host
        if ($LASTEXITCODE -ne 0) { throw 'Upgrade cancelled: connections did not drain. Existing worker retained.' }
    }
    $journal.Phase = 'mutating'
    Write-UpgradeJournal $journal $transactionDirectory
$newManifest = @()
foreach ($path in $linkPaths) {
    $old = @($originals | Where-Object { $_.Path -eq $path })
    if ($old.Count -gt 0) { $newManifest += [pscustomobject]@{Path=$old[0].Path;Backup=$old[0].Backup;Created=[bool]$old[0].Created}; continue }
    $created = -not (Test-Path -LiteralPath $path)
    $backupPath = $null
    if (-not $created) {
        $hash = [Security.Cryptography.SHA256]::Create()
        try { $digest = [BitConverter]::ToString($hash.ComputeHash([Text.Encoding]::UTF8.GetBytes($path.ToLowerInvariant()))).Replace('-','').Substring(0,16) } finally { $hash.Dispose() }
        $backupPath = Join-Path (Join-Path $installRoot 'backup') ($digest + '.lnk')
        Copy-Item -LiteralPath $path -Destination $backupPath
    }
    $newManifest += [pscustomobject]@{Path=$path;Backup=$backupPath;Created=$created}
}
$newManifest | ConvertTo-Json -Depth 4 | Set-Content -LiteralPath $installedManifest -Encoding UTF8
$installedNative = Join-Path $installRoot $nativeName
# Native executables must exit before Windows permits replacing their image file.
if (Test-Path -LiteralPath $installedNative) {
    if (-not $seamless) {
        & $installedNative --stop | Out-Host
        if ($LASTEXITCODE -ne 0) { throw 'Could not stop the installed native worker.' }
    }
    Copy-Item -LiteralPath $installedNative -Destination (Join-Path $installRoot 'AntigravitySystemProxy.previous.exe') -Force
}
foreach ($name in @($nativeName,'restore-shortcuts.ps1','README.md')) {
    $source = [IO.Path]::GetFullPath((Join-Path $sourceRoot $name))
    $destination = [IO.Path]::GetFullPath((Join-Path $installRoot $name))
    if ($source -ne $destination) { Copy-Item -LiteralPath $source -Destination $destination -Force }
}
# Stop the old worker using its old port before publishing the new configuration.
$configToWrite | ConvertTo-Json -Depth 3 | Set-Content -LiteralPath $installedConfig -Encoding UTF8
foreach ($entry in $newManifest) {
    New-Item -ItemType Directory -Path (Split-Path -Parent $entry.Path) -Force | Out-Null
    $shortcut = $shellObject.CreateShortcut($entry.Path)
    $shortcut.TargetPath = $installedNative
    $shortcut.Arguments = ''
    $shortcut.WorkingDirectory = Split-Path -Parent $appExe
    if (-not $entry.Created -and $entry.Backup) {
        $originalShortcut = $shellObject.CreateShortcut($entry.Backup)
        if ($originalShortcut.Arguments) { $shortcut.Arguments = $originalShortcut.Arguments }
        if ($originalShortcut.WorkingDirectory) { $shortcut.WorkingDirectory = $originalShortcut.WorkingDirectory }
    }
    $shortcut.IconLocation = $appExe + ',0'
    $shortcut.Description = 'Antigravity - follow Windows system proxy'
    $shortcut.Save()
    $proxyAppId = $null
    if ([IO.Path]::GetFileName($entry.Path) -eq 'Antigravity (System Proxy).lnk') {
        $proxyAppId = 'Antigravity.SystemProxy.' + $guardKey.Substring(0,24)
    }
    $proxyRelaunch = '"' + $installedNative + '"'
    if ($shortcut.Arguments) { $proxyRelaunch += ' ' + $shortcut.Arguments }
    Set-ProxyShortcutIdentity $entry.Path $proxyAppId $proxyRelaunch
}
if ($seamless) {
    & $installedNative --upgrade $installedNative | Out-Host
    if ($LASTEXITCODE -ne 0) { throw 'Seamless upgrade was not confirmed. Existing connections retained; inspect diagnostics before retrying.' }
} else {
    & $installedNative --activate | Out-Host
    if ($LASTEXITCODE -ne 0) { throw 'Could not activate the candidate release.' }
    & $installedNative --ensure | Out-Host
    if ($LASTEXITCODE -ne 0) { throw 'Native worker failed to become healthy. Check supervisor.log.' }
}
$verificationRaw = (& $installedNative --diagnostics | Out-String)
if ($LASTEXITCODE -ne 0) { throw 'Candidate runtime diagnostics failed after startup.' }
$verification = $verificationRaw | ConvertFrom-Json
if ([string]$verification.revision -ne $script:candidateRevision -or $verification.draining) { throw 'Candidate runtime version or readiness verification failed.' }
$runtimeStatus = Get-NativeStatus $installedNative
if (-not $runtimeStatus.ready) { throw 'Candidate is alive but routing readiness validation failed.' }
& $installedNative --follow-app | Out-Host
if ($LASTEXITCODE -ne 0) { throw 'Could not arm application lifetime monitoring.' }
$journal.Phase = 'committed'
Write-UpgradeJournal $journal $transactionDirectory
$upgradeCommitted = $true
} catch {
    $upgradeError = $_
    try {
        Restore-UpgradeJournal $transactionDirectory
        if ($seamless) { Write-Host 'Upgrade failed; previous launcher files and shortcuts restored, running generations retained.' }
        else { Write-Host 'Upgrade failed; previous files, shortcuts and running state restored.' }
    } catch {
        throw ('Upgrade failed and automatic rollback could not finish. Snapshot retained at ' + $transactionDirectory + '. Rollback error: ' + $_.Exception.Message + '. Original error: ' + $upgradeError.Exception.Message)
    }
    throw $upgradeError
}
if ($upgradeCommitted) {
    try { Clear-UpgradeJournal $journal $transactionDirectory } catch { Write-Warning ('Upgrade succeeded, but snapshot cleanup failed: ' + $_.Exception.Message) }
}
$plan | ConvertTo-Json -Depth 5
Write-Host 'Installed for the current Windows user. Close and reopen Antigravity after first install or changing the proxy port.'
} finally {
    if ($guardHeld) { $installGuard.ReleaseMutex() }
    $installGuard.Dispose()
}
