param([switch]$InspectOnly)
$ErrorActionPreference = 'Stop'

function Assert-RestorePath([string]$Path, [string]$Boundary) {
    $cursor = $Path
    while (-not $cursor.Equals($Boundary,[StringComparison]::OrdinalIgnoreCase)) {
        if (Test-Path -LiteralPath $cursor) {
            $item = Get-Item -LiteralPath $cursor -Force
            if (($item.Attributes -band [IO.FileAttributes]::ReparsePoint) -ne 0) {
                throw 'Restore paths cannot traverse links below their allowed root.'
            }
        }
        $parent = Split-Path -Parent $cursor
        if (-not $parent -or $parent -eq $cursor) { throw 'Restore path escaped its allowed root.' }
        $cursor = $parent
    }
}

function Get-ValidatedRestorePlan($Entries, [string]$Root, [string[]]$Folders) {
    $rootFull = [IO.Path]::GetFullPath($Root).TrimEnd('\')
    $backupPrefix = (Join-Path $rootFull 'backup') + '\'
    $seen = @{}
    $plan = @()
    foreach ($entry in @($Entries)) {
        if ($null -eq $entry -or -not [IO.Path]::IsPathRooted([string]$entry.Path)) { throw 'Invalid restore manifest entry.' }
        # Original manifests predate Created. A missing flag means restoring a
        # backup; an explicit non-boolean flag must never become a deletion.
        $created = $false
        if ($null -ne $entry.PSObject.Properties['Created']) {
            if ($entry.Created -isnot [bool]) { throw 'Invalid Created flag in restore manifest.' }
            $created = $entry.Created
        }
        $full = [IO.Path]::GetFullPath([string]$entry.Path)
        if ([IO.Path]::GetExtension($full) -ne '.lnk' -or $seen.ContainsKey($full)) { throw 'Restore targets must be unique shortcuts.' }
        $allowedRoot = $null
        foreach ($folder in $Folders) {
            if (-not $folder) { continue }
            $folderFull = [IO.Path]::GetFullPath($folder).TrimEnd('\')
            if ($full.StartsWith($folderFull + '\',[StringComparison]::OrdinalIgnoreCase)) { $allowedRoot = $folderFull; break }
        }
        if (-not $allowedRoot) { throw 'Restore target is outside this Windows profile desktop and Programs folders.' }
        Assert-RestorePath $full $allowedRoot
        if ((Test-Path -LiteralPath $full) -and -not (Test-Path -LiteralPath $full -PathType Leaf)) { throw 'Restore target is not a file.' }
        $backup = $null
        if (-not $created) {
            if (-not [IO.Path]::IsPathRooted([string]$entry.Backup)) { throw 'Invalid shortcut backup path.' }
            $backup = [IO.Path]::GetFullPath([string]$entry.Backup)
            if (-not $backup.StartsWith($backupPrefix,[StringComparison]::OrdinalIgnoreCase) -or [IO.Path]::GetExtension($backup) -ne '.lnk' -or -not (Test-Path -LiteralPath $backup -PathType Leaf)) { throw 'Shortcut backup is missing or outside the installation backup folder.' }
            Assert-RestorePath $backup $rootFull
            if (-not (Test-Path -LiteralPath (Split-Path -Parent $full) -PathType Container)) { throw 'Restore destination directory is missing.' }
            $readable = [IO.File]::OpenRead($backup)
            $readable.Dispose()
        } elseif (Test-Path -LiteralPath $full) {
            if (((Get-Item -LiteralPath $full -Force).Attributes -band [IO.FileAttributes]::ReadOnly) -ne 0) { throw 'Created shortcut is read-only.' }
        }
        $seen[$full] = $true
        $plan += [pscustomobject]@{Path=$full;Backup=$backup;Created=$created}
    }
    return $plan
}
$adapterRoot = Join-Path $env:LOCALAPPDATA 'AntigravitySystemProxy'
$restoreDirectory = Split-Path -Parent $MyInvocation.MyCommand.Path
if ((Test-Path -LiteralPath (Join-Path $restoreDirectory 'config.json')) -and (Test-Path -LiteralPath (Join-Path $restoreDirectory 'shortcuts.json'))) {
    $adapterRoot = $restoreDirectory
} else {
    $restoreShell = New-Object -ComObject WScript.Shell
    foreach ($folder in @([Environment]::GetFolderPath('Desktop'),[Environment]::GetFolderPath('Programs'))) {
        foreach ($link in @(Get-ChildItem -LiteralPath $folder -Filter '*Antigravity*.lnk' -Recurse -File -ErrorAction SilentlyContinue)) {
            $shortcut = $restoreShell.CreateShortcut($link.FullName)
            if ([IO.Path]::GetFileName($shortcut.TargetPath) -eq 'AntigravitySystemProxy.exe') {
                $candidateRoot = Split-Path -Parent $shortcut.TargetPath
                if (Test-Path -LiteralPath (Join-Path $candidateRoot 'shortcuts.json')) { $adapterRoot = $candidateRoot }
            }

        }
    }
}
$native = Join-Path $adapterRoot 'AntigravitySystemProxy.exe'
if (Test-Path -LiteralPath $native -PathType Leaf) {
    $adapterRoot = (& $native --resolve-dir $adapterRoot | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) { throw 'Could not resolve the installation directory.' }
    $native = Join-Path $adapterRoot 'AntigravitySystemProxy.exe'
}
$guardHash = [Security.Cryptography.SHA256]::Create()
try { $guardKey = [BitConverter]::ToString($guardHash.ComputeHash([Text.Encoding]::UTF8.GetBytes($adapterRoot.ToLowerInvariant()))).Replace('-','') } finally { $guardHash.Dispose() }
$restoreGuard = New-Object Threading.Mutex($false,('Local\AntigravitySystemProxy-install-' + $guardKey))
$guardHeld = $false
try {
try { $guardHeld = $restoreGuard.WaitOne(0) } catch [Threading.AbandonedMutexException] { $guardHeld = $true }
if (-not $guardHeld) { throw 'Another install or restore is working on this installation.' }
if (Test-Path -LiteralPath (Join-Path $adapterRoot 'upgrade-pending')) { throw 'Interrupted upgrade detected. Run install.cmd to recover before restoring.' }
if (-not $InspectOnly -and (Get-Process Antigravity -ErrorAction SilentlyContinue)) {
    Write-Host 'Close Antigravity before restoring its original shortcuts.'
    exit 1
}
$adapterManifest = Get-Content -LiteralPath (Join-Path $adapterRoot 'shortcuts.json') -Raw | ConvertFrom-Json
$restorePlan = @(Get-ValidatedRestorePlan $adapterManifest $adapterRoot @([Environment]::GetFolderPath('Desktop'),[Environment]::GetFolderPath('Programs')))
if ($InspectOnly) { $restorePlan | ConvertTo-Json -Depth 4; exit 0 }
if (Test-Path -LiteralPath $native) {
    & $native --stop | Out-Host
    if ($LASTEXITCODE -ne 0) { throw 'Could not stop the native adapter. Shortcuts retained.' }
}
foreach ($entry in $restorePlan) {
    if ($entry.Created) {
        if (Test-Path -LiteralPath $entry.Path) { Remove-Item -LiteralPath $entry.Path }
    } else {
        Copy-Item -LiteralPath $entry.Backup -Destination $entry.Path -Force
    }
}
Write-Host 'Original shortcuts restored. Adapter stopped.'
} finally {
    if ($guardHeld) { $restoreGuard.ReleaseMutex() }
    $restoreGuard.Dispose()
}
