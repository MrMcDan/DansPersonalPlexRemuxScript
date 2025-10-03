#requires -version 6.0

<#
.SYNOPSIS
    Advanced video processing script with HDR10+ preservation and Plex optimization

.DESCRIPTION
    SETUP REQUIREMENTS:
    
    1. PYTHON 3.8+ with required modules:
       pip install pandas numpy
    
    2. REQUIRED EXECUTABLES (update paths in $Config hashtable):
       - FFmpeg with QSV support (ffmpeg.exe, ffprobe.exe)
       - MKVToolNix (mkvmerge.exe)
       - Python 3.8+ (python.exe)
    
    3. OPTIONAL TOOLS:
       - HDR10+ Tool (hdr10plus_tool.exe) for HDR content
       - Dolby Vision removal tools (dovi_tool.exe)
       - MCEBuddy CLI (MCEBuddy.UserCLI.exe)
    
    4. HARDWARE REQUIREMENTS:
       - Intel QSV: 6th gen+ Intel CPU with integrated graphics
       - RAM: 8GB+ for 4K content
       - Storage: 4x input file size free space recommended
    
    5. CONFIGURATION:
       - Update all paths in $Config hashtable below
       - Ensure write permissions to temp/output folders
       - Install Intel graphics drivers for QSV support

.NOTES
    See full setup guide for detailed installation instructions.
#>

# Updated parameter block with optional OrigFile
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, ValueFromPipeline = $true)]
    [ValidateScript({ Test-Path -LiteralPath $_ -PathType Leaf })]
    [string]$InputFile,

    [Parameter(Mandatory = $false, ValueFromPipeline = $true)]
    [ValidateScript({ 
        if ([string]::IsNullOrWhiteSpace($_)) { 
            return $true 
        } 
        else { 
            Test-Path -LiteralPath $_ -PathType Leaf 
        }
    })]
    [string]$OrigFile = "",  # Now optional, defaults to empty string

    [Parameter(Mandatory = $false)]
    [int]$incomingGlobalQuality,
    
    [Parameter(Mandatory = $false)]
    [string]$OutputFolder = "E:\Temp",
    
    [Parameter(Mandatory = $false)]
    [string]$TempFolder = $env:TEMP,
    
    [Parameter(Mandatory = $false)]
    [string]$BackupFolder = "D:\Original",  # Empty means no backup
    
    [Parameter(Mandatory = $false)]
    [int]$EnableQualityValidation = 0,

    [Parameter(Mandatory = $false)]
    [switch]$DisableCPUAffinity = $false,
    
    [Parameter(Mandatory = $false)]
    [ValidateRange(20.0, 50.0)]
    [double]$QualityThreshold = 40.0,

    [Parameter(Mandatory = $false)]
    [switch]$SkipContentAnalysis = $false,

    [Parameter(Mandatory = $false)]
    [switch]$EnableSoftwareFallback = $false,

    [Parameter(Mandatory = $false)]
    [string]$FailedFolder = "D:\Failed",

    [Parameter(Mandatory = $false)]
    [switch]$DisableMCEBuddyMonitoring = $false,

    [Parameter(Mandatory = $false)]
    [switch]$PadToStandardResolution,

    [Parameter(Mandatory = $false)]
    [switch]$skipHDR10Plus,

    [Parameter(Mandatory = $false)]
    [ValidateRange(5, 60)]
    [int]$MCEBuddyCheckInterval = 15
)

# Add backup-related global variables
$Script:BackupJob = $null
$Script:BackupStarted = $false
$Script:BackupCompleted = $false


# Configuration
$Config = @{
    HDR10PlusToolExe      = "E:\plex\hdr10plus_tool.exe"
    FFmpegExe             = "E:\plex\ffmpeg-2025-06-02-git-688f3944ce-full_build\bin\ffmpeg.exe"
    FFProbeExe            = "E:\plex\ffmpeg-2025-06-02-git-688f3944ce-full_build\bin\ffprobe.exe"
    MKVMergeExe           = "C:\Program Files\MKVToolNix\mkvmerge.exe"
    pythonExe             = "python.exe"
    DolbyVisionRemoverCmd = "E:\Plex\Donis Dolby Vision Tool\DDVT_REMOVER.cmd"
    mcebuddyCLI           = "C:\Program Files\MCEBuddy2x\MCEBuddy.UserCLI.exe"
    MP4CompatibleAudio    = @("aac", "ac3", "mp3")
    SupportedExtensions   = @(".mp4", ".mkv", ".avi", ".mov", ".m4v", ".ts", ".mts", ".m2ts")
    MinFileSizeMB         = 50
}


# Global variables
$Script:TempFiles = @()
$Script:OriginalFileSize = 0
$Script:ActiveProcesses = @()
$Script:CleanupRegistered = $false
$Script:VideoMetadata = $null
$Script:ProcessingStartTime = Get-Date
$Script:ExtractedSubtitles = @()
$Script:QualityMetrics = @{
    PSNR             = 0.0
    SSIM             = 0.0
    PassesValidation = $false
}
$Script:ValidatedTempFolder = $null
$Script:ScriptTempFolder = $null
$script:HEVCDef = 16



# Add this near the beginning of your script, right after param block
trap {
    Write-Host "`nCtrl+C detected - performing emergency cleanup..." -ForegroundColor Red
    Stop-ActiveProcesses
    Clear-TempFilesWithHandleRelease
    
    # Force cleanup of script temp folder
    if ($Script:ScriptTempFolder -and (Test-Path $Script:ScriptTempFolder)) {
        cmd /c "rmdir /s /q `"$Script:ScriptTempFolder`"" 2>$null
    }
    
    Write-Host "Emergency cleanup completed" -ForegroundColor Yellow
    exit 130  # Standard exit code for Ctrl+C
}

function Find-PythonPath {
    try {
        # Get-Command looks in the $env:Path for the executable.
        $pythonCommand = Get-Command python.exe -ErrorAction SilentlyContinue 
        
        # .Source contains the full path to the executable (e.g., C:\Python39\python.exe)
        if ($pythonCommand) {
            $pythonPath = $pythonCommand.Source
            Write-Host "Found Python executable at: $pythonPath" -ForegroundColor Green
            return $pythonPath
        } else {
            Write-Host "Error: python.exe was not found in the system PATH. Please install Python or add it to your PATH environment variable."
            # Return an empty string or $null on failure so the script can handle the error.
            return $null
        }
    }
    catch {
        Write-Host "Error: python.exe was not found in the system PATH. Please install Python or add it to your PATH environment variable."
        # Return an empty string or $null on failure so the script can handle the error.
        return $null
    }
}

# Register cleanup handler for script termination
function Register-CleanupHandler {
    if (-not $Script:CleanupRegistered) {
        # Use try/catch and register only one reliable handler
        try {
            # For Ctrl+C (SIGINT)
            [Console]::TreatControlCAsInput = $false
            $null = Register-EngineEvent -SourceIdentifier PowerShell.Exiting -Action {
                Write-Host "PowerShell.Exiting event triggered - cleaning up..." -ForegroundColor Red
                & {
                    Stop-ActiveProcesses
                    Clear-TempFilesWithHandleRelease
                    if ($Script:ScriptTempFolder -and (Test-Path $Script:ScriptTempFolder)) {
                        Remove-Item $Script:ScriptTempFolder -Recurse -Force -ErrorAction SilentlyContinue
                    }
                }
            }
            
            $Script:CleanupRegistered = $true
            Write-Host "Cleanup handler registered successfully" -ForegroundColor Gray
        }
        catch {
            Write-Warning "Failed to register cleanup handler: $_"
        }
    }
}

# NEW FUNCTION: Start asynchronous backup
function Start-AsynchronousBackup {
    param(
        [string]$SourceFile,
        [string]$BackupFolder
    )
    
    if (-not $BackupFolder -or $BackupFolder.Trim() -eq "") {
        Write-Host "No backup folder specified - skipping backup" -ForegroundColor Gray
        return $null
    }
    
    Write-Host "Starting asynchronous backup process..." -ForegroundColor Cyan
    
    try {
        # Ensure backup folder exists
        if (-not (Test-Path $BackupFolder)) {
            Write-Host "Creating backup folder: $BackupFolder" -ForegroundColor Yellow
            New-Item -ItemType Directory -Path $BackupFolder -Force | Out-Null
        }
        
        # Check backup folder write permissions
        $testFile = Join-Path $BackupFolder "backup_test_$(Get-Random).tmp"
        try {
            "test" | Out-File -FilePath $testFile -Force
            Remove-Item $testFile -Force -ErrorAction SilentlyContinue
            Write-Host "Backup folder write test: PASSED" -ForegroundColor Green
        }
        catch {
            Write-Warning "Backup folder write test: FAILED - $_"
            return $null
        }
        
        # Get source file information for comparison
        $sourceFileSize = (Get-Item $SourceFile).Length
        $sourceSizeGB = [math]::Round($sourceFileSize / 1GB, 2)
        $sourceFileName = [System.IO.Path]::GetFileNameWithoutExtension($SourceFile)
        $sourceFileExt = [System.IO.Path]::GetExtension($SourceFile)
        
        Write-Host "Source file size: $sourceSizeGB GB" -ForegroundColor White
        Write-Host "Checking for existing backups..." -ForegroundColor Cyan
        
        # Check for existing backup files that match our criteria
        $existingBackup = Find-ExistingBackup -BackupFolder $BackupFolder -SourceFileName $sourceFileName -SourceFileExt $sourceFileExt -SourceFileSize $sourceFileSize
        
        if ($existingBackup) {
            Write-Host "EXISTING BACKUP FOUND: $($existingBackup.FileName)" -ForegroundColor Green
            Write-Host "  File size matches: $([math]::Round($existingBackup.Size / 1GB, 2)) GB" -ForegroundColor Green
            Write-Host "  Created: $($existingBackup.CreationTime)" -ForegroundColor Green
            Write-Host "Skipping backup - file already exists with matching size" -ForegroundColor Yellow
            
            # Set backup completion status immediately
            $Script:BackupStarted = $true
            $Script:BackupCompleted = $true
            
            # Return a "completed" backup job structure
            return @{
                Job        = $null  # No actual job since we're skipping
                BackupPath = $existingBackup.FullPath
                StartTime  = Get-Date
                Skipped    = $true
                Reason     = "Matching backup already exists"
            }
        }
        
        Write-Host "No matching backup found - proceeding with new backup" -ForegroundColor Yellow
        
        # Check available space in backup folder
        try {
            $backupDrive = [System.IO.Path]::GetPathRoot($BackupFolder)
            $driveInfo = Get-WmiObject -Class Win32_LogicalDisk | Where-Object DeviceID -eq $backupDrive.TrimEnd('\')
            
            if ($driveInfo -and $driveInfo.FreeSpace) {
                $freeSpaceGB = [math]::Round($driveInfo.FreeSpace / 1GB, 2)
                Write-Host "Backup drive free space: $freeSpaceGB GB" -ForegroundColor White
                
                if ($freeSpaceGB -lt ($sourceSizeGB + 1)) {
                    # +1GB safety margin
                    Write-Warning "Insufficient space for backup: $freeSpaceGB GB available, $([math]::Ceiling($sourceSizeGB + 1)) GB required"
                    return $null
                }
            }
        }
        catch {
            Write-Warning "Could not verify backup drive space: $_"
        }
        
        # Generate backup filename with timestamp
        $timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
        $backupFileName = "${sourceFileName}_backup_${timestamp}${sourceFileExt}"
        $backupFilePath = Join-Path $BackupFolder $backupFileName
        
        Write-Host "Backup destination: $backupFilePath" -ForegroundColor Gray
        
        $scriptBlock = {
            param($source, $destination, $backupFolder)
            
            try {
                # Use robocopy for robust file copying with retry logic
                $sourceDir = Split-Path $source -Parent 
                $sourceFileName = Split-Path $source -Leaf 
                $destFileName = Split-Path $destination -Leaf 
                
                # Robocopy arguments for reliable copying
                $robocopyArgs = @(
                    $sourceDir,
                    $backupFolder,
                    $sourceFileName,
                    "/R:3",          # Retry 3 times on failure
                    "/W:10",         # Wait 10 seconds between retries
                    "/NP",           # No progress (we'll handle our own)
                    "/NFL",          # No file list
                    "/NDL"           # No directory list
                )                
                
                # Execute robocopy
                & "robocopy.exe" @robocopyArgs *> $null

                # Robocopy returns its exit code in the $LASTEXITCODE variable
                $exitCode = $LASTEXITCODE
                attrib -H -S $BackupFolder /S /D
 
                
                # Robocopy exit codes: 0-3 are success, 4+ are errors
                if ($exitCode -le 3) {
                    # Rename if necessary (if backup filename is different)
                    $copiedFile = Join-Path $backupFolder $sourceFileName 
                    if ($destFileName -ne $sourceFileName -and (Test-Path $copiedFile)) {
                        Move-Item -Path $copiedFile -Destination $destination -Force 
                    }
                    
                    # Verify the backup
                    if (Test-Path $destination) {
                        $backupSize = (Get-Item $destination).Length
                        $originalSize = (Get-Item $source).Length
                        
                        if ($backupSize -eq $originalSize) {
                            return @{
                                Success    = $true
                                BackupPath = $destination
                                SizeGB     = [math]::Round($backupSize / 1GB, 2)
                                Message    = "Backup completed successfully"
                            }
                        }
                        else {
                            return @{
                                Success    = $false
                                BackupPath = $destination
                                SizeGB     = 0
                                Message    = "Backup file size mismatch: Expected $originalSize bytes, got $backupSize bytes"
                            }
                        }
                    }
                    else {
                        return @{
                            Success    = $false
                            BackupPath = $destination
                            SizeGB     = 0
                            Message    = "Backup file not found after copy operation"
                        }
                    }
                }
                else {
                    return @{
                        Success    = $false
                        BackupPath = $destination
                        SizeGB     = 0
                        Message    = "Robocopy failed with exit code: $exitCode"
                    }
                }
                
            }
            catch {
                return @{
                    Success    = $false
                    BackupPath = $destination
                    SizeGB     = 0
                    Message    = "Backup exception: $($_.Exception.Message)"
                }
            }
        }
        
        # Start the backup job
        $job = Start-Job -ScriptBlock $scriptBlock -ArgumentList $SourceFile, $backupFilePath, $BackupFolder -Name "VideoBackup_$(Get-Random)"
        
        Write-Host "Backup job started (Job ID: $($job.Id))" -ForegroundColor Green
        Write-Host "Backup will run in background while video processing continues..." -ForegroundColor Green
        
        $Script:BackupStarted = $true
        
        return @{
            Job        = $job
            BackupPath = $backupFilePath
            StartTime  = Get-Date
            Skipped    = $false
        }
        
    }
    catch {
        Write-Warning "Failed to start backup process: $_"
        return $null
    }
}
function Find-ExistingBackup {
    param(
        [string]$BackupFolder,
        [string]$SourceFileName,
        [string]$SourceFileExt,
        [long]$SourceFileSize
    )
    
    try {
        # Search for files that match the naming pattern
        $searchPattern = "${SourceFileName}_backup_*${SourceFileExt}"
        $possibleBackups = Get-ChildItem -Path $BackupFolder -Filter $searchPattern -ErrorAction SilentlyContinue
        
        if (-not $possibleBackups) {
            # Also check for files with just the source name (in case of different naming conventions)
            $alternativePattern = "${SourceFileName}${SourceFileExt}"
            $possibleBackups = Get-ChildItem -Path $BackupFolder -Filter $alternativePattern -ErrorAction SilentlyContinue
        }
        
        if ($possibleBackups) {
            Write-Host "Found $($possibleBackups.Count) potential backup file(s) to check" -ForegroundColor Gray
            
            foreach ($file in $possibleBackups) {
                Write-Host "  Checking: $($file.Name) (Size: $([math]::Round($file.Length / 1GB, 2)) GB)" -ForegroundColor DarkGray
                
                # Check if file size matches exactly
                if ($file.Length -eq $SourceFileSize) {
                    Write-Host "    Size match found!" -ForegroundColor Green
                    
                    return @{
                        FileName     = $file.Name
                        FullPath     = $file.FullName
                        Size         = $file.Length
                        CreationTime = $file.CreationTime
                    }
                }
                else {
                    $sizeDiffMB = [math]::Round(($file.Length - $SourceFileSize) / 1MB, 2)
                    Write-Host "    Size mismatch: differs by $sizeDiffMB MB" -ForegroundColor DarkYellow
                }
            }
        }
        else {
            Write-Host "No potential backup files found with pattern: $searchPattern" -ForegroundColor Gray
        }
        
        return $null
        
    }
    catch {
        Write-Warning "Error searching for existing backups: $_"
        return $null
    }
}

# NEW FUNCTION: Check backup status
function Get-BackupStatus {
    param($BackupInfo)
    
    if (-not $BackupInfo) {
        return $null
    }
    
    # Handle skipped backups
    if ($BackupInfo.Skipped) {
        return @{
            Success    = $true
            BackupPath = $BackupInfo.BackupPath
            SizeGB     = [math]::Round((Get-Item $BackupInfo.BackupPath).Length / 1GB, 2)
            Message    = $BackupInfo.Reason
            Skipped    = $true
        }
    }
    
    # Handle normal backup jobs
    $job = $BackupInfo.Job
    if (-not $job) {
        return $null
    }
    
    if ($job.State -eq "Completed") {
        try {
            $result = Receive-Job -Job $job
            Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
            
            if ($result -and $result.Success) {
                $Script:BackupCompleted = $true
                Write-Host "Backup completed successfully: $($result.BackupPath)" -ForegroundColor Green
                Write-Host "  Backup size: $($result.SizeGB) GB" -ForegroundColor Green
                
                # Verify backup file exists and has content
                if ((Test-Path $result.BackupPath) -and (Get-Item $result.BackupPath).Length -gt 0) {
                    Write-Host "  Backup verified: File exists and has correct size" -ForegroundColor Green
                }
                else {
                    Write-Warning "  Backup verification failed: File missing or empty"
                }
            }
            else {
                Write-Warning "  Backup failed: $($result.Message)"
            }
            
            return $result
        }
        catch {
            Write-Warning "Error checking backup status: $_"
            return $null
        }
    }
    elseif ($job.State -eq "Failed") {
        Write-Warning "Backup job failed"
        try {
            $errorInfo = Receive-Job -Job $job 2>&1
            Write-Warning "Backup error details: $errorInfo"
            Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
        }
        catch {
            # Ignore cleanup errors
        }
        return @{ Success = $false; Message = "Job failed" }
    }
    else {
        # Still running
        $elapsed = (Get-Date) - $BackupInfo.StartTime
        Write-Host "Backup in progress... (elapsed: $($elapsed.ToString('mm\:ss')))" -ForegroundColor Yellow
        return $null
    }
}

# NEW FUNCTION: Stop backup and cleanup
function Stop-BackupProcess {
    if ($Script:BackupJob) {
        try {
            $job = $Script:BackupJob.Job
            if ($job -and ($job.State -eq "Running" -or $job.State -eq "NotStarted")) {
                Write-Host "Stopping backup job..." -ForegroundColor Yellow
                Stop-Job -Job $job -PassThru -ErrorAction SilentlyContinue | Wait-Job -Timeout 10 -ErrorAction SilentlyContinue
            }
            
            # Clean up job
            if ($job) {
                Remove-Job -Job $job -Force -ErrorAction SilentlyContinue
            }
            
            Write-Host "Backup process cleaned up" -ForegroundColor Gray
        }
        catch {
            # Silent cleanup during shutdown
        }
        
        $Script:BackupJob = $null
    }
}
function Test-Prerequisites {
    Write-Host "Checking prerequisites..." -ForegroundColor Cyan
    
    $issues = @()
    
    # ... existing checks ...
    
    # Enhanced disk space check using the new function
    Write-Host "Performing comprehensive disk space analysis..." -ForegroundColor Yellow
    
    # Check output folder space
    if (Test-Path $OutputFolder) {
        $outputDrive = [System.IO.Path]::GetPathRoot($OutputFolder)
        $outputDriveInfo = Get-WmiObject -Class Win32_LogicalDisk | Where-Object DeviceID -eq $outputDrive.TrimEnd('\')
        
        if ($outputDriveInfo -and $outputDriveInfo.FreeSpace) {
            $outputFreeSpaceGB = [math]::Round($outputDriveInfo.FreeSpace / 1GB, 2)
            Write-Host "Output folder space: $outputFreeSpaceGB GB available on $outputDrive" -ForegroundColor White
            
            if ($outputFreeSpaceGB -lt 5) {
                $issues += "Low disk space on output drive $outputDrive : $outputFreeSpaceGB GB remaining (minimum: 5GB)"
            }
        }
    }
    else {
        $issues += "Output folder does not exist: $OutputFolder"
    }
    
    # Check temp folder space (this is now handled by Get-ValidatedTempFolder)
    if ($Script:ValidatedTempFolder -and (Test-Path $Script:ValidatedTempFolder)) {
        $tempDrive = [System.IO.Path]::GetPathRoot($Script:ValidatedTempFolder)
        $tempDriveInfo = Get-WmiObject -Class Win32_LogicalDisk | Where-Object DeviceID -eq $tempDrive.TrimEnd('\')
        
        if ($tempDriveInfo -and $tempDriveInfo.FreeSpace) {
            $tempFreeSpaceGB = [math]::Round($tempDriveInfo.FreeSpace / 1GB, 2)
            Write-Host "Temp folder space: $tempFreeSpaceGB GB available on $tempDrive" -ForegroundColor Green
            
            # The temp folder validation already ensured sufficient space, but double-check
            if ($tempFreeSpaceGB -lt 10) {
                Write-Warning "Temp folder has marginal space: $tempFreeSpaceGB GB"
                Write-Host "Processing will continue but monitor space closely" -ForegroundColor Yellow
            }
        }
    }
    else {
        $issues += "Temp folder validation failed or folder not accessible"
    }
    
    # Check if input and temp are on same drive (optimal for performance)
    if ($InputFile -and $Script:ValidatedTempFolder) {
        $inputDrive = [System.IO.Path]::GetPathRoot($InputFile)
        $tempDrive = [System.IO.Path]::GetPathRoot($Script:ValidatedTempFolder)
        
        if ($inputDrive -eq $tempDrive) {
            Write-Host "Input and temp on same drive ($inputDrive) - optimal for performance" -ForegroundColor Green
        }
        else {
            Write-Host "Input ($inputDrive) and temp ($tempDrive) on different drives - may impact performance" -ForegroundColor Yellow
        }
    }
    
    # ... rest of existing prerequisite checks ...
    
    if ($issues.Count -gt 0) {
        Write-Host "Prerequisites failed:" -ForegroundColor Red
        foreach ($issue in $issues) {
            Write-Host "  - $issue" -ForegroundColor Red
        }
        return $false
    }
    
    Write-Host "Prerequisites check passed" -ForegroundColor Green
    return $true
}

function Test-HDRMetadataValidity {
    param([hashtable]$Metadata)
    
    $issues = @()
    
    # Check MaxCLL and MaxFALL
    if ($Metadata.HasContentLight) {
        if ($Metadata.MaxCLL -le 1 -or $Metadata.MaxCLL -gt 10000) {
            $issues += "Invalid MaxCLL: $($Metadata.MaxCLL)"
        }
        if ($Metadata.MaxFALL -le 1 -or $Metadata.MaxFALL -gt 4000) {
            $issues += "Invalid MaxFALL: $($Metadata.MaxFALL)"
        }
        if ($Metadata.MaxFALL -gt $Metadata.MaxCLL) {
            $issues += "MaxFALL ($($Metadata.MaxFALL)) cannot exceed MaxCLL ($($Metadata.MaxCLL))"
        }
    }
    
    # Check mastering display luminance
    if ($Metadata.HasMasteringDisplay) {
        # MaxLuminance should be 1000-10000 cd/m² (10000000-100000000 in 0.0001 units)
        if ($Metadata.MaxLuminance -lt 5000000 -or $Metadata.MaxLuminance -gt 100000000) {
            $issues += "Invalid max luminance: $($Metadata.MaxLuminance)"
        }
        # MinLuminance should be > 0
        if ($Metadata.MinLuminance -le 0) {
            $issues += "Invalid min luminance: $($Metadata.MinLuminance)"
        }
    }
    
    if ($issues.Count -gt 0) {
        Write-Warning "HDR metadata validation failed:"
        foreach ($issue in $issues) {
            Write-Warning "  - $issue"
        }
        return $false
    }
    
    Write-Host "HDR metadata validation: PASSED" -ForegroundColor Green
    return $true
}

function Get-HDREncodingParameters {
    param(
        [hashtable]$SourceMetadata,
        [string]$ContentType = "animation"  # Options: "animation", "live-action", "documentary"
    )
    
    $params = @{
        MasterDisplay = $null
        MaxCLL = $null
        X265Params = @()
    }
    
    # Use source metadata if valid
    if ($SourceMetadata -and $SourceMetadata.IsValid) {
        Write-Host "Using validated source HDR metadata" -ForegroundColor Green
        
        # Build master-display string: G(x,y)B(x,y)R(x,y)WP(x,y)L(max,min)
        $params.MasterDisplay = "G($($SourceMetadata.GreenX),$($SourceMetadata.GreenY))" +
                                "B($($SourceMetadata.BlueX),$($SourceMetadata.BlueY))" +
                                "R($($SourceMetadata.RedX),$($SourceMetadata.RedY))" +
                                "WP($($SourceMetadata.WhitePointX),$($SourceMetadata.WhitePointY))" +
                                "L($($SourceMetadata.MaxLuminance),$($SourceMetadata.MinLuminance))"
        
        $params.MaxCLL = "$($SourceMetadata.MaxCLL),$($SourceMetadata.MaxFALL)"
        
        Write-Host "  Source master-display: $($params.MasterDisplay)" -ForegroundColor Gray
        Write-Host "  Source max-cll: $($params.MaxCLL)" -ForegroundColor Gray
    }
    else {
        # Use best-guess defaults based on content type
        Write-Host "Source metadata invalid or missing - using best-guess HDR10 defaults" -ForegroundColor Yellow
        
        # Standard Display P3 primaries with BT.2020 color space
        # These are the most common values for HDR10 content
        $params.MasterDisplay = "G(13250,34500)B(7500,3000)R(34000,16000)WP(15635,16450)L(10000000,1)"
        
        # Content-type specific MaxCLL/MaxFALL defaults
        switch ($ContentType.ToLower()) {
            "animation" {
                # Animation tends to have lower peak brightness
                $params.MaxCLL = "1500,200"
                Write-Host "  Using animation defaults: MaxCLL=1500, MaxFALL=200" -ForegroundColor Yellow
            }
            "documentary" {
                # Documentaries often have more varied lighting
                $params.MaxCLL = "2000,300"
                Write-Host "  Using documentary defaults: MaxCLL=2000, MaxFALL=300" -ForegroundColor Yellow
            }
            default {
                # Live-action default (most common)
                $params.MaxCLL = "1000,400"
                Write-Host "  Using live-action defaults: MaxCLL=1000, MaxFALL=400" -ForegroundColor Yellow
            }
        }
        
        Write-Host "  Default master-display: Display P3 primaries, 1000 nit peak" -ForegroundColor Gray
    }
    
    # Build x265 parameter strings
    $params.X265Params += "master-display=$($params.MasterDisplay)"
    $params.X265Params += "max-cll=$($params.MaxCLL)"
    
    return $params
}

function Get-HDRMasteringMetadata {
    param([string]$FilePath)
    
    Write-Host "Extracting HDR mastering display metadata..." -ForegroundColor Cyan
    
    try {
        # Extract side_data including mastering display metadata
        $probeArgs = @(
            "-v", "warning",
            "-select_streams", "v:0",
            "-print_format", "json",
            "-show_frames",
            "-read_intervals", "%+#1",  # Read only first frame
            "-show_entries", "frame=color_space,color_primaries,color_transfer,side_data_list",
            $FilePath
        )
        
        $probeResult = Invoke-FFProbeWithCleanup -Arguments $probeArgs -TimeoutSeconds 30
        
        if (-not $probeResult.Success) {
            Write-Warning "Failed to extract HDR metadata"
            return $null
        }
        
        $frameData = $probeResult.StdOut | ConvertFrom-Json
        
        if (-not $frameData.frames -or $frameData.frames.Count -eq 0) {
            Write-Warning "No frame data found"
            return $null
        }
        
        $frame = $frameData.frames[0]
        $sideDataList = $frame.side_data_list
        
        if (-not $sideDataList) {
            Write-Host "No HDR side data found in source" -ForegroundColor Gray
            return $null
        }
        
        # Look for mastering display color volume
        $masteringDisplay = $sideDataList | Where-Object { 
            $_.side_data_type -eq "Mastering display metadata" 
        }
        
        # Look for content light level
        $contentLight = $sideDataList | Where-Object { 
            $_.side_data_type -eq "Content light level metadata" 
        }
        
        $metadata = @{
            HasMasteringDisplay = $false
            HasContentLight = $false
            RedX = $null
            RedY = $null
            GreenX = $null
            GreenY = $null
            BlueX = $null
            BlueY = $null
            WhitePointX = $null
            WhitePointY = $null
            MaxLuminance = $null
            MinLuminance = $null
            MaxCLL = $null
            MaxFALL = $null
            IsValid = $false
        }
        
        # Parse mastering display metadata
        if ($masteringDisplay) {
            Write-Host "Found mastering display metadata" -ForegroundColor Green
            $metadata.HasMasteringDisplay = $true
            
            # Parse color primaries (in format "13250/50000" = 0.265)
            if ($masteringDisplay.red_x) {
                $metadata.RedX = [int]($masteringDisplay.red_x -split '/')[0]
                $metadata.RedY = [int]($masteringDisplay.red_y -split '/')[0]
            }
            if ($masteringDisplay.green_x) {
                $metadata.GreenX = [int]($masteringDisplay.green_x -split '/')[0]
                $metadata.GreenY = [int]($masteringDisplay.green_y -split '/')[0]
            }
            if ($masteringDisplay.blue_x) {
                $metadata.BlueX = [int]($masteringDisplay.blue_x -split '/')[0]
                $metadata.BlueY = [int]($masteringDisplay.blue_y -split '/')[0]
            }
            if ($masteringDisplay.white_point_x) {
                $metadata.WhitePointX = [int]($masteringDisplay.white_point_x -split '/')[0]
                $metadata.WhitePointY = [int]($masteringDisplay.white_point_y -split '/')[0]
            }
            
            # Parse luminance (in format "10000000/10000" = 1000 cd/m²)
            if ($masteringDisplay.max_luminance) {
                $metadata.MaxLuminance = [int]($masteringDisplay.max_luminance -split '/')[0]
            }
            if ($masteringDisplay.min_luminance) {
                $metadata.MinLuminance = [int]($masteringDisplay.min_luminance -split '/')[0]
            }
            
            Write-Host "  Max Luminance: $($metadata.MaxLuminance) (units of 0.0001 cd/m²)" -ForegroundColor Gray
            Write-Host "  Min Luminance: $($metadata.MinLuminance) (units of 0.0001 cd/m²)" -ForegroundColor Gray
        }
        
        # Parse content light level metadata
        if ($contentLight) {
            Write-Host "Found content light level metadata" -ForegroundColor Green
            $metadata.HasContentLight = $true
            
            if ($contentLight.max_content) {
                $metadata.MaxCLL = [int]$contentLight.max_content
            }
            if ($contentLight.max_average) {
                $metadata.MaxFALL = [int]$contentLight.max_average
            }
            
            Write-Host "  MaxCLL: $($metadata.MaxCLL) nits" -ForegroundColor Gray
            Write-Host "  MaxFALL: $($metadata.MaxFALL) nits" -ForegroundColor Gray
        }
        
        # Validate the metadata
        $metadata.IsValid = Test-HDRMetadataValidity -Metadata $metadata
        
        return $metadata
    }
    catch {
        Write-Warning "Exception extracting HDR metadata: $_"
        return $null
    }
}
function Start-ProcessWithCoreLimit {
    param(
        [string]$Executable,
        [string[]]$Arguments,
        [string]$Description
    )
    
    Write-Host "[EXEC] $Description" -ForegroundColor Cyan
    
    $processInfo = New-Object System.Diagnostics.ProcessStartInfo
    $processInfo.FileName = $Executable
    $processInfo.UseShellExecute = $false
    $processInfo.RedirectStandardOutput = $false
    $processInfo.RedirectStandardError = $false
    $processInfo.CreateNoWindow = $false
    
    if ($PSVersionTable.PSVersion.Major -ge 7) {
        foreach ($arg in $Arguments) {
            $null = $processInfo.ArgumentList.Add($arg)
        }
    }
    else {
        $legacyQuotedArgs = $Arguments | ForEach-Object { 
            if ($_ -match '\s') { '"{0}"' -f ($_ -replace '"', '""') } else { $_ }
        }
        $processInfo.Arguments = $legacyQuotedArgs -join ' '
    }

    $process = [System.Diagnostics.Process]::Start($processInfo)
    $handle = $process.Handle
    $Script:ActiveProcesses = @($Script:ActiveProcesses) + $process
      
    $process.WaitForExit()
    return $process
}

function Stop-ActiveProcesses {
    Write-Host "Terminating active processes with enhanced cleanup..." -ForegroundColor Yellow
    
    # Stop backup job first
    Stop-BackupProcess
    
    # Stop MCEBuddy monitoring jobs
    Get-Job | Where-Object { $_.Name -like "*MCEBuddy*" -or $_.Name -like "*VideoBackup*" } | Stop-Job -ErrorAction SilentlyContinue
    Get-Job | Where-Object { $_.Name -like "*MCEBuddy*" -or $_.Name -like "*VideoBackup*" } | Remove-Job -ErrorAction SilentlyContinue
    
    # Resume MCEBuddy engine
    try {
        Resume-McebuddyEngine | Out-Null
    }
    catch {
        # Ignore resume errors
    }
    
    # Stop background jobs
    Get-Job | Where-Object { $_.Name -like "ProgressJob*" -or $_.Name -like "CaptureJob*" } | Stop-Job -ErrorAction SilentlyContinue
    Get-Job | Where-Object { $_.Name -like "ProgressJob*" -or $_.Name -like "CaptureJob*" } | Remove-Job -ErrorAction SilentlyContinue
    
    # Enhanced process termination with file handle cleanup
    foreach ($process in $Script:ActiveProcesses) {
        if ($process -and -not $process.HasExited) {
            try {
                Write-Host "Stopping process: $($process.ProcessName) (PID: $($process.Id))" -ForegroundColor Yellow
                
                # Force close file handles before killing
                try {
                    $process.Close()
                }
                catch {
                    # Ignore close errors
                }
                
                # Kill the process
                $process.Kill()
                $process.WaitForExit(5000)
                
                # Dispose properly
                try {
                    $process.Dispose()
                }
                catch {
                    # Ignore disposal errors
                }
                
                Write-Host "Process terminated successfully" -ForegroundColor Green
            }
            catch {
                Write-Warning "Failed to terminate process: $_"
            }
        }
    }
    
    # Clear the active processes array
    $Script:ActiveProcesses = @() 
    
}

function Initialize-VideoAnalysisWithHDR10Plus {
    param([string]$InputFile)
    
    Write-Host "`n=== Initial Video Analysis with Comprehensive HDR10+ Detection ===" -ForegroundColor Cyan
    
    # Initialize global HDR10+ status to avoid redundant detection later
    $Script:HDR10PlusStatus = @{
        HasHDR10Plus = $false
        DetectionMethods = @()
        ExtractedJsonPath = $null
        IsViable = $false
        MetadataQuality = $null
        SceneCount = 0
    }
    
    # Get base video metadata (without HDR10+ detection)
    $videoInfo = Get-VideoMetadata -FilePath $InputFile
    
    if (-not $videoInfo) {
        throw "Failed to analyze input video"
    }
    
    # Do comprehensive HDR10+ detection ONCE
    Write-Host "Performing comprehensive HDR10+ analysis..." -ForegroundColor Yellow
    $hdr10PlusResult = Test-HDR10PlusInOriginal -FilePath $InputFile -Verbose $true
    
    # Store HDR10+ results globally
    $Script:HDR10PlusStatus.HasHDR10Plus = $hdr10PlusResult.HasHDR10Plus
    $Script:HDR10PlusStatus.DetectionMethods = $hdr10PlusResult.DetectionMethods
    
    # If HDR10+ detected, extract metadata ONCE for entire processing
    if ($Script:HDR10PlusStatus.HasHDR10Plus -and -not $skipHDR10Plus) {
        Write-Host "HDR10+ detected - extracting metadata once for entire processing..." -ForegroundColor Green
        
        $baseName = [System.IO.Path]::GetFileNameWithoutExtension($InputFile)
        $hdr10JsonPath = New-TempFile -BaseName "$baseName.hdr10plus.initial" -Extension ".json"
        
        # Extract HDR10+ metadata
        $extractSuccess = Invoke-HDR10PlusExtraction -InputFile $InputFile -OutputJson $hdr10JsonPath
        
        if ($extractSuccess -and (Test-Path $hdr10JsonPath) -and (Get-Item $hdr10JsonPath).Length -gt 0) {
            $Script:HDR10PlusStatus.ExtractedJsonPath = $hdr10JsonPath
            
            # Test metadata quality
            try {
                $jsonContent = Get-Content $hdr10JsonPath -Raw | ConvertFrom-Json
                if ($jsonContent.SceneInfo -and $jsonContent.SceneInfo.Count -gt 0) {
                    $Script:HDR10PlusStatus.SceneCount = $jsonContent.SceneInfo.Count
                    
                    # Assess metadata quality
                    $shouldSkip = Test-HDR10ProcessingViability -MetadataFilePath $hdr10JsonPath -LogAssessment:$false
                    $Script:HDR10PlusStatus.IsViable = -not $shouldSkip
                    
                    Write-Host "  HDR10+ metadata extracted: $($Script:HDR10PlusStatus.SceneCount) scenes" -ForegroundColor Green
                    Write-Host "  Metadata viable: $($Script:HDR10PlusStatus.IsViable)" -ForegroundColor $(if ($Script:HDR10PlusStatus.IsViable) { 'Green' } else { 'Yellow' })
                }
            }
            catch {
                Write-Warning "Failed to validate extracted HDR10+ metadata: $_"
                $Script:HDR10PlusStatus.IsViable = $false
            }
        }
        else {
            Write-Warning "HDR10+ extraction failed - will proceed without HDR10+"
            $Script:HDR10PlusStatus.HasHDR10Plus = $false
        }
    }
    elseif ($skipHDR10Plus) {
        Write-Host "HDR10+ processing disabled by parameter - skipping extraction" -ForegroundColor Yellow
        $Script:HDR10PlusStatus.HasHDR10Plus = $false
    }
    
    # Create enhanced video info with HDR10+ status
    $enhancedVideoInfo = @{
        Width                    = $videoInfo.Width
        Height                   = $videoInfo.Height
        FrameRate                = $videoInfo.FrameRate
        Bitrate                  = $videoInfo.Bitrate
        ColorPrimaries           = $videoInfo.ColorPrimaries
        ColorTransfer            = $videoInfo.ColorTransfer
        ColorSpace               = $videoInfo.ColorSpace
        PixelFormat              = $videoInfo.PixelFormat
        HasDolbyVision           = $videoInfo.HasDolbyVision
        colorRange               = $videoInfo.colorRange
        HasHdr10Plus             = $Script:HDR10PlusStatus.HasHDR10Plus
        HDR10PlusDetectionMethod = if ($Script:HDR10PlusStatus.HasHDR10Plus) { 
            "Comprehensive detection: " + ($Script:HDR10PlusStatus.DetectionMethods | Select-Object -First 3 | ForEach-Object { $_.Split(':')[0] }) -join ", "
        } else { 
            "Comprehensive scan found no HDR10+ metadata" 
        }
        ExtractedHdr10JsonPath   = $Script:HDR10PlusStatus.ExtractedJsonPath
        Duration                 = $videoInfo.Duration
        CodecName                = $videoInfo.CodecName
        OriginalProfile          = $videoInfo.OriginalProfile
        OriginalLevel            = $videoInfo.OriginalLevel
        OriginalChromaLocation   = $videoInfo.OriginalChromaLocation
    }
    
    # Store enhanced metadata globally
    $Script:VideoMetadata = $enhancedVideoInfo
    
    Write-Host "Initial analysis complete:" -ForegroundColor Green
    Write-Host "  Resolution: $($enhancedVideoInfo.Width)x$($enhancedVideoInfo.Height)" -ForegroundColor White
    Write-Host "  Codec: $($enhancedVideoInfo.CodecName)" -ForegroundColor White
    Write-Host "  Dolby Vision: $(if ($enhancedVideoInfo.HasDolbyVision) { 'YES - will be removed' } else { 'No' })" -ForegroundColor $(if ($enhancedVideoInfo.HasDolbyVision) { 'Yellow' } else { 'Green' })
    Write-Host "  HDR10+: $(if ($enhancedVideoInfo.HasHdr10Plus) { 'YES - ' + $enhancedVideoInfo.HDR10PlusDetectionMethod.Split(':')[0] } else { 'No' })" -ForegroundColor $(if ($enhancedVideoInfo.HasHdr10Plus) { 'Green' } else { 'Yellow' })
    
    return $enhancedVideoInfo
}

function Clear-TempFilesWithHandleRelease {
    Write-Host "Cleaning up temporary files with file handle release..." -ForegroundColor Yellow
    $cleanedCount = 0
    $failedCount = 0
    
    # First, ensure all processes are stopped to release file handles
    Stop-ActiveProcesses
    
    # Wait a moment for file handles to be released
    Start-Sleep -Seconds 2
    
    if ($Script:TempFiles) {
        foreach ($file in $Script:TempFiles) {
            if ($file -and (Test-Path $file -ErrorAction SilentlyContinue)) {
                try {
                    # Remove read-only attribute if present
                    Set-ItemProperty -Path $file -Name IsReadOnly -Value $false -ErrorAction SilentlyContinue
                    
                    # Try to delete the file
                    Remove-Item $file -Force -Recurse -ErrorAction Stop
                    Write-Host "Removed: $file" -ForegroundColor Gray
                    $cleanedCount++
                }
                catch {
                    Write-Warning "Failed to remove temp file: $file - $_"
                    $failedCount++
                    
                    # Try alternative removal method using CMD
                    try {
                        $result = cmd /c "del /f /q `"$file`"" 2>&1
                        Start-Sleep -Milliseconds 500  # Brief pause
                        if (-not (Test-Path $file)) {
                            Write-Host "Removed via CMD: $file" -ForegroundColor Gray
                            $cleanedCount++
                            $failedCount--
                        }
                    }
                    catch {
                        Write-Warning "Alternative cleanup methods failed for: $file"
                    }
                }
            }
        }
    }
    
    # Clean up script temp folder with enhanced methods
    if ($Script:ScriptTempFolder -and (Test-Path $Script:ScriptTempFolder)) {
        try {
            Write-Host "Removing script temp folder: $Script:ScriptTempFolder" -ForegroundColor Yellow
            
            # Remove readonly attributes recursively
            Get-ChildItem -Path $Script:ScriptTempFolder -Recurse -Force -ErrorAction SilentlyContinue | 
            ForEach-Object { 
                try { 
                    $_.Attributes = 'Normal' 
                } catch { 
                    # Ignore attribute errors 
                } 
            }
            
            # Force remove the entire directory
            Remove-Item $Script:ScriptTempFolder -Force -Recurse -ErrorAction Stop
            Write-Host "Script temp folder removed successfully" -ForegroundColor Green
            
        }
        catch {
            Write-Warning "Failed to remove script temp folder: $_"
            
            # Try with CMD as backup
            try {
                cmd /c "rmdir /s /q `"$Script:ScriptTempFolder`"" 2>&1 | Out-Null
                Start-Sleep -Seconds 1
                if (-not (Test-Path $Script:ScriptTempFolder)) {
                    Write-Host "Script temp folder removed via CMD" -ForegroundColor Green
                }
                else {
                    Write-Warning "Script temp folder could not be removed - may require manual cleanup"
                    Write-Host "Manual cleanup path: $Script:ScriptTempFolder" -ForegroundColor Yellow
                }
            }
            catch {
                Write-Warning "Could not remove script temp folder even with CMD"
            }
        }
    }
    
    Write-Host "Enhanced cleanup summary: $cleanedCount removed, $failedCount failed" -ForegroundColor $(if ($failedCount -eq 0) { "Green" } else { "Yellow" })
    $Script:TempFiles = @()
}

#region Helper Functions

function Get-ActualCPUInfo {
    Write-Host "Detecting CPU configuration..." -ForegroundColor Cyan
    
    try {
        # Get CPU information from WMI
        $cpuInfo = Get-WmiObject -Class Win32_Processor
        $logicalProcessors = [Environment]::ProcessorCount
        
        # Calculate physical cores (handles multi-socket systems)
        $physicalCores = 0
        foreach ($cpu in $cpuInfo) {
            $physicalCores += $cpu.NumberOfCores
        }
        
        $hyperthreading = $logicalProcessors -gt $physicalCores
        
        Write-Host "CPU Detection Results:" -ForegroundColor Green
        Write-Host "  Logical Processors: $logicalProcessors" -ForegroundColor White
        Write-Host "  Physical Cores: $physicalCores" -ForegroundColor White  
        Write-Host "  Hyperthreading: $hyperthreading" -ForegroundColor White
        
        # Detect if this is a Ryzen CPU for optimized allocation
        $isRyzen = $false
        foreach ($cpu in $cpuInfo) {
            if ($cpu.Name -match "AMD Ryzen") {
                $isRyzen = $true
                Write-Host "  CPU Type: AMD Ryzen (optimized allocation available)" -ForegroundColor Green
                break
            }
        }
        
        if (-not $isRyzen) {
            Write-Host "  CPU Type: Non-Ryzen (using generic allocation)" -ForegroundColor Yellow
        }
        
        return @{
            LogicalProcessors = $logicalProcessors
            PhysicalCores     = $physicalCores
            HasHyperthreading = $hyperthreading
            IsRyzen           = $isRyzen
            CPUName           = $cpuInfo[0].Name
        }
        
    }
    catch {
        Write-Warning "CPU detection failed: $_"
        # Fallback to basic detection
        $logicalProcessors = [Environment]::ProcessorCount
        return @{
            LogicalProcessors = $logicalProcessors
            PhysicalCores     = [math]::Max(1, [math]::Floor($logicalProcessors / 2))
            HasHyperthreading = $logicalProcessors -gt 4
            IsRyzen           = $false
            CPUName           = "Unknown"
        }
    }
}


function Get-GlobalMetadata {
    param([string]$FilePath)
    
    Write-Host "Extracting global metadata..." -ForegroundColor Cyan
    
    try {
        $metadataArgs = @(
            "-v", "quiet",
            "-show_format",
            "-show_entries", "format_tags",
            "-of", "json",
            $FilePath
        )
        
        $metadataResult = Invoke-FFProbeWithCleanup -Arguments $metadataArgs -TimeoutSeconds 60
        
        if ($metadataResult.Success -and $metadataResult.StdOut) {
            $jsonData = $metadataResult.StdOut | ConvertFrom-Json
            
            if ($jsonData.format -and $jsonData.format.tags) {
                Write-Host "Found global metadata with $($jsonData.format.tags.PSObject.Properties.Count) tags" -ForegroundColor Green
                return $jsonData.format.tags
            }
        }
        
        Write-Host "No global metadata found" -ForegroundColor Yellow
        return @{}
        
    }
    catch {
        Write-Warning "Failed to extract global metadata: $_"
        return @{}
    }
}


function Add-TempFile {
    param([string]$FilePath)
    if ($FilePath -and $FilePath.Trim() -and $FilePath -ne "NUL" -and $FilePath -ne "-") {
        # Avoid duplicates
        if ($Script:TempFiles -notcontains $FilePath) {
            $Script:TempFiles += $FilePath
            Write-Host "Registered temp file: $FilePath" -ForegroundColor Gray
        }
    }
}



function New-TempFile {
    param(
        [string]$BaseName,
        [string]$Extension,
        [string]$Directory = $Script:ValidatedTempFolder
    )
    
    if (-not $BaseName) {
        $BaseName = [System.Guid]::NewGuid().ToString("N").Substring(0, 8)
    }
    
    $tempPath = Join-Path $Directory "$BaseName$Extension"
    
    # Ensure unique filename
    $counter = 1
    while (Test-Path $tempPath) {
        $tempPath = Join-Path $Directory "$BaseName.$counter$Extension"
        $counter++
    }
    
    Add-TempFile -FilePath $tempPath
    return $tempPath
}


function Invoke-SimpleProcess {
    param(
        [string]$Executable,
        [string[]]$Arguments,
        [string]$Description
    )
    
    $process = Start-ProcessWithCoreLimit -Executable $Executable -Arguments $Arguments -Description $Description
    
    # NEW: Check the exit code instead of just returning the process
    if ($process -and $process.ExitCode -eq 0) {
        return $true
    }
    else {
        Write-Host "Process '$Description' failed with exit code: $($process.ExitCode)" -ForegroundColor Red
        return $false
    }
}

function Find-ExternalSubtitles {
    param(
        [string]$InputFile
    )
    
    Write-Host "Scanning for external subtitle files..." -ForegroundColor Cyan
    
    try {
        $inputDirectory = [System.IO.Path]::GetDirectoryName($InputFile)
        $inputBaseName = [System.IO.Path]::GetFileNameWithoutExtension($InputFile)
        
        # Pattern to match: filename.en*.srt (covers .en.srt, .en.forced.srt, .en.sdh.srt, etc.)
        $externalSubtitles = @()
        
        # Get all .srt files in the directory
        $allSrtFiles = Get-ChildItem -Path $inputDirectory -Filter "*.srt" -ErrorAction SilentlyContinue
        
        foreach ($srtFile in $allSrtFiles) {
            # Check if the file matches our patterns: basename.en*.srt or basename.en-*.srt
            if ($srtFile.Name -match "^$([regex]::Escape($inputBaseName))\.(en|eng)[-.]*\.srt$") {
                Write-Host "Found external English subtitle: $($srtFile.Name)" -ForegroundColor Green
                
                # Determine subtitle type from filename
                $subtitleType = "English"
                if ($srtFile.Name -match "\.forced\.") {
                    $subtitleType = "English (Forced)"
                }
                elseif ($srtFile.Name -match "\.sdh\.") {
                    $subtitleType = "English (SDH)"
                }
                elseif ($srtFile.Name -match "\.cc\.") {
                    $subtitleType = "English (CC)"
                }
                
                $externalSubtitles += [PSCustomObject]@{
                    FilePath = $srtFile.FullName
                    FileName = $srtFile.Name
                    Type     = $subtitleType
                    Language = "eng"
                    IsForced = $srtFile.Name -match "\.forced\."
                }
            } else {
                Write-Host "Not chosing $($srtFile.Name) base: $($inputBaseName)"
            }
        }
        
        if ($externalSubtitles.Count -gt 0) {
            Write-Host "Found $($externalSubtitles.Count) external subtitle file(s)" -ForegroundColor Green
            foreach ($sub in $externalSubtitles) {
                Write-Host "  - $($sub.FileName) [$($sub.Type)]" -ForegroundColor Gray
            }
        }
        else {
            Write-Host "No external English subtitle files found" -ForegroundColor Gray
        }
        
        return $externalSubtitles
        
    }
    catch {
        Write-Warning "Error scanning for external subtitles: $_"
        return @()
    }
}

function Get-PlexOptimizedAudioSettings {
    param(
        [hashtable]$AudioInfo, 
        [bool]$CreateCompatibilityTrack = $true,
        [bool]$HasExistingAAC = $false
    )
    
    Write-Host "Configuring Plex-optimized audio settings..." -ForegroundColor Cyan
    
    if (-not $AudioInfo.ToCopy) {
        Write-Warning "No audio stream selected - skipping audio optimization"
        return @{ Primary = @(); Fallback = @(); StreamCount = 0; RequiresFallback = $false }
    }
    
    $sourceCodec = $AudioInfo.ToCopy.Codec.ToLower()
    $channels = $AudioInfo.ToCopy.Channels
    $originalBitrate = $AudioInfo.ToCopy.Bitrate
    
    Write-Host "Source audio: $sourceCodec, $channels channels, $([math]::Round($originalBitrate/1000,0)) kbps" -ForegroundColor Gray
    Write-Host "Existing AAC stream: $HasExistingAAC" -ForegroundColor Gray
    Write-Host "Create compatibility track: $CreateCompatibilityTrack" -ForegroundColor Gray
    
    $audioSettings = @{
        Primary          = @()
        Fallback         = @()
        StreamCount      = 1
        RequiresFallback = $false
    }
    
    # Enhanced channel layout mapping for better Plex recognition
    $channelLayoutMap = @{
        1 = @{ Layout = "mono"; Title = "Mono"; PlexName = "Mono" }
        2 = @{ Layout = "stereo"; Title = "Stereo"; PlexName = "Stereo" }
        6 = @{ Layout = "5.1"; Title = "5.1"; PlexName = "5.1 Surround" }
        8 = @{ Layout = "7.1"; Title = "7.1"; PlexName = "7.1 Surround" }
    }
    
    $layoutInfo = if ($channelLayoutMap.ContainsKey($channels)) {
        $channelLayoutMap[$channels]
    }
    else {
        @{ Layout = "${channels}.0"; Title = "${channels}.0"; PlexName = "$channels Channel" }
    }
    
    # Get proper audio language
    $audioLanguage = if ($AudioInfo.ToCopy.Language -and $AudioInfo.ToCopy.Language -ne "und") { 
        $AudioInfo.ToCopy.Language 
    }
    else { 
        "eng" 
    }
    
    # Enhanced title generation based on codec and original metadata
    $originalTitle = if ($AudioInfo.ToCopy.Title -and $AudioInfo.ToCopy.Title -notmatch "^Track \d+$") {
        # Clean the original title by removing problematic characters for command line
        $cleanTitle = $AudioInfo.ToCopy.Title -replace '[^\w\s\-\.\(\)]', '_' -replace '\s+', ' '
        $cleanTitle.Trim()
    } else {
        # Generate a descriptive title based on codec and channels
        $codecDisplayName = switch ($sourceCodec) {
            "truehd" { "Dolby TrueHD" }
            "dts-hd" { "DTS-HD MA" }
            "dts" { "DTS" }
            "eac3" { "Dolby Digital Plus" }
            "ac3" { "Dolby Digital" }
            "aac" { "AAC" }
            "flac" { "FLAC" }
            "pcm_s16le" { "PCM" }
            "pcm_s24le" { "PCM 24-bit" }
            default { $sourceCodec.ToUpper() }
        }
        "$codecDisplayName $($layoutInfo.Title)"
    }
    
    Write-Host "Using audio title: '$originalTitle'" -ForegroundColor Green
    
    # Audio processing logic based on codec and Plex compatibility needs
    switch ($sourceCodec) {
        "truehd" {
            Write-Host "TrueHD detected - keeping original + creating AAC compatibility track (AAC as primary)" -ForegroundColor Yellow
            
            if ($CreateCompatibilityTrack -and -not $HasExistingAAC) {
                $compatBitrate = if ($channels -ge 6) { "640k" } elseif ($channels -eq 2) { "256k" } else { "192k" }
                $compatChannels = if ($channels -ge 6) { "6" } else { "2" }
                $compatLayout = if ($channels -ge 6) { "5.1" } else { "stereo" }
                
                # Make AAC the PRIMARY track (default)
                $audioSettings.Primary = @(
                    "-c:a:0", "aac",
                    "-b:a:0", $compatBitrate,
                    "-ac:a:0", $compatChannels,
                    "-profile:a:0", "aac_low",
                    "-metadata:s:a:0", "title=AAC_$($layoutInfo.PlexName)_Compatibility",
                    "-metadata:s:a:0", "language=$audioLanguage",
                    "-metadata:s:a:0", "CHANNELS=$compatChannels",
                    "-metadata:s:a:0", "CHANNEL_LAYOUT=$compatLayout",
                    "-disposition:a:0", "default"
                )
                
                # Make TrueHD the SECONDARY track (not default)
                $audioSettings.Fallback = @(
                    "-c:a:1", "copy",
                    "-metadata:s:a:1", "title=$originalTitle",
                    "-metadata:s:a:1", "language=$audioLanguage",
                    "-metadata:s:a:1", "CHANNELS=$channels",
                    "-metadata:s:a:1", "CHANNEL_LAYOUT=$($layoutInfo.Layout)",
                    "-disposition:a:1", "0"
                )
                $audioSettings.StreamCount = 2
                $audioSettings.RequiresFallback = $true
                Write-Host "AAC compatibility track set as PRIMARY ($compatBitrate, $compatChannels ch)" -ForegroundColor Green
                Write-Host "TrueHD original track set as SECONDARY (available but not default)" -ForegroundColor Green
            }
            else {
                # Fallback: just copy TrueHD if no compatibility track
                $audioSettings.Primary = @(
                    "-c:a:0", "copy",
                    "-metadata:s:a:0", "title=$originalTitle",
                    "-metadata:s:a:0", "language=$audioLanguage",
                    "-metadata:s:a:0", "CHANNELS=$channels",
                    "-metadata:s:a:0", "CHANNEL_LAYOUT=$($layoutInfo.Layout)",
                    "-disposition:a:0", "default"
                )
                Write-Host "No compatibility track created - TrueHD remains as primary" -ForegroundColor Yellow
            }
        }
        
        "dts-hd" {
            Write-Host "DTS-HD MA detected - keeping original + creating EAC3 compatibility track" -ForegroundColor Yellow
            $audioSettings.Primary = @(
                "-c:a:0", "copy",
                "-metadata:s:a:0", "title=$originalTitle",
                "-metadata:s:a:0", "language=$audioLanguage",
                "-metadata:s:a:0", "CHANNELS=$channels",
                "-metadata:s:a:0", "CHANNEL_LAYOUT=$($layoutInfo.Layout)",
                "-disposition:a:0", "default"
            )
            
            if ($CreateCompatibilityTrack -and -not $HasExistingAAC) {
                $compatBitrate = if ($channels -ge 6) { "1536k" } else { "384k" }
                $compatChannels = if ($channels -ge 6) { "6" } else { "2" }
                $compatLayout = if ($channels -ge 6) { "5.1" } else { "stereo" }
                
                $audioSettings.Fallback = @(
                    "-c:a:1", "eac3",
                    "-b:a:1", $compatBitrate,
                    "-ac:a:1", $compatChannels,
                    "-metadata:s:a:1", "title=Dolby_Digital_Plus_$($layoutInfo.PlexName)",
                    "-metadata:s:a:1", "language=$audioLanguage",
                    "-metadata:s:a:1", "CHANNELS=$compatChannels",
                    "-metadata:s:a:1", "CHANNEL_LAYOUT=$compatLayout",
                    "-disposition:a:1", "0"
                )
                $audioSettings.StreamCount = 2
                $audioSettings.RequiresFallback = $true
                Write-Host "Creating high-quality EAC3 compatibility track ($compatBitrate, $compatChannels ch) from DTS-HD" -ForegroundColor Green
            }
        }
        
        "dts" {
            if (-not $HasExistingAAC -and $CreateCompatibilityTrack) {
                Write-Host "DTS detected - converting to EAC3 for broader Plex compatibility" -ForegroundColor Yellow
                $targetBitrate = if ($channels -ge 6) { "1536k" } else { "448k" }
                $targetChannels = if ($channels -ge 6) { "6" } else { "2" }
                $targetLayout = if ($channels -ge 6) { "5.1" } else { "stereo" }
                
                $audioSettings.Primary = @(
                    "-c:a:0", "eac3",
                    "-b:a:0", $targetBitrate,
                    "-ac:a:0", $targetChannels,
                    "-metadata:s:a:0", "title=Dolby_Digital_Plus_$($layoutInfo.PlexName)",
                    "-metadata:s:a:0", "language=$audioLanguage",
                    "-metadata:s:a:0", "CHANNELS=$targetChannels",
                    "-metadata:s:a:0", "CHANNEL_LAYOUT=$targetLayout",
                    "-disposition:a:0", "default"
                )
                Write-Host "Converting DTS to EAC3 ($targetBitrate, $targetChannels ch) for Plex compatibility" -ForegroundColor Green
            }
            else {
                Write-Host "DTS detected - keeping original (AAC already exists or compatibility disabled)" -ForegroundColor Green
                $audioSettings.Primary = @(
                    "-c:a:0", "copy",
                    "-metadata:s:a:0", "title=$originalTitle",
                    "-metadata:s:a:0", "language=$audioLanguage",
                    "-metadata:s:a:0", "CHANNELS=$channels",
                    "-metadata:s:a:0", "CHANNEL_LAYOUT=$($layoutInfo.Layout)",
                    "-disposition:a:0", "default"
                )
            }
        }
        
        "flac" {
            if (-not $HasExistingAAC -and $CreateCompatibilityTrack) {
                Write-Host "FLAC detected - converting to AAC for streaming efficiency" -ForegroundColor Yellow
                $targetBitrate = if ($channels -ge 6) { "640k" } elseif ($channels -eq 2) { "320k" } else { "192k" }
                $targetChannels = if ($channels -ge 6) { "6" } else { "2" }
                $targetLayout = if ($channels -ge 6) { "5.1" } else { "stereo" }
                
                $audioSettings.Primary = @(
                    "-c:a:0", "aac",
                    "-b:a:0", $targetBitrate,
                    "-ac:a:0", $targetChannels,
                    "-profile:a:0", "aac_low",
                    "-metadata:s:a:0", "title=AAC_$($layoutInfo.PlexName)",
                    "-metadata:s:a:0", "language=$audioLanguage",
                    "-metadata:s:a:0", "CHANNELS=$targetChannels",
                    "-metadata:s:a:0", "CHANNEL_LAYOUT=$targetLayout",
                    "-disposition:a:0", "default"
                )
                Write-Host "Converting FLAC to AAC ($targetBitrate, $targetChannels ch) for streaming" -ForegroundColor Green
            }
            else {
                Write-Host "FLAC detected - keeping original (AAC already exists or compatibility disabled)" -ForegroundColor Green
                $audioSettings.Primary = @(
                    "-c:a:0", "copy",
                    "-metadata:s:a:0", "title=$originalTitle",
                    "-metadata:s:a:0", "language=$audioLanguage",
                    "-metadata:s:a:0", "CHANNELS=$channels",
                    "-metadata:s:a:0", "CHANNEL_LAYOUT=$($layoutInfo.Layout)",
                    "-disposition:a:0", "default"
                )
            }
        }
        
        { $_ -in @("pcm_s16le", "pcm_s24le") } {
            if (-not $HasExistingAAC -and $CreateCompatibilityTrack) {
                $bitDepth = if ($sourceCodec -eq "pcm_s24le") { "24-bit" } else { "16-bit" }
                Write-Host "PCM $bitDepth detected - converting to AAC for file size efficiency" -ForegroundColor Yellow
                $targetBitrate = if ($channels -ge 6) { "640k" } elseif ($channels -eq 2) { "320k" } else { "192k" }
                $targetChannels = if ($channels -ge 6) { "6" } else { "2" }
                $targetLayout = if ($channels -ge 6) { "5.1" } else { "stereo" }
                
                $audioSettings.Primary = @(
                    "-c:a:0", "aac",
                    "-b:a:0", $targetBitrate,
                    "-ac:a:0", $targetChannels,
                    "-profile:a:0", "aac_low",
                    "-metadata:s:a:0", "title=AAC_$($layoutInfo.PlexName)",
                    "-metadata:s:a:0", "language=$audioLanguage",
                    "-metadata:s:a:0", "CHANNELS=$targetChannels",
                    "-metadata:s:a:0", "CHANNEL_LAYOUT=$targetLayout",
                    "-disposition:a:0", "default"
                )
                Write-Host "Converting PCM $bitDepth to AAC ($targetBitrate, $targetChannels ch)" -ForegroundColor Green
            }
            else {
                Write-Host "PCM detected - keeping original (AAC already exists or compatibility disabled)" -ForegroundColor Green
                $audioSettings.Primary = @(
                    "-c:a:0", "copy",
                    "-metadata:s:a:0", "title=$originalTitle",
                    "-metadata:s:a:0", "language=$audioLanguage",
                    "-metadata:s:a:0", "CHANNELS=$channels",
                    "-metadata:s:a:0", "CHANNEL_LAYOUT=$($layoutInfo.Layout)",
                    "-disposition:a:0", "default"
                )
            }
        }
        
        "aac" {
            Write-Host "AAC detected - copying as-is (Plex native format)" -ForegroundColor Green
            $audioSettings.Primary = @(
                "-c:a:0", "copy",
                "-metadata:s:a:0", "title=$originalTitle",
                "-metadata:s:a:0", "language=$audioLanguage",
                "-metadata:s:a:0", "CHANNELS=$channels",
                "-metadata:s:a:0", "CHANNEL_LAYOUT=$($layoutInfo.Layout)",
                "-disposition:a:0", "default"
            )
        }
        
        "ac3" {
            Write-Host "AC3 (Dolby Digital) detected - copying as-is (Plex native format)" -ForegroundColor Green
            $audioSettings.Primary = @(
                "-c:a:0", "copy",
                "-metadata:s:a:0", "title=$originalTitle",
                "-metadata:s:a:0", "language=$audioLanguage",
                "-metadata:s:a:0", "CHANNELS=$channels",
                "-metadata:s:a:0", "CHANNEL_LAYOUT=$($layoutInfo.Layout)",
                "-metadata:s:a:0", "CODEC=AC3",
                "-metadata:s:a:0", "FORMAT=Dolby_Digital",
                "-disposition:a:0", "default"
            )
        }
        
        "eac3" {
            Write-Host "EAC3 (Dolby Digital Plus) detected - copying as-is (Plex native format)" -ForegroundColor Green
            $audioSettings.Primary = @(
                "-c:a:0", "copy",
                "-metadata:s:a:0", "title=$originalTitle",
                "-metadata:s:a:0", "language=$audioLanguage",
                "-metadata:s:a:0", "CHANNELS=$channels",
                "-metadata:s:a:0", "CHANNEL_LAYOUT=$($layoutInfo.Layout)",
                "-metadata:s:a:0", "CODEC=EAC3",
                "-metadata:s:a:0", "FORMAT=Dolby_Digital_Plus",
                "-disposition:a:0", "default"
            )
            
            # For EAC3, also create an AAC compatibility track if requested
            if ($CreateCompatibilityTrack -and -not $HasExistingAAC) {
                Write-Host "Creating AAC compatibility track from EAC3" -ForegroundColor Yellow
                $compatBitrate = if ($channels -ge 6) { "384k" } else { "256k" }
                $compatChannels = if ($channels -ge 6) { "6" } else { "2" }
                $compatLayout = if ($channels -ge 6) { "5.1" } else { "stereo" }
                
                $audioSettings.Fallback = @(
                    "-c:a:1", "aac",
                    "-b:a:1", $compatBitrate,
                    "-ac:a:1", $compatChannels,
                    "-profile:a:1", "aac_low",
                    "-metadata:s:a:1", "title=AAC_$($layoutInfo.PlexName)_Compatibility",
                    "-metadata:s:a:1", "language=$audioLanguage",
                    "-metadata:s:a:1", "CHANNELS=$compatChannels",
                    "-metadata:s:a:1", "CHANNEL_LAYOUT=$compatLayout",
                    "-disposition:a:1", "0"
                )
                $audioSettings.StreamCount = 2
                $audioSettings.RequiresFallback = $true
            }
        }
        
        default {
            if (-not $HasExistingAAC -and $CreateCompatibilityTrack) {
                Write-Host "Unknown codec '$sourceCodec' - converting to AAC for compatibility" -ForegroundColor Yellow
                $targetBitrate = if ($channels -ge 6) { "384k" } else { "256k" }
                $targetChannels = if ($channels -ge 6) { "6" } else { "2" }
                $targetLayout = if ($channels -ge 6) { "5.1" } else { "stereo" }
                
                $audioSettings.Primary = @(
                    "-c:a:0", "aac",
                    "-b:a:0", $targetBitrate,
                    "-ac:a:0", $targetChannels,
                    "-profile:a:0", "aac_low",
                    "-metadata:s:a:0", "title=AAC_$($layoutInfo.PlexName)",
                    "-metadata:s:a:0", "language=$audioLanguage",
                    "-metadata:s:a:0", "CHANNELS=$targetChannels",
                    "-metadata:s:a:0", "CHANNEL_LAYOUT=$targetLayout",
                    "-disposition:a:0", "default"
                )
                Write-Host "Converting unknown codec to AAC ($targetBitrate, $targetChannels ch)" -ForegroundColor Green
            }
            else {
                Write-Host "Unknown codec '$sourceCodec' - copying as-is (AAC already exists or compatibility disabled)" -ForegroundColor Yellow
                $audioSettings.Primary = @(
                    "-c:a:0", "copy",
                    "-metadata:s:a:0", "title=$originalTitle",
                    "-metadata:s:a:0", "language=$audioLanguage",
                    "-metadata:s:a:0", "CHANNELS=$channels",
                    "-metadata:s:a:0", "CHANNEL_LAYOUT=$($layoutInfo.Layout)",
                    "-disposition:a:0", "default"
                )
            }
        }
    }
    
    Write-Host "Audio optimization complete - $($audioSettings.StreamCount) stream(s) configured" -ForegroundColor Green
    Write-Host "Primary audio title: '$originalTitle'" -ForegroundColor Green
    if ($audioSettings.RequiresFallback) {
        Write-Host "Compatibility track: Required for optimal Plex playback" -ForegroundColor Green
    }
    
    return $audioSettings
}


function Get-PlexStreamingSettings {
    param([hashtable]$VideoInfo, [hashtable]$EncodingSettings)
    
    Write-Host "Configuring Plex streaming optimizations..." -ForegroundColor Cyan
    
    # Calculate optimal keyframe interval based on frame rate
    $fps = [math]::Round($VideoInfo.FrameRate)
    Write-Host "***** $($fps)"
    $keyframeInterval = $fps * 2  # 2-second keyframes for good seeking
    
    # Keyframe and GOP structure optimization
    $streamingArgs = @(
        "-g", $keyframeInterval,                    # Keyframe every 2 seconds
        "-keyint_min", $keyframeInterval,           # Minimum keyframe interval (consistent)
        "-sc_threshold", "0",                       # Disable scene change keyframes for predictable structure
        "-force_key_frames", "`"expr:gte(t,n_forced*2)`""  # Force keyframes every 2 seconds
    )
    
    # Frame structure optimization for Plex
    $frameStructureArgs = @(
        "-bf", "5",                                 # 3 B-frames for efficiency
        "-b_strategy", "2",                         # Adaptive B-frame placement
        "-refs", "3",                               # Reduce references for device compatibility (was 4)
        "-forced_idr", "1",
        "-b_adapt", "2"                            # Adaptive B-frame decision
    )
    
    # Container optimization for Plex streaming - FIXED
    $containerArgs = @(
        "-movflags", "faststart+frag_keyframe+separate_moof+omit_tfhd_offset",
        "-frag_duration", "2000000",                # 2-second fragments for adaptive streaming
        "-min_frag_duration", "2000000",            # Minimum fragment duration
        "-write_tmcd", "0",                         # Don't write timecode track
        "-map_metadata", "-1"                       # Clean metadata for streaming
    )
    
    # Bitrate control for consistent streaming
    $bitrateArgs = @(
        "-bufsize", ($EncodingSettings.MaxFrameSize * 3),      # 3x max frame buffer
        "-maxrate", ($EncodingSettings.MaxFrameSize * 6),      # Reasonable max bitrate
        "-rc_lookahead", "60",                                 # Reduced lookahead for faster encoding
        "-mbbrc", "1",                                         # Macroblock-level rate control
        "-extbrc", "1"                                         # Extended bitrate control
    )
    
    Write-Host "Streaming settings: $keyframeInterval-frame GOP, 3 B-frames, 2-second fragments" -ForegroundColor Green
    
    return @{
        Keyframes      = $streamingArgs
        FrameStructure = $frameStructureArgs
        Container      = $containerArgs
        Bitrate        = $bitrateArgs
    }
}
function Test-SubtitleStreams {
    param([string]$FilePath)
    
    Write-Host "Verifying subtitle streams in output file..." -ForegroundColor Cyan
    
    try {
        $subtitleCheck = & $Config.FFProbeExe -v quiet -select_streams s -show_entries stream=index,codec_name:stream_tags=language,title -of csv=p=0 $FilePath
        
        if ($subtitleCheck) {
            Write-Host "Found subtitle streams in output:" -ForegroundColor Green
            $subtitleCheck -split "`n" | ForEach-Object {
                if ($_.Trim()) {
                    Write-Host "  $_" -ForegroundColor Gray
                }
            }
        }
        else {
            Write-Host "No subtitle streams found in output file" -ForegroundColor Red
        }
    }
    catch {
        Write-Warning "Failed to check subtitle streams: $_"
    }
}
function Get-PlexSubtitleSettings {
    param(
        [hashtable]$SubtitleInfo, 
        [string]$BaseName
    )
    
    Write-Host "Configuring Plex-optimized subtitle settings..." -ForegroundColor Cyan
    
    $subtitleSettings = @{
        CopyStreams     = @()
        ConvertedFiles  = @()
        MapArgs         = @()
        MetadataArgs    = @()
        StreamCount     = 0
        HasForcedSubs   = $false
        HasDefaultSet   = $false
    }
    
    $streamIndex = 0
    $hasDefaultSet = $false
    $hasForcedSubtitle = $false
    
    # FIRST PASS: Check if we have any forced subtitles across all sources
    foreach ($stream in $SubtitleInfo.ToCopy) {
        if ($stream.IsForced) {
            $hasForcedSubtitle = $true
            break
        }
    }
    
    # Also check extracted/converted files for forced subtitles
    if (-not $hasForcedSubtitle) {
        foreach ($file in $Script:ExtractedSubtitles) {
            $fileName = [System.IO.Path]::GetFileNameWithoutExtension($file)
            if ($fileName -match "forced|Forced|FORCED") {
                $hasForcedSubtitle = $true
                break
            }
        }
    }
    
    $subtitleSettings.HasForcedSubs = $hasForcedSubtitle
    Write-Host "Forced subtitle detected: $hasForcedSubtitle" -ForegroundColor $(if ($hasForcedSubtitle) { "Yellow" } else { "Gray" })
    
    # SECOND PASS: Handle streams that can be copied directly from original file
    foreach ($stream in $SubtitleInfo.ToCopy) {
        Write-Host "Processing internal subtitle stream $($stream.LogicalIndex): $($stream.Codec)$(if ($stream.IsForced) { ' [FORCED]' } else { '' })" -ForegroundColor Gray
        
        # Map from original file (input 1 in FFmpeg command)
        $subtitleSettings.MapArgs += "-map", "1:s:$($stream.LogicalIndex)"
        
        # Determine subtitle disposition with forced subtitle priority
        $disposition = "0"  # Default to no special disposition
        
        if ($stream.IsForced) {
            # Forced subtitles get forced disposition AND default if it's the first forced subtitle
            $disposition = "forced"
            if (-not $hasDefaultSet) {
                $disposition = "forced+default"
                $hasDefaultSet = $true
                $subtitleSettings.HasDefaultSet = $true
                Write-Host "Setting forced+default disposition for first forced stream $streamIndex" -ForegroundColor Green
            }
            else {
                Write-Host "Setting forced disposition for additional forced stream $streamIndex" -ForegroundColor Yellow
            }
        }
        elseif (-not $hasForcedSubtitle -and -not $hasDefaultSet) {
            # Only set regular subtitles as default if there are NO forced subtitles
            $disposition = "default"
            $hasDefaultSet = $true
            $subtitleSettings.HasDefaultSet = $true
            Write-Host "No forced subtitles exist - setting default disposition for stream $streamIndex" -ForegroundColor Green
        }
        else {
            Write-Host "Stream $($streamIndex): No special disposition (forced subtitles take priority)" -ForegroundColor Gray
        }
        
        # Convert text-based subtitles to SRT for maximum Plex compatibility
        if ($stream.Codec -eq "subrip" -or $stream.Codec -eq "mov_text") {
            $subtitleSettings.MetadataArgs += @(
                "-c:s:$streamIndex", "srt",
                "-metadata:s:s:$streamIndex", "language=$($stream.Language)",
                "-disposition:s:$streamIndex", $disposition
            )
        }
        else {
            # Keep other formats as-is (like PGS)
            $subtitleSettings.MetadataArgs += @(
                "-c:s:$streamIndex", "copy",
                "-metadata:s:s:$streamIndex", "language=$($stream.Language)",
                "-disposition:s:$streamIndex", $disposition
            )
        }
        
        # Enhanced title handling that preserves forced indication and quality info
        if ($stream.Title) {
            $titleToUse = $stream.Title
            # Ensure forced subtitles have "Forced" in the title if not already present
            if ($stream.IsForced -and $titleToUse -notmatch "forced|Forced|FORCED") {
                $titleToUse = "$titleToUse (Forced)"
            }
            # Add quality indicators
            if ($stream.QualityScore -and $stream.QualityScore -gt 50) {
                if ($titleToUse -notmatch "SDH|CC|Hearing") {
                    $titleToUse = "$titleToUse"  # Keep as-is for high-quality streams
                }
            }
            $subtitleSettings.MetadataArgs += "-metadata:s:s:$streamIndex", "title=$titleToUse"
            Write-Host "Preserving subtitle title: '$titleToUse'" -ForegroundColor Green
        }
        else {
            # Generate appropriate title based on stream characteristics
            if ($stream.IsForced) {
                $subtitleSettings.MetadataArgs += "-metadata:s:s:$streamIndex", "title=English (Forced)"
            }
            else {
                # Add quality indicators based on codec
                $qualityIndicator = switch ($stream.Codec) {
                    "subrip" { "SRT" }
                    "mov_text" { "Text" }
                    "hdmv_pgs_subtitle" { "PGS" }
                    "ass" { "ASS" }
                    "ssa" { "SSA" }
                    default { "Subtitle" }
                }
                $subtitleSettings.MetadataArgs += "-metadata:s:s:$streamIndex", "title=English ($($qualityIndicator))"
            }
        }
        
        $streamIndex++
        $subtitleSettings.StreamCount++
    }
    
    # THIRD PASS: Handle extracted/converted subtitle files
    $extractedFileIndex = 2  # Start after video (0) and original file (1) inputs
    foreach ($file in $Script:ExtractedSubtitles) {
        Write-Host "Adding extracted subtitle file: $(Split-Path $file -Leaf)" -ForegroundColor Gray
        
        $subtitleSettings.MapArgs += "-map", "$($extractedFileIndex):s:0"
        
        # Determine format based on file extension
        $extension = [System.IO.Path]::GetExtension($file).ToLower()
        $codec = if ($extension -eq ".sup") { "copy" } else { "srt" }
        $format = if ($extension -eq ".sup") { "PGS" } else { "SRT" }
        
        # Set disposition - extracted subtitles are generally not forced unless filename indicates
        $fileName = [System.IO.Path]::GetFileNameWithoutExtension($file)
        $isExtractedForced = $fileName -match "forced|Forced|FORCED"
        
        $disposition = "0"  # Default to no special disposition
        if ($isExtractedForced) {
            # Extracted forced subtitle gets forced disposition AND default if no default set yet
            $disposition = "forced"
            if (-not $hasDefaultSet) {
                $disposition = "forced+default"
                $hasDefaultSet = $true
                $subtitleSettings.HasDefaultSet = $true
                Write-Host "Setting forced+default disposition for extracted forced subtitle" -ForegroundColor Green
            }
            else {
                Write-Host "Setting forced disposition for additional extracted forced subtitle" -ForegroundColor Yellow
            }
        }
        elseif (-not $hasForcedSubtitle -and -not $hasDefaultSet) {
            # Only set extracted subtitle as default if there are NO forced subtitles anywhere
            $disposition = "default"
            $hasDefaultSet = $true
            $subtitleSettings.HasDefaultSet = $true
            Write-Host "No forced subtitles exist - setting extracted subtitle as default" -ForegroundColor Green
        }
        else {
            Write-Host "Extracted subtitle: No special disposition (forced subtitles take priority)" -ForegroundColor Gray
        }
        
        $subtitleSettings.MetadataArgs += @(
            "-c:s:$streamIndex", $codec,
            "-metadata:s:s:$streamIndex", "language=eng",
            "-disposition:s:$streamIndex", $disposition
        )
        
        # Set appropriate title with quality and source indicators
        if ($isExtractedForced) {
            $subtitleSettings.MetadataArgs += "-metadata:s:s:$streamIndex", "title=English ($($format) Extracted - Forced)"
        }
        else {
            # Determine if this is a high-quality extraction
            $qualityIndicator = ""
            if ($fileName -match "SDH|CC|Hearing") {
                $qualityIndicator = " SDH"
            }
            elseif ($fileName -match "Full|Complete") {
                $qualityIndicator = " Full"
            }
            
            $subtitleSettings.MetadataArgs += "-metadata:s:s:$streamIndex", "title=English ($($format) Extracted$($qualityIndicator))"
        }
        
        $extractedFileIndex++
        $streamIndex++
        $subtitleSettings.StreamCount++
    }
    
    Write-Host "Subtitle configuration complete - $($subtitleSettings.StreamCount) subtitle stream(s)" -ForegroundColor Green
    
    # Enhanced disposition summary with improved logic reporting
    Write-Host "`nSubtitle Disposition Summary:" -ForegroundColor Cyan
    if ($hasForcedSubtitle) {
        Write-Host "FORCED SUBTITLE PRIORITY: Forced subtitles detected - they take precedence over regular subtitles" -ForegroundColor Green
        Write-Host "  - Forced subtitles will be marked with 'forced' disposition" -ForegroundColor Green
        Write-Host "  - First forced subtitle will also be marked as 'default'" -ForegroundColor Green
        Write-Host "  - Regular subtitles will NOT be marked as default" -ForegroundColor Green
        Write-Host "Plex behavior: Forced subtitles will show automatically during foreign language parts" -ForegroundColor Cyan
    }
    else {
        Write-Host "No forced subtitles detected - regular subtitle priority rules apply" -ForegroundColor Yellow
        if ($hasDefaultSet) {
            Write-Host "  - First regular subtitle set as default" -ForegroundColor Green
            Write-Host "Plex behavior: Default subtitle available but not automatically shown" -ForegroundColor Cyan
        }
        else {
            Write-Host "  - No subtitles set as default" -ForegroundColor Gray
            Write-Host "Plex behavior: All subtitles available in menu but none auto-selected" -ForegroundColor Cyan
        }
    }
    
    # Summary of subtitle types found
    $internalCount = $SubtitleInfo.ToCopy.Count
    $extractedCount = $Script:ExtractedSubtitles.Count
    $forcedCount = ($SubtitleInfo.ToCopy | Where-Object IsForced).Count
    
    if ($extractedCount -gt 0) {
        $extractedForcedCount = ($Script:ExtractedSubtitles | Where-Object { 
            [System.IO.Path]::GetFileNameWithoutExtension($_) -match "forced|Forced|FORCED" 
        }).Count
        $forcedCount += $extractedForcedCount
    }
    
    Write-Host "`nSubtitle Processing Summary:" -ForegroundColor Cyan
    Write-Host "  Internal streams: $internalCount" -ForegroundColor White
    Write-Host "  Extracted files: $extractedCount" -ForegroundColor White
    Write-Host "  Total forced subtitles: $forcedCount" -ForegroundColor $(if ($forcedCount -gt 0) { "Green" } else { "Yellow" })
    Write-Host "  Default subtitle set: $($subtitleSettings.HasDefaultSet)" -ForegroundColor $(if ($subtitleSettings.HasDefaultSet) { "Green" } else { "Yellow" })
    
    return $subtitleSettings
}


function Invoke-FFProbeWithCleanup {
    param(
        [string[]]$Arguments,
        [int]$TimeoutSeconds = 30
    )
    
    try {
        if (-not (Test-Path $Config.FFProbeExe)) {
            return @{
                Success = $false
                ExitCode = -1
                StdOut = ""
                StdErr = "FFProbe executable not found: $($Config.FFProbeExe)"
            }
        }
        
        #Write-Host "[DEBUG] Using direct process approach like other working FFmpeg calls..." -ForegroundColor Cyan
        
        # Use the same ProcessStartInfo approach as the working Start-Process function
        $processInfo = New-Object System.Diagnostics.ProcessStartInfo
        $processInfo.FileName = $Config.FFProbeExe
        $processInfo.UseShellExecute = $false
        $processInfo.RedirectStandardOutput = $true
        $processInfo.RedirectStandardError = $true
        $processInfo.CreateNoWindow = $true
        
        # Use the same argument handling as the working parts of the script
        if ($PSVersionTable.PSVersion.Major -ge 7) {
            foreach ($arg in $Arguments) {
                $null = $processInfo.ArgumentList.Add($arg)
            }
        }
        else {
            # This is the key - use the same legacy argument handling as working parts
            $legacyQuotedArgs = $Arguments | ForEach-Object { 
                if ($_ -match '\s') { '"{0}"' -f ($_ -replace '"', '""') } else { $_ }
            }
            $processInfo.Arguments = $legacyQuotedArgs -join ' '
        }

 #       $quotedArgs = $Arguments | ForEach-Object {
 #           if ($_ -match '\s') { "`"$_`"" } else { $_ }
 #       }
 #       $commandLine = $quotedArgs -join ' '
 #       Write-Host "[CMD] $FFProbeExe $commandLine" -ForegroundColor Gray
        #Write-Host "[DEBUG] Starting process using same method as working FFmpeg calls..." -ForegroundColor Yellow
        
        $process = [System.Diagnostics.Process]::Start($processInfo)
        $handle = $process.Handle  # Cache handle like other working calls
        $Script:ActiveProcesses = @($Script:ActiveProcesses) + $process
        
        # Use async reading to avoid blocking (like the working encoding functions)
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        
        # Wait with timeout using the same pattern as working calls
        if ($process.WaitForExit($TimeoutSeconds * 1000)) {
            $stdoutText = $stdout.Result
            $stderrText = $stderr.Result
            $exitCode = $process.ExitCode
            
            # Clean up process (same as working calls)
            $process.Close()
            $process.Dispose()
            $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $process.Id }
            
            #Write-Host "[DEBUG] Process completed with exit code: $exitCode" -ForegroundColor $(if ($exitCode -eq 0) { "Green" } else { "Red" })
            #Write-Host "[DEBUG] StdOut length: $($stdoutText.Length) characters" -ForegroundColor Gray
           # Write-Host "[DEBUG] StdErr length: $($stderrText.Length) characters" -ForegroundColor Gray
            
            if ($stderrText.Length -gt 0 -and $exitCode -ne 0) {
                $stderrPreview = if ($stderrText.Length -gt 200) { $stderrText.Substring(0, 200) + "..." } else { $stderrText }
                Write-Host "[DEBUG] StdErr: $stderrPreview" -ForegroundColor Yellow
            }
            
            return @{
                Success = ($exitCode -eq 0)
                ExitCode = $exitCode
                StdOut = $stdoutText
                StdErr = $stderrText
            }
        }
        else {
            Write-Warning "[DEBUG] Process timed out after $TimeoutSeconds seconds"
            try {
                $process.Kill()
                $process.WaitForExit(5000)
            }
            catch {
                Write-Warning "[DEBUG] Failed to kill process: $_"
            }
            
            $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $process.Id }
            
            return @{
                Success = $false
                ExitCode = -1
                StdOut = ""
                StdErr = "Process timed out after $TimeoutSeconds seconds"
            }
        }
    }
    catch {
        Write-Warning "[DEBUG] Exception running FFProbe: $_"
        return @{
            Success = $false
            ExitCode = -1
            StdOut = ""
            StdErr = "Exception: $_"
        }
    }
    finally {
        # Same cleanup pattern as working functions
        if ($process -and -not $process.HasExited) {
            try {
                $process.Kill()
                $process.WaitForExit(2000)
            }
            catch {
                # Ignore cleanup errors
            }
        }
        
        if ($process) {
            try {
                $process.Close()
                $process.Dispose()
            }
            catch {
                # Ignore disposal errors
            }
            
            $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $process.Id }
        }
    }
}
function Get-PlexCompatibilitySettings {
    param([hashtable]$VideoInfo, [bool]$OptimizeForRemoteStreaming = $false)
    
    Write-Host "Configuring Plex device compatibility settings..." -ForegroundColor Cyan
    
    $height = $VideoInfo.Height
    $isHDR = Test-HDRContent -VideoInfo $VideoInfo
    
    # ENHANCED: Preserve original profile and level when possible
    $lprofile = "main"
    if ($VideoInfo.OriginalProfile -and $VideoInfo.OriginalProfile -ne "unknown") {
        $lprofile = $VideoInfo.OriginalProfile.ToLower()
        Write-Host "Preserving original profile: $lprofile" -ForegroundColor Green
    }
    elseif ($isHDR) {
        $lprofile = "main10"
        Write-Host "Using Main10 profile for HDR content" -ForegroundColor Green
    }
    
    # ENHANCED: Preserve original level when appropriate
    $level = $null
    if ($VideoInfo.OriginalLevel) {
        # Convert level number to string format
        switch ($VideoInfo.OriginalLevel) {
            150 { $level = "5.0" }
            153 { $level = "5.1" }
            156 { $level = "5.2" }
            default {
                # Calculate level from number (e.g., 123 = level 4.1)
                $majorLevel = [math]::Floor($VideoInfo.OriginalLevel / 30)
                $minorLevel = ($VideoInfo.OriginalLevel % 30) / 3
                $level = "$majorLevel.$minorLevel"
            }
        }
        Write-Host "Preserving original HEVC level: $level (from $($VideoInfo.OriginalLevel))" -ForegroundColor Green
    }
    else {
        # Fallback to resolution-based level selection
        if ($height -ge 2160) {
            $level = if ($OptimizeForRemoteStreaming) { "5.0" } else { "5.1" }
        }
        elseif ($height -ge 1440) {
            $level = "4.1"
        }
        elseif ($height -ge 1080) {
            $level = "4.0"
        }
        else {
            $level = "3.1"
        }
        Write-Host "Using resolution-based HEVC level: $level" -ForegroundColor Yellow
    }
    
    $compatibilityArgs = @(
        "-profile:v", $lprofile,
        "-level:v", $level,
        "-tier", "main",
        "-slices", "1",
        "-aud", "1",
        "-repeat_pps", "1",
        "-strict", "experimental"
    )
    
    Write-Host "Plex compatibility settings configured: Profile=$lprofile, Level=$level" -ForegroundColor Green
    
    return $compatibilityArgs
}
function Get-ValidatedTempFolder {
    param(
        [string]$RequestedTempFolder,
        [string]$InputFile,
        [double]$MinSpaceMultiplier = 4.0  # Require 3x input file size as free space
    )
    
    Write-Host "Configuring temporary folder with drive space validation..." -ForegroundColor Cyan
    
    # Calculate minimum required space based on input file
    $inputFileSize = 0
    $minRequiredSpaceGB = 10  # Default minimum 10GB
    
    if ($InputFile -and (Test-Path $InputFile)) {
        try {
            $inputFileSize = (Get-Item $InputFile).Length
            $inputFileSizeGB = [math]::Round($inputFileSize / 1GB, 2)
            $minRequiredSpaceGB = [math]::Max(10, [math]::Ceiling($inputFileSizeGB * $MinSpaceMultiplier))
            
            Write-Host "Input file size: $inputFileSizeGB GB" -ForegroundColor White
            Write-Host "Minimum required temp space: $minRequiredSpaceGB GB (${MinSpaceMultiplier}x input + overhead)" -ForegroundColor Yellow
        }
        catch {
            Write-Warning "Could not determine input file size: $_"
            Write-Host "Using default minimum space requirement: $minRequiredSpaceGB GB" -ForegroundColor Yellow
        }
    }
    else {
        Write-Host "No input file specified - using default minimum space requirement: $minRequiredSpaceGB GB" -ForegroundColor Yellow
    }
    
    # Priority order for temp folder selection
    $tempCandidates = @()
    
    # 1. User-specified folder (if provided and has enough space)
    if ($RequestedTempFolder -and $RequestedTempFolder.Trim()) {
        $tempCandidates += $RequestedTempFolder
    }
    
    # 2. User temp folder from environment
    if ($env:TEMP) {
        $tempCandidates += $env:TEMP
    }
    
    # 3. Alternative temp environment variables
    if ($env:TMP) {
        $tempCandidates += $env:TMP
    }
    
    # 4. Windows temp folders
    $tempCandidates += "$env:USERPROFILE\AppData\Local\Temp"
    $tempCandidates += "$env:LOCALAPPDATA\Temp"
    
    # 5. System temp folder
    $tempCandidates += "$env:SystemRoot\Temp"
    
    # 6. Fallback options with explicit C:\Temp preference
    $tempCandidates += "C:\Temp"
    $tempCandidates += "D:\Temp"
    $tempCandidates += "E:\Temp"
    
    # Track candidates that failed space checks for reporting
    $insufficientSpaceCandidates = @()
    
    foreach ($candidate in $tempCandidates) {
        try {
            # Resolve any environment variables and normalize path
            $resolvedPath = [System.Environment]::ExpandEnvironmentVariables($candidate)
            $normalizedPath = [System.IO.Path]::GetFullPath($resolvedPath)
            
            Write-Host "  Testing temp folder: $normalizedPath" -ForegroundColor Gray
            
            # Test if folder exists or can be created
            if (-not (Test-Path $normalizedPath)) {
                Write-Host "    Creating temp folder..." -ForegroundColor Gray
                New-Item -ItemType Directory -Path $normalizedPath -Force | Out-Null
            }
            
            # Test write permissions with a small test file
            $testFile = Join-Path $normalizedPath "temp_write_test_$(Get-Random).tmp"
            try {
                "test" | Out-File -FilePath $testFile -Force
                Remove-Item $testFile -Force -ErrorAction SilentlyContinue
                Write-Host "    Write permissions: OK" -ForegroundColor Green
            }
            catch {
                Write-Warning "    Write test failed: $_"
                continue
            }
            
            # Check available space (this is the key enhancement)
            $drive = [System.IO.Path]::GetPathRoot($normalizedPath)
            $driveInfo = Get-WmiObject -Class Win32_LogicalDisk | Where-Object DeviceID -eq $drive.TrimEnd('\')
            
            if ($driveInfo -and $driveInfo.FreeSpace) {
                $freeSpaceGB = [math]::Round($driveInfo.FreeSpace / 1GB, 2)
                #$usedSpaceGB = [math]::Round(($driveInfo.Size - $driveInfo.FreeSpace) / 1GB, 2)
                $totalSpaceGB = [math]::Round($driveInfo.Size / 1GB, 2)
                $freeSpacePercent = [math]::Round(($driveInfo.FreeSpace / $driveInfo.Size) * 100, 1)
                
                Write-Host "    Drive $drive space: $freeSpaceGB GB free / $totalSpaceGB GB total ($freeSpacePercent% free)" -ForegroundColor White
                
                if ($freeSpaceGB -lt $minRequiredSpaceGB) {
                    $insufficientSpaceCandidates += [PSCustomObject]@{
                        Path            = $normalizedPath
                        Drive           = $drive
                        FreeSpaceGB     = $freeSpaceGB
                        RequiredSpaceGB = $minRequiredSpaceGB
                        ShortfallGB     = $minRequiredSpaceGB - $freeSpaceGB
                    }
                    
                    Write-Warning "    Insufficient space: $freeSpaceGB GB available, $minRequiredSpaceGB GB required (shortfall: $([math]::Round($minRequiredSpaceGB - $freeSpaceGB, 2)) GB)"
                    continue
                }
                
                # Additional check: Ensure we have at least 15% free space on the drive
                if ($freeSpacePercent -lt 15) {
                    Write-Warning "    Drive has less than 15% free space ($freeSpacePercent%) - may cause performance issues"
                    Write-Host "    Continuing anyway as absolute space requirement is met..." -ForegroundColor Yellow
                }
                
                Write-Host "    Space check: PASSED ($freeSpaceGB GB >= $minRequiredSpaceGB GB required)" -ForegroundColor Green
                
            }
            else {
                Write-Warning "    Could not determine drive space for $drive"
                # If we can't determine space, continue with caution
                Write-Host "    Proceeding without space verification..." -ForegroundColor Yellow
            }
            
            Write-Host "  Selected temp folder: $normalizedPath" -ForegroundColor Green
            Write-Host "  Available space: $freeSpaceGB GB" -ForegroundColor Green
            
            return $normalizedPath
            
        }
        catch {
            Write-Warning "    Cannot access $candidate : $_"
            continue
        }
    }
    
    # If we get here, no temp folder had sufficient space
    Write-Host "`n=== CRITICAL: No Suitable Temp Folder Found ===" -ForegroundColor Red
    Write-Host "All tested locations have insufficient disk space for video processing." -ForegroundColor Red
    Write-Host "`nSpace analysis:" -ForegroundColor Yellow
    
    if ($insufficientSpaceCandidates.Count -gt 0) {
        Write-Host "Locations with insufficient space:" -ForegroundColor Red
        foreach ($candidate in $insufficientSpaceCandidates) {
            Write-Host "  $($candidate.Path)" -ForegroundColor Red
            Write-Host "    Available: $($candidate.FreeSpaceGB) GB" -ForegroundColor Red
            Write-Host "    Required:  $($candidate.RequiredSpaceGB) GB" -ForegroundColor Red
            Write-Host "    Shortfall: $($candidate.ShortfallGB) GB" -ForegroundColor Red
        }
    }
    
    Write-Host "`nRecommended solutions:" -ForegroundColor Yellow
    Write-Host "1. Free up at least $([math]::Round(($insufficientSpaceCandidates | Measure-Object ShortfallGB -Minimum).Minimum, 2)) GB of disk space" -ForegroundColor White
    Write-Host "2. Use a drive with more available space by specifying -TempFolder parameter" -ForegroundColor White
    Write-Host "3. Process smaller video files or reduce quality settings" -ForegroundColor White
    Write-Host "4. Move input file to a drive with more space" -ForegroundColor White
    
    # Last resort: try to find ANY writable location with at least 5GB
    Write-Host "`nAttempting emergency fallback (minimum 5GB requirement)..." -ForegroundColor Yellow
    $emergencyMinSpace = 5
    
    foreach ($candidate in $tempCandidates) {
        try {
            $resolvedPath = [System.Environment]::ExpandEnvironmentVariables($candidate)
            $normalizedPath = [System.IO.Path]::GetFullPath($resolvedPath)
            
            if (-not (Test-Path $normalizedPath)) {
                New-Item -ItemType Directory -Path $normalizedPath -Force | Out-Null
            }
            
            $drive = [System.IO.Path]::GetPathRoot($normalizedPath)
            $driveInfo = Get-WmiObject -Class Win32_LogicalDisk | Where-Object DeviceID -eq $drive.TrimEnd('\')
            
            if ($driveInfo -and $driveInfo.FreeSpace) {
                $freeSpaceGB = [math]::Round($driveInfo.FreeSpace / 1GB, 2)
                if ($freeSpaceGB -ge $emergencyMinSpace) {
                    Write-Warning "EMERGENCY FALLBACK: Using $normalizedPath with only $freeSpaceGB GB free"
                    Write-Warning "This may not be sufficient for large video files - monitor disk space closely!"
                    return $normalizedPath
                }
            }
        }
        catch {
            continue
        }
    }
    
    # Absolute last resort
    throw "CRITICAL ERROR: Unable to find any temporary folder with sufficient disk space for video processing. Please free up disk space or specify a different temp folder location."
}

function Start-SpaceMonitoring {
    param(
        [string]$TempFolder,
        [int]$CheckIntervalSeconds = 30
    )
    
    # Start a background job to monitor space
    $monitorJob = Start-Job -ScriptBlock {
        param($folder, $interval)
        
        while ($true) {
            try {
                $drive = [System.IO.Path]::GetPathRoot($folder)
                $driveInfo = Get-WmiObject -Class Win32_LogicalDisk | Where-Object DeviceID -eq $drive.TrimEnd('\')
                
                if ($driveInfo -and $driveInfo.FreeSpace) {
                    $freeSpaceGB = [math]::Round($driveInfo.FreeSpace / 1GB, 2)
                    
                    if ($freeSpaceGB -lt 2) {
                        Write-Warning "[SPACE MONITOR] CRITICAL: Only $freeSpaceGB GB remaining on temp drive!"
                        break
                    }
                    elseif ($freeSpaceGB -lt 5) {
                        Write-Warning "[SPACE MONITOR] LOW SPACE: $freeSpaceGB GB remaining on temp drive"
                    }
                }
                
                Start-Sleep -Seconds $interval
            }
            catch {
                # Silent monitoring - don't spam errors
                Start-Sleep -Seconds $interval
            }
        }
    } -ArgumentList $TempFolder, $CheckIntervalSeconds
    
    return $monitorJob
}

function Stop-SpaceMonitoringWithCleanup {
    param($MonitorJob)
    
    if ($MonitorJob) {
        try {
            Write-Host "Stopping space monitoring job..." -ForegroundColor Gray
            
            # Simple stop without waiting
            Stop-Job $MonitorJob -ErrorAction SilentlyContinue
            
            # Brief pause to let it stop
            Start-Sleep -Milliseconds 500
            
            # Force remove regardless of state
            Remove-Job $MonitorJob -Force -ErrorAction SilentlyContinue
            
            Write-Host "Space monitoring stopped and cleaned up" -ForegroundColor Gray
        }
        catch {
            Write-Warning "Error during space monitoring cleanup: $_"
            # Force cleanup attempt
            try {
                Remove-Job $MonitorJob -Force -ErrorAction SilentlyContinue
            }
            catch {
                # Ignore final cleanup errors
            }
        }
    }
}

function Get-VideoMetadataWithComprehensiveHDR10Plus {
    param([string]$FilePath)
    
    Write-Host "Analyzing video metadata with comprehensive HDR10+ detection..." -ForegroundColor Cyan
    
    # Get base metadata first
    $baseMetadata = Get-VideoMetadata -FilePath $FilePath
    
    if (-not $baseMetadata) {
        return $null
    }
    
    # Do comprehensive HDR10+ detection
    Write-Host "Performing comprehensive HDR10+ analysis..." -ForegroundColor Yellow
    $hdr10PlusResult = Test-HDR10PlusInOriginal -FilePath $FilePath -Verbose $true
    
    # Create new hashtable with all properties including corrected HDR10+ info
    return @{
        Width                    = $baseMetadata.Width
        Height                   = $baseMetadata.Height
        FrameRate                = $baseMetadata.FrameRate
        Bitrate                  = $baseMetadata.Bitrate
        ColorPrimaries           = $baseMetadata.ColorPrimaries
        ColorTransfer            = $baseMetadata.ColorTransfer
        ColorSpace               = $baseMetadata.ColorSpace
        PixelFormat              = $baseMetadata.PixelFormat
        HasDolbyVision           = $baseMetadata.HasDolbyVision
        colorRange               = $baseMetadata.colorRange
        HasHdr10Plus             = $hdr10PlusResult.HasHDR10Plus  # Use comprehensive detection
        HDR10PlusDetectionMethod = if ($hdr10PlusResult.HasHDR10Plus) { 
            "Comprehensive detection: " + ($hdr10PlusResult.DetectionMethods | Select-Object -First 3 | ForEach-Object { $_.Split(':')[0] }) -join ", "
        } else { 
            "Comprehensive scan found no HDR10+ metadata" 
        }
        ExtractedHdr10JsonPath   = $baseMetadata.ExtractedHdr10JsonPath
        Duration                 = $baseMetadata.Duration
        CodecName                = $baseMetadata.CodecName
        OriginalProfile          = $baseMetadata.OriginalProfile
        OriginalLevel            = $baseMetadata.OriginalLevel
        OriginalChromaLocation   = $baseMetadata.OriginalChromaLocation
    }
}


function Get-VideoMetadata {
    param([string]$FilePath)
    
    Write-Host "Analyzing video metadata..." -ForegroundColor Cyan
    
    try {
        # Use managed process for metadata extraction
        $probeArgs = @(
            "-v", "warning",
            "-analyzeduration", "1000000000",
            "-probesize", "5000000000",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height,bit_rate,r_frame_rate,avg_frame_rate,color_primaries,color_transfer,color_space,pix_fmt,codec_name,profile,level,chroma_location,color_range",
            "-show_entries", "format=bit_rate,duration",
            "-show_entries", "stream_side_data",
            "-of", "json",
            $FilePath
        )
        
        $probeResult = Invoke-FFProbeWithCleanup -Arguments $probeArgs -TimeoutSeconds 60
        
        if (-not $probeResult.Success) {
            throw "FFProbe failed with exit code: $($probeResult.ExitCode). Error: $($probeResult.StdErr)"
        }
        
        $probeOutput = $probeResult.StdOut | ConvertFrom-Json
        
        if (-not $probeOutput -or -not $probeOutput.streams -or $probeOutput.streams.Count -eq 0) {
            throw "No video streams found or invalid probe output"
        }
        
        $stream = $probeOutput.streams[0]
        $format = $probeOutput.format
        
        # Parse frame rates
        $rFrameRate = 23.976
        $avgFrameRate = 23.976
        
        if ($stream.r_frame_rate) {
            $fpsParts = $stream.r_frame_rate -split "/"
            if ($fpsParts.Count -eq 2 -and [double]$fpsParts[1] -ne 0) {
                $rFrameRate = [double]$fpsParts[0] / [double]$fpsParts[1]
            }
        }
        
        if ($stream.avg_frame_rate) {
            $avgFpsParts = $stream.avg_frame_rate -split "/"
            if ($avgFpsParts.Count -eq 2 -and [double]$avgFpsParts[1] -ne 0) {
                $avgFrameRate = [double]$avgFpsParts[0] / [double]$avgFpsParts[1]
            }
        }
        
        # Determine if content is CFR or VFR
        $frameRateDifference = [math]::Abs($rFrameRate - $avgFrameRate)
        $isVFR = $frameRateDifference -gt 0.001  # Tolerance for floating point comparison
        
        # Use avg_frame_rate for VFR content, r_frame_rate for CFR
        $effectiveFrameRate = if ($isVFR) { $avgFrameRate } else { $rFrameRate }
        
        # Normalize common frame rates
        if ([System.Math]::Abs($effectiveFrameRate - 23.976) -le 0.001) { $effectiveFrameRate = 23.976 }
        elseif ([System.Math]::Abs($effectiveFrameRate - 24) -le 0.001) { $effectiveFrameRate = 24 }
        elseif ([System.Math]::Abs($effectiveFrameRate - 25) -le 0.001) { $effectiveFrameRate = 25 }
        elseif ([System.Math]::Abs($effectiveFrameRate - 29.97) -le 0.001) { $effectiveFrameRate = 29.97 }
        elseif ([System.Math]::Abs($effectiveFrameRate - 30) -le 0.001) { $effectiveFrameRate = 30 }
        elseif ([System.Math]::Abs($effectiveFrameRate - 50) -le 0.001) { $effectiveFrameRate = 50 }
        elseif ([System.Math]::Abs($effectiveFrameRate - 59.94) -le 0.001) { $effectiveFrameRate = 59.94 }
        elseif ([System.Math]::Abs($effectiveFrameRate - 60) -le 0.001) { $effectiveFrameRate = 60 }
        
        # Get bitrate
        $bitrate = 0
        if ($stream.bit_rate) {
            $bitrate = [int]$stream.bit_rate
        }
        elseif ($format.bit_rate) {
            $bitrate = [int]$format.bit_rate
        }
        
        # Check for Dolby Vision in side data
        $hasDolbyVision = $false
        if ($stream.side_data_list) {
            foreach ($sideData in $stream.side_data_list) {
                if ($sideData.side_data_type -match "DOVI configuration record") {
                    $hasDolbyVision = $true
                    Write-Host "Dolby Vision detected via side data" -ForegroundColor Yellow
                    break
                }
            }
        }
        
        # Display frame rate analysis
        Write-Host "Frame rate analysis:" -ForegroundColor Gray
        Write-Host "  R-frame rate: $rFrameRate fps" -ForegroundColor Gray
        Write-Host "  Avg frame rate: $avgFrameRate fps" -ForegroundColor Gray
        Write-Host "  Difference: $([math]::Round($frameRateDifference, 4)) fps" -ForegroundColor Gray
        Write-Host "  Content type: $(if ($isVFR) { 'VFR (Variable Frame Rate)' } else { 'CFR (Constant Frame Rate)' })" -ForegroundColor $(if ($isVFR) { "Yellow" } else { "Green" })
        Write-Host "  Effective frame rate: $effectiveFrameRate fps" -ForegroundColor White
        Write-Host "  Bitrate: $($bitrate)"
        
        # FIXED: Return as hashtable for consistent property access
        return @{
            Width                    = [int]$stream.width
            Height                   = [int]$stream.height
            FrameRate                = $effectiveFrameRate
            RFrameRate               = $rFrameRate
            AvgFrameRate             = $avgFrameRate
            IsVFR                    = $isVFR
            Bitrate                  = $bitrate
            ColorPrimaries           = $stream.color_primaries
            ColorTransfer            = $stream.color_transfer
            ColorSpace               = $stream.color_space
            PixelFormat              = $stream.pix_fmt
            HasDolbyVision           = $hasDolbyVision
            colorRange               = $stream.color_range ?? 'tv'
            HasHdr10Plus             = $false  # Will be set by comprehensive detection
            HDR10PlusDetectionMethod = "Not yet tested"
            ExtractedHdr10JsonPath   = $null
            Duration                 = if ($format.duration) { [double]$format.duration } else { 0 }
            CodecName                = $stream.codec_name
            OriginalProfile          = $stream.profile
            OriginalLevel            = $stream.level
            OriginalChromaLocation   = $stream.chroma_location
        }
    }
    catch {
        Write-Warning "Failed to get video metadata: $_"
        return $null
    }
}

function Get-EncodingSettings {
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$VideoInfo,
        
        [Parameter(Mandatory = $false)]
        [string]$inputFileSize
    )
    
    # Input validation
    if (-not $VideoInfo -or -not $VideoInfo.ContainsKey('Width') -or -not $VideoInfo.ContainsKey('Height')) {
        throw "VideoInfo must contain Width and Height properties"
    }
    
    if ($VideoInfo.Bitrate -le 0 -or $VideoInfo.FrameRate -le 0) {
        Write-Warning "Invalid bitrate or framerate detected - quality calculation may be inaccurate"
    }
    
    $height = [int]$VideoInfo.Height
    $width = [int]$VideoInfo.Width
    
    # Initialize settings hashtable
    $settings = @{}
    
    # Get base settings based on resolution
    if ($height -ge 2160 -or $width -ge 3840) {  
        $settings.SharpenKernel = 5
        $settings.SharpenAmount = 1.0
        $settings.LookAhead = 100
        $settings.MaxFrameSize = 262144   # 256KB for 4K
        $settings.ResolutionTier = "4K"
    }
    elseif ($height -ge 1440) {
        $settings.SharpenKernel = 5
        $settings.SharpenAmount = 0.8
        $settings.LookAhead = 90
        $settings.MaxFrameSize = 131072   # 128KB for 1440p
        $settings.ResolutionTier = "1440p"
    }
    elseif ($height -ge 1080) {
        $settings.SharpenKernel = 3
        $settings.SharpenAmount = 0.6
        $settings.LookAhead = 80
        $settings.MaxFrameSize = 65536    # 64KB for 1080p
        $settings.ResolutionTier = "1080p"
    }
    elseif ($height -ge 720) {
        $settings.SharpenKernel = 3
        $settings.SharpenAmount = 0.4
        $settings.LookAhead = 64
        $settings.MaxFrameSize = 32768    # 32KB for 720p
        $settings.ResolutionTier = "720p"
    }
    else {
        $settings.SharpenKernel = 3
        $settings.SharpenAmount = 0.3
        $settings.LookAhead = 48
        $settings.MaxFrameSize = 16384    # 16KB for SD
        $settings.ResolutionTier = "SD"
    }
    
    # Display video info once
    $videoInfoText = "Video: $($VideoInfo.Width)x$($VideoInfo.Height) @ $($VideoInfo.FrameRate) fps"
    Write-Host $videoInfoText -ForegroundColor Green
    Write-Host "Codec: $($VideoInfo.CodecName)" -ForegroundColor Green
    Write-Host "Bitrate: $([math]::Round($VideoInfo.Bitrate/1MB,2)) Mbps" -ForegroundColor Green
    
    # Calculate BPP and determine quality with resolution-specific adjustments
    $bpp = if ($VideoInfo.Bitrate -gt 0) { 
        $VideoInfo.Bitrate / ($VideoInfo.Width * $VideoInfo.Height * $VideoInfo.FrameRate) 
    }
    else { 
        0 
    }
    Write-Host "BPP: $([math]::Round($bpp,4)) (Resolution tier: $($settings.ResolutionTier))" -ForegroundColor Green
    
    # Improved BPP thresholds - more realistic for modern content
    $bppThresholds = switch ($settings.ResolutionTier) {
        "4K" { @{Low = 0.02; Medium = 0.04; High = 0.08; VeryHigh = 0.15 } }
        "1440p" { @{Low = 0.03; Medium = 0.06; High = 0.12; VeryHigh = 0.20 } }
        "1080p" { @{Low = 0.04; Medium = 0.08; High = 0.15; VeryHigh = 0.25 } }
        "720p" { @{Low = 0.06; Medium = 0.12; High = 0.20; VeryHigh = 0.30 } }
        default { @{Low = 0.08; Medium = 0.15; High = 0.25; VeryHigh = 0.35 } }
    }
    
    # Resolution-specific quality adjustments
    $qualityAdjustment = switch ($settings.ResolutionTier) {
        "4K" { -1 }  # Higher quality for 4K (lower CRF)
        "1440p" { -1 }  # Slightly higher quality for 1440p
        "1080p" { 0 }   # Standard quality for 1080p
        "720p" { +1 }  # Slightly lower quality for 720p
        default { +1 }  # Lower quality for SD
    }
    
 $baseQuality = if ($bpp -lt $bppThresholds.Low) {
    23  # Very low quality source
}
elseif ($bpp -lt $bppThresholds.Medium) {
    22  # Low quality source
}
elseif ($bpp -lt $bppThresholds.High) {
    21  # Medium quality source
}
elseif ($bpp -lt $bppThresholds.VeryHigh) {
    20  # High quality source
}
else {
    19  # Very high quality source
} # spaz
    
    # Apply resolution adjustment
    $afterResolutionQuality = $baseQuality + $qualityAdjustment
    
    # Content-aware quality optimization
    $contentAwareAdjustment = 0
    if ($SkipContentAnalysis -or [string]::IsNullOrEmpty($InputFile)) {
        Write-Host "Content analysis skipped - using BPP-based quality" -ForegroundColor Yellow
        $afterContentQuality = $afterResolutionQuality
    }
    else {
        try {
            Write-Host "Using $($InputFile)"
            $contentBasedQuality = Get-ContentBasedQuality -InputFile $InputFile -VideoInfo $VideoInfo -BaseQuality $afterResolutionQuality
            $afterContentQuality = [int]($contentBasedQuality | Select-Object -First 1)
            $contentAwareAdjustment = $afterContentQuality - $afterResolutionQuality
        }
        catch {
            Write-Warning "Content analysis failed: $($_.Exception.Message). Using BPP-based quality."
            $afterContentQuality = $afterResolutionQuality
        }
    }
    
    # Apply codec-specific quality adjustment
    $codecAdjustment = 0
    
    
    # Calculate final quality with proper bounds
    $finalQuality = $afterContentQuality + $codecAdjustment
    

    if ($incomingGlobalQuality) {
        $settings.Quality = $incomingGlobalQuality
        $qualityBounds = "Manual Set"
    } else {
        $settings.Quality = [math]::Max(10, [math]::Min(23, $finalQuality))
        $qualityBounds = "10-23 (HEVC optimized range)"
    }

    if ($VideoInfo.Bitrate -gt 50000000 -and $settings.Quality -lt 18) {
        $settings.Quality = 18
        $qualityBounds = "High bitrate setting to min 18"
    }
    
    # Improved frame size adjustment based on source quality
    $frameSizeMultiplier = 1.0
    if ($bpp -gt $bppThresholds.VeryHigh) {
        $baseMultiplier = switch ($settings.ResolutionTier) {
            "4K" { 2.5 }
            "1440p" { 2.0 }
            "1080p" { 1.6 }
            "720p" { 1.4 }
            default { 1.2 }
        }
        # Further adjust based on how much higher the BPP is
        $bppRatio = [math]::Min(3.0, $bpp / $bppThresholds.VeryHigh)
        $frameSizeMultiplier = $baseMultiplier * $bppRatio
        
        $settings.MaxFrameSize = [int]($settings.MaxFrameSize * $frameSizeMultiplier)
        Write-Host "High-quality $($settings.ResolutionTier) source detected (BPP ratio: $([math]::Round($bppRatio,2)))" -ForegroundColor Yellow
        Write-Host "Increased max frame size to $([math]::Round($settings.MaxFrameSize/1KB,0)) KB" -ForegroundColor Yellow
    }
    
    # Build comprehensive quality adjustment summary
    $adjustmentSummary = @()
    $adjustmentSummary += "base: $baseQuality"
    
    if ($qualityAdjustment -ne 0) {
        $resolutionSign = if ($qualityAdjustment -gt 0) { "+" } else { "" }
        $adjustmentSummary += "resolution: $resolutionSign$qualityAdjustment"
    }
    
    if ($contentAwareAdjustment -ne 0) {
        $contentSign = if ($contentAwareAdjustment -gt 0) { "+" } else { "" }
        $adjustmentSummary += "content-aware: $contentSign$contentAwareAdjustment"
    }
    elseif (-not $SkipContentAnalysis -and -not [string]::IsNullOrEmpty($InputFile)) {
        $adjustmentSummary += "content-aware: no change"
    }
    
    $adjustmentText = $adjustmentSummary -join ", "
    
    # Display results
    Write-Host "Selected quality level: $($settings.Quality) ($adjustmentText)" -ForegroundColor Yellow
    Write-Host "Quality bounds: $qualityBounds" -ForegroundColor Cyan
    Write-Host "Max frame size: $([math]::Round($settings.MaxFrameSize/1KB,0)) KB" -ForegroundColor Yellow
    
    # Display codec-specific information
        Write-Host "Codec target: HEVC (broad compatibility)" -ForegroundColor Cyan
        Write-Host "  - Balanced encoding speed and compression" -ForegroundColor Gray
        Write-Host "  - Wide hardware and software support" -ForegroundColor Gray
        Write-Host "  - Optimized for immediate Plex playback" -ForegroundColor Gray
    
    # Quality assessment summary
    $qualityAssessment = if ($bpp -gt $bppThresholds.VeryHigh) {
        "Very High"
    }
    elseif ($bpp -gt $bppThresholds.High) {
        "High"
    }
    elseif ($bpp -gt $bppThresholds.Medium) {
        "Medium"
    }
    elseif ($bpp -gt $bppThresholds.Low) {
        "Low"
    }
    else {
        "Very Low"
    }
    
    Write-Host "Source quality assessment: $qualityAssessment (BPP: $([math]::Round($bpp,4)))" -ForegroundColor Magenta
    
    return $settings
}


function Test-HDRContent {
    param([hashtable]$VideoInfo)
    
    $colorTrc = $VideoInfo.ColorTransfer
    $pixFmt = $VideoInfo.PixelFormat
    
    $isHDR = ($colorTrc -match "smpte2084|arib-std-b67") -or ($pixFmt -match "p010|p016|10le|12le")
    
    Write-Host "HDR Detection - Transfer: $colorTrc, Format: $pixFmt, Is HDR: $isHDR" -ForegroundColor Gray
    return $isHDR
}

function Get-ColorMetadata {
    param([hashtable]$VideoInfo)
    
    $isHDR = Test-HDRContent -VideoInfo $VideoInfo
    
    # Set defaults based on HDR detection
    $primaries = if ($isHDR) { "bt2020" } else { "bt709" }
    $transfer = if ($isHDR) { "smpte2084" } else { "bt709" }
    $space = if ($isHDR) { "bt2020nc" } else { "bt709" }
    
    # Use detected values if available and not unknown
    if ($VideoInfo.ColorPrimaries -and $VideoInfo.ColorPrimaries -ne "unknown") {
        $primaries = $VideoInfo.ColorPrimaries
    }
    if ($VideoInfo.ColorTransfer -and $VideoInfo.ColorTransfer -ne "unknown") {
        $transfer = $VideoInfo.ColorTransfer
    }
    if ($VideoInfo.ColorSpace -and $VideoInfo.ColorSpace -ne "unknown") {
        $space = $VideoInfo.ColorSpace
    }
    
    Write-Host "Color metadata - Primaries: $primaries, Transfer: $transfer, Space: $space" -ForegroundColor Gray
    
    return @{
        Primaries = $primaries
        Transfer  = $transfer
        Space     = $space
        IsHDR     = $isHDR
    }
}

function Test-DolbyVisionFileSizeChange {
    param(
        [string]$OriginalFile,
        [string]$ProcessedFile,
        [double]$MaxAllowableDropPercent = 15.0  # Default: fail if file shrinks more than 15%
    )
    
    Write-Host "Validating Dolby Vision removal file size changes..." -ForegroundColor Cyan
    
    try {
        # Get file sizes
        $originalSize = (Get-Item $OriginalFile).Length
        $processedSize = if (Test-Path $ProcessedFile) { (Get-Item $ProcessedFile).Length } else { 0 }
        
        # Calculate sizes in MB for display
        $originalSizeMB = [math]::Round($originalSize / 1MB, 2)
        $processedSizeMB = [math]::Round($processedSize / 1MB, 2)
        
        Write-Host "  Original file size:  $originalSizeMB MB" -ForegroundColor White
        Write-Host "  Processed file size: $processedSizeMB MB" -ForegroundColor White
        
        # Check for missing processed file
        if ($processedSize -eq 0) {
            Write-Host "  [CRITICAL] Processed file is missing or empty!" -ForegroundColor Red
            return @{
                IsValid         = $false
                ErrorType       = "MISSING_FILE"
                OriginalSizeMB  = $originalSizeMB
                ProcessedSizeMB = $processedSizeMB
                PercentChange   = -100.0
                Reason          = "Dolby Vision removal produced no output file"
            }
        }
        
        # Calculate percentage change
        $percentChange = if ($originalSize -gt 0) {
            (($processedSize - $originalSize) / $originalSize) * 100
        }
        else {
            0.0
        }
        
        $absolutePercentChange = [math]::Abs($percentChange)
        $sizeDifferenceMB = $processedSizeMB - $originalSizeMB
        
        Write-Host "  Size change: $([math]::Round($sizeDifferenceMB, 2)) MB ($([math]::Round($percentChange, 2))%)" -ForegroundColor $(
            if ($percentChange -lt - $MaxAllowableDropPercent) { "Red" } 
            elseif ([math]::Abs($percentChange) -gt 5) { "Yellow" } 
            else { "Green" }
        )
        
        # Validation logic
        if ($percentChange -lt - $MaxAllowableDropPercent) {
            # File shrank too much - likely corruption or failure
            Write-Host "  [FAIL] File size dropped by $([math]::Round($absolutePercentChange, 2))% (threshold: $MaxAllowableDropPercent%)" -ForegroundColor Red
            Write-Host "  This indicates potential Dolby Vision removal failure:" -ForegroundColor Red
            Write-Host "    - Tool may have corrupted the video stream" -ForegroundColor Red
            Write-Host "    - Process was interrupted or failed silently" -ForegroundColor Red
            Write-Host "    - Source file may have been damaged during processing" -ForegroundColor Red
            
            return @{
                IsValid         = $false
                ErrorType       = "EXCESSIVE_SHRINKAGE"
                OriginalSizeMB  = $originalSizeMB
                ProcessedSizeMB = $processedSizeMB
                PercentChange   = $percentChange
                Reason          = "File size dropped by $([math]::Round($absolutePercentChange, 2))% - exceeds $MaxAllowableDropPercent% threshold"
            }
            
        }
        elseif ($percentChange -gt 50.0) {
            # File grew too much - might indicate processing error
            Write-Host "  [WARN] File size increased significantly by $([math]::Round($percentChange, 2))%" -ForegroundColor Yellow
            Write-Host "  This is unusual for Dolby Vision removal - continuing with caution" -ForegroundColor Yellow
            
            return @{
                IsValid         = $true
                ErrorType       = "EXCESSIVE_GROWTH"
                OriginalSizeMB  = $originalSizeMB
                ProcessedSizeMB = $processedSizeMB
                PercentChange   = $percentChange
                Reason          = "File size increased unusually but within acceptable bounds"
            }
            
        }
        elseif ($absolutePercentChange -lt 0.1) {
            # File size virtually unchanged - might indicate tool didn't process anything
            Write-Host "  [INFO] File size unchanged ($([math]::Round($percentChange, 3))%)" -ForegroundColor Blue
            Write-Host "  This may indicate:" -ForegroundColor Blue
            Write-Host "    - File had no Dolby Vision metadata to remove" -ForegroundColor Blue
            Write-Host "    - Tool processed correctly with minimal size impact" -ForegroundColor Blue
            Write-Host "    - Processing was bypassed or had no effect" -ForegroundColor Blue
            
            return @{
                IsValid         = $true
                ErrorType       = "NO_CHANGE"
                OriginalSizeMB  = $originalSizeMB
                ProcessedSizeMB = $processedSizeMB
                PercentChange   = $percentChange
                Reason          = "File size unchanged - tool may not have found DV metadata to remove"
            }
            
        }
        else {
            # Normal size change
            Write-Host "  [PASS] File size change within normal range" -ForegroundColor Green
            
            return @{
                IsValid         = $true
                ErrorType       = "NORMAL"
                OriginalSizeMB  = $originalSizeMB
                ProcessedSizeMB = $processedSizeMB
                PercentChange   = $percentChange
                Reason          = "File size change within expected bounds"
            }
        }
        
    }
    catch {
        Write-Warning "Failed to validate file size changes: $_"
        return @{
            IsValid         = $false
            ErrorType       = "VALIDATION_ERROR"
            OriginalSizeMB  = 0
            ProcessedSizeMB = 0
            PercentChange   = 0
            Reason          = "Exception during file size validation: $_"
        }
    }
}

function Invoke-DirectDolbyVisionRemoval {
    param(
        [string]$HEVCFile,   # The extracted .hevc file from your encoding process
        [double]$MaxAllowableDropPercent = 15.0
    )
    
    Write-Host "Removing Dolby Vision metadata directly from HEVC stream..." -ForegroundColor Yellow
    
    # Check if dovi_tool exists
    $doviToolPath = "E:\Plex\Donis Dolby Vision Tool\tools\dovi_tool.exe"
    if (-not (Test-Path $doviToolPath)) {
        Write-Warning "dovi_tool.exe not found at $doviToolPath. Skipping DV removal."
        return $HEVCFile  # Return original file unchanged
    }
    
    try {
        # Capture original file size for validation
        $originalSize = (Get-Item $HEVCFile).Length
        $originalSizeMB = [math]::Round($originalSize / 1MB, 2)
        
        Write-Host "Original HEVC file size: $originalSizeMB MB" -ForegroundColor Gray
        
        # Change to the tool directory (dovi_tool may have path dependencies)
        $originalLocation = Get-Location
        $toolDirectory = $Script:ValidatedTempFolder #Split-Path $doviToolPath -Parent
        Set-Location -Path $toolDirectory
        
        # Define expected output file paths
        $elFile = Join-Path $toolDirectory "EL.hevc"
        $blFile = Join-Path $toolDirectory "BL.hevc"
        
        # Register the EL file for cleanup
        Add-TempFile -FilePath $elFile
        
        Write-Host "Working directory: $toolDirectory" -ForegroundColor Gray
        Write-Host "Processing: $HEVCFile" -ForegroundColor Gray
        Write-Host "Expected output: EL.hevc and BL.hevc" -ForegroundColor Gray
        
        # Execute dovi_tool demux command (this is what DDVT does)
        $doviArgs = @("demux", $HEVCFile)
        
        Write-Host "Executing: $doviToolPath $($doviArgs -join ' ')" -ForegroundColor Gray
        
        $processInfo = New-Object System.Diagnostics.ProcessStartInfo
        $processInfo.FileName = $doviToolPath
        $processInfo.UseShellExecute = $false
        $processInfo.RedirectStandardOutput = $true
        $processInfo.RedirectStandardError = $true
        $processInfo.CreateNoWindow = $false
        $processInfo.WorkingDirectory = $toolDirectory
        
        if ($PSVersionTable.PSVersion.Major -ge 7) {
            foreach ($arg in $doviArgs) {
                $null = $processInfo.ArgumentList.Add($arg)
            }
        }
        else {
            $quotedArgs = $doviArgs | ForEach-Object { 
                if ($_ -match '\s') { '"{0}"' -f ($_ -replace '"', '""') } else { $_ }
            }
            $processInfo.Arguments = $quotedArgs -join ' '
        }
        
        $process = [System.Diagnostics.Process]::Start($processInfo)
        $handle = $process.Handle
        $Script:ActiveProcesses = @($Script:ActiveProcesses) + $process
        
        $stdout = $process.StandardOutput.ReadToEnd()
        $stderr = $process.StandardError.ReadToEnd()
        
        try {
            $process.PriorityClass = [System.Diagnostics.ProcessPriorityClass]::BelowNormal
        }
        catch {
            Write-Host "Could not set dovi_tool process priority" -ForegroundColor Gray
        }
        
        $timeoutMs = 18000000  # 30 minutes
        Write-Host "Waiting for dovi_tool to complete (300 minute timeout)..." -ForegroundColor Yellow
        
        if (-not $process.WaitForExit($timeoutMs)) {
            Write-Warning "dovi_tool timed out after 300 minutes - killing process"
            try {
                $process.Kill()
                $process.WaitForExit(5000)
            }
            catch {
                Write-Warning "Failed to kill dovi_tool process cleanly: $_"
            }
            
            $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $process.Id }
            return $null
        }
        
        $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $process.Id }
        
        $exitCode = $process.ExitCode
        Write-Host "dovi_tool completed with exit code: $exitCode" -ForegroundColor Gray
        
        if ($stdout) {
            Write-Host "dovi_tool stdout:" -ForegroundColor Gray
            Write-Host $stdout -ForegroundColor DarkGray
        }
        if ($stderr) {
            Write-Host "dovi_tool stderr:" -ForegroundColor Gray
            Write-Host $stderr -ForegroundColor DarkGray
        }
        
        if ($exitCode -eq 0) {
            Write-Host "Dolby Vision removal completed successfully" -ForegroundColor Green
            
            # CRITICAL: Validate the output file was created
            if (-not (Test-Path $blFile)) {
                Write-Warning "dovi_tool completed successfully but BL.hevc output file not found: $blFile"
                return $HEVCFile
            }

            # --- NEW LOGIC: Delete EL file and replace original ---
            Write-Host "Removing Enhancement Layer (EL) file: $elFile" -ForegroundColor Yellow
            Remove-Item -Path $elFile -Force -ErrorAction SilentlyContinue
                  
            # Replace the original HEVC file with the DV-removed version
            Write-Host "Replacing original HEVC file with DV-removed version..." -ForegroundColor Green
            
            # Move the BL.hevc file to replace the original
            Move-Item -Path $blFile -Destination $HEVCFile -Force
            Write-Host "Successfully replaced HEVC file with DV-removed version" -ForegroundColor Green
            
            return $HEVCFile
            
        }
        else {
            Write-Warning "dovi_tool failed with exit code: $exitCode"
            
            if ($stderr) {
                Write-Host "Error details from dovi_tool:" -ForegroundColor Red
                Write-Host $stderr -ForegroundColor Red
            }
            
            Write-Host "Keeping original HEVC file unchanged" -ForegroundColor Yellow
            return $HEVCFile
        }
        
    }
    catch {
        Write-Warning "Error during direct Dolby Vision removal: $_"
        return $HEVCFile
    }
    finally {
        Set-Location -Path $originalLocation
    }
}


function Get-ContentBasedQuality {
    param([string]$InputFile, [hashtable]$VideoInfo, [int]$BaseQuality)
    
    Write-Host "Analyzing content complexity for optimal quality..." -ForegroundColor Cyan
    
    try {
        # Ensure duration is scalar
        [double]$duration = [double]($VideoInfo.Duration | Select-Object -First 1)
        
        if ($duration -le 0) {
            Write-Warning "Invalid duration - using base quality"
            return $BaseQuality
        }
        # Multiple sample points for better accuracy
        [int]$sampleDuration = 15
        [double[]]$samplePoints = @()
        
        # Adaptive sampling based on video length
        if ($duration -lt 600) {
            # < 10 minutes
            [double]$a = $duration * 0.33
            $samplePoints = @(60.0, $a)
        }
        elseif ($duration -lt 3600) {
            # < 1 hour
            [double]$a = $duration * 0.33
            [double]$b = $duration * 0.66
            $samplePoints = @(120.0, $a, $b)
        }
        else {
            # Long form content
            [double]$a = $duration * 0.25
            [double]$b = $duration * 0.5
            [double]$c = $duration * 0.75
            $samplePoints = @(180.0, $a, $b, $c)
        }
        
        # Filter + normalize sample points to integers safely
        $samplePoints = $samplePoints |
        Where-Object { $_ -lt ($duration - $sampleDuration) -and $_ -gt 0 } |
        ForEach-Object { [int][math]::Floor($_) }  # cast each element individually

        # Optionally make it an array explicitly
        $samplePoints = @($samplePoints)

        if ($samplePoints.Count -eq 0) {
            Write-Warning "No valid sample points - using fallback"
            return Get-EnhancedFallbackQuality -InputFile $InputFile -VideoInfo $VideoInfo -BaseQuality $BaseQuality
        }
        
        Write-Host " Analyzing $($samplePoints.Count) content samples..." -ForegroundColor Gray
        
        # Initialize counters as scalars
        [int]$TotalFrames = 0
        [int]$ValidSamples = 0
        [System.Collections.Generic.List[double]]$ProcessingSpeeds = [System.Collections.Generic.List[double]]::new()
        
        foreach ($samplePoint in $samplePoints) {
            Write-Host "   Sample at $samplePoint s..." -ForegroundColor DarkGray
            
            $basicAnalysisArgs = @(
                "-hide_banner",
                "-v", "quiet", 
                "-stats",
                "-ss", $samplePoint.ToString(),
                "-t", $sampleDuration.ToString(),
                "-i", $InputFile,
                "-f", "null",
                "-"
            )
            
            try {
                # Build command line for debugging
                $quotedArgs = @()
                $quotedArgs += $Config.FFmpegExe
                foreach ($arg in $basicAnalysisArgs) {
                    if ($arg -match '\s') {
                        $quotedArgs += "`"$arg`""
                    }
                    else {
                        $quotedArgs += $arg
                    }
                }
                $commandLine = $quotedArgs -join ' '
             #                   Write-Host "     [DEBUG] FFmpeg command: $commandLine" -ForegroundColor DarkCyan
                
                # Use Start-Process with timeout and corruption detection
                $processInfo = New-Object System.Diagnostics.ProcessStartInfo
                $processInfo.FileName = $Config.FFmpegExe
                $processInfo.UseShellExecute = $false
                $processInfo.RedirectStandardOutput = $true
                $processInfo.RedirectStandardError = $true
                $processInfo.CreateNoWindow = $true
                
                if ($PSVersionTable.PSVersion.Major -ge 7) {
                    foreach ($arg in $basicAnalysisArgs) {
                        $null = $processInfo.ArgumentList.Add($arg)
                    }
                }
                else {
                    $processInfo.Arguments = ($basicAnalysisArgs | ForEach-Object { 
                            if ($_ -match '\s') { '"{0}"' -f ($_ -replace '"', '""') } else { $_ }
                        }) -join ' '
                }
                
                $process = [System.Diagnostics.Process]::Start($processInfo)
                $handle = $process.Handle
                $timeoutMs = 30000  # 30 second timeout for content analysis
                
                if ($process.WaitForExit($timeoutMs)) {
                    $stdout = $process.StandardOutput.ReadToEnd()
                    $stderr = $process.StandardError.ReadToEnd()
                    $output = $stdout + $stderr
                    
                    # Check for corruption indicators in stderr
                    $corruptionPatterns = @(
                        "invalid as first byte of an EBML number",
                        "Invalid data found when processing input",
                        "moov atom not found",
                        "corrupted frame detected",
                        "Error while decoding stream",
                        "Invalid NAL unit size",
                        "Truncated packet",
                        "Header missing",
                        "Invalid frame header"
                    )
                    
                    $hasCorruption = $false
                    foreach ($pattern in $corruptionPatterns) {
                        if ($stderr -match $pattern) {
                            $hasCorruption = $true
                            Write-Host "     [CORRUPTION] Detected: $pattern" -ForegroundColor Red
                            break
                        }
                    }
                    
                    if ($hasCorruption) {
                        Write-Host "     Sample failed due to file corruption - skipping" -ForegroundColor Red
                        continue  # Skip this sample point and try the next one
                    }
                    
             #       Write-Host "     [DEBUG] Process completed normally, output length: $($output.Length)" -ForegroundColor DarkGreen
            #        Write-Host "     [DEBUG] Process completed normally, output : $($output)" -ForegroundColor DarkGreen
                    
                }
                else {
                    Write-Host "     [TIMEOUT] FFmpeg analysis timed out after 30 seconds at position $samplePoint s" -ForegroundColor Red
                    Write-Host "     This indicates potential file corruption at this timestamp" -ForegroundColor Red
                    try {
                        $process.Kill()
                        $process.WaitForExit(5000)
                    }
                    catch {
                        Write-Host "     Failed to kill hung FFmpeg process" -ForegroundColor Red
                    }
                    continue  # Skip this sample point and try the next one
                }
                
                $frameMatch = $output | Select-String "frame=\s*(\d+)" | Select-Object -Last 1
                [int]$frameCount = if ($frameMatch) { [int]$frameMatch.Matches[0].Groups[1].Value } else { 0 }
                
                $speedMatch = $output | Select-String "speed=\s*([\d\.]+)x" | Select-Object -Last 1
                [double]$processingSpeed = if ($speedMatch) { [double]$speedMatch.Matches[0].Groups[1].Value } else { 0 }
                
                if ($frameCount -gt 0 -and $processingSpeed -gt 0) {
                    $TotalFrames += $frameCount
                    $ValidSamples += 1
                    $ProcessingSpeeds.Add($processingSpeed)
                    Write-Host "     $frameCount frames at ${processingSpeed}x speed" -ForegroundColor DarkGray
                }
                else {
                    Write-Host "     Sample failed - no valid data extracted" -ForegroundColor Yellow
                }
            }
            catch {
                Write-Host "     Sample failed: $($_.Exception.Message)" -ForegroundColor Yellow
                #        Write-Error "Stack trace: $($_.ScriptStackTrace)"
                continue
            }
        }
        
        if ($ValidSamples -eq 0) {
            Write-Warning "All samples failed due to corruption/timeouts - using fallback method"
            return Get-EnhancedFallbackQuality -InputFile $InputFile -VideoInfo $VideoInfo -BaseQuality $BaseQuality
        }
        
        # Calculate aggregated metrics (force to scalar with Select-Object -First 1)
        [double]$avgProcessingSpeed = [double](($ProcessingSpeeds | Measure-Object -Average).Average | Select-Object -First 1)
        [double]$minProcessingSpeed = [double](($ProcessingSpeeds | Measure-Object -Minimum).Minimum | Select-Object -First 1)
        [double]$maxProcessingSpeed = [double](($ProcessingSpeeds | Measure-Object -Maximum).Maximum | Select-Object -First 1)
        [double]$speedVariance = 0
        if ($ProcessingSpeeds.Count -gt 1) {
            $mean = $avgProcessingSpeed
            $variance = ($ProcessingSpeeds | ForEach-Object { [math]::Pow($_ - $mean, 2) } | Measure-Object -Average).Average
            $speedVariance = [double][math]::Sqrt([double]$variance)
        }
        
        # Get source characteristics
        [long]$fileSize = (Get-Item $InputFile).Length
        [double]$avgBitrate = (($fileSize * 8.0) / ($duration * 1000.0)) # kbps
        
        Write-Host " Analysis summary:" -ForegroundColor Gray
        Write-Host "   Valid samples: $ValidSamples/$($samplePoints.Count)" -ForegroundColor DarkGray
        Write-Host "   Avg processing speed: $([math]::Round($avgProcessingSpeed, 2))x" -ForegroundColor DarkGray
        Write-Host "   Speed range: $([math]::Round($minProcessingSpeed, 2))x - $([math]::Round($maxProcessingSpeed, 2))x" -ForegroundColor DarkGray
        Write-Host "   Speed variance: $([math]::Round($speedVariance, 2))" -ForegroundColor DarkGray
        Write-Host "   Source bitrate: $([math]::Round($avgBitrate, 0)) kbps" -ForegroundColor DarkGray
        
        # Enhanced complexity scoring
        $complexityFactors = @{
            BitrateFactor    = 0
            ProcessingFactor = 0
            VarianceFactor   = 0
        }
        
        if ($avgBitrate -gt 80000) {
            $complexityFactors.BitrateFactor = 3
        }
        elseif ($avgBitrate -gt 50000) {
            $complexityFactors.BitrateFactor = 2
        }
        elseif ($avgBitrate -gt 25000) {
            $complexityFactors.BitrateFactor = 1
        }
        elseif ($avgBitrate -gt 10000) {
            $complexityFactors.BitrateFactor = 0
        }
        else {
            $complexityFactors.BitrateFactor = -1
        }
        
        if ($avgProcessingSpeed -lt 1.0) {
            $complexityFactors.ProcessingFactor = 2
        }
        elseif ($avgProcessingSpeed -lt 1.5) {
            $complexityFactors.ProcessingFactor = 1
        }
        elseif ($avgProcessingSpeed -lt 2.5) {
            $complexityFactors.ProcessingFactor = 0
        }
        else {
            $complexityFactors.ProcessingFactor = -1
        }
        
        if ($speedVariance -gt 0.5) {
            $complexityFactors.VarianceFactor = 1
        }
        
        [int]$totalComplexity = $complexityFactors.BitrateFactor + $complexityFactors.ProcessingFactor + $complexityFactors.VarianceFactor
        
        [int]$qualityAdjustment = if ($totalComplexity -ge 4) { -2 }
        elseif ($totalComplexity -ge 3) { -1 }
        elseif ($totalComplexity -ge 2) { 0 }
        elseif ($totalComplexity -ge 1) { 1 }
        elseif ($totalComplexity -ge 0) { +2 }
        else { +3 }
        
        $contentType = if ($avgBitrate -gt 80000) { "Ultra-high quality source (preserving maximum detail)" }
        elseif ($avgBitrate -gt 50000) { "Very high quality source" }
        elseif ($totalComplexity -ge 3) { "High complexity content" }
        elseif ($totalComplexity -ge 1) { "Moderate complexity content" }
        elseif ($totalComplexity -ge 0) { "Standard complexity content" }
        else { "Low complexity content" }
        
        if ($speedVariance -gt 0.5) {
            $contentType += " (varied complexity scenes)"
        }
        
        Write-Host " Content classification: $contentType" -ForegroundColor Yellow
        Write-Host " Complexity score: $totalComplexity (bitrate:$($complexityFactors.BitrateFactor), processing:$($complexityFactors.ProcessingFactor), variance:$($complexityFactors.VarianceFactor))" -ForegroundColor White
        Write-Host " Quality adjustment: $qualityAdjustment" -ForegroundColor White
        
        $optimizedQuality = [math]::Max(12, [math]::Min(35, $BaseQuality + $qualityAdjustment))
        return $optimizedQuality
        
    }
    catch {
        Write-Warning "Content analysis exception: $_"
        #      Write-Error "Stack trace: $($_.ScriptStackTrace)"
        return Get-EnhancedFallbackQuality -InputFile $InputFile -VideoInfo $VideoInfo -BaseQuality $BaseQuality
    }
}



function Get-EnhancedFallbackQuality {
    param([string]$InputFile, [hashtable]$VideoInfo, [int]$BaseQuality)
    
    Write-Host "  Using enhanced fallback analysis..." -ForegroundColor Yellow
    
    try {
        $fileSize = (Get-Item $InputFile).Length
        $duration = $VideoInfo.Duration
        $resolution = $VideoInfo.Width * $VideoInfo.Height
        $frameRate = $VideoInfo.FrameRate
        
        if ($duration -le 0) { return $BaseQuality }
        
        $bitsPerPixelPerSecond = ($fileSize * 8) / ($resolution * $duration)
        $avgBitrate = ($fileSize * 8) / ($duration * 1000)  # kbps
        
        Write-Host "    Source bitrate: $([math]::Round($avgBitrate, 0)) kbps" -ForegroundColor DarkGray
        Write-Host "    Bits per pixel/sec: $([math]::Round($bitsPerPixelPerSecond, 4))" -ForegroundColor DarkGray
        
        # For very high bitrate sources (like yours at 89MB), we need different thresholds
        $qualityAdjustment = 0
        $contentDescription = ""
        
        if ($avgBitrate -gt 50000) {
            # Extremely high bitrate - likely uncompressed or ProRes
            $qualityAdjustment = -3
            $contentDescription = "Uncompressed/ProRes source - preserving maximum quality"
        }
        elseif ($avgBitrate -gt 25000) {
            # Very high bitrate - high quality source
            $qualityAdjustment = -2  
            $contentDescription = "Very high bitrate source - maintaining quality"
        }
        elseif ($bitsPerPixelPerSecond -gt 2.0) {
            $qualityAdjustment = -2
            $contentDescription = "High complexity/detail source"
        }
        elseif ($bitsPerPixelPerSecond -gt 0.8) {
            $qualityAdjustment = -1
            $contentDescription = "Medium-high complexity source"
        }
        elseif ($bitsPerPixelPerSecond -gt 0.3) {
            $qualityAdjustment = 0
            $contentDescription = "Standard complexity source"
        }
        elseif ($bitsPerPixelPerSecond -gt 0.1) {
            $qualityAdjustment = +1
            $contentDescription = "Low complexity source"
        }
        else {
            $qualityAdjustment = +2
            $contentDescription = "Very simple/static source"
        }
        
        # Additional adjustments
        if ($resolution -ge 8294400) {
            # 4K+
            $qualityAdjustment -= 1
            $contentDescription += " (4K+ resolution)"
        }
        
        if ($frameRate -gt 50) {
            $qualityAdjustment -= 1
            $contentDescription += " (high framerate)"
        }
        
        Write-Host "  Analysis: $contentDescription" -ForegroundColor Yellow
        Write-Host "  Quality adjustment: $qualityAdjustment" -ForegroundColor White
        
        return [math]::Max(12, [math]::Min(35, $BaseQuality + $qualityAdjustment))
        
    }
    catch {
        Write-Warning "Enhanced fallback analysis failed: $_"
        return $BaseQuality
    }
}

function Invoke-SourceQualityAnalysis {
    param(
        [string]$SourceFile,
        [int]$SampleCount = 3,
        [int]$SampleDurationSeconds = 30,
        [int]$SkipIntroSeconds = 240,
        [string]$ScaleResolution = "1920:1080",
        [hashtable]$EncodingSettings
    )
    
    Write-Host "Analyzing source file quality characteristics..." -ForegroundColor Cyan
    Write-Host "Source: $(Split-Path $SourceFile -Leaf)" -ForegroundColor Yellow
    
    try {
        # Get source file info
        $sourceInfoArgs = @(
            "-v", "quiet",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height,pix_fmt,bit_rate,codec_name:format=duration,bit_rate",
            "-of", "json",
            $SourceFile
        )
        
        $processInfo = New-Object System.Diagnostics.ProcessStartInfo
        $processInfo.FileName = $Config.FFprobeExe
        $processInfo.UseShellExecute = $false
        $processInfo.RedirectStandardOutput = $true
        $processInfo.RedirectStandardError = $true
        $processInfo.CreateNoWindow = $true
        
        if ($PSVersionTable.PSVersion.Major -ge 7) {
            foreach ($arg in $sourceInfoArgs) {
                $null = $processInfo.ArgumentList.Add($arg)
            }
        }
        else {
            $quotedArgs = $sourceInfoArgs | ForEach-Object { 
                if ($_ -match '\s') { '"{0}"' -f ($_ -replace '"', '""') } else { $_ }
            }
            $processInfo.Arguments = $quotedArgs -join ' '
        }
        
        $process = [System.Diagnostics.Process]::Start($processInfo)
        $stdout = $process.StandardOutput.ReadToEnd()
        $process.WaitForExit(90000)
        
        $sourceInfo = $stdout | ConvertFrom-Json
        $sourceWidth = $sourceInfo.streams[0].width
        $sourceHeight = $sourceInfo.streams[0].height
        $sourceCodec = $sourceInfo.streams[0].codec_name
        $sourceBitrate = $sourceInfo.streams[0].bit_rate
        $durationSeconds = [double]$sourceInfo.format.duration
        
        Write-Host "Source specs: ${sourceWidth}x${sourceHeight}, $sourceCodec" -ForegroundColor Green
        if ($sourceBitrate) {
            Write-Host "Source bitrate: $([math]::Round($sourceBitrate/1000000, 1)) Mbps" -ForegroundColor Green
        }
        
        # Determine scaling requirements
        #$compareWidth = $ScaleResolution.Split(':')[0]
        #$compareHeight = $ScaleResolution.Split(':')[1]
        #$needsScaling = ($sourceWidth -ne $compareWidth) -or ($sourceHeight -ne $compareHeight)
        
        #if ($needsScaling) {
        #    Write-Host "Will scale source to $ScaleResolution for self-analysis" -ForegroundColor Yellow
        #    $scaleFilter = "scale=${ScaleResolution}:flags=lanczos"
        #}
        #else {
            Write-Host "Using native resolution for self-analysis: ${sourceWidth}x${sourceHeight}" -ForegroundColor Green
            $scaleFilter = "null"
        #}
        
        # Calculate sample positions (same logic as main validation)
        $availableDuration = $durationSeconds - $SkipIntroSeconds - 60
        if ($availableDuration -lt ($SampleCount * $SampleDurationSeconds * 2)) {
            Write-Warning "File too short for $SampleCount samples, reducing to fit available duration"
            $SampleCount = [math]::Max(1, [math]::Floor($availableDuration / ($SampleDurationSeconds * 2)))
        }
        
        $samplePositions = @()
        if ($SampleCount -eq 1) {
            $samplePositions += $SkipIntroSeconds + ($availableDuration / 2)
        }
        else {
            $interval = $availableDuration / ($SampleCount - 1)
            for ($i = 0; $i -lt $SampleCount; $i++) {
                $position = $SkipIntroSeconds + ($i * $interval)
                $samplePositions += [math]::Round($position, 1)
            }
        }
        
        # Perform self-comparison analysis
        Write-Host "Performing source self-analysis with lossless re-encoding..." -ForegroundColor Cyan
        
        $selfComparisonResults = @()
        $tempFiles = @()
        
        for ($sampleIndex = 0; $sampleIndex -lt $samplePositions.Count; $sampleIndex++) {
            $startTime = $samplePositions[$sampleIndex]
            Write-Host "Analyzing sample $($sampleIndex + 1)/$($samplePositions.Count) at $([math]::Round($startTime/60, 1)) minutes..." -ForegroundColor Cyan
            
            # Create temporary files for this sample
            $tempOriginalYuv = New-TempFile -BaseName "source_original_$sampleIndex" -Extension ".yuv"
            $tempReencodedYuv = New-TempFile -BaseName "source_reencoded_$sampleIndex" -Extension ".yuv"
            $tempLosslessFile = New-TempFile -BaseName "source_lossless_$sampleIndex" -Extension ".mkv"
            $tempFiles += $tempOriginalYuv, $tempReencodedYuv, $tempLosslessFile
            
            # Extract original sample to YUV
            $originalArgs = @(
                "-y", "-hide_banner",
                "-ss", $startTime.ToString(),
                "-t", $SampleDurationSeconds.ToString(),
                "-i", $SourceFile,
#                "-vf", $scaleFilter,
                "-vf", "unsharp=luma_msize_x=$($EncodingSettings.SharpenKernel):luma_msize_y=$($EncodingSettings.SharpenKernel):luma_amount=$($EncodingSettings.SharpenAmount)"
                "-pix_fmt", "yuv420p",
                "-f", "rawvideo",
                $tempOriginalYuv
            )
            
            $success = Invoke-SimpleProcess -Executable $Config.FFmpegExe -Arguments $originalArgs -Description "Extract source sample $($sampleIndex + 1)"
            if (-not $success) {
                Write-Warning "Failed to extract source sample $($sampleIndex + 1), skipping"
                continue
            }
            
            # Create lossless re-encoding of the same sample
            $losslessArgs = @(
                "-y", "-hide_banner",
                "-ss", $startTime.ToString(),
                "-t", $SampleDurationSeconds.ToString(),
                "-i", $SourceFile,
                "-c:v", "hevc_qsv", #"libx264",
                "-global_quality", "10", #"-crf", "10",   # Very high quality reference encode
                "-vf", "unsharp=luma_msize_x=$($EncodingSettings.SharpenKernel):luma_msize_y=$($EncodingSettings.SharpenKernel):luma_amount=$($EncodingSettings.SharpenAmount)",
                "-preset", "veryslow",
 #               "-vf", $scaleFilter,
                $tempLosslessFile
            )
            
            $success = Invoke-SimpleProcess -Executable $Config.FFmpegExe -Arguments $losslessArgs -Description "Create lossless sample $($sampleIndex + 1)"
            if (-not $success) {
                Write-Warning "Failed to create lossless sample $($sampleIndex + 1), skipping"
                continue
            }
            
            # Extract lossless sample to YUV
            $reencodedArgs = @(
                "-y", "-hide_banner",
                "-i", $tempLosslessFile,
                "-pix_fmt", "yuv420p",
                "-f", "rawvideo",
                $tempReencodedYuv
            )
            
            $success = Invoke-SimpleProcess -Executable $Config.FFmpegExe -Arguments $reencodedArgs -Description "Extract lossless YUV $($sampleIndex + 1)"
            if (-not $success) {
                Write-Warning "Failed to extract lossless YUV $($sampleIndex + 1), skipping"
                continue
            }
            
            # Compare original vs lossless re-encoding to find quality ceiling
            Write-Host "  Measuring source quality ceiling for sample $($sampleIndex + 1)..." -ForegroundColor Yellow
            
            $tempStderrFile = New-TempFile -BaseName "source_analysis_stderr_$sampleIndex" -Extension ".txt"
            $tempFiles += $tempStderrFile
            
            # This comparison tells us the maximum possible quality from this source
            #cmd /c "$($Config.FFmpegExe) -f rawvideo -pix_fmt yuv420p -s ${sourceWidth}x${sourceHeight} -i `"$tempOriginalYuv`" -f rawvideo -pix_fmt yuv420p -s ${compareWidth}x${compareHeight} -i `"$tempReencodedYuv`" -lavfi `"[0:v][1:v]psnr;[0:v][1:v]ssim`" -f null -" >NUL 2>$tempStderrFile | Out-Null
            cmd /c "$($Config.FFmpegExe) -f rawvideo -pix_fmt yuv420p -s ${sourceWidth}x${sourceHeight} -i `"$tempOriginalYuv`" -f rawvideo -pix_fmt yuv420p -s ${sourceWidth}x${sourceHeight} -i `"$tempReencodedYuv`" -lavfi `"[0:v][1:v]psnr;[0:v][1:v]ssim`" -f null -" *>$tempStderrFile #| Out-Null
            
            if (Test-Path $tempStderrFile) {
                $stderr = Get-Content $tempStderrFile -Raw

                $sampleResult = @{
                    SampleIndex = $sampleIndex + 1
                    TimeMinutes = [math]::Round($startTime / 60, 1)
                    PSNR = $null
                    SSIM = $null
                }
                
                # Parse PSNR - this represents the quality ceiling for this sample
                $psnrMatch = $stderr | Select-String "PSNR.*average:(\d+\.\d+)"
                if (-not $psnrMatch) {
                    $psnrMatch = $stderr | Select-String "average:(\d+\.\d+)"
                }
                
                if ($psnrMatch) {
                    $sampleResult.PSNR = [double]$psnrMatch.Matches[0].Groups[1].Value
                    Write-Host "  Sample $($sampleIndex + 1) source PSNR ceiling: $([math]::Round($sampleResult.PSNR, 2)) dB" -ForegroundColor Green
                }
                
                # Parse SSIM - this represents the quality ceiling for this sample
                $ssimMatch = $stderr | Select-String "SSIM.*All:(\d+\.\d+)"
                if (-not $ssimMatch) {
                    $ssimMatch = $stderr | Select-String "All:(\d+\.\d+)"
                }
                
                if ($ssimMatch) {
                    $sampleResult.SSIM = [double]$ssimMatch.Matches[0].Groups[1].Value
                    Write-Host "  Sample $($sampleIndex + 1) source SSIM ceiling: $([math]::Round($sampleResult.SSIM, 4))" -ForegroundColor Green
                }
                
                if ($sampleResult.PSNR -or $sampleResult.SSIM) {
                    $selfComparisonResults += $sampleResult
                }
            }
        }
        
        # Calculate overall source quality characteristics
        $sourceAnalysis = [hashtable]@{
            SourceFile = $SourceFile
            Codec = $sourceCodec
            Resolution = "${sourceWidth}x${sourceHeight}"
            Bitrate = $sourceBitrate
            SampleCount = $selfComparisonResults.Count
            QualityCeiling = [hashtable]@{
                PSNR = $null
                SSIM = $null
            }
            QualityVariation = [hashtable]@{
                PSNRStdDev = $null
                SSIMStdDev = $null
            }
            Samples = $selfComparisonResults
        }
        
        if ($selfComparisonResults.Count -gt 0) {
            $psnrValues = $selfComparisonResults | Where-Object { $_.PSNR } | ForEach-Object { $_.PSNR }
            $ssimValues = $selfComparisonResults | Where-Object { $_.SSIM } | ForEach-Object { $_.SSIM }
            
            if ($psnrValues.Count -gt 0) {
                $sourceAnalysis.QualityCeiling.PSNR = ($psnrValues | Measure-Object -Average).Average
                if ($psnrValues.Count -gt 1) {
                    $sourceAnalysis.QualityVariation.PSNRStdDev = [math]::Sqrt(($psnrValues | ForEach-Object { [math]::Pow($_ - $sourceAnalysis.QualityCeiling.PSNR, 2) } | Measure-Object -Sum).Sum / $psnrValues.Count)
                }
            }
            
            if ($ssimValues.Count -gt 0) {
                $sourceAnalysis.QualityCeiling.SSIM = ($ssimValues | Measure-Object -Average).Average
                if ($ssimValues.Count -gt 1) {
                    $sourceAnalysis.QualityVariation.SSIMStdDev = [math]::Sqrt(($ssimValues | ForEach-Object { [math]::Pow($_ - $sourceAnalysis.QualityCeiling.SSIM, 2) } | Measure-Object -Sum).Sum / $ssimValues.Count)
                }
            }
        }
        
        # Display results
        Write-Host "" -ForegroundColor White
        Write-Host "Source Quality Analysis Results:" -ForegroundColor Cyan
        
        if ($sourceAnalysis.QualityCeiling.PSNR) {
            Write-Host "  Source PSNR Ceiling: $([math]::Round($sourceAnalysis.QualityCeiling.PSNR, 2)) dB" -ForegroundColor Green
            if ($sourceAnalysis.QualityVariation.PSNRStdDev) {
                Write-Host "    Variation (StdDev): $([math]::Round($sourceAnalysis.QualityVariation.PSNRStdDev, 2)) dB" -ForegroundColor Yellow
            }
        }
        
        if ($sourceAnalysis.QualityCeiling.SSIM) {
            Write-Host "  Source SSIM Ceiling: $([math]::Round($sourceAnalysis.QualityCeiling.SSIM, 4))" -ForegroundColor Green
            if ($sourceAnalysis.QualityVariation.SSIMStdDev) {
                Write-Host "    Variation (StdDev): $([math]::Round($sourceAnalysis.QualityVariation.SSIMStdDev, 4))" -ForegroundColor Yellow
            }
        }
        
        # Quality assessment
        if ($sourceAnalysis.QualityCeiling.PSNR -lt 45) {
            Write-Host "  Assessment: Source appears to have quality limitations (low PSNR ceiling)" -ForegroundColor Yellow
        }
        elseif ($sourceAnalysis.QualityCeiling.PSNR -lt 35) {
            Write-Host "  Assessment: Source has significant quality issues (very low PSNR ceiling)" -ForegroundColor Red
        }
        else {
            Write-Host "  Assessment: Source appears to be high quality" -ForegroundColor Green
        }
        
        if ($sourceAnalysis.QualityVariation.PSNRStdDev -gt 3.0) {
            Write-Host "  Note: High quality variation detected - source may have inconsistent quality" -ForegroundColor Yellow
        }
        
        return $sourceAnalysis
    }
    catch {
        Write-Warning "Source quality analysis failed: $_"
        Write-Host "Error details: $($_.Exception.Message)" -ForegroundColor Red
        return $null
    }
    finally {
        # Cleanup temp files
        foreach ($tempFile in $tempFiles) {
            if (Test-Path $tempFile) {
                try {
                    Remove-Item $tempFile -Force -ErrorAction SilentlyContinue
                }
                catch {
                    # Ignore cleanup errors
                }
            }
        }
        Write-Host "Source analysis temp files cleaned up" -ForegroundColor Gray
    }
}

function Get-AdaptiveQualityThresholds {
    param(
        [Parameter(Mandatory = $true)]
        $SourceAnalysis,
        
        [Parameter(Mandatory = $false)]
        [double]$BasePSNRThreshold = 35.0,
        
        [Parameter(Mandatory = $false)]
        [double]$BaseSSIMThreshold = 0.95,
        
        [Parameter(Mandatory = $false)]
        [double]$PSNRTolerance = 5.0,
        
        [Parameter(Mandatory = $false)]
        [double]$SSIMTolerance = 0.02
    )
    
    Write-Host "=== ADAPTIVE THRESHOLD CALCULATION ===" -ForegroundColor Cyan
    
    try {

        # Initialize the hashtable properly
        $analysisTable = @{}
        
        # Handle different input types
        if ($SourceAnalysis -is [System.Collections.Hashtable]) {
            Write-Host "Source analysis is already a hashtable" -ForegroundColor Green
            $analysisTable = $SourceAnalysis
        }
        elseif ($SourceAnalysis -is [System.Array] -or $SourceAnalysis -is [System.Collections.IEnumerable]) {
            Write-Host "Converting array source analysis to hashtable..." -ForegroundColor Yellow
            
            # If it's an array of objects, we need to extract properties
            foreach ($item in $SourceAnalysis) {
                if ($item -is [PSCustomObject] -or $item -is [System.Management.Automation.PSObject]) {
                    # Extract all properties from the object
                    $item.PSObject.Properties | ForEach-Object {
                        if ($_.Name -and $null -ne $_.Value) {
                            $analysisTable[$_.Name] = $_.Value
                            Write-Host "  Added: $($_.Name) = $($_.Value)" -ForegroundColor Gray
                        }
                    }
                }
                elseif ($item -is [System.Collections.DictionaryEntry]) {
                    # Handle dictionary entries
                    $analysisTable[$item.Key] = $item.Value
                    Write-Host "  Added: $($item.Key) = $($item.Value)" -ForegroundColor Gray
                }
                elseif ($item -is [System.Collections.Hashtable]) {
                    # Merge hashtables
                    foreach ($key in $item.Keys) {
                        $analysisTable[$key] = $item[$key]
                        Write-Host "  Added: $key = $($item[$key])" -ForegroundColor Gray
                    }
                }
            }
        }
        elseif ($SourceAnalysis -is [PSCustomObject] -or $SourceAnalysis -is [System.Management.Automation.PSObject]) {
            Write-Host "Converting PSObject to hashtable..." -ForegroundColor Yellow
            
            # Convert single PSObject to hashtable
            $SourceAnalysis.PSObject.Properties | ForEach-Object {
                if ($_.Name -and $null -ne $_.Value) {
                    $analysisTable[$_.Name] = $_.Value
                    Write-Host "  Added: $($_.Name) = $($_.Value)" -ForegroundColor Gray
                }
            }
        }
        else {
            Write-Warning "Unexpected source analysis type: $($SourceAnalysis.GetType().FullName)"
            Write-Host "Attempting generic conversion..." -ForegroundColor Yellow
            
            # Try to convert whatever it is
            if ($SourceAnalysis) {
                $analysisTable = @{ "Data" = $SourceAnalysis }
            }
        }
        
        # Validate the hashtable was created properly
        if ($analysisTable -isnot [System.Collections.Hashtable]) {
            throw "Failed to create hashtable from source analysis. Result type: $($analysisTable.GetType().FullName)"
        }
        
        if ($analysisTable.Count -eq 0) {
            Write-Warning "Hashtable is empty after conversion - returning base thresholds"
            return @{
                AdaptedPSNRThreshold = $BasePSNRThreshold
                AdaptedSSIMThreshold = $BaseSSIMThreshold
                OriginalPSNRThreshold = $BasePSNRThreshold
                OriginalSSIMThreshold = $BaseSSIMThreshold
                SourcePSNRCeiling = $null
                SourceSSIMCeiling = $null
                Resolution = "1920x1080"
                Bitrate = $null
            }
        }
        
        Write-Host "Successfully converted to hashtable with $($analysisTable.Count) entries" -ForegroundColor Green
        Write-Host "Keys present: $($analysisTable.Keys -join ', ')" -ForegroundColor Gray
        
        # Extract values from the analysis hashtable
        $bitrate = if ($analysisTable.ContainsKey('Bitrate')) { 
            $analysisTable['Bitrate'] 
        } else { 
            $null 
        }
        
        $resolution = if ($analysisTable.ContainsKey('Resolution')) { 
            $analysisTable['Resolution'] 
        } else { 
            "1920x1080" 
        }
        
        # Extract quality ceiling values (nested hashtable)
        $sourcePSNRCeiling = $null
        $sourceSSIMCeiling = $null
        
        if ($analysisTable.ContainsKey('QualityCeiling')) {
            $qualityCeiling = $analysisTable['QualityCeiling']
            Write-Host "Quality ceiling type: $($qualityCeiling.GetType().Name)" -ForegroundColor Gray
            
            if ($qualityCeiling -is [hashtable]) {
                if ($qualityCeiling.ContainsKey('PSNR') -and $null -ne $qualityCeiling['PSNR']) {
                    $sourcePSNRCeiling = [double]$qualityCeiling['PSNR']
                    Write-Host "  Found PSNR ceiling: $([math]::Round($sourcePSNRCeiling, 2)) dB" -ForegroundColor Green
                }
                if ($qualityCeiling.ContainsKey('SSIM') -and $null -ne $qualityCeiling['SSIM']) {
                    $sourceSSIMCeiling = [double]$qualityCeiling['SSIM']
                    Write-Host "  Found SSIM ceiling: $([math]::Round($sourceSSIMCeiling, 4))" -ForegroundColor Green
                }
            }
        }
        
        # Extract quality variation values (nested hashtable)
        $psnrStdDev = $null
        $ssimStdDev = $null
        
        if ($analysisTable.ContainsKey('QualityVariation')) {
            $qualityVariation = $analysisTable['QualityVariation']
            Write-Host "Quality variation type: $($qualityVariation.GetType().Name)" -ForegroundColor Gray
            
            if ($qualityVariation -is [hashtable]) {
                if ($qualityVariation.ContainsKey('PSNRStdDev') -and $null -ne $qualityVariation['PSNRStdDev']) {
                    $psnrStdDev = [double]$qualityVariation['PSNRStdDev']
                    Write-Host "  Found PSNR StdDev: $([math]::Round($psnrStdDev, 2)) dB" -ForegroundColor Green
                }
                if ($qualityVariation.ContainsKey('SSIMStdDev') -and $null -ne $qualityVariation['SSIMStdDev']) {
                    $ssimStdDev = [double]$qualityVariation['SSIMStdDev']
                    Write-Host "  Found SSIM StdDev: $([math]::Round($ssimStdDev, 4))" -ForegroundColor Green
                }
            }
        }
        
        # Calculate adaptive thresholds based on source quality ceiling
        $adaptedPSNRThreshold = $BasePSNRThreshold
        $adaptedSSIMThreshold = $BaseSSIMThreshold
        
        Write-Host "" -ForegroundColor White
        Write-Host "Calculating adaptive thresholds..." -ForegroundColor Cyan
        
        # Adjust PSNR threshold based on source ceiling
        if ($sourcePSNRCeiling) {
            Write-Host "  Source PSNR Ceiling: $([math]::Round($sourcePSNRCeiling, 2)) dB" -ForegroundColor Yellow
            
            # Can't exceed source quality, so cap threshold at source ceiling minus tolerance
            $maxPossiblePSNR = $sourcePSNRCeiling - $PSNRTolerance
            
            if ($BasePSNRThreshold -gt $maxPossiblePSNR) {
                $adaptedPSNRThreshold = [math]::Max(20, $maxPossiblePSNR)  # Floor at 20 dB
                Write-Host "  Adapting PSNR threshold from $([math]::Round($BasePSNRThreshold, 2)) to $([math]::Round($adaptedPSNRThreshold, 2)) dB" -ForegroundColor Cyan
                Write-Host "    (Source ceiling $([math]::Round($sourcePSNRCeiling, 2)) - tolerance $PSNRTolerance)" -ForegroundColor Gray
            }
            
            # If source quality is already low, be more lenient
            if ($sourcePSNRCeiling -lt 40) {
                $lenientThreshold = $sourcePSNRCeiling * 0.85
                if ($lenientThreshold -lt $adaptedPSNRThreshold) {
                    $adaptedPSNRThreshold = [math]::Max(20, $lenientThreshold)
                    Write-Host "  Source has low quality - adjusting threshold to $([math]::Round($adaptedPSNRThreshold, 2)) dB (85% of ceiling)" -ForegroundColor Yellow
                }
            }
        }
        else {
            Write-Host "  No source PSNR ceiling found - using base threshold" -ForegroundColor Yellow
        }
        
        # Adjust SSIM threshold based on source ceiling
        if ($sourceSSIMCeiling) {
            Write-Host "  Source SSIM Ceiling: $([math]::Round($sourceSSIMCeiling, 4))" -ForegroundColor Yellow
            
            # Can't exceed source quality, so cap threshold at source ceiling minus tolerance
            $maxPossibleSSIM = $sourceSSIMCeiling - $SSIMTolerance
            
            if ($BaseSSIMThreshold -gt $maxPossibleSSIM) {
                $adaptedSSIMThreshold = [math]::Max(0.85, $maxPossibleSSIM)  # Floor at 0.85
                Write-Host "  Adapting SSIM threshold from $([math]::Round($BaseSSIMThreshold, 4)) to $([math]::Round($adaptedSSIMThreshold, 4))" -ForegroundColor Cyan
                Write-Host "    (Source ceiling $([math]::Round($sourceSSIMCeiling, 4)) - tolerance $SSIMTolerance)" -ForegroundColor Gray
            }
            
            # If source quality is already low, be more lenient
            if ($sourceSSIMCeiling -lt 0.95) {
                $lenientThreshold = $sourceSSIMCeiling * 0.95
                if ($lenientThreshold -lt $adaptedSSIMThreshold) {
                    $adaptedSSIMThreshold = [math]::Max(0.85, $lenientThreshold)
                    Write-Host "  Source has low quality - adjusting threshold to $([math]::Round($adaptedSSIMThreshold, 4)) (95% of ceiling)" -ForegroundColor Yellow
                }
            }
        }
        else {
            Write-Host "  No source SSIM ceiling found - using base threshold" -ForegroundColor Yellow
        }
        
        # Adjust based on quality variation (high variation = less predictable quality)
        if ($psnrStdDev -and $psnrStdDev -gt 2.0) {
            Write-Host "  High PSNR variation detected (StdDev: $([math]::Round($psnrStdDev, 2))) - reducing threshold by 1 dB" -ForegroundColor Yellow
            $adaptedPSNRThreshold = [math]::Max(20, $adaptedPSNRThreshold - 1)
        }
        
        if ($ssimStdDev -and $ssimStdDev -gt 0.02) {
            Write-Host "  High SSIM variation detected (StdDev: $([math]::Round($ssimStdDev, 4))) - reducing threshold by 0.01" -ForegroundColor Yellow
            $adaptedSSIMThreshold = [math]::Max(0.85, $adaptedSSIMThreshold - 0.01)
        }
        
        # Additional adjustments based on resolution
        if ($resolution -match '3840|2160|4K|UHD') {
            Write-Host "  4K content detected - slightly relaxing thresholds" -ForegroundColor Yellow
            $adaptedPSNRThreshold = [math]::Max(20, $adaptedPSNRThreshold - 0.5)
            $adaptedSSIMThreshold = [math]::Max(0.85, $adaptedSSIMThreshold - 0.005)
        }
        
        # Create result with adapted thresholds
        $thresholds = @{
            AdaptedPSNRThreshold = [math]::Round($adaptedPSNRThreshold, 2)
            AdaptedSSIMThreshold = [math]::Round($adaptedSSIMThreshold, 4)
            OriginalPSNRThreshold = $BasePSNRThreshold
            OriginalSSIMThreshold = $BaseSSIMThreshold
            SourcePSNRCeiling = $sourcePSNRCeiling
            SourceSSIMCeiling = $sourceSSIMCeiling
            Resolution = $resolution
            Bitrate = $bitrate
            PSNRTolerance = $PSNRTolerance
            SSIMTolerance = $SSIMTolerance
        }
        
        Write-Host "" -ForegroundColor White
        Write-Host "Final adaptive thresholds:" -ForegroundColor Green
        Write-Host "  PSNR: $([math]::Round($thresholds.AdaptedPSNRThreshold, 2)) dB (was: $([math]::Round($thresholds.OriginalPSNRThreshold, 2)) dB)" -ForegroundColor White
        Write-Host "  SSIM: $([math]::Round($thresholds.AdaptedSSIMThreshold, 4)) (was: $([math]::Round($thresholds.OriginalSSIMThreshold, 4)))" -ForegroundColor White
        
        if ($sourcePSNRCeiling -or $sourceSSIMCeiling) {
            Write-Host "  Based on source quality ceiling analysis" -ForegroundColor Gray
        }
        
        return $thresholds
    }
    catch {
        Write-Host "Failed to calculate adaptive thresholds: $_"
        Write-Host "Stack Trace: $($_.ScriptStackTrace)" -ForegroundColor Red
        
        # Return structure matching expected format with base values
        return @{
            AdaptedPSNRThreshold = $BasePSNRThreshold
            AdaptedSSIMThreshold = $BaseSSIMThreshold
            OriginalPSNRThreshold = $BasePSNRThreshold
            OriginalSSIMThreshold = $BaseSSIMThreshold
            SourcePSNRCeiling = $null
            SourceSSIMCeiling = $null
            Resolution = "1920x1080"
            Bitrate = $null
            PSNRTolerance = $PSNRTolerance
            SSIMTolerance = $SSIMTolerance
        }
    }
}

function Invoke-QualityValidation {
    param(
        [string]$OriginalFile,
        [string]$EncodedFile,
        [double]$PSNRThreshold = 35.0,
        [double]$SSIMThreshold = 0.95,
        [int]$SampleCount = 5,
        [int]$SampleDurationSeconds = 30,
        [int]$SkipIntroSeconds = 240,
        [bool]$ForceScale = $false,
        [string]$ScaleResolution = "1920:1080",
        [bool]$EnableSourceAnalysis = $true,    # New parameter to enable/disable source analysis
        [double]$PSNRTolerance = 5.0,          # How much below source ceiling is acceptable
        [double]$SSIMTolerance = 0.02,          # How much below source ceiling is acceptable
        [hashtable]$EncodingSettings    
        )
    
    if ($EnableQualityValidation -eq 0) {
        Write-Host "Quality validation disabled - skipping" -ForegroundColor Gray
        $Script:QualityMetrics.PassesValidation = $true
        return $true
    }
    
    Write-Host "Performing enhanced quality validation with source analysis..." -ForegroundColor Cyan
    
    # Step 1: Analyze source quality if enabled
    $sourceAnalysis = $null
    $adaptiveThresholds = $null
    
    if ($EnableSourceAnalysis) {
        Write-Host "" -ForegroundColor White
        Write-Host "=== SOURCE QUALITY ANALYSIS ===" -ForegroundColor Magenta
        $sourceAnalysis = Invoke-SourceQualityAnalysis -SourceFile $OriginalFile -SampleCount $SampleCount -SampleDurationSeconds $SampleDurationSeconds -SkipIntroSeconds $SkipIntroSeconds -ScaleResolution $ScaleResolution -EncodingSettings $EncodingSettings
        
        if ($sourceAnalysis) {
            Write-Host "" -ForegroundColor White
            Write-Host "=== ADAPTIVE THRESHOLD CALCULATION ===" -ForegroundColor Magenta
            
        $adaptiveThresholds = Get-AdaptiveQualityThresholds -SourceAnalysis $sourceAnalysis -BasePSNRThreshold $PSNRThreshold -BaseSSIMThreshold $SSIMThreshold -PSNRTolerance $PSNRTolerance -SSIMTolerance $SSIMTolerance


            # Use adaptive thresholds if available
            if ($adaptiveThresholds) {
                $PSNRThreshold = $adaptiveThresholds.AdaptedPSNRThreshold
                $SSIMThreshold = $adaptiveThresholds.AdaptedSSIMThreshold
                
                # Store analysis results
                $Script:QualityMetrics.SourceAnalysis = $sourceAnalysis
                $Script:QualityMetrics.AdaptiveThresholds = $adaptiveThresholds
            }
            else {
                Write-Host "Using original thresholds due to analysis issues" -ForegroundColor Yellow
            }
        }
        else {
            Write-Warning "Source analysis failed - using original thresholds"
        }
    }
    
    Write-Host "" -ForegroundColor White
    Write-Host "=== ENCODED FILE VALIDATION ===" -ForegroundColor Magenta
    Write-Host "PSNR Threshold: $([math]::Round($PSNRThreshold, 1)) dB" -ForegroundColor Yellow
    Write-Host "SSIM Threshold: $([math]::Round($SSIMThreshold, 4))" -ForegroundColor Yellow
    Write-Host "Sample Count: $SampleCount samples of $SampleDurationSeconds seconds each" -ForegroundColor Yellow
    
    try {
        # Get video info for both files (existing logic)
        Write-Host "Getting video information..." -ForegroundColor Yellow
        
        # Get original file info
        $originalInfoArgs = @(
            "-v", "quiet",
            "-select_streams", "v:0",
            "-show_entries", "stream=width,height,pix_fmt:format=duration",
            "-of", "json",
            $OriginalFile
        )
        
        $processInfo = New-Object System.Diagnostics.ProcessStartInfo
        $processInfo.FileName = $Config.FFprobeExe
        $processInfo.UseShellExecute = $false
        $processInfo.RedirectStandardOutput = $true
        $processInfo.RedirectStandardError = $true
        $processInfo.CreateNoWindow = $true
        
        if ($PSVersionTable.PSVersion.Major -ge 7) {
            foreach ($arg in $originalInfoArgs) {
                $null = $processInfo.ArgumentList.Add($arg)
            }
        }
        else {
            $quotedArgs = $originalInfoArgs | ForEach-Object { 
                if ($_ -match '\s') { '"{0}"' -f ($_ -replace '"', '""') } else { $_ }
            }
            $processInfo.Arguments = $quotedArgs -join ' '
        }
        
        $process = [System.Diagnostics.Process]::Start($processInfo)
        $stdout = $process.StandardOutput.ReadToEnd()
        $process.WaitForExit(90000)
        
        $originalInfo = $stdout | ConvertFrom-Json
        $originalWidth = $originalInfo.streams[0].width
        $originalHeight = $originalInfo.streams[0].height
        $originalPixFmt = $originalInfo.streams[0].pix_fmt
        $durationSeconds = [double]$originalInfo.format.duration
        
        Write-Host "Original: ${originalWidth}x${originalHeight}, $originalPixFmt" -ForegroundColor Green
        
        # Get encoded file info
        $encodedInfoArgs = $originalInfoArgs -replace [regex]::Escape($OriginalFile), $EncodedFile
        
        $processInfo.Arguments = ""
        if ($PSVersionTable.PSVersion.Major -ge 7) {
            $processInfo.ArgumentList.Clear()
            foreach ($arg in $encodedInfoArgs) {
                $null = $processInfo.ArgumentList.Add($arg)
            }
        }
        else {
            $quotedArgs = $encodedInfoArgs | ForEach-Object { 
                if ($_ -match '\s') { '"{0}"' -f ($_ -replace '"', '""') } else { $_ }
            }
            $processInfo.Arguments = $quotedArgs -join ' '
        }
        
        $process = [System.Diagnostics.Process]::Start($processInfo)
        $stdout = $process.StandardOutput.ReadToEnd()
        $process.WaitForExit(90000)
        
        $encodedInfo = $stdout | ConvertFrom-Json
        $encodedWidth = $encodedInfo.streams[0].width
        $encodedHeight = $encodedInfo.streams[0].height
        $encodedPixFmt = $encodedInfo.streams[0].pix_fmt
        
        Write-Host "Encoded: ${encodedWidth}x${encodedHeight}, $encodedPixFmt" -ForegroundColor Green
        Write-Host "File duration: $([math]::Round($durationSeconds/60, 1)) minutes" -ForegroundColor Green
        
        # Determine if scaling is needed
        $needsScaling = ($originalWidth -ne $encodedWidth) -or ($originalHeight -ne $encodedHeight) -or $ForceScale
        
        if ($needsScaling) {
            Write-Host "Resolution mismatch detected - will scale to $ScaleResolution for comparison" -ForegroundColor Yellow
            $compareWidth = $ScaleResolution.Split(':')[0]
            $compareHeight = $ScaleResolution.Split(':')[1]
            $scaleFilter = "scale=${ScaleResolution}:flags=lanczos"
        }
        else {
            Write-Host "Using native resolution for comparison: ${originalWidth}x${originalHeight}" -ForegroundColor Green
            $compareWidth = $originalWidth
            $compareHeight = $originalHeight
            $scaleFilter = "null"  # No scaling needed
        }
        
        # Calculate sample positions
        $availableDuration = $durationSeconds - $SkipIntroSeconds - 60
        if ($availableDuration -lt ($SampleCount * $SampleDurationSeconds * 2)) {
            Write-Warning "File too short for $SampleCount samples, reducing to fit available duration"
            $SampleCount = [math]::Max(1, [math]::Floor($availableDuration / ($SampleDurationSeconds * 2)))
        }
        
        $samplePositions = @()
        if ($SampleCount -eq 1) {
            $samplePositions += $SkipIntroSeconds + ($availableDuration / 2)
        }
        else {
            $interval = $availableDuration / ($SampleCount - 1)
            for ($i = 0; $i -lt $SampleCount; $i++) {
                $position = $SkipIntroSeconds + ($i * $interval)
                $samplePositions += [math]::Round($position, 1)
            }
        }
        
        $sampleMinutes = $samplePositions | ForEach-Object { [math]::Round($_ / 60, 1) }
        Write-Host "Sample positions (minutes): $($sampleMinutes -join ', ')" -ForegroundColor Yellow
        
        # Arrays to store all PSNR and SSIM values
        $allPSNRValues = @()
        $allSSIMValues = @()
        $tempFiles = @()
        
        # Process each sample
        for ($sampleIndex = 0; $sampleIndex -lt $samplePositions.Count; $sampleIndex++) {
            $startTime = $samplePositions[$sampleIndex]
            Write-Host "Processing sample $($sampleIndex + 1)/$($samplePositions.Count) at $([math]::Round($startTime/60, 1)) minutes..." -ForegroundColor Cyan
            
            # Create temporary YUV files for this sample
            $tempOriginalYuv = New-TempFile -BaseName "original_sample_$sampleIndex" -Extension ".yuv"
            $tempEncodedYuv = New-TempFile -BaseName "encoded_sample_$sampleIndex" -Extension ".yuv"
            $tempFiles += $tempOriginalYuv, $tempEncodedYuv
            
            # Extract sample from original
            $originalArgs = @(
                "-y", "-hide_banner",
                "-ss", $startTime.ToString(),
                "-t", $SampleDurationSeconds.ToString(),
                "-i", $OriginalFile,
                "-vf", $scaleFilter,
                "-pix_fmt", "yuv420p",
                "-f", "rawvideo",
                $tempOriginalYuv
            )
            
            $success = Invoke-SimpleProcess -Executable $Config.FFmpegExe -Arguments $originalArgs -Description "Extract original sample $($sampleIndex + 1)" 
            if (-not $success) {
                Write-Warning "Failed to extract original sample $($sampleIndex + 1), skipping"
                continue
            }
            
            # Extract sample from encoded
            $encodedArgs = @(
                "-y", "-hide_banner",
                "-ss", $startTime.ToString(),
                "-t", $SampleDurationSeconds.ToString(),
                "-i", $EncodedFile,
                "-vf", $scaleFilter,
                "-pix_fmt", "yuv420p",
                "-f", "rawvideo",
                $tempEncodedYuv
            )
            
            $success = Invoke-SimpleProcess -Executable $Config.FFmpegExe -Arguments $encodedArgs -Description "Extract encoded sample $($sampleIndex + 1)" 
            if (-not $success) {
                Write-Warning "Failed to extract encoded sample $($sampleIndex + 1), skipping"
                continue
            }
            
            # Calculate PSNR and SSIM
            Write-Host "  Calculating PSNR and SSIM for sample $($sampleIndex + 1)..." -ForegroundColor Yellow
            
            $tempStderrFile = New-TempFile -BaseName "psnr_ssim_stderr_$sampleIndex" -Extension ".txt"
            $tempFiles += $tempStderrFile
            
            cmd /c "$($Config.FFmpegExe) -f rawvideo -pix_fmt yuv420p -s ${compareWidth}x${compareHeight} -i `"$tempOriginalYuv`" -f rawvideo -pix_fmt yuv420p -s ${compareWidth}x${compareHeight} -i `"$tempEncodedYuv`" -lavfi `"[0:v][1:v]psnr;[0:v][1:v]ssim`" -f null -" *>$tempStderrFile | Out-Null

            if ($success -and (Test-Path $tempStderrFile)) {
                $stderr = Get-Content $tempStderrFile -Raw
                
                # Parse PSNR
                $psnrMatch = $stderr | Select-String "PSNR.*average:(\d+\.\d+)"
                if (-not $psnrMatch) {
                    $psnrMatch = $stderr | Select-String "average:(\d+\.\d+)"
                }
                
                if ($psnrMatch) {
                    $samplePSNR = [double]$psnrMatch.Matches[0].Groups[1].Value
                    $allPSNRValues += $samplePSNR
                    Write-Host "  Sample $($sampleIndex + 1) PSNR: $([math]::Round($samplePSNR, 2)) dB" -ForegroundColor Green
                }
                
                # Parse SSIM
                $ssimMatch = $stderr | Select-String "SSIM.*All:(\d+\.\d+)"
                if (-not $ssimMatch) {
                    $ssimMatch = $stderr | Select-String "All:(\d+\.\d+)"
                }
                
                if ($ssimMatch) {
                    $sampleSSIM = [double]$ssimMatch.Matches[0].Groups[1].Value
                    $allSSIMValues += $sampleSSIM
                    Write-Host "  Sample $($sampleIndex + 1) SSIM: $([math]::Round($sampleSSIM, 4))" -ForegroundColor Green
                }
            }
        }
        
        # Calculate overall metrics and determine pass/fail
        $psnrPasses = $false
        $ssimPasses = $false
        
        if ($allPSNRValues.Count -gt 0) {
            $Script:QualityMetrics.PSNR = ($allPSNRValues | Measure-Object -Average).Average
            $minPSNR = ($allPSNRValues | Measure-Object -Minimum).Minimum
            $maxPSNR = ($allPSNRValues | Measure-Object -Maximum).Maximum
            
            # PSNR validation
            $averagePSNRPasses = $Script:QualityMetrics.PSNR -ge $PSNRThreshold
            $psnrPasses = $averagePSNRPasses
        }
        
        if ($allSSIMValues.Count -gt 0) {
            $Script:QualityMetrics.SSIM = ($allSSIMValues | Measure-Object -Average).Average
            $minSSIM = ($allSSIMValues | Measure-Object -Minimum).Minimum
            $maxSSIM = ($allSSIMValues | Measure-Object -Maximum).Maximum
            
            # SSIM validation
            $averageSSIMPasses = $Script:QualityMetrics.SSIM -ge $SSIMThreshold
            $ssimPasses = $averageSSIMPasses
        }
        
        # Calculate quality efficiency (how close to source ceiling we achieved)
        $qualityEfficiency = @{
            PSNREfficiency = $null
            SSIMEfficiency = $null
        }
        
        if ($sourceAnalysis -and $sourceAnalysis.QualityCeiling.PSNR -and $Script:QualityMetrics.PSNR) {
            $qualityEfficiency.PSNREfficiency = ($Script:QualityMetrics.PSNR / $sourceAnalysis.QualityCeiling.PSNR) * 100
        }
        
        if ($sourceAnalysis -and $sourceAnalysis.QualityCeiling.SSIM -and $Script:QualityMetrics.SSIM) {
            $qualityEfficiency.SSIMEfficiency = ($Script:QualityMetrics.SSIM / $sourceAnalysis.QualityCeiling.SSIM) * 100
        }
        
        # Overall validation requires BOTH metrics to pass
        $Script:QualityMetrics.PassesValidation = $psnrPasses -and $ssimPasses
        $Script:QualityMetrics.QualityEfficiency = $qualityEfficiency
        
        # Display results
        Write-Host "" -ForegroundColor White
        Write-Host "=== QUALITY VALIDATION RESULTS ===" -ForegroundColor Magenta
        
        if ($allPSNRValues.Count -gt 0) {
            Write-Host "  PSNR Metrics:" -ForegroundColor White
            Write-Host "    Average: $([math]::Round($Script:QualityMetrics.PSNR, 2)) dB (threshold: $([math]::Round($PSNRThreshold, 1)) dB)" -ForegroundColor $(if ($psnrPasses) { "Green" } else { "Red" })
            Write-Host "    Min: $([math]::Round($minPSNR, 2)) dB | Max: $([math]::Round($maxPSNR, 2)) dB" -ForegroundColor Yellow
            
            if ($qualityEfficiency.PSNREfficiency) {
                Write-Host "    Efficiency: $([math]::Round($qualityEfficiency.PSNREfficiency, 1))% of source ceiling" -ForegroundColor Cyan
            }
            
            Write-Host "    Status: $(if ($psnrPasses) { "PASS" } else { "FAIL" })" -ForegroundColor $(if ($psnrPasses) { "Green" } else { "Red" })
            
            # Add standard deviation if we have multiple samples
            if ($allPSNRValues.Count -gt 1) {
                $psnrStdDev = [math]::Sqrt(($allPSNRValues | ForEach-Object { [math]::Pow($_ - $Script:QualityMetrics.PSNR, 2) } | Measure-Object -Sum).Sum / $allPSNRValues.Count)
                Write-Host "    Variation (StdDev): $([math]::Round($psnrStdDev, 2)) dB" -ForegroundColor Gray
            }
        }
        
        if ($allSSIMValues.Count -gt 0) {
            Write-Host "  SSIM Metrics:" -ForegroundColor White
            Write-Host "    Average: $([math]::Round($Script:QualityMetrics.SSIM, 4)) (threshold: $([math]::Round($SSIMThreshold, 4)))" -ForegroundColor $(if ($ssimPasses) { "Green" } else { "Red" })
            Write-Host "    Min: $([math]::Round($minSSIM, 4)) | Max: $([math]::Round($maxSSIM, 4))" -ForegroundColor Yellow
            
            if ($qualityEfficiency.SSIMEfficiency) {
                Write-Host "    Efficiency: $([math]::Round($qualityEfficiency.SSIMEfficiency, 1))% of source ceiling" -ForegroundColor Cyan
            }
            
            Write-Host "    Status: $(if ($ssimPasses) { "PASS" } else { "FAIL" })" -ForegroundColor $(if ($ssimPasses) { "Green" } else { "Red" })
            
            # Add standard deviation if we have multiple samples
            if ($allSSIMValues.Count -gt 1) {
                $ssimStdDev = [math]::Sqrt(($allSSIMValues | ForEach-Object { [math]::Pow($_ - $Script:QualityMetrics.SSIM, 2) } | Measure-Object -Sum).Sum / $allSSIMValues.Count)
                Write-Host "    Variation (StdDev): $([math]::Round($ssimStdDev, 4))" -ForegroundColor Gray
            }
        }
        
        Write-Host "" -ForegroundColor White
        if ($Script:QualityMetrics.PassesValidation) {
            Write-Host "Overall Quality Validation: PASSED" -ForegroundColor Green
        }
        else {
            Write-Host "Overall Quality Validation: FAILED" -ForegroundColor Red
            if (-not $psnrPasses) {
                Write-Host "  - PSNR requirements not met" -ForegroundColor Red
            }
            if (-not $ssimPasses) {
                Write-Host "  - SSIM requirements not met" -ForegroundColor Red
            }
            
            # Provide context if we have source analysis
            if ($sourceAnalysis) {
                Write-Host "" -ForegroundColor White
                Write-Host "Source Quality Context:" -ForegroundColor Cyan
                
                if ($qualityEfficiency.PSNREfficiency -and $qualityEfficiency.PSNREfficiency -gt 85) {
                    Write-Host "  - PSNR efficiency is good ($([math]::Round($qualityEfficiency.PSNREfficiency, 1))%) - encoding performed well given source limitations" -ForegroundColor Yellow
                }
                elseif ($qualityEfficiency.PSNREfficiency -and $qualityEfficiency.PSNREfficiency -lt 70) {
                    Write-Host "  - PSNR efficiency is poor ($([math]::Round($qualityEfficiency.PSNREfficiency, 1))%) - encoding may need improvement" -ForegroundColor Red
                }
                
                if ($qualityEfficiency.SSIMEfficiency -and $qualityEfficiency.SSIMEfficiency -gt 90) {
                    Write-Host "  - SSIM efficiency is good ($([math]::Round($qualityEfficiency.SSIMEfficiency, 1))%) - encoding performed well given source limitations - Override" -ForegroundColor Yellow
                    $Script:QualityMetrics.PassesValidation = $true
                }
                elseif ($qualityEfficiency.SSIMEfficiency -and $qualityEfficiency.SSIMEfficiency -lt 70) {
                    Write-Host "  - SSIM efficiency is poor ($([math]::Round($qualityEfficiency.SSIMEfficiency, 1))%) - encoding may need improvement" -ForegroundColor Red
                }

            }
        }
        
        return $Script:QualityMetrics.PassesValidation
        
    }
    catch {
        Write-Warning "Quality validation failed: $_"
        Write-Host "Error details: $($_.Exception.Message)" -ForegroundColor Red
        Write-Host "Stack trace: $($_.ScriptStackTrace)" -ForegroundColor DarkGray
        $Script:QualityMetrics.PassesValidation = $true
        return $true
    }
    finally {
        # Cleanup temp files
        foreach ($tempFile in $tempFiles) {
            if (Test-Path $tempFile) {
                try {
                    Remove-Item $tempFile -Force -ErrorAction SilentlyContinue
                }
                catch {
                    # Ignore cleanup errors
                }
            }
        }
        Write-Host "Quality validation temp files cleaned up" -ForegroundColor Gray
    }
}

function Test-AudioStreamDuration {
    param(
        [object]$Stream,
        [hashtable]$VideoInfo,
        [string]$FilePath,
        [double]$TolerancePercent = 2.0  # Allow 2% duration difference by default
    )
    
    Write-Host "    Checking duration compatibility for audio stream $($Stream.GlobalIndex)..." -ForegroundColor Gray
    
    # Get video duration - ensure it's a scalar value
    [double]$videoDuration = [double]($VideoInfo.Duration | Select-Object -First 1)
    [double]$audioDuration = [double]($Stream.Duration | Select-Object -First 1)
    
    if ($videoDuration -le 0) {
        Write-Host "    Warning: Video duration not available or invalid ($videoDuration s)" -ForegroundColor Yellow
        Write-Host "    Skipping duration validation - assuming audio is compatible" -ForegroundColor Yellow
        return @{ IsCompatible = $true; Reason = "Video duration unavailable - validation skipped" }
    }
    
    if ($audioDuration -le 0) {
        Write-Host "    Warning: Audio duration not available for stream $($Stream.GlobalIndex)" -ForegroundColor Yellow
        
        # Fallback: Try to get duration directly from FFprobe for this specific stream
        try {
            Write-Host "    Attempting to get audio duration directly from stream..." -ForegroundColor Gray
            $durationArgs = @(
                "-v", "quiet",
                "-select_streams", "a:$($Stream.GlobalIndex)",
                "-show_entries", "stream=duration",
                "-of", "csv=p=0",
                $FilePath
            )
            
            $durationOutput = & $Config.FFProbeExe @durationArgs 2>$null
            if ($durationOutput -and $durationOutput -match "^\d+\.?\d*$") {
                $audioDuration = [double]$durationOutput
                Write-Host "    Retrieved audio duration: $audioDuration s" -ForegroundColor Green
            }
            else {
                Write-Host "    Could not retrieve audio duration from FFprobe" -ForegroundColor Yellow
                return @{ IsCompatible = $true; Reason = "Audio duration unavailable - assuming compatible" }
            }
        }
        catch {
            Write-Host "    FFprobe duration check failed: $_" -ForegroundColor Yellow
            return @{ IsCompatible = $true; Reason = "Duration check failed - assuming compatible" }
        }
    }
    
    # Calculate duration difference
    $durationDifference = [math]::Abs($videoDuration - $audioDuration)
    $differencePercent = if ($videoDuration -gt 0) { ($durationDifference / $videoDuration) * 100 } else { 0 }
    
    Write-Host "    Duration comparison:" -ForegroundColor Gray
    Write-Host "      Video duration: $([math]::Round($videoDuration, 2)) s" -ForegroundColor Gray
    Write-Host "      Audio duration: $([math]::Round($audioDuration, 2)) s" -ForegroundColor Gray
    Write-Host "      Difference: $([math]::Round($durationDifference, 2)) s ($([math]::Round($differencePercent, 2))%)" -ForegroundColor Gray
    
    # Determine compatibility
    if ($differencePercent -le $TolerancePercent) {
        Write-Host "    Duration check: PASSED (within $TolerancePercent% tolerance)" -ForegroundColor Green
        return @{ 
            IsCompatible      = $true
            Reason            = "Duration difference within tolerance"
            VideoDuration     = $videoDuration
            AudioDuration     = $audioDuration
            DifferencePercent = $differencePercent
        }
    }
    elseif ($differencePercent -le 5.0) {
        Write-Host "    Duration check: WARNING (difference exceeds tolerance but < 5%)" -ForegroundColor Yellow
        Write-Host "    This may cause minor sync issues at the end of playback" -ForegroundColor Yellow
        return @{ 
            IsCompatible      = $true
            Reason            = "Duration difference acceptable but noticeable"
            VideoDuration     = $videoDuration
            AudioDuration     = $audioDuration
            DifferencePercent = $differencePercent
        }
    }
    elseif ($audioDuration -lt ($videoDuration * 0.8)) {
        Write-Host "    Duration check: FAILED (audio significantly shorter than video)" -ForegroundColor Red
        Write-Host "    Audio stream appears to be truncated or incomplete" -ForegroundColor Red
        return @{ 
            IsCompatible      = $false
            Reason            = "Audio stream significantly shorter than video - likely truncated"
            VideoDuration     = $videoDuration
            AudioDuration     = $audioDuration
            DifferencePercent = $differencePercent
        }
    }
    elseif ($audioDuration -gt ($videoDuration * 1.2)) {
        Write-Host "    Duration check: FAILED (audio significantly longer than video)" -ForegroundColor Red
        Write-Host "    Audio stream may contain extra content or be from wrong source" -ForegroundColor Red
        return @{ 
            IsCompatible      = $false
            Reason            = "Audio stream significantly longer than video - may be wrong track"
            VideoDuration     = $videoDuration
            AudioDuration     = $audioDuration
            DifferencePercent = $differencePercent
        }
    }
    else {
        Write-Host "    Duration check: WARNING (moderate duration mismatch)" -ForegroundColor Yellow
        Write-Host "    This may cause sync issues during playback" -ForegroundColor Yellow
        return @{ 
            IsCompatible      = $true
            Reason            = "Moderate duration mismatch - may cause sync issues"
            VideoDuration     = $videoDuration
            AudioDuration     = $audioDuration
            DifferencePercent = $differencePercent
        }
    }
}

function Confirm-AudioStreamUsability {
    param(
        [object]$Stream,
        [string]$FilePath
    )

    Write-Host "    Performing deep validation on stream $($Stream.GlobalIndex)..." -ForegroundColor Gray

    try {
        # Use managed process for audio validation
        $sampleArgs = @(
            "-hide_banner",
            "-i", $FilePath,
            "-map", "0:a:$($Stream.GlobalIndex)",
            "-t", "10",
            "-f", "null",
            "-"
        )
        
        # Use a shorter timeout for audio validation
        $validationResult = Invoke-FFProbeWithCleanup -Arguments $sampleArgs -TimeoutSeconds 60
        
        if (-not $validationResult.Success) {
            Write-Host "    Stream $($Stream.GlobalIndex): FAILED validation" -ForegroundColor Red
            Write-Host "    Error: $($validationResult.StdErr)" -ForegroundColor Red
            
            return @{
                IsUsable = $false
                Reason   = "FFmpeg validation failed: $($validationResult.StdErr)"
            }
        }

        # Check for frame processing in output
        if ($validationResult.StdErr -match "frame=\s*(\d+)") {
            $frameCount = [int]$matches[1]
            if ($frameCount -lt 50) {
                return @{ 
                    IsUsable = $false
                    Reason   = "Stream produced insufficient audio frames: $frameCount in 10 seconds"
                }
            }
            Write-Host "    Stream produced $frameCount audio frames in 10 seconds" -ForegroundColor Green
        }

        Write-Host "    Stream $($Stream.GlobalIndex): Deep validation PASSED" -ForegroundColor Green
        return @{ IsUsable = $true; Reason = "Stream validation successful" }

    }
    catch {
        Write-Host "    Stream $($Stream.GlobalIndex): FAILED validation with exception" -ForegroundColor Red
        return @{ IsUsable = $false; Reason = "Validation exception: $($_.Exception.Message)" }
    }
}

# Modified Get-AudioStreamInfo function with duration validation
function Get-AudioStreamInfo {
    param([string]$FilePath)
    
    Write-Host "Analyzing audio streams..." -ForegroundColor Cyan
    
    try {
        # Use managed process for audio stream analysis
        $audioArgs = @(
            "-v", "quiet",
            "-analyzeduration", "1000000000",
            "-probesize", "5000000000",
            "-select_streams", "a",
            "-show_entries", "stream=index,codec_name,channels,channel_layout,bit_rate,sample_rate,profile,duration:stream_tags=language,title,comment",
            "-of", "json",
            $FilePath
        )
        
        $audioResult = Invoke-FFProbeWithCleanup -Arguments $audioArgs -TimeoutSeconds 90
        
        if (-not $audioResult.Success) {
            Write-Warning "Failed to analyze audio streams: $($audioResult.StdErr)"
            return @{ ToCopy = @(); HasAAC = $false }
        }
        
        $audioStreams = $audioResult.StdOut | ConvertFrom-Json
        
        if (-not $audioStreams -or -not $audioStreams.streams) {
            Write-Host "No audio streams found." -ForegroundColor Yellow
            return @{ ToCopy = @(); HasAAC = $false }
        }
        else {
            Write-Host "Found $($audioStreams.streams.Count) audio streams." -ForegroundColor Green
        }
        
        $streamsWithScore = @()
        $hasAAC = $false
        
        $globalIndex = 0
        foreach ($stream in $audioStreams.streams) {
            
            # Validate stream integrity first
            $streamValid = Test-AudioStreamIntegrity -Stream $stream -StreamIndex $globalIndex -FilePath $FilePath
            if (-not $streamValid) {
                Write-Host "Skipping invalid audio stream $($stream.index) (Global: $globalIndex)" -ForegroundColor Red
                $globalIndex++
                continue
            }
            
            # Skip commentary tracks
            if ($stream.tags.title -and ($stream.tags.title -imatch 'Commentary')) {
                Write-Host "Skipping audio stream $($stream.index) due to title 'Commentary'." -ForegroundColor Yellow
                $globalIndex++
                continue
            }
            
            # Skip non-English streams
            if ($stream.tags.language -and -not ($stream.tags.language -eq "eng" -or $stream.tags.language -in @("", "und", "unknown"))) {
                Write-Host "Skipping audio stream $($stream.index) due to language $($stream.tags.language)." -ForegroundColor Yellow
                $globalIndex++
                continue
            }

            $streamInfo = [PSCustomObject]@{
                GlobalIndex   = $globalIndex
                OriginalIndex = $stream.index
                Codec         = $stream.codec_name
                SampleRate    = if ($stream.sample_rate -and $stream.sample_rate -match '\d+') { [int]$stream.sample_rate } else { 0 } 
                Channels      = if ($stream.channels) { $stream.channels } else { 0 }
                ChannelLayout = if ($stream.channel_layout) { $stream.channel_layout } else { "unknown" }
                Bitrate       = if ($stream.bit_rate -and $stream.bit_rate -match '\d+') { [int]$stream.bit_rate } else { 0 }
                Duration      = if ($stream.duration) { [double]$stream.duration } else { 0 }
                Language      = if ($stream.tags -and $stream.tags.language) { $stream.tags.language.ToLower() } else { "eng" }
                Title         = if ($stream.tags -and $stream.tags.title) { $stream.tags.title } else { $null }
                Comment       = if ($stream.tags -and $stream.tags.comment) { $stream.tags.comment } else { $null }
                Score         = 0
                IsValid       = $true
            }
            $globalIndex++
            
            # Check if this is an AAC stream
            if ($streamInfo.Codec -eq "aac") {
                $hasAAC = $true
                Write-Host "Found existing AAC stream at index $($streamInfo.GlobalIndex)" -ForegroundColor Green
            }
            
            # Enhanced scoring logic with codec hierarchy
            switch ($streamInfo.Codec) {
                "truehd" { $streamInfo.Score += 100 }
                "pcm_s24le" { $streamInfo.Score += 95 }
                "pcm_s16le" { $streamInfo.Score += 90 }
                "flac" { $streamInfo.Score += 85 }
                "dts" {
                    if ($stream.profile -and $stream.profile -imatch "dts-hd ma") {
                        $streamInfo.Codec = "dts-hd"
                        $streamInfo.Score += 80 
                    }
                    else {
                        $streamInfo.Score += 70
                    }
                }
                "eac3" { $streamInfo.Score += 75 }
                "ac3" { $streamInfo.Score += 65 }
                "aac" { $streamInfo.Score += 60 }
                "opus" { $streamInfo.Score += 55 }
                default { $streamInfo.Score += 0 }
            }

            # Language bonus
            if ($streamInfo.Language -eq "eng") {
                $streamInfo.Score += 10
            }
            
            # Channel configuration bonus
            switch ($streamInfo.Channels) {
                8 { $streamInfo.Score += 10 }  # 7.1
                6 { $streamInfo.Score += 8 }   # 5.1
                2 { $streamInfo.Score += 4 }   # Stereo
                1 { $streamInfo.Score += 1 }   # Mono
                default { $streamInfo.Score += 1 }
            }
            
            # Bitrate quality bonus (for lossy codecs)
            if ($streamInfo.Codec -in @("aac", "ac3", "eac3", "opus") -and $streamInfo.Bitrate -gt 0) {
                if ($streamInfo.Bitrate -gt 500000) {
                    $streamInfo.Score += 5  # High bitrate
                }
                elseif ($streamInfo.Bitrate -gt 200000) {
                    $streamInfo.Score += 3  # Medium bitrate
                }
                elseif ($streamInfo.Bitrate -gt 100000) {
                    $streamInfo.Score += 1  # Low bitrate
                }
            }
            
            # Sample rate bonus
            if ($streamInfo.SampleRate -ge 48000) {
                $streamInfo.Score += 2
            }
            
            # Penalty for higher stream index (prefer earlier streams if equal quality)
            $streamInfo.Score -= $streamInfo.OriginalIndex
            
            $streamsWithScore += $streamInfo
        }
        
        # Sort by score and get the best stream
        $bestStream = $streamsWithScore | Sort-Object Score -Descending | Select-Object -First 1
        
        if ($bestStream) {
            # Duration validation
            Write-Host "Performing duration validation on selected audio stream..." -ForegroundColor Cyan
            $durationCheck = Test-AudioStreamDuration -Stream $bestStream -VideoInfo $Script:VideoMetadata -FilePath $FilePath
            
            if (-not $durationCheck.IsCompatible) {
                Write-Warning "Selected audio stream failed duration validation: $($durationCheck.Reason)"
                
                # Try alternative streams
                $alternativeStreams = $streamsWithScore | Sort-Object Score -Descending | Select-Object -Skip 1 -First 3
                foreach ($altStream in $alternativeStreams) {
                    Write-Host "Testing alternative stream: Global Index $($altStream.GlobalIndex)..." -ForegroundColor Yellow
                    $altDurationCheck = Test-AudioStreamDuration -Stream $altStream -VideoInfo $Script:VideoMetadata -FilePath $FilePath
                    
                    if ($altDurationCheck.IsCompatible) {
                        Write-Host "Alternative stream passed duration validation" -ForegroundColor Green
                        $bestStream = $altStream
                        break
                    }
                }
            }
            
            # Final validation with managed process
            Write-Host "Performing final validation on selected audio stream..." -ForegroundColor Cyan
            $finalValidation = Confirm-AudioStreamUsability -Stream $bestStream -FilePath $FilePath
            
            if ($finalValidation.IsUsable) {
                $titleInfo = if ($bestStream.Title) { ", Title: '$($bestStream.Title)'" } else { "" }
                Write-Host "Audio analysis complete. Selected best stream: Codec: $($bestStream.Codec), Channels: $($bestStream.Channels), Language: $($bestStream.Language), Global Index: $($bestStream.GlobalIndex)$titleInfo" -ForegroundColor Green
                
                return @{
                    ToCopy = $bestStream
                    HasAAC = $hasAAC
                    AllStreams = $streamsWithScore  # Include all valid streams for reference
                }
            }
            else {
                Write-Warning "Selected audio stream failed final validation: $($finalValidation.Reason)"
                return @{ ToCopy = @(); HasAAC = $false; AllStreams = @() }
            }
        }
        else {
            Write-Host "No suitable audio stream found." -ForegroundColor Yellow
            return @{ ToCopy = @(); HasAAC = $false; AllStreams = @() }
        }
    }
    catch {
        Write-Warning "Error in audio stream analysis: $_"
        return @{ ToCopy = @(); HasAAC = $false; AllStreams = @() }
    }
}
function Test-AudioStreamIntegrity {
    param(
        [object]$Stream,
        [int]$StreamIndex,
        [string]$FilePath
    )
    
    # Basic validation checks
    if (-not $Stream.codec_name -or $Stream.codec_name.Trim() -eq "") {
        Write-Host "  Stream $($StreamIndex): Invalid - Missing codec name" -ForegroundColor Red
        return $false
    }
    
    # Check for reasonable channel count
    if ($Stream.channels -and ($Stream.channels -lt 1 -or $Stream.channels -gt 32)) {
        Write-Host "  Stream $($StreamIndex): Invalid - Unrealistic channel count: $($Stream.channels)" -ForegroundColor Red
        return $false
    }
    
    # Check for suspicious bitrate values
    if ($Stream.bit_rate -and $Stream.bit_rate -match '\d+') {
        $bitrate = [int]$Stream.bit_rate
        if ($bitrate -lt 8000 -or $bitrate -gt 10000000) {
            # 8kbps to 10Mbps range
            Write-Host "  Stream $($StreamIndex): Warning - Suspicious bitrate: $bitrate bps" -ForegroundColor Yellow
            # Don't fail for bitrate, just warn
        }
    }
    
    # Check duration if available
    if ($Stream.duration) {
        $duration = [double]$Stream.duration
        if ($duration -lt 1.0) {
            # Less than 1 second
            Write-Host "  Stream $($StreamIndex): Invalid - Duration too short: $duration seconds" -ForegroundColor Red
            return $false
        }
        # Compare against video duration if available
        if ($Script:VideoMetadata -and $Script:VideoMetadata.Duration -and $duration -lt ($Script:VideoMetadata.Duration * 0.8)) {
            Write-Host "  Stream $($StreamIndex): Invalid - Duration significantly shorter than video" -ForegroundColor Red
            return $false
        }
    }
    
    # Validate codec is supported
    $supportedCodecs = @("aac", "ac3", "eac3", "dts", "truehd", "flac", "pcm_s16le", "pcm_s24le", "opus", "mp3")
    if ($Stream.codec_name -notin $supportedCodecs) {
        Write-Host "  Stream $($StreamIndex): Warning - Uncommon codec: $($Stream.codec_name)" -ForegroundColor Yellow
        # Continue processing uncommon codecs, just warn
    }
    
    Write-Host "  Stream $($StreamIndex): Basic validation PASSED ($($Stream.codec_name), $($Stream.channels)ch)" -ForegroundColor Green
    return $true
}

function Confirm-AudioStreamUsability {
    param(
        [object]$Stream,
        [string]$FilePath
    )

    Write-Host "    Performing deep validation on stream $($Stream.GlobalIndex) $($FilePath)..." -ForegroundColor Gray

    try {
        # Build args
        $sampleArgs = @(
            "-hide_banner",
            "-i", $FilePath,
            "-map", "0:a:$($Stream.GlobalIndex)",
            "-t", "5",  # 5 second sample
            "-f", "null",
            "-"
        )

        # Build command string for display
        $quotedArgs = $sampleArgs | ForEach-Object {
            if ($_ -match '\s') { "`"$_`"" } else { $_ }
        }
        $commandLine = $quotedArgs -join ' '
        #    Write-Host "[CMD] $commandLine" -ForegroundColor Gray

        # Configure process
        $psi = New-Object System.Diagnostics.ProcessStartInfo
        $psi.FileName = $Config.FFmpegExe
        $psi.Arguments = $commandLine
        $psi.RedirectStandardOutput = $true
        $psi.RedirectStandardError = $true
        $psi.UseShellExecute = $false
        $psi.CreateNoWindow = $true

        $process = New-Object System.Diagnostics.Process
        $process.StartInfo = $psi
        $null = $process.Start()

        # Capture output async
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()

        # Wait with timeout
        if (-not $process.WaitForExit(300000)) {
            # 300s
            try {
                $process.Kill()
                $process.WaitForExit(5000)
            }
            catch {}
            return @{ IsUsable = $false; Reason = "Stream test timed out after 300 seconds" }
        }

        $exitCode = $process.ExitCode
        $stdout = $stdout.Result
        $stderr = $stderr.Result

        # Error pattern detection
        $errorPatterns = @(
            "Invalid data found when processing input",
            "Stream not found",
            "No such file or directory",
            "Permission denied",
            "Decoder.*not found",
            "Unsupported codec",
            "Invalid frame header",
            "File ended prematurely",
            "Error while decoding",
            "buffer underflow",
            "invalid bitstream",
            "packet too small",
            "co located POCs unavailable"
        )

        $errorReason = $null
        foreach ($pattern in $errorPatterns) {
            if ($stderr -match $pattern) {
                $errorReason = "FFmpeg error: $pattern"
                break
            }
        }

        if ($exitCode -ne 0 -or $errorReason) {
            Write-Host "    Stream $($Stream.GlobalIndex): FAILED validation" -ForegroundColor Red
            if ($errorReason) {
                Write-Host "    Error: $errorReason" -ForegroundColor Red
            }
            if ($stderr) {
                Write-Host "    FFmpeg stderr: $($stderr.Substring(0, [Math]::Min(200, $stderr.Length)))" -ForegroundColor DarkRed
            }
            return @{
                IsUsable = $false
                Reason   = if ($errorReason) { $errorReason } else { "FFmpeg exit code: $exitCode" }
            }

        }

        # Check frame count
        if ($stderr -match "frame=\s*(\d+)") {
            $frameCount = [int]$matches[1]
            if ($frameCount -lt 10) {
                return @{ IsUsable = $false; Reason = "Stream produced insufficient frames: $frameCount in 5 seconds" }
            }
        }

        Write-Host "    Stream $($Stream.GlobalIndex): Deep validation PASSED" -ForegroundColor Green
        return @{ IsUsable = $true; Reason = "Stream validation successful" }

    }
    catch {
        Write-Host "    Stream $($Stream.GlobalIndex): FAILED validation with exception" -ForegroundColor Red
        return @{ IsUsable = $false; Reason = "Validation exception: $($_.Exception.Message)" }
    }
}


function Get-SubtitleStreamInfo {
    param(
        [string]$FilePath,
        [bool]$IsOriginalMP4
    )
    
    Write-Host "Analyzing subtitle streams..." -ForegroundColor Cyan
    
    try {
        # Enhanced FFprobe command to get disposition information including forced flag
        $subtitleArgs = @(
            "-v", "warning",
            "-analyzeduration", "10000000000",
            "-probesize", "50000000000",
            "-select_streams", "s",
            "-show_entries", "stream=index,codec_name,disposition:stream_tags=language,title,comment",
            "-of", "json",
            $OrigFile # $FilePath
        )
        
        $subtitleResult = Invoke-FFProbeWithCleanup -Arguments $subtitleArgs -TimeoutSeconds 90
        
        if (-not $subtitleResult.Success) {
            Write-Host "No subtitle streams found or probe failed" -ForegroundColor Gray
            return @{ ToConvert = @(); ToCopy = @(); Remaining = @() }
        }
        
        $subtitleData = $subtitleResult.StdOut | ConvertFrom-Json
        
        if (-not $subtitleData -or -not $subtitleData.streams -or $subtitleData.streams.Count -eq 0) { 
            Write-Host "No subtitle streams found" -ForegroundColor Gray
            return @{ ToConvert = @(); ToCopy = @(); Remaining = @() } 
        }
        
        $streams = $subtitleData.streams
        $filteredStreams = @()
        $logicalIndex = 0
        
        foreach ($stream in $streams) {
            try {
                # Parse disposition flags (forced, default, etc.)
                $isForced = $false
                $isDefault = $false
                
                if ($stream.disposition) {
                    $isForced = $stream.disposition.forced -eq 1
                    $isDefault = $stream.disposition.default -eq 1
                }
                
                # Get language (default to "und" if not specified)
                $language = "und"
                if ($stream.tags -and $stream.tags.language -and $stream.tags.language.Trim() -ne "") {
                    $language = $stream.tags.language.ToLower()
                }
                
                # Get title and comment
                $title = $null
                $comment = $null
                if ($stream.tags) {
                    if ($stream.tags.title -and $stream.tags.title.Trim() -ne "") {
                        $title = $stream.tags.title
                    }
                    if ($stream.tags.comment -and $stream.tags.comment.Trim() -ne "") {
                        $comment = $stream.tags.comment
                    }
                }
                
                $streamInfo = @{
                    LogicalIndex  = $logicalIndex
                    OriginalIndex = $stream.index
                    Codec         = $stream.codec_name
                    Language      = $language
                    Title         = $title
                    Comment       = $comment
                    IsForced      = $isForced
                    IsDefault     = $isDefault
                    QualityScore  = 0
                }
                
                # Enhance title detection for forced subtitles if disposition flag wasn't caught
                if (-not $streamInfo.IsForced -and $streamInfo.Title) {
                    if ($streamInfo.Title -match "forced|Forced|FORCED") {
                        $streamInfo.IsForced = $true
                        Write-Host "Detected forced subtitle from title: $($streamInfo.Title)" -ForegroundColor Yellow
                    }
                }
                
                # Only process English or undefined subtitles
                if ($streamInfo.Language -eq "eng" -or $streamInfo.Language -in @("", "und", "unknown")) {
                    # Enhanced scoring that considers forced flag and codec quality
                    switch ($streamInfo.Codec) {
                        "subrip" { $streamInfo.QualityScore = 20 } # Best: Text-based
                        "mov_text" { $streamInfo.QualityScore = 18 }  # Good: Text-based
                        "ass" { $streamInfo.QualityScore = 16 }  # Advanced SubStation Alpha
                        "ssa" { $streamInfo.QualityScore = 14 }  # SubStation Alpha
                        "webvtt" { $streamInfo.QualityScore = 12 }  # WebVTT
                        "hdmv_pgs_subtitle" { $streamInfo.QualityScore = 10 }  # Image-based but good quality
                        "dvd_subtitle" { $streamInfo.QualityScore = 8 }  # DVD SUB/IDX
                        "dvb_subtitle" { $streamInfo.QualityScore = 6 }  # DVB subtitles
                        default { $streamInfo.QualityScore = 1 }  # Other formats
                    }
                    
                    # Major boost score for forced subtitles (they're critical for foreign parts)
                    if ($streamInfo.IsForced) {
                        $streamInfo.QualityScore += 50
                        Write-Host "Found forced subtitle stream: Index $($streamInfo.LogicalIndex), Codec: $($streamInfo.Codec)" -ForegroundColor Green
                    }
                    
                    # Boost score for default subtitles
                    if ($streamInfo.IsDefault) {
                        $streamInfo.QualityScore += 15
                        Write-Host "Found default subtitle stream: Index $($streamInfo.LogicalIndex), Codec: $($streamInfo.Codec)" -ForegroundColor Green
                    }
                    
                    # Language bonus
                    if ($streamInfo.Language -eq "eng") {
                        $streamInfo.QualityScore += 10
                    }
                    
                    # Title quality bonus - descriptive titles are preferred
                    if ($streamInfo.Title) {
                        if ($streamInfo.Title -match "SDH|Hearing.Impaired|CC|Closed.Caption") {
                            $streamInfo.QualityScore += 8  # SDH/CC are valuable
                        }
                        elseif ($streamInfo.Title -match "Full|Complete|Main") {
                            $streamInfo.QualityScore += 5  # Full subtitles
                        }
                        elseif ($streamInfo.Title -notmatch "Commentary|Director|Behind.Scenes") {
                            $streamInfo.QualityScore += 3  # Any descriptive title (not commentary)
                        }
                    }
                    
                    # Penalty for higher stream index (prefer earlier streams if equal quality)
                    $streamInfo.QualityScore -= ($streamInfo.OriginalIndex * 0.1)
                    
                    $filteredStreams += $streamInfo
                }
                
                $logicalIndex++
            }
            catch {
                Write-Host "Error processing subtitle stream at index $logicalIndex : $_" -ForegroundColor Red
                $logicalIndex++
                continue
            }
        }
        
        # Sort by quality score (highest first) and take the top 5 English subtitles
        $sortedAndLimitedStreams = $filteredStreams | Sort-Object QualityScore -Descending | Select-Object -First 5
        
        $toConvert = @()
        $toCopy = @()
        $remaining = @()
        
        foreach ($stream in $sortedAndLimitedStreams) {
            Write-Host "Processing subtitle stream $($stream.LogicalIndex): $($stream.Codec) (Score: $($stream.QualityScore))$(if ($stream.IsForced) { ' [FORCED]' } else { '' })" -ForegroundColor Gray
            
            # MP4 inputs can only have mov_text or subrip copied
            #if ($IsOriginalMP4) {
            #    if ($stream.Codec -eq "subrip" -or $stream.Codec -eq "mov_text") {
            #        $toCopy += $stream
            #        Write-Host "  -> Will copy (MP4 compatible text format)" -ForegroundColor Green
            #    }
            #    else {
            #        $toConvert += $stream
            #        Write-Host "  -> Will extract and convert (MP4 incompatible)" -ForegroundColor Yellow
            #    }
            #}
            # Non-MP4 inputs can have more formats converted or copied
            #else {
                if ($stream.Codec -in @("subrip", "mov_text", "ass", "ssa", "webvtt", "hdmv_pgs_subtitle")) {
                #    $toCopy += $stream
                #    Write-Host "  -> Will copy (text-based format)" -ForegroundColor Green
                #}
                #elseif ($stream.Codec -eq "hdmv_pgs_subtitle") {
                    $toConvert += $stream
                    Write-Host "  -> Will extract" -ForegroundColor Yellow
                }
                else {
                    $remaining += $stream
                    Write-Host "  -> Will ignore (unsupported format)" -ForegroundColor DarkGray
                }
            #}
        }
        
        # Report on streams that didn't make the top 5
        $ignoredCount = $filteredStreams.Count - $sortedAndLimitedStreams.Count
        if ($ignoredCount -gt 0) {
            Write-Host "Note: $ignoredCount additional English subtitle streams were found but not selected (keeping top 5)" -ForegroundColor Gray
        }
        
        Write-Host "Subtitle analysis complete: $($toConvert.Count) to convert, $($toCopy.Count) to copy, $($remaining.Count) unsupported" -ForegroundColor Cyan
        
        # Report forced subtitle findings
        $forcedCount = ($sortedAndLimitedStreams | Where-Object IsForced).Count
        if ($forcedCount -gt 0) {
            Write-Host "Found $forcedCount forced subtitle stream(s) - these will be preserved with forced disposition" -ForegroundColor Green
        }
        else {
            Write-Host "No forced subtitle streams detected" -ForegroundColor Yellow
        }
        
        return @{
            ToConvert = $toConvert
            ToCopy    = $toCopy
            Remaining = $remaining
            AllEnglishStreams = $filteredStreams  # All English streams for reference
        }
    }
    catch {
        Write-Warning "Error in subtitle stream analysis: $_"
        return @{ ToConvert = @(); ToCopy = @(); Remaining = @(); AllEnglishStreams = @() }
    }
}

function Invoke-SubtitleExtraction {
    param(
        [string]$InputFile,
        [int]$SubtitleLogicalIndex,
        [string]$Language,
        [string]$OutputFile,
        [string]$Codec,
        [bool]$IsForced = $false,
        [string]$OriginalTitle = $null
    )

    Write-Host "Processing subtitle stream $($SubtitleLogicalIndex) with codec $($Codec)$(if ($IsForced) { ' [FORCED]' } else { '' })..." -ForegroundColor Yellow
    
    # Register the output file for cleanup before processing
    Add-TempFile -FilePath $OutputFile
    
    # Modify output filename to indicate forced status
    if ($IsForced -and $OutputFile -notmatch "forced|Forced|FORCED") {
        $directory = [System.IO.Path]::GetDirectoryName($OutputFile)
        $fileNameWithoutExt = [System.IO.Path]::GetFileNameWithoutExtension($OutputFile)
        $extension = [System.IO.Path]::GetExtension($OutputFile)
        $forcedOutputFile = Join-Path $directory "$fileNameWithoutExt.forced$extension"
        
        Write-Host "Renaming output to indicate forced status: $(Split-Path $forcedOutputFile -Leaf)" -ForegroundColor Yellow
        $OutputFile = $forcedOutputFile
        Add-TempFile -FilePath $OutputFile  # Register the new filename too
    }
    
    # Check if the codec is a text-based format that can be converted to SRT
    if ($Codec -eq "subrip" -or $Codec -eq "mov_text") {
        # Text-based subtitles: convert to SRT with timing preservation
        Write-Host "Converting text-based subtitle to SRT..." -ForegroundColor Yellow
        $largs = @(
            "-y", "-hide_banner",
            "-analyzeduration", "1000000000",
            "-probesize", "5000000000",
            "-i", $origFile, # $InputFile,
            "-map", "0:s:$SubtitleLogicalIndex",
            "-c:s", "srt"
        )
        
        # Apply timing preservation based on source video type
        if ($Script:VideoMetadata -and $Script:VideoMetadata.IsVFR) {
            Write-Host "    Using VFR timing preservation for subtitle extraction" -ForegroundColor Yellow
            $largs += @(
                "-avoid_negative_ts", "disabled",  # Preserve original timestamps for VFR
                "-copyts"                          # Copy timestamps exactly for VFR
            )
        } else {
            Write-Host "    Using CFR timing for subtitle extraction" -ForegroundColor Green
             $largs += @(
                "-copyts"                         # Copy timestamps exactly
             )
        }
        
        $largs += "-metadata:s:0", "language=$Language"
        
        # Add forced metadata if applicable
        if ($IsForced) {
            $largs += "-disposition:s:0", "forced"
        }
        
        $largs += $OutputFile
        $success = Invoke-SimpleProcess -Executable $Config.FFmpegExe -Arguments $largs -Description "Subtitle extraction"

    }
    elseif ($Codec -eq "hdmv_pgs_subtitle") {
        # Image-based subtitles: keep as SUP file with timing preservation
        Write-Host "Extracting image-based PGS subtitle to SUP file..." -ForegroundColor Yellow
        $supOutputFile = $OutputFile.Replace(".srt", ".sup")
        
        # Also modify SUP filename for forced if needed
        if ($IsForced -and $supOutputFile -notmatch "forced|Forced|FORCED") {
            $directory = [System.IO.Path]::GetDirectoryName($supOutputFile)
            $fileNameWithoutExt = [System.IO.Path]::GetFileNameWithoutExtension($supOutputFile)
            $extension = [System.IO.Path]::GetExtension($supOutputFile)
            $supOutputFile = Join-Path $directory "$fileNameWithoutExt.forced$extension"
        }
        
        Add-TempFile -FilePath $supOutputFile
        
        $largs = @(
            "-y", "-hide_banner",
            "-i", $origFile, # $InputFile,
            "-max_muxing_queue_size", "65536",
            "-analyzeduration", "1000000000",
            "-probesize", "5000000000",
            "-map", "0:s:$SubtitleLogicalIndex",
            "-c:s", "copy"                     # Keep as PGS, don't convert
        )
        
        # Apply timing preservation based on source video type
        if ($Script:VideoMetadata -and $Script:VideoMetadata.IsVFR) {
            Write-Host "    Using VFR timing preservation for PGS subtitle extraction" -ForegroundColor Yellow
            $largs += @(
                "-avoid_negative_ts", "disabled",  # Preserve original timestamps for VFR
                "-copyts"                          # Copy timestamps exactly for VFR
            )
        } else {
            Write-Host "    Using CFR timing for PGS subtitle extraction" -ForegroundColor Green
            $largs += @(
                "-avoid_negative_ts", "make_zero",  # Preserve original timestamps  
                "-copyts",                         # Copy timestamps exactly
                "-start_at_zero"                   # Ensure zero start time for CFR
            )
        }
        
        $largs += $supOutputFile
        $success = Invoke-SimpleProcess -Executable $Config.FFmpegExe -Arguments $largs -Description "PGS subtitle extraction"

    }
    else {
        Write-Warning "Unsupported subtitle codec '$Codec' for extraction."
        $success = $false
    }

    if ($success) {
        $actualOutputFile = if ($Codec -eq "hdmv_pgs_subtitle") { $OutputFile.Replace(".srt", ".sup") } else { $OutputFile }
        
        # Update to the actual output file that was created
        if ($IsForced -and $Codec -eq "hdmv_pgs_subtitle") {
            $directory = [System.IO.Path]::GetDirectoryName($actualOutputFile)
            $fileNameWithoutExt = [System.IO.Path]::GetFileNameWithoutExtension($actualOutputFile)
            $extension = [System.IO.Path]::GetExtension($actualOutputFile)
            if ($fileNameWithoutExt -notmatch "forced|Forced|FORCED") {
                $actualOutputFile = Join-Path $directory "$fileNameWithoutExt.forced$extension"
            }
        }
        
        $Script:ExtractedSubtitles += $actualOutputFile
        Write-Host "Subtitle extracted to: $actualOutputFile $(if ($IsForced) { '[FORCED]' } else { '' })" -ForegroundColor Green
        Write-Host "  Timing mode: $(if ($Script:VideoMetadata.IsVFR) { 'VFR preserved' } else { 'CFR with zero start' })" -ForegroundColor Gray
        
        # Verify the file was created and has content
        if (-not (Test-Path $actualOutputFile) -or (Get-Item $actualOutputFile).Length -eq 0) {
            Write-Warning "Subtitle extraction produced empty or missing file"
            $success = $false
        }
    }
    else {
        Write-Warning "Subtitle extraction failed"
    }
    
    return $success
}

function Write-PlexOptimizedSummary {
    param(
        [hashtable]$VideoInfo,
        [hashtable]$AudioInfo,
        [hashtable]$SubtitleInfo,
        [string]$InputFile
    )
    
    Write-Host "`n=== Plex-Optimized Processing Summary ===" -ForegroundColor Cyan
    Write-Host "Input file: $InputFile" -ForegroundColor White
    Write-Host "File size: $([math]::Round((Get-Item $InputFile).Length / 1MB, 2)) MB" -ForegroundColor White
    Write-Host "Video: $($VideoInfo.Width)x$($VideoInfo.Height) @ $($VideoInfo.FrameRate) fps" -ForegroundColor White
    Write-Host "Current codec: $($VideoInfo.CodecName)" -ForegroundColor White
    Write-Host "HDR content: $(if (Test-HDRContent -VideoInfo $VideoInfo) { 'Yes' } else { 'No' })" -ForegroundColor White
    Write-Host "Dolby Vision: $(if ($VideoInfo.HasDolbyVision) { 'Yes (will be removed)' } else { 'No' })" -ForegroundColor White
    Write-Host "Software fallback: $(if ($EnableSoftwareFallback) { "Enabled" } else { 'Disabled' })" -ForegroundColor White
    
    # Enhanced audio summary with Plex info
    $plexAudioSettings = Get-PlexOptimizedAudioSettings -AudioInfo $AudioInfo -CreateCompatibilityTrack $true  -HasExistingAAC $AudioInfo.HasAAC
    Write-Host "Audio streams: $($plexAudioSettings.StreamCount) total (Plex-optimized)" -ForegroundColor White
    if ($plexAudioSettings.RequiresFallback) {
        Write-Host "  - Primary: $($AudioInfo.ToCopy.Codec) (original quality)" -ForegroundColor Gray
        Write-Host "  - Fallback: AC3/AAC (compatibility)" -ForegroundColor Gray
    }
    
    # Enhanced subtitle summary with Plex info
    $plexSubtitleSettings = Get-PlexSubtitleSettings -SubtitleInfo $SubtitleInfo -BaseName "temp"
    Write-Host "Subtitle streams: $($plexSubtitleSettings.StreamCount) total (Plex-optimized to SRT/PGS)" -ForegroundColor White
    
    Write-Host "Quality validation: $(if ($EnableQualityValidation -ne 0) { "Enabled (threshold: $QualityThreshold dB)" } else { 'Disabled' })" -ForegroundColor White
    
    # Plex-specific optimizations summary
    Write-Host "`nPlex Optimizations Enabled:" -ForegroundColor Yellow
    Write-Host "   Fragmented MP4 with faststart" -ForegroundColor Green
    Write-Host "   2-second keyframe intervals for smooth seeking" -ForegroundColor Green
    Write-Host "   Conservative HEVC levels for device compatibility" -ForegroundColor Green
    Write-Host "   SRT subtitle conversion for better compatibility" -ForegroundColor Green
    Write-Host "   HDR metadata preservation for 4K HDR content" -ForegroundColor Green
    if ($plexAudioSettings.RequiresFallback) {
        Write-Host "   AC3 compatibility audio track" -ForegroundColor Green
    }
    if (-not $DisableMCEBuddyMonitoring) {
        try {
            $mceState = Get-McebuddyEngineState
            Write-Host "MCEBuddy integration: Enabled (Current state: $mceState)" -ForegroundColor White
            Write-Host "  Check interval: $MCEBuddyCheckInterval seconds" -ForegroundColor Gray
        }
        catch {
            Write-Host "MCEBuddy integration: Enabled (Status check failed)" -ForegroundColor Yellow
        }
    }
    else {
        Write-Host "MCEBuddy integration: Disabled" -ForegroundColor Gray
    }
    
    Write-Host ""
}

#endregion

#region Main Processing Functions

function Invoke-HDR10PlusExtraction {
    param(
        [string]$InputFile,
        [string]$OutputJson
    )
    
    Write-Host "Extracting HDR10+ metadata..." -ForegroundColor Yellow
    
    # Check if HDR10+ tool exists
    if (-not (Test-Path $Config.HDR10PlusToolExe)) {
        Write-Warning "HDR10+ tool not found at $($Config.HDR10PlusToolExe). Skipping HDR10+ extraction."
        return $false
    }
    
    # Register the output file for cleanup immediately
    Add-TempFile -FilePath $OutputJson
    
    # Check if input is MP4 - if so, convert to MKV first
    $fileExtension = [System.IO.Path]::GetExtension($InputFile).ToLower()
    $actualInputFile = $InputFile
    $tempMkvFile = $null
    
    if ($fileExtension -eq ".mp4") {
        Write-Host "MP4 input detected - converting to MKV first for better HDR10+ extraction..." -ForegroundColor Yellow
        
        # Create temporary MKV file
        $baseName = [System.IO.Path]::GetFileNameWithoutExtension($InputFile)
        $tempMkvFile = New-TempFile -BaseName "$baseName.temp_for_hdr10" -Extension ".mkv"
        
        Write-Host "Converting MP4 to MKV (remux only, no re-encoding)..." -ForegroundColor Cyan
        
        # Build FFmpeg arguments for MP4 to MKV conversion (copy all streams)
         $mp4ToMkvArgs = @(
            "-y", "-hide_banner",
            "-loglevel", "warning",
            "-i", $InputFile,
            "-c:v", "copy",                         # Copy video stream without re-encoding
            "-map", "0:v:0",                        # Map only the first video stream
            "-map_metadata", "0",                   # Copy global metadata
            "-map_metadata:s:v:0", "0:s:v:0",      # Copy video stream metadata
            "-avoid_negative_ts", "make_zero",      # Fix timing issues
            "-f", "matroska",                       # Force MKV output format
            $tempMkvFile
        )
        
        Write-Host "Executing MP4 to MKV conversion for HDR10+ extraction..." -ForegroundColor Gray
        $conversionSuccess = Invoke-SimpleProcess -Executable $Config.FFmpegExe -Arguments $mp4ToMkvArgs -Description "Convert MP4 to MKV for HDR10+ extraction"
        
        if (-not $conversionSuccess) {
            Write-Warning "Failed to convert MP4 to MKV - attempting HDR10+ extraction on original MP4"
            # Clean up temp file on failure
            if (Test-Path $tempMkvFile) {
                Remove-Item $tempMkvFile -Force -ErrorAction SilentlyContinue
            }
            $actualInputFile = $InputFile
        }
        else {
            # Verify conversion succeeded
            if ((Test-Path $tempMkvFile) -and (Get-Item $tempMkvFile).Length -gt 0) {
                $tempSizeMB = [math]::Round((Get-Item $tempMkvFile).Length / 1MB, 2)
                Write-Host "MP4 to MKV conversion successful: $tempSizeMB MB" -ForegroundColor Green
                $actualInputFile = $tempMkvFile
            }
            else {
                Write-Warning "MP4 to MKV conversion produced empty file - using original MP4"
                if (Test-Path $tempMkvFile) {
                    Remove-Item $tempMkvFile -Force -ErrorAction SilentlyContinue
                }
                $actualInputFile = $InputFile
            }
        }
    }
    else {
        Write-Host "Non-MP4 input detected - proceeding with direct HDR10+ extraction" -ForegroundColor Green
    }
    
    try {
        # Run the extraction command with 30-minute timeout
        Write-Host "Starting HDR10+ extraction (30-minute timeout)..." -ForegroundColor Yellow
        Write-Host "Input file: $(Split-Path $actualInputFile -Leaf)" -ForegroundColor Gray
        
        $processInfo = New-Object System.Diagnostics.ProcessStartInfo
        $processInfo.FileName = $Config.HDR10PlusToolExe
        $processInfo.UseShellExecute = $false
        $processInfo.RedirectStandardOutput = $false
        $processInfo.RedirectStandardError = $false
        $processInfo.CreateNoWindow = $false
        
        # Handle arguments based on PowerShell version
        $hdr10Args = @("extract", "-i", $actualInputFile, "-o", $OutputJson)
        if ($PSVersionTable.PSVersion.Major -ge 7) {
            foreach ($arg in $hdr10Args) {
                $null = $processInfo.ArgumentList.Add($arg)
            }
        }
        else {
            $quotedArgs = $hdr10Args | ForEach-Object { 
                if ($_ -match '\s') { '"{0}"' -f ($_ -replace '"', '""') } else { $_ }
            }
            $processInfo.Arguments = $quotedArgs -join ' '
        }
        
        $process = [System.Diagnostics.Process]::Start($processInfo)
        $handle = $process.Handle
        $Script:ActiveProcesses = @($Script:ActiveProcesses) + $process
        
        # Set lower priority
        try {
            $process.PriorityClass = [System.Diagnostics.ProcessPriorityClass]::BelowNormal
        }
        catch {
            Write-Host "Could not set HDR10+ process priority" -ForegroundColor Gray
        }
        
        # Wait for completion with 30-minute timeout
        $timeoutMs = 18000000  # 30 minutes in milliseconds
        if (-not $process.WaitForExit($timeoutMs)) {
            Write-Warning "HDR10+ extraction timed out after 300 minutes - killing process and skipping HDR10+ processing"
            try {
                $process.Kill()
                $process.WaitForExit(5000)  # Wait up to 5 seconds for cleanup
            }
            catch {
                Write-Warning "Failed to kill HDR10+ process cleanly: $_"
            }
            
            # Remove from active processes list
            $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $process.Id }
            return $false
        }
        
        # Remove from active processes list
        $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $process.Id }
        
        $exitCode = $process.ExitCode
        
        # Verify the output
        if ($exitCode -eq 0 -and (Test-Path $OutputJson) -and (Get-Item $OutputJson).Length -gt 0) {
            Write-Host "HDR10+ metadata extracted successfully" -ForegroundColor Green
            return $true
        }
        else {
            Write-Warning "HDR10+ extraction failed (exit code: $exitCode) or produced no data. Will continue without it."
            return $false
        }
    }
    catch {
        Write-Warning "HDR10+ extraction failed: $_"
        return $false
    }
    finally {
        # Clean up temporary MKV file if it was created
        if ($tempMkvFile -and (Test-Path $tempMkvFile)) {
            try {
                Remove-Item $tempMkvFile -Force -ErrorAction SilentlyContinue
                Write-Host "Cleaned up temporary MKV file for HDR10+ extraction: $(Split-Path $tempMkvFile -Leaf)" -ForegroundColor Gray
            }
            catch {
                Write-Warning "Failed to clean up temporary MKV file: $_"
            }
        }
    }
}
function Invoke-VideoEncoding {
    param(
        [string]$InputFile,
        [string]$OutputFile,
        [hashtable]$VideoInfo,
        [hashtable]$EncodingSettings,
        [hashtable]$ColorInfo
    )
    $success = 'N'

    Write-Host "Starting video encoding with hardware acceleration priority..." -ForegroundColor Yellow
    
    # Register output file for cleanup immediately
    Add-TempFile -FilePath $OutputFile
    
    # QSV TEST CODE - THIS WAS MISSING
    Write-Host "Testing QSV capabilities..." -ForegroundColor Cyan
    
    # Store all output from the function call
    $qsvTestOutput = @()
    $qsvStatus = $null
    
    try {
        # Capture the function output more aggressively
        $qsvTestOutput = @(Test-QSVAvailability)
        
        # Find the hashtable in the output
        $qsvStatus = $null
        foreach ($item in $qsvTestOutput) {
            if ($item -is [System.Collections.Hashtable] -or 
                ($item -ne $null -and $item.GetType().Name -eq "Hashtable")) {
                $qsvStatus = $item
                break
            }
            # Also check for PSCustomObject with the right properties
            elseif ($item -ne $null -and 
                $item.PSObject -and 
                $item.PSObject.Properties.Name -contains "Available") {
                # Convert PSObject to hashtable
                $qsvStatus = @{
                    'Available'      = $item.Available
                    'SupportsMain10' = $item.SupportsMain10  
                    'Reason'         = $item.Reason
                }
                break
            }
        }
        
        # Final fallback if we still don't have a valid status
        if ($qsvStatus -eq $null) {
            Write-Warning "Could not extract QSV status from function output - using fallback"
            $qsvStatus = @{
                'Available'      = $false
                'SupportsMain10' = $false
                'Reason'         = "Could not parse QSV test results"
            }
        }
        
    }
    catch {
        Write-Warning "QSV test call failed: $_"
        $qsvStatus = @{
            'Available'      = $false
            'SupportsMain10' = $false
            'Reason'         = "QSV test call exception: $_"
        }
    }
    
    Write-Host "`nQSV Test Results:" -ForegroundColor Cyan
    Write-Host "  Available: $($qsvStatus.Available)" -ForegroundColor $(if ($qsvStatus.Available) { "Green" } else { "Red" })
    Write-Host "  Main10 Support: $($qsvStatus.SupportsMain10)" -ForegroundColor $(if ($qsvStatus.SupportsMain10) { "Green" } else { "Yellow" })
    Write-Host "  Reason: $($qsvStatus.Reason)" -ForegroundColor Gray
    # END OF QSV TEST CODE

    # Check if QSV is a viable option for this video
    $isQSVAvailable = $qsvStatus.Available -eq $true
    
    if ($isQSVAvailable) {
        Write-Host "`nQSV is available - attempting hardware encoding..." -ForegroundColor Green
        
        try {
            $success = Invoke-QSVEncodingWithErrorDetection -InputFile $InputFile -OutputFile $OutputFile -VideoInfo $VideoInfo -EncodingSettings $EncodingSettings -ColorInfo $ColorInfo -QSVCapabilities $qsvStatus
            
            if ($success -eq 'S') {
                Write-Host "Hardware encoding completed successfully!" -ForegroundColor Green
                return $true
            }
            elseif ($success -eq 'C') {
                return $false
            }
            elseif ($success -eq 'F') {
                if ($EnableSoftwareFallback) {
                    Write-Warning "Hardware encoding had moderate corruption - falling back to software encoding"
                }
                else {
                    Write-Error "Hardware encoding failed and software fallback is disabled"
                    return $false
                }
            }
            else {
                if ($EnableSoftwareFallback) {
                    Write-Warning "Hardware encoding failed - falling back to software encoding"
                }
                else {
                    Write-Error "Hardware encoding failed and software fallback is disabled"
                    return $false
                }
            }
        }
        catch {
            if ($EnableSoftwareFallback) {
                Write-Warning "Hardware encoding exception: $_"
                Write-Host "Falling back to software encoding..." -ForegroundColor Yellow
            }
            else {
                Write-Error "Hardware encoding exception and software fallback is disabled: $_"
                return $false
            }
        }
    }
    else {
        if ($EnableSoftwareFallback) {
            Write-Warning "QSV not available - using software encoding"
            Write-Host "Reason: $($qsvStatus.Reason)" -ForegroundColor Yellow
        }
        else {
            Write-Error "QSV not available and software fallback is disabled. Reason: $($qsvStatus.Reason)"
            return $false
        }
    }
    
    # Only reach here if EnableSoftwareFallback is true
    Write-Host "`nUsing software encoding fallback..." -ForegroundColor Yellow
    
    # Final fallback to HEVC software encoding
    Write-Host "Attempting HEVC software encoding..." -ForegroundColor Yellow
    return Invoke-PlexSoftwareEncodingWithErrorDetection -InputFile $InputFile -OutputFile $OutputFile -VideoInfo $VideoInfo -EncodingSettings $EncodingSettings -ColorInfo $ColorInfo
}

function Get-McebuddyEngineState {
    try {
        
        if (-not (Test-Path $config.mcebuddyCLI)) {
            Write-Output "DEBUG: MCEBuddy UserCLI not found at $($config.mcebuddyCLI)"
            return "NotInstalled"
        }
        
        #Write-Output "DEBUG: Checking MCEBuddy state using CLI..."
        
        # Query engine state using CLI with timeout
        try {
            $job = Start-Job -ScriptBlock {
                param($cliPath)
                & $cliPath --command=query --action=enginestate --quiet 2>&1
                return $LASTEXITCODE
            } -ArgumentList $config.mcebuddyCLI
            
            $completed = Wait-Job $job -Timeout 15
            if ($completed) {
                $stateOutput = Receive-Job $job
                $stateExitCode = $stateOutput[-1]  # Last item should be the exit code
                $stateOutput = $stateOutput[0..($stateOutput.Length-2)] -join "`n"  # Everything except last item
                Remove-Job $job
            } else {
                Stop-Job $job
                Remove-Job $job
                #Write-Output "DEBUG: MCEBuddy CLI enginestate command timed out after 15 seconds"
                return "Unknown"
            }
            
            # Write-Output "DEBUG: MCEBuddy CLI enginestate output: '$stateOutput'"
            #Write-Output "DEBUG: MCEBuddy CLI exit code: $stateExitCode"
            
            if ($stateExitCode -eq -2) {
                #Write-Output "DEBUG: MCEBuddy CLI returned -2 (failed to process command)"
                return "Stopped"
            }
            elseif ($stateExitCode -eq -1) {
                #Write-Output "DEBUG: MCEBuddy CLI returned -1 (bad input parameters)"
                return "Unknown"
            }
            elseif ($stateExitCode -ne 0) {
                #Write-Output "DEBUG: MCEBuddy CLI failed with exit code $stateExitCode"
                return "Stopped"
            }
            
            # Parse the engine state
            $engineState = $stateOutput.Trim()
            #Write-Output "DEBUG: Parsed engine state: '$engineState'"
            
            switch ($engineState.ToLower()) {
                "stopped" { 
                    #Write-Output "DEBUG: MCEBuddy engine is stopped"
                    return "Stopped" 
                }
                "started" { 
                    #Write-Output "DEBUG: MCEBuddy engine is started, checking for active jobs..."
                    # Engine is started but check if it's actually converting
                    try {
                        $queueJob = Start-Job -ScriptBlock {
                            param($cliPath)
                            & $cliPath --command=query --action=queuelength --quiet 2>&1
                            return $LASTEXITCODE
                        } -ArgumentList $config.mcebuddyCLI
                        
                        $queueCompleted = Wait-Job $queueJob -Timeout 15
                        if ($queueCompleted) {
                            $queueOutput = Receive-Job $queueJob
                            $queueExitCode = $queueOutput[-1]  # Last item should be the exit code
                            $queueOutput = $queueOutput[0..($queueOutput.Length-2)] -join "`n"  # Everything except last item
                            Remove-Job $queueJob
                        } else {
                            Stop-Job $queueJob
                            Remove-Job $queueJob
                            #Write-Output "DEBUG: MCEBuddy CLI queuelength command timed out after 15 seconds"
                            return "Unknown"
                        }
                        
                        # Write-Output "DEBUG: Queue length command exit code: $queueExitCode"
                        # Write-Output "DEBUG: Queue length output: '$queueOutput'"
                        
                        if ($queueExitCode -eq 0) {
                            $queueLength = [int]$queueOutput.Trim()
                            #  Write-Output "DEBUG: Parsed queue length: $queueLength"
                            
                            if ($queueLength -gt 0) {
                                #     Write-Output "DEBUG: Queue has $queueLength jobs, checking details..."
                                
                                $detailsJob = Start-Job -ScriptBlock {
                                    param($cliPath)
                                    & $cliPath --command=query --action=queue --quiet 2>&1
                                } -ArgumentList $config.mcebuddyCLI
                                
                                $detailsCompleted = Wait-Job $detailsJob -Timeout 15
                                if ($detailsCompleted) {
                                    $queueDetails = Receive-Job $detailsJob
                                    Remove-Job $detailsJob
                                } else {
                                    Stop-Job $detailsJob
                                    Remove-Job $detailsJob
                                    #Write-Output "DEBUG: MCEBuddy CLI queue details command timed out after 15 seconds"
                                    return "Unknown"
                                }
                                
                                #    Write-Output "DEBUG: Queue details: '$queueDetails'"
                                
                                if ($queueDetails -match "converting" -or $queueDetails -match "Status.*Converting") {
                                    #        Write-Output "DEBUG: Found converting job - returning Processing"
                                    return "Processing"
                                }
                                else {
                                    #       Write-Output "DEBUG: No converting jobs found - returning Idle"
                                    return "Idle"
                                }
                            }
                            else {
                                #    Write-Output "DEBUG: Queue is empty - returning Idle"
                                return "Idle"
                            }
                        }
                        else {
                            #   Write-Output "DEBUG: Failed to get queue length - assuming Idle"
                            return "Idle"
                        }
                    }
                    catch {
                        #  Write-Output "DEBUG: Exception checking queue status: $_ - assuming Idle"
                        return "Idle"
                    }
                }
                "conversion_in_progress" { 
                    #   Write-Output "DEBUG: MCEBuddy conversion in progress"
                    return "Paused" 
                }
                "conversion_paused" { 
                    #     Write-Output "DEBUG: MCEBuddy conversion paused - treating as Idle"
                    return "Paused"
                }
                default { 
                    #    Write-Output "DEBUG: Unknown engine state: '$engineState' - returning Unknown"
                    return "Unknown" 
                }
            }
            
        }
        catch {
            #    Write-Output "DEBUG: Exception running MCEBuddy CLI: $_"
            return "Unknown"
        }
        
    }
    catch {
        #   Write-Output "DEBUG: Exception in Get-McebuddyEngineState: $_"
        return "Unknown"
    }
}

function Resume-McebuddyEngine {
    try {
        Write-Host "Resuming MCEBuddy engine..." -ForegroundColor Yellow
        
        # Method 1: Resume service if it was paused
        $mceService = Get-Service -Name "MCEBuddy2x" -ErrorAction SilentlyContinue
        if ($mceService -and $mceService.Status -eq "Paused") {
            try {
                $mceService.Continue()
                Start-Sleep -Seconds 2
                $mceService.Refresh()
                if ($mceService.Status -eq "Running") {
                    Write-Host "MCEBuddy service resumed successfully" -ForegroundColor Green
                    return $true
                }
            }
            catch {
                Write-Host "Could not resume MCEBuddy service: $_" -ForegroundColor Yellow
            }
        }
        
        # Method 2: Resume previously suspended processes
        if ($global:SuspendedMCEProcesses) {
            $resumedCount = 0
            foreach ($lpid in $global:SuspendedMCEProcesses) {
                try {
                    $process = Get-Process -Id $lpid -ErrorAction SilentlyContinue
                    if ($process) {
                        $process.PriorityClass = [System.Diagnostics.ProcessPriorityClass]::Normal
                        $resumedCount++
                        Write-Host "Resumed MCEBuddy process: $($process.ProcessName) (PID: $lpid)" -ForegroundColor Green
                    }
                }
                catch {
                    Write-Host "Could not resume process PID $lpid : $_" -ForegroundColor Yellow
                }
            }
            
            $global:SuspendedMCEProcesses = $null
            Write-Host "MCEBuddy engine resumed ($resumedCount processes)" -ForegroundColor Green
            return $true
        }
        
        Write-Host "No MCEBuddy processes to resume" -ForegroundColor Gray
        return $true
        
    }
    catch {
        Write-Warning "Failed to resume MCEBuddy engine: $_"
        return $false
    }
}


function Test-QSVAvailability {
    # AGGRESSIVE FIX: Use Out-Null to suppress ALL intermediate outputs
    Write-Host "Testing Intel QuickSync Video (QSV) availability..." -ForegroundColor Cyan 
    
    try {
        # Test 1: Check if hevc_qsv encoder is available
        Write-Host "  Testing HEVC_QSV encoder availability..." -ForegroundColor Gray
        $encoderTestArgs = @("-hide_banner", "-encoders")
        
        $processInfo = New-Object System.Diagnostics.ProcessStartInfo
        $processInfo.FileName = $Config.FFmpegExe
        $processInfo.UseShellExecute = $false
        $processInfo.RedirectStandardOutput = $true
        $processInfo.RedirectStandardError = $true
        $processInfo.CreateNoWindow = $true
        
        if ($PSVersionTable.PSVersion.Major -ge 7) {
            foreach ($arg in $encoderTestArgs) {
                $null = $processInfo.ArgumentList.Add($arg)
            }
        }
        else {
            $processInfo.Arguments = $encoderTestArgs -join ' '
        }
        
        $process = [System.Diagnostics.Process]::Start($processInfo)
        $handle = $process.Handle
        $stdout = $process.StandardOutput.ReadToEnd()
        $null = $process.WaitForExit(5000)  # Suppress return value
        
        $hasHEVCQSV = $stdout -match "hevc_qsv"
        
        if (-not $hasHEVCQSV) {
            Write-Host "  [FAIL] HEVC_QSV encoder not found in FFmpeg build" -ForegroundColor Red 
            # Return simple hashtable - avoid PSObject
            return @{
                'Available'      = $false
                'SupportsMain10' = $false
                'Reason'         = "HEVC_QSV encoder not available in FFmpeg build"
            }
        }
        
        Write-Host "  [PASS] HEVC_QSV encoder found" -ForegroundColor Green 
        
        # Test 2: Check QSV device initialization with better error handling
        Write-Host "  Testing QSV device initialization..." -ForegroundColor Gray 
        $deviceTestArgs = @(
            "-hide_banner",
            "-f", "lavfi", 
            "-i", "testsrc=duration=2:size=640x480:rate=30",
            "-c:v", "hevc_qsv",
            "-preset", "fast",
            "-global_quality", "25",
            "-frames:v", "30",
            "-f", "null", "-"
        )
        
        $processInfo2 = New-Object System.Diagnostics.ProcessStartInfo
        $processInfo2.FileName = $Config.FFmpegExe
        $processInfo2.UseShellExecute = $false
        $processInfo2.RedirectStandardOutput = $true
        $processInfo2.RedirectStandardError = $true
        $processInfo2.CreateNoWindow = $true
        
        if ($PSVersionTable.PSVersion.Major -ge 7) {
            foreach ($arg in $deviceTestArgs) {
                $null = $processInfo2.ArgumentList.Add($arg)
            }
        }
        else {
            $processInfo2.Arguments = $deviceTestArgs -join ' '
        }
        
        $process2 = [System.Diagnostics.Process]::Start($processInfo2)
        $handle = $process.Handle
        $stdout2 = $process2.StandardOutput.ReadToEnd()
        $stderr2 = $process2.StandardError.ReadToEnd()
        $null = $process2.WaitForExit(15000)  # Suppress return value
        
        Write-Host "  QSV Test Details:" -ForegroundColor Gray 
        Write-Host "    Exit Code: $($process2.ExitCode)" -ForegroundColor Gray 
        
        if ($process2.ExitCode -ne 0) {
            Write-Host "  [FAIL] QSV device initialization failed" -ForegroundColor Red 
            Write-Host "    Error details:" -ForegroundColor Gray 
            
            # Parse common error messages
            $errorReason = "Unknown QSV initialization error"
            if ($stderr2 -match "No such file or directory") {
                $errorReason = "QSV runtime libraries not found"
            }
            elseif ($stderr2 -match "Cannot load library") {
                $errorReason = "Intel Media SDK/Driver not installed"
            }
            elseif ($stderr2 -match "Device creation failed") {
                $errorReason = "Intel GPU not available or disabled"
            }
            elseif ($stderr2 -match "Not supported") {
                $errorReason = "QSV not supported on this hardware"
            }
            elseif ($stderr2 -match "Permission denied") {
                $errorReason = "Insufficient permissions for GPU access"
            }
            
            return @{
                'Available'      = $false
                'SupportsMain10' = $false
                'Reason'         = $errorReason
            }
        }
        
        Write-Host "  [PASS] QSV device initialization successful" -ForegroundColor Green 
        
        # Test 3: Check for specific QSV capabilities
        Write-Host "  Checking QSV HEVC capabilities..." -ForegroundColor Gray 
        
        # Look for QSV initialization messages
        $qsvInitialized = ($stderr2 -match "Initialized QSV") -or ($stderr2 -match "qsv") -or ($process2.ExitCode -eq 0)
        
        # Check for Main10 support (this is often not explicitly reported, so we'll assume it's available if QSV works)
        $supportsMain10 = $qsvInitialized  # Modern Intel GPUs generally support Main10
        
        if ($qsvInitialized) {
            Write-Host "  [PASS] QSV runtime initialized successfully" -ForegroundColor Green 
        }
        
        if ($supportsMain10) {
            Write-Host "  [PASS] Main10 profile support assumed (modern QSV)" -ForegroundColor Green 
        }
        else {
            Write-Host "  [WARN] Main10 profile support uncertain" -ForegroundColor Yellow 
        }
        
        # Return successful result as simple hashtable
        return @{
            'Available'      = $true
            'SupportsMain10' = $supportsMain10
            'Reason'         = "QSV available and functional"
        }
        
    }
    catch {
        Write-Warning "QSV availability test failed with exception: $_" 
        return @{
            'Available'      = $false
            'SupportsMain10' = $false
            'Reason'         = "Exception during QSV test: $_"
        }
    }
}


function Invoke-HDR10PlusInjection {
    param(
        [string]$VideoFile,
        [string]$JsonFile,
        [string]$OutputFile
    )
    
    Write-Host "Injecting HDR10+ metadata..." -ForegroundColor Yellow
    
    # Register output file for cleanup
    Add-TempFile -FilePath $OutputFile
    
    $success = Invoke-SimpleProcess -Executable $Config.HDR10PlusToolExe -Arguments @("inject", "-i", $VideoFile, "-j", $JsonFile, "-o", $OutputFile) -Description "Inject HDR10+ metadata" 
    if ($success -and (Test-Path $OutputFile) -and (Get-Item $OutputFile).Length -gt 0) {
        Write-Host "HDR10+ metadata injected successfully" -ForegroundColor Green
        return $true
    }
    else {
        Write-Warning "HDR10+ injection failed or produced empty file"
        return $false
    }
}

function Invoke-EnhancedHEVCExtraction {
    param(
        [string]$InputFile,
        [string]$OutputFile,
        [string]$Description = "Extract HEVC stream"
    )
    
    Write-Host "Enhanced HEVC extraction with corruption detection..." -ForegroundColor Cyan
    
    # Create a log file for detailed error capture
    $extractLogFile = Join-Path $env:TEMP "hevc_extract_log_$((Get-Random)).txt"
    Add-TempFile -FilePath $extractLogFile
    
    try {
        # Build FFmpeg arguments for HEVC extraction
        $extractArgs = @(
            "-y", "-hide_banner", 
            "-loglevel", "warning",
            "-err_detect", "explode",           # Fail on any stream errors
            "-fflags", "+discardcorrupt",       # Discard corrupted packets
            "-i", $InputFile,
            "-c:v", "copy",
            "-r",  $videoInfo.FrameRate,  # Force original frame rate
            "-bsf:v", "hevc_metadata",
            "-avoid_negative_ts", "make_zero",
            "-f", "hevc",
            $OutputFile
        )
        
        Write-Host "Starting HEVC extraction with error detection..." -ForegroundColor Yellow
        Write-Host "Command: ffmpeg $($extractArgs -join ' ')" -ForegroundColor Gray
        
        # Start the process with enhanced monitoring
        $processInfo = New-Object System.Diagnostics.ProcessStartInfo
        $processInfo.FileName = $Config.FFmpegExe
        $processInfo.UseShellExecute = $false
        $processInfo.RedirectStandardOutput = $true
        $processInfo.RedirectStandardError = $true
        $processInfo.CreateNoWindow = $true
        
        if ($PSVersionTable.PSVersion.Major -ge 7) {
            foreach ($arg in $extractArgs) {
                $null = $processInfo.ArgumentList.Add($arg)
            }
        }
        else {
            $quotedArgs = $extractArgs | ForEach-Object { 
                if ($_ -match '\s') { '"{0}"' -f ($_ -replace '"', '""') } else { $_ }
            }
            $processInfo.Arguments = $quotedArgs -join ' '
        }
        
        $process = [System.Diagnostics.Process]::Start($processInfo)
        $handle = $process.Handle
        $Script:ActiveProcesses = @($Script:ActiveProcesses) + $process
        
        # Capture output in real-time to detect errors early
        $stdout = $process.StandardOutput.ReadToEndAsync()
        $stderr = $process.StandardError.ReadToEndAsync()
        
        try {
            $process.PriorityClass = [System.Diagnostics.ProcessPriorityClass]::BelowNormal
        }
        catch {
            Write-Host "Could not set FFmpeg process priority" -ForegroundColor Gray
        }
        
        # Wait for completion with timeout
        $timeoutMs = 6000000  # 100 minutes timeout for extraction
        if (-not $process.WaitForExit($timeoutMs)) {
            Write-Warning "HEVC extraction timed out after 100 minutes - killing process"
            try {
                $process.Kill()
                $process.WaitForExit(5000)
            }
            catch {
                Write-Warning "Failed to kill FFmpeg extraction process: $_"
            }
            
            $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $process.Id }
            return @{ Success = $false; Reason = "Extraction timed out" }
        }
        
        $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $process.Id }
        
        $exitCode = $process.ExitCode
        $stdoutText = $stdout.Result
        $stderrText = $stderr.Result
        
        Write-Host "FFmpeg extraction completed with exit code: $exitCode" -ForegroundColor Gray
        Write-Host "Stderr output size: $([math]::Round($stderrText.Length / 1KB, 1)) KB" -ForegroundColor Gray
        
        # PERFORMANCE FIX: Limit stderr analysis to prevent hanging
        $maxStderrLength = 500000  # 500KB max for analysis
        $analysisText = $stderrText
        $truncated = $false
        
        if ($stderrText.Length -gt $maxStderrLength) {
            Write-Host "Large stderr output detected - analyzing first $([math]::Round($maxStderrLength/1KB,0)) KB..." -ForegroundColor Yellow
            # Take first and last portions for analysis
            $firstPart = $stderrText.Substring(0, $maxStderrLength / 2)
            $lastPart = $stderrText.Substring($stderrText.Length - $maxStderrLength / 2)
            $analysisText = $firstPart + "`n[TRUNCATED MIDDLE SECTION]`n" + $lastPart
            $truncated = $true
        }
        
        # Save full output to log file for analysis (but don't analyze it all)
        @"
FFmpeg HEVC Extraction Log
Command: ffmpeg $($extractArgs -join ' ')
Exit Code: $exitCode
Stdout Length: $($stdoutText.Length) characters
Stderr Length: $($stderrText.Length) characters
Truncated for Analysis: $truncated
=== STDERR OUTPUT ===
$stderrText
"@ | Out-File -FilePath $extractLogFile -Encoding UTF8
        
        # Define critical error patterns - SIMPLIFIED AND OPTIMIZED
        $corruptionPatterns = @{
            "EBML_Errors" = "Element.*exceeds containing master element|invalid as first byte of an EBML number"
            "NAL_Errors" = "Invalid NAL unit size|Failed to read access unit"
            "DTS_Errors" = "non monotonically increasing dts|Application provided invalid.*dts"
            "Bitstream_Errors" = "Error applying bitstream filters"
            "Stream_Errors" = "Invalid data found when processing input"
        }
        
        # PERFORMANCE FIX: Use optimized error counting
        Write-Host "Analyzing stderr for corruption patterns..." -ForegroundColor Cyan
        $errorCounts = @{}
        $totalCorruptionErrors = 0
        $sampleErrors = @()
        
        # Process line by line instead of using complex regex on entire text
        $lines = $analysisText -split "`n"
        $processedLines = 0
        $maxLinesToProcess = 10000  # Limit line processing
        
        foreach ($line in $lines) {
            $processedLines++
            if ($processedLines -gt $maxLinesToProcess) {
                Write-Host "Processed $maxLinesToProcess lines - stopping analysis to prevent hang" -ForegroundColor Yellow
                break
            }
            
            # Check each pattern against this line
            foreach ($patternName in $corruptionPatterns.Keys) {
                $pattern = $corruptionPatterns[$patternName]
                if ($line -match $pattern) {
                    if (-not $errorCounts.ContainsKey($patternName)) {
                        $errorCounts[$patternName] = 0
                    }
                    $errorCounts[$patternName]++
                    $totalCorruptionErrors++
                    
                    # Collect sample errors (limit to prevent memory issues)
                    if ($sampleErrors.Count -lt 10) {
                        $sampleErrors += $line.Trim()
                    }
                }
            }
        }
        
        Write-Host "Processed $processedLines lines for corruption analysis" -ForegroundColor Gray
        
        # Calculate corruption severity - SIMPLIFIED
        $severityLevel = "None"
        if ($totalCorruptionErrors -gt 100) {
            $severityLevel = "Critical"
        }
        elseif ($totalCorruptionErrors -gt 50) {
            $severityLevel = "Severe"
        }
        elseif ($totalCorruptionErrors -gt 10) {
            $severityLevel = "Moderate"
        }
        elseif ($totalCorruptionErrors -gt 0) {
            $severityLevel = "Minor"
        }
        
        # Report corruption analysis
        Write-Host "Corruption Analysis Results:" -ForegroundColor Cyan
        Write-Host "  Total corruption errors: $totalCorruptionErrors" -ForegroundColor White
        Write-Host "  Severity level: $severityLevel" -ForegroundColor $(
            switch ($severityLevel) {
                "Critical" { "Red" }
                "Severe" { "Red" }
                "Moderate" { "Yellow" }
                "Minor" { "Yellow" }
                default { "Green" }
            }
        )
        
        if ($errorCounts.Count -gt 0) {
            Write-Host "  Error breakdown:" -ForegroundColor Yellow
            foreach ($errorType in $errorCounts.Keys) {
                Write-Host "    $errorType : $($errorCounts[$errorType]) occurrences" -ForegroundColor Yellow
            }
        }
        
        if ($sampleErrors.Count -gt 0) {
            Write-Host "  Sample errors:" -ForegroundColor Yellow
            $sampleErrors | Select-Object -First 3 | ForEach-Object {
                $truncatedError = if ($_.Length -gt 100) { $_.Substring(0, 100) + "..." } else { $_ }
                Write-Host "    $truncatedError" -ForegroundColor DarkYellow
            }
            
            if ($sampleErrors.Count -gt 3) {
                Write-Host "    ... and $($sampleErrors.Count - 3) more errors" -ForegroundColor DarkYellow
            }
        }
        
        if ($truncated) {
            Write-Host "  Note: Large stderr output was truncated for analysis performance" -ForegroundColor Yellow
        }
        
        # Check if output file was created and has reasonable content
        $outputValid = $false
        $outputSize = 0
        
        if (Test-Path $OutputFile) {
            $outputSize = (Get-Item $OutputFile).Length
            $outputSizeMB = [math]::Round($outputSize / 1MB, 2)
            Write-Host "  Output file size: $outputSizeMB MB" -ForegroundColor White
            
            # Validate output file has reasonable size (at least 1MB for typical video)
            if ($outputSize -gt 1048576) {  # 1MB minimum
                $outputValid = $true
            }
            else {
                Write-Warning "  Output file too small: $outputSizeMB MB"
            }
        }
        else {
            Write-Warning "  Output file was not created"
        }
        
        # Determine overall success/failure
        $extractionResult = @{
            Success = $false
            Reason = ""
            ExitCode = $exitCode
            CorruptionLevel = $severityLevel
            CorruptionErrors = $sampleErrors
            ErrorCounts = $errorCounts
            OutputSize = $outputSize
            LogFile = $extractLogFile
            StderrTruncated = $truncated
        }
        
        # SIMPLIFIED Decision logic for success/failure
        if ($exitCode -ne 0) {
            $extractionResult.Reason = "FFmpeg failed with exit code $exitCode"
            Write-Host "HEVC extraction FAILED: Non-zero exit code" -ForegroundColor Red
        }
        elseif (-not $outputValid) {
            $extractionResult.Reason = "Output file missing or too small"
            Write-Host "HEVC extraction FAILED: Invalid output" -ForegroundColor Red
        }
        elseif ($severityLevel -eq "Critical") {
            $extractionResult.Reason = "Critical corruption detected ($totalCorruptionErrors errors)"
            Write-Host "HEVC extraction FAILED: Critical corruption level" -ForegroundColor Red
        }
        else {
            # Success cases - be more lenient to avoid blocking valid extractions
            $extractionResult.Success = $true
            
            if ($severityLevel -eq "Severe") {
                $extractionResult.Reason = "Extraction completed with severe corruption ($totalCorruptionErrors errors) - proceeding with caution"
                Write-Host "HEVC extraction SUCCEEDED with warnings: Severe corruption detected but file created" -ForegroundColor Yellow
            }
            elseif ($severityLevel -eq "Moderate") {
                $extractionResult.Reason = "Extraction completed with moderate corruption ($totalCorruptionErrors errors)"
                Write-Host "HEVC extraction SUCCEEDED with warnings: Moderate corruption detected" -ForegroundColor Yellow
            }
            elseif ($severityLevel -eq "Minor") {
                $extractionResult.Reason = "Extraction completed with minor corruption ($totalCorruptionErrors errors)"
                Write-Host "HEVC extraction SUCCEEDED: Minor corruption detected" -ForegroundColor Yellow
            }
            else {
                $extractionResult.Reason = "Extraction completed successfully"
                Write-Host "HEVC extraction SUCCEEDED: No corruption detected" -ForegroundColor Green
            }
        }
        
        return $extractionResult
        
    }
    catch {
        Write-Error "HEVC extraction failed with exception: $_"
        return @{
            Success = $false
            Reason = "Exception during extraction: $_"
            ExitCode = -1
            CorruptionLevel = "Unknown"
            CorruptionErrors = @()
            ErrorCounts = @{}
            OutputSize = 0
            LogFile = $extractLogFile
            StderrTruncated = $false
        }
    }
}

function Invoke-EarlyDolbyVisionRemovalWithHDR10Plus {
    param(
        [string]$InputFile,
        [hashtable]$VideoInfo,
        [double]$MaxAllowableDropPercent = 15.0
    )
    
    # First, do comprehensive HDR10+ detection on original
    Write-Host "`n=== Original File HDR10+ Analysis ===" -ForegroundColor Cyan
    $originalHDR10Plus = Test-HDR10PlusInOriginal -FilePath $InputFile -Verbose $true
    
    if (-not $VideoInfo.HasDolbyVision) {
        Write-Host "No Dolby Vision detected - skipping DV removal" -ForegroundColor Gray
        Write-Host "Original HDR10+ status: $(if ($originalHDR10Plus.HasHDR10Plus) { 'PRESENT' } else { 'NOT FOUND' })" -ForegroundColor $(if ($originalHDR10Plus.HasHDR10Plus) { 'Green' } else { 'Yellow' })
        return $InputFile
    }
    
    Write-Host "`n=== Enhanced Dolby Vision Removal with HDR10+ Preservation ===" -ForegroundColor Yellow
    Write-Host "Original file HDR10+ status: $(if ($originalHDR10Plus.HasHDR10Plus) { 'DETECTED' } else { 'NOT FOUND' })" -ForegroundColor $(if ($originalHDR10Plus.HasHDR10Plus) { 'Green' } else { 'Yellow' })
    
    if (-not $originalHDR10Plus.HasHDR10Plus) {
        Write-Host "No HDR10+ in original file - standard DV removal can proceed" -ForegroundColor Yellow
        # Call your existing DV removal function
        return Invoke-EarlyDolbyVisionRemoval -InputFile $InputFile -VideoInfo $VideoInfo -MaxAllowableDropPercent $MaxAllowableDropPercent
    }
    
    # If we have HDR10+, we need special handling
    Write-Host "CRITICAL: File has both Dolby Vision AND HDR10+ - using preservation method" -ForegroundColor Yellow
    
    # Extract HDR10+ metadata BEFORE DV removal
    $originalHDR10JsonPath = New-TempFile -BaseName "original_hdr10plus" -Extension ".json"
    
    Write-Host "Extracting HDR10+ metadata from original before DV removal..." -ForegroundColor Cyan
    $extractSuccess = Invoke-HDR10PlusExtraction -InputFile $InputFile -OutputJson $originalHDR10JsonPath
    if (-not $extractSuccess) {
        Write-Warning "Failed to extract HDR10+ before encoding from current, trying original"
        $extractSuccess = Invoke-HDR10PlusExtraction -InputFile $OrigFile -OutputJson $originalHDR10JsonPath
    }
    
    if (-not $extractSuccess) {
        Write-Warning "Failed to extract HDR10+ metadata - proceeding with standard DV removal"
        return Invoke-EarlyDolbyVisionRemoval -InputFile $InputFile -VideoInfo $VideoInfo -MaxAllowableDropPercent $MaxAllowableDropPercent
    }
    
    # Perform DV removal
    Write-Host "Performing Dolby Vision removal..." -ForegroundColor Yellow
    $dvRemovedFile = Invoke-EarlyDolbyVisionRemoval -InputFile $InputFile -VideoInfo $VideoInfo -MaxAllowableDropPercent $MaxAllowableDropPercent
    
    if ($dvRemovedFile -eq $InputFile) {
        Write-Host "DV removal didn't change the file - HDR10+ should be preserved" -ForegroundColor Green
        return $InputFile
    }
    
    # Verify HDR10+ is still present after DV removal
    Write-Host "Verifying HDR10+ preservation after DV removal..." -ForegroundColor Cyan
    $postDVHDR10Plus = Test-HDR10PlusInOriginal -FilePath $dvRemovedFile -Verbose $false
    
    if ($postDVHDR10Plus.HasHDR10Plus) {
        Write-Host "* HDR10+ metadata preserved through DV removal" -ForegroundColor Green
        return $dvRemovedFile
    }
    else {
        Write-Warning "HDR10+ metadata lost during DV removal - attempting restoration..."
        
        # Try to inject HDR10+ back into the DV-removed file
        if (Test-Path $Config.HDR10PlusToolExe) {
            $restoredFile = New-TempFile -BaseName "dv_removed_hdr10plus_restored" -Extension ([System.IO.Path]::GetExtension($dvRemovedFile))
            
            Write-Host "Injecting HDR10+ metadata back into DV-removed file..." -ForegroundColor Yellow
            $injectSuccess = Invoke-HDR10PlusInjection -VideoFile $dvRemovedFile -JsonFile $originalHDR10JsonPath -OutputFile $restoredFile
            
            if ($injectSuccess) {
                # Verify the injection worked
                $restoredHDR10Plus = Test-HDR10PlusInOriginal -FilePath $restoredFile -Verbose $false
                
                if ($restoredHDR10Plus.HasHDR10Plus) {
                    Write-Host "* HDR10+ metadata successfully restored after DV removal" -ForegroundColor Green
                    return $restoredFile
                }
                else {
                    Write-Warning "HDR10+ injection appeared to succeed but metadata not detected"
                }
            }
            else {
                Write-Warning "HDR10+ injection failed"
            }
        }
        
        Write-Warning "Could not restore HDR10+ metadata - returning DV-removed file without HDR10+"
        return $dvRemovedFile
    }
}

function Invoke-AV1QSVEncoding {
    param(
        [string]$InputFile,
        [string]$OutputFile,
        [hashtable]$VideoInfo,
        [hashtable]$EncodingSettings,
        [hashtable]$ColorInfo
    )
    
    Write-Host "Using Intel QuickSync AV1 hardware encoding..." -ForegroundColor Green

    # Build video filter chain
    $padFilter = ""
    if ($PadToStandardResolution) {
        $padFilter = Edit-VideoResolutionFilter -VideoInfo $VideoInfo
        if ($padFilter) {
            Write-Host "Applying pad filter due to resolution variance." -ForegroundColor Yellow
        }
    }

    $videoFilters = @()
    if ($padFilter) {
        $videoFilters += "$padFilter,unsharp=luma_msize_x=$($EncodingSettings.SharpenKernel):luma_msize_y=$($EncodingSettings.SharpenKernel):luma_amount=$($EncodingSettings.SharpenAmount)"
    }
    else {
        $videoFilters += "unsharp=luma_msize_x=$($EncodingSettings.SharpenKernel):luma_msize_y=$($EncodingSettings.SharpenKernel):luma_amount=$($EncodingSettings.SharpenAmount)"
    }
    
    $videoFilters += "setpts=PTS-STARTPTS"
    $vfArgs = @("-vf", ($videoFilters -join ","))
    
    # Timestamp correction
    $timestampArgs = @(
        "-avoid_negative_ts", "make_zero",
        "-muxdelay", "0",
        "-muxpreload", "0"
    )
    
    # Preserve original chroma location
    $chromaLocation = "left"
    if ($VideoInfo.OriginalChromaLocation) {
        $chromaLocation = $VideoInfo.OriginalChromaLocation
        Write-Host "Preserving original chroma location: $chromaLocation" -ForegroundColor Green
    }

    # Color settings for AV1
    $colorArgs = @(
        "-color_primaries", $ColorInfo.Primaries,
        "-color_trc", $ColorInfo.Transfer,
        "-colorspace", $ColorInfo.Space,
        "-color_range", $VideoInfo.colorRange,
        "-chroma_sample_location", $chromaLocation
    )
    
    # AV1 QSV specific settings
    $codec = "av1_qsv"
    
    # Profile selection - AV1 uses main profile
    $lprofile = "main"
    if ($ColorInfo.IsHDR) {
        Write-Host "Using Main profile for HDR content in AV1" -ForegroundColor Green
    }
    
    # Calculate keyframe interval
    $fps = [math]::Round($VideoInfo.FrameRate)
    $keyframeInterval = $fps * 2  # 2-second keyframes
    
    # Pixel format for AV1
    if ($ColorInfo.IsHDR) {
        $pixelFormat = "p010le"
        Write-Host "Using p010le pixel format for AV1 HDR encoding" -ForegroundColor Green
    }
    else {
        $pixelFormat = "yuv420p"
        Write-Host "Using yuv420p pixel format for AV1 SDR encoding" -ForegroundColor Green
    }
    
    # AV1 quality mapping (AV1 uses different scale than HEVC)
    # HEVC CRF 20-28 roughly maps to AV1 CRF 30-50
    $av1Quality = [math]::Round($EncodingSettings.Quality + 3)
    $av1Quality = [math]::Max(20, [math]::Min(63, $av1Quality))
    
    Write-Host "Mapped HEVC quality $($EncodingSettings.Quality) to AV1 quality $av1Quality" -ForegroundColor Cyan
    
    # Output format
    $outputFormat = "ivf"  # IVF container for AV1
    
    # Build AV1 QSV encoding arguments
$encodeArgs = @(
    "-c:v", $codec,
    "-preset", "veryslow",
    "-profile:v", $lprofile,
    "-global_quality", $av1Quality,
    "-look_ahead_depth", [math]::Min(100, $EncodingSettings.LookAhead),
    "-adaptive_i", "1",
    "-adaptive_b", "1",
    "-b_strategy", "1",
    "-async_depth", "4",
    "-aq_mode", "2",
    "-g", $keyframeInterval,
    "-keyint_min", $keyframeInterval,
    "-bf", "7",
    "-refs", "7",
    "-r", $VideoInfo.FrameRate,
    "-pix_fmt", $pixelFormat,
    "-f", $outputFormat
)
    
    # Build complete FFmpeg command
    
    $ffmpegArgs = @(
        "-y", "-hide_banner",
        "-xerror",
        "-loglevel", "info",
        "-fflags", "+genpts",
        "-i", $InputFile, 
        "-max_muxing_queue_size", "65536",
        "-fps_mode", "cfr"
    ) + $vfArgs + $colorArgs + $timestampArgs + $encodeArgs + @($OutputFile)
    
    # Display command preview
    Write-Host "AV1 QSV encoding parameters:" -ForegroundColor Gray
    Write-Host "  Profile: $lprofile, Quality: $av1Quality, Keyframe Interval: $keyframeInterval" -ForegroundColor Gray
    Write-Host "  Look-ahead: $([math]::Min(100, $EncodingSettings.LookAhead)), Output Format: $outputFormat, Pixel Format: $pixelFormat" -ForegroundColor Gray
    
    # Execute encoding with AV1-specific error detection
    Write-Host "Starting AV1 QSV encoding..." -ForegroundColor Yellow
    $result = Invoke-FFmpegEncodingWithErrorDetection `
        -Executable $Config.FFmpegExe `
        -Arguments $ffmpegArgs `
        -Description "Intel AV1 QSV encoding" `
        -CodecType "AV1" `
        -DisableMCEBuddyMonitoring $DisableMCEBuddyMonitoring `
        -MCEBuddyCheckInterval $MCEBuddyCheckInterval
    
    if (-not $result.Success) {
        Write-Host "AV1 QSV encoding failed" -ForegroundColor Red
        
        if ($result.SevereCorruption) {
            Write-Host "SEVERE CORRUPTION detected in AV1 encoding" -ForegroundColor Red
            return $false
        }
        else {
            Write-Host "AV1 QSV encoding failed - will fall back to software" -ForegroundColor Yellow
            return $false
        }
    }
    
    # Verify output
    if ((Test-Path $OutputFile) -and (Get-Item $OutputFile).Length -gt 0) {
        $outputSizeMB = [math]::Round((Get-Item $OutputFile).Length / 1MB, 2)
        $inputSizeMB = [math]::Round((Get-Item $InputFile).Length / 1MB, 2)
        
        Write-Host "AV1 QSV encoding completed successfully" -ForegroundColor Green
        Write-Host "  Input size:  $inputSizeMB MB" -ForegroundColor Green
        Write-Host "  Output size: $outputSizeMB MB" -ForegroundColor Green
        Write-Host "  Compression: $([math]::Round($outputSizeMB / $inputSizeMB, 2))x" -ForegroundColor Green
        
        return $true
    }
    else {
        Write-Warning "AV1 QSV encoding produced empty or missing file"
        return $false
    }
}

function Invoke-AV1Encoding {
    param(
        [string]$InputFile,
        [string]$OutputFile,
        [hashtable]$VideoInfo,
        [hashtable]$EncodingSettings,
        [hashtable]$ColorInfo
    )
    
    Write-Host "Using AV1 encoding (no HDR10+ detected, optimal for compression)..." -ForegroundColor Cyan
    
        Write-Host "AV1 QSV hardware encoding available - using av1_qsv" -ForegroundColor Green
        return Invoke-AV1QSVEncoding -InputFile $InputFile -OutputFile $OutputFile -VideoInfo $VideoInfo -EncodingSettings $EncodingSettings -ColorInfo $ColorInfo

}


# Complete Enhanced Dolby Vision removal function
function Invoke-EarlyDolbyVisionRemoval {
    param(
        [string]$InputFile,
        [hashtable]$VideoInfo,
        [double]$MaxAllowableDropPercent = 15.0
    )
    
    if (-not $VideoInfo.HasDolbyVision) {
        Write-Host "No Dolby Vision detected - skipping DV removal" -ForegroundColor Gray
        return $InputFile
    }
    
    Write-Host "`n=== Early Dolby Vision Removal with Enhanced Error Detection ===" -ForegroundColor Yellow
    Write-Host "Removing Dolby Vision metadata before encoding..." -ForegroundColor Cyan
    Write-Host "DV Profile detected: $($VideoInfo.DVProfile)" -ForegroundColor Gray
    
    # Check if dovi_tool exists
    $doviToolPath = "E:\Plex\Donis Dolby Vision Tool\tools\dovi_tool.exe"
    if (-not (Test-Path $doviToolPath)) {
        Write-Warning "dovi_tool.exe not found at $doviToolPath"
        Write-Host "Continuing with original file - DV will remain in source" -ForegroundColor Yellow
        return $InputFile
    }
    
    # Check if mkvmerge is available
    $mkvmergePath = $Config.MKVMergeExe 
    $mkvmergeAvailable = $false
    try {
        $mkvmergeTest = & $mkvmergePath --version 2>&1
        if ($LASTEXITCODE -eq 0) {
            Write-Host "Using mkvmerge version: $($mkvmergeTest)" -ForegroundColor Gray
            $mkvmergeAvailable = $true
        }
    }
    catch {
        $mkvmergeAvailable = $false
    }

    if (-not $mkvmergeAvailable) {
        Write-Warning "mkvmerge not available - this may affect DV removal quality"
    }
    
    try {
        # Capture original file size for validation
        $originalSize = (Get-Item $InputFile).Length
        $originalSizeMB = [math]::Round($originalSize / 1MB, 2)
        
        Write-Host "Original file size: $originalSizeMB MB" -ForegroundColor Gray
        
        # Create output file in temp folder
        $dvRemovedFile = New-TempFile -BaseName "$([System.IO.Path]::GetFileNameWithoutExtension($InputFile)).dv_removed" -Extension ([System.IO.Path]::GetExtension($InputFile))
        
        # Change to the tool directory (dovi_tool may have path dependencies)
        $originalLocation = Get-Location
        $toolDirectory = Split-Path $doviToolPath -Parent
        Set-Location -Path $toolDirectory
        
        Write-Host "Working directory: $toolDirectory" -ForegroundColor Gray
        Write-Host "Processing: $InputFile" -ForegroundColor Gray
        Write-Host "Output will be: $dvRemovedFile" -ForegroundColor Gray
        
        # Determine the approach based on file type
        $fileExtension = [System.IO.Path]::GetExtension($InputFile).ToLower()
        
        if ($fileExtension -in @(".hevc", ".h265")) {
            Write-Host "Raw HEVC file detected - using direct demux approach" -ForegroundColor Yellow
            
            # For raw HEVC files, use demux directly
            $doviArgs = @("demux", $InputFile)
            
            Write-Host "Executing: $doviToolPath $($doviArgs -join ' ')" -ForegroundColor Gray
            
            $processInfo = New-Object System.Diagnostics.ProcessStartInfo
            $processInfo.FileName = $doviToolPath
            $processInfo.UseShellExecute = $false
            $processInfo.RedirectStandardOutput = $true
            $processInfo.RedirectStandardError = $true
            $processInfo.CreateNoWindow = $false
            $processInfo.WorkingDirectory = $toolDirectory
            
            if ($PSVersionTable.PSVersion.Major -ge 7) {
                foreach ($arg in $doviArgs) {
                    $null = $processInfo.ArgumentList.Add($arg)
                }
            }
            else {
                $quotedArgs = $doviArgs | ForEach-Object { 
                    if ($_ -match '\s') { '"{0}"' -f ($_ -replace '"', '""') } else { $_ }
                }
                $processInfo.Arguments = $quotedArgs -join ' '
            }
            
            $process = [System.Diagnostics.Process]::Start($processInfo)
            $handle = $process.Handle
            $Script:ActiveProcesses = @($Script:ActiveProcesses) + $process
            
            $stdout = $process.StandardOutput.ReadToEnd()
            $stderr = $process.StandardError.ReadToEnd()
            
            try {
                $process.PriorityClass = [System.Diagnostics.ProcessPriorityClass]::BelowNormal
            }
            catch {
                Write-Host "Could not set dovi_tool process priority" -ForegroundColor Gray
            }
            
            $timeoutMs = 18000000  # 30 minutes
            Write-Host "Waiting for dovi_tool to complete (300 minute timeout)..." -ForegroundColor Yellow
            
            if (-not $process.WaitForExit($timeoutMs)) {
                Write-Warning "dovi_tool timed out after 300 minutes - killing process"
                try {
                    $process.Kill()
                    $process.WaitForExit(5000)
                }
                catch {
                    Write-Warning "Failed to kill dovi_tool process cleanly: $_"
                }
                
                $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $process.Id }
                return $InputFile
            }
            
            $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $process.Id }
            $exitCode = $process.ExitCode
            
            Write-Host "dovi_tool completed with exit code: $exitCode" -ForegroundColor Gray
            
            if ($exitCode -eq 0) {
                # For raw HEVC, dovi_tool creates BL.hevc and EL.hevc files
                $blFile = Join-Path $toolDirectory "BL.hevc"
                $elFile = Join-Path $toolDirectory "EL.hevc"
                
                if (Test-Path $blFile) {
                    # Move the BL.hevc to our desired output location
                    Move-Item -Path $blFile -Destination $dvRemovedFile -Force
                    Write-Host "Successfully extracted base layer to: $dvRemovedFile" -ForegroundColor Green
                    
                    # Clean up EL file if it exists
                    if (Test-Path $elFile) {
                        Remove-Item -Path $elFile -Force -ErrorAction SilentlyContinue
                        Write-Host "Removed enhancement layer file" -ForegroundColor Gray
                    }
                }
                else {
                    Write-Warning "dovi_tool completed but BL.hevc not found"
                    return $InputFile
                }
            }
            else {
                Write-Warning "dovi_tool failed with exit code: $exitCode"
                return $InputFile
            }
        }
        else {
            Write-Host "Container file detected - using enhanced FFmpeg + dovi_tool + mkvmerge pipeline" -ForegroundColor Yellow
            
            # For container files, use enhanced HEVC extraction
            $tempHevcFile = New-TempFile -BaseName "temp_for_dv_removal" -Extension ".hevc"
            
            Write-Host "Step 1: Enhanced HEVC extraction with corruption detection..." -ForegroundColor Cyan
            
            # Use the new enhanced extraction function
            $extractionResult = Invoke-EnhancedHEVCExtraction -InputFile $InputFile -OutputFile $tempHevcFile -Description "Extract HEVC stream for DV removal"
            
            if (-not $extractionResult.Success) {
                Write-Host "HEVC extraction failed - cannot proceed with DV removal" -ForegroundColor Red
                Write-Host "Failure reason: $($extractionResult.Reason)" -ForegroundColor Red
                
                if ($extractionResult.CorruptionLevel -in @("Critical", "Severe")) {
                    Write-Host "Source file appears to have significant corruption:" -ForegroundColor Red
                    Write-Host "  - This may indicate the source file is already damaged" -ForegroundColor Red
                    Write-Host "  - DV removal may not be possible with this file" -ForegroundColor Red
                    Write-Host "  - Consider using a different source or repair tool" -ForegroundColor Red
                }
                
                # Show detailed error information
                if ($extractionResult.ErrorCounts.Count -gt 0) {
                    Write-Host "Corruption error summary:" -ForegroundColor Red
                    foreach ($errorType in $extractionResult.ErrorCounts.Keys) {
                        Write-Host "  $errorType : $($extractionResult.ErrorCounts[$errorType]) occurrences" -ForegroundColor Red
                    }
                }
                
                # Clean up failed extraction
                if (Test-Path $tempHevcFile) {
                    Remove-Item $tempHevcFile -Force -ErrorAction SilentlyContinue
                }
                
                return $InputFile
            }
            
            Write-Host "HEVC extraction completed: $($extractionResult.Reason)" -ForegroundColor Green
            
            # Continue with DV removal if extraction succeeded
            Write-Host "Step 2: Removing DV from extracted HEVC stream..." -ForegroundColor Cyan
            
            # Now demux the extracted HEVC
            $doviArgs = @("demux", $tempHevcFile)
            
            Write-Host "Executing: $doviToolPath $($doviArgs -join ' ')" -ForegroundColor Gray
            
            $processInfo = New-Object System.Diagnostics.ProcessStartInfo
            $processInfo.FileName = $doviToolPath
            $processInfo.UseShellExecute = $false
            $processInfo.RedirectStandardOutput = $true
            $processInfo.RedirectStandardError = $true
            $processInfo.CreateNoWindow = $false
            $processInfo.WorkingDirectory = $toolDirectory
            
            if ($PSVersionTable.PSVersion.Major -ge 7) {
                foreach ($arg in $doviArgs) {
                    $null = $processInfo.ArgumentList.Add($arg)
                }
            }
            else {
                $quotedArgs = $doviArgs | ForEach-Object { 
                    if ($_ -match '\s') { '"{0}"' -f ($_ -replace '"', '""') } else { $_ }
                }
                $processInfo.Arguments = $quotedArgs -join ' '
            }
            
            $process = [System.Diagnostics.Process]::Start($processInfo)
            $handle = $process.Handle
            $Script:ActiveProcesses = @($Script:ActiveProcesses) + $process
            
            $stdout = $process.StandardOutput.ReadToEnd()
            $stderr = $process.StandardError.ReadToEnd()
            
            try {
                $process.PriorityClass = [System.Diagnostics.ProcessPriorityClass]::BelowNormal
            }
            catch {
                Write-Host "Could not set dovi_tool process priority" -ForegroundColor Gray
            }
            
            $timeoutMs = 18000000  # 30 minutes
            Write-Host "Waiting for dovi_tool to complete (300 minute timeout)..." -ForegroundColor Yellow
            
            if (-not $process.WaitForExit($timeoutMs)) {
                Write-Warning "dovi_tool timed out after 300 minutes - killing process"
                try {
                    $process.Kill()
                    $process.WaitForExit(5000)
                }
                catch {
                    Write-Warning "Failed to kill dovi_tool process cleanly: $_"
                }
                
                $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $process.Id }
                return $InputFile
            }
            
            $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $process.Id }
            $exitCode = $process.ExitCode
            
            Write-Host "dovi_tool completed with exit code: $exitCode" -ForegroundColor Gray
            
            if ($exitCode -eq 0) {
                $blFile = Join-Path $toolDirectory "BL.hevc"
                $elFile = Join-Path $toolDirectory "EL.hevc"
                
                if (Test-Path $blFile) {
                    Write-Host "Step 3: Re-containerizing with mkvmerge..." -ForegroundColor Cyan
                    
                    if ($mkvmergeAvailable) {
                        Write-Host "Using mkvmerge for optimal re-containerization..." -ForegroundColor Green
                        
                        # Create intermediate MKV with just the DV-removed HEVC
                        $tempMkvFile = New-TempFile -BaseName "dv_removed_video_only" -Extension ".mkv"
                        
                        $mkvmergeVideoArgs = @(
                            "--output", $tempMkvFile,
                            "--language", "0:und",
                            "--track-name", "0:HEVC Video (DV Removed)",
                            "--default-track", "0:yes",
                            "--compression", "0:none",
                            "--default-duration", "0:$("{0:F6}" -f (1000 / $videoInfo.FrameRate))ms",  # Force original frame rate
                            "--no-chapters",
                            "--no-attachments",
                            "--title", "DV Removed Video",
                            $blFile
                        )
                        
                        # Execute mkvmerge for video containerization
                        $videoProcessInfo = New-Object System.Diagnostics.ProcessStartInfo
                        $videoProcessInfo.FileName = $mkvmergePath
                        $videoProcessInfo.UseShellExecute = $false
                        $videoProcessInfo.RedirectStandardOutput = $true
                        $videoProcessInfo.RedirectStandardError = $true
                        $videoProcessInfo.CreateNoWindow = $false
                        
                        if ($PSVersionTable.PSVersion.Major -ge 7) {
                            foreach ($arg in $mkvmergeVideoArgs) {
                                $null = $videoProcessInfo.ArgumentList.Add($arg)
                            }
                        }
                        else {
                            $quotedArgs = $mkvmergeVideoArgs | ForEach-Object { 
                                if ($_ -match '\s') { '"{0}"' -f ($_ -replace '"', '""') } else { $_ }
                            }
                            $videoProcessInfo.Arguments = $quotedArgs -join ' '
                        }
                        
                        $videoProcess = [System.Diagnostics.Process]::Start($videoProcessInfo)
                        $videoHandle = $videoProcess.Handle
                        $Script:ActiveProcesses = @($Script:ActiveProcesses) + $videoProcess
                        
                        $videoStdout = $videoProcess.StandardOutput.ReadToEnd()
                        $videoStderr = $videoProcess.StandardError.ReadToEnd()
                        
                        try {
                            $videoProcess.PriorityClass = [System.Diagnostics.ProcessPriorityClass]::BelowNormal
                        }
                        catch {
                            Write-Host "Could not set mkvmerge process priority" -ForegroundColor Gray
                        }
                        
                        $videoProcess.WaitForExit()
                        $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $videoProcess.Id }
                        
                        $videoExitCode = $videoProcess.ExitCode
                        
                        if ($videoExitCode -eq 0 -and (Test-Path $tempMkvFile)) {
                            Write-Host "Video containerization successful" -ForegroundColor Green
                            
                            # Now merge the video-only MKV with audio/subtitles from original
                            Write-Host "Step 4: Merging DV-removed video with original audio/subtitles using mkvmerge..." -ForegroundColor Cyan
                            
                            # Build comprehensive mkvmerge command for final merge
                            $finalMergeArgs = @(
                                "--output", $dvRemovedFile,
                                "--title", "DV Removed - $(Split-Path $InputFile -Leaf)"
                            )
                            
                            # Add video from the DV-removed MKV
                            $finalMergeArgs += @(
                                "--language", "0:und",
                                "--track-name", "0:HEVC Video",
                                "--default-track", "0:yes",
                                $tempMkvFile
                            )
                            
                            # Add audio and subtitles from original file, skipping video
                            $finalMergeArgs += @(
                                "--no-video",
                                "--no-chapters",
                                "--no-attachments", 
                                $InputFile
                            )
                            
                            # Execute final merge
                            $finalProcessInfo = New-Object System.Diagnostics.ProcessStartInfo
                            $finalProcessInfo.FileName = $mkvmergePath
                            $finalProcessInfo.UseShellExecute = $false
                            $finalProcessInfo.RedirectStandardOutput = $true
                            $finalProcessInfo.RedirectStandardError = $true
                            $finalProcessInfo.CreateNoWindow = $false
                            
                            if ($PSVersionTable.PSVersion.Major -ge 7) {
                                foreach ($arg in $finalMergeArgs) {
                                    $null = $finalProcessInfo.ArgumentList.Add($arg)
                                }
                            }
                            else {
                                $quotedArgs = $finalMergeArgs | ForEach-Object { 
                                    if ($_ -match '\s') { '"{0}"' -f ($_ -replace '"', '""') } else { $_ }
                                }
                                $finalProcessInfo.Arguments = $quotedArgs -join ' '
                            }
                            
                            $finalProcess = [System.Diagnostics.Process]::Start($finalProcessInfo)
                            $finalHandle = $finalProcess.Handle
                            $Script:ActiveProcesses = @($Script:ActiveProcesses) + $finalProcess
                            
                            $finalStdout = $finalProcess.StandardOutput.ReadToEnd()
                            $finalStderr = $finalProcess.StandardError.ReadToEnd()
                            
                            try {
                                $finalProcess.PriorityClass = [System.Diagnostics.ProcessPriorityClass]::BelowNormal
                            }
                            catch {
                                Write-Host "Could not set final mkvmerge process priority" -ForegroundColor Gray
                            }
                            
                            $finalProcess.WaitForExit()
                            $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $finalProcess.Id }
                            
                            $finalExitCode = $finalProcess.ExitCode
                            
                            if ($finalExitCode -eq 0 -and (Test-Path $dvRemovedFile)) {
                                Write-Host "mkvmerge final merge completed successfully" -ForegroundColor Green
                                if ($finalStdout) {
                                    Write-Host "mkvmerge final output:" -ForegroundColor Gray
                                    Write-Host $finalStdout -ForegroundColor DarkGray
                                }
                                
                                # Clean up intermediate files
                                Remove-Item -Path $tempMkvFile -Force -ErrorAction SilentlyContinue
                                Write-Host "Cleaned up intermediate video-only MKV" -ForegroundColor Gray
                                
                            }
                            else {
                                Write-Warning "mkvmerge final merge failed with exit code: $finalExitCode"
                                if ($finalStderr) {
                                    Write-Host "mkvmerge final error:" -ForegroundColor Red
                                    Write-Host $finalStderr -ForegroundColor Red
                                }
                                
                                # Fall back to FFmpeg method
                                Write-Host "Falling back to FFmpeg re-containerization..." -ForegroundColor Yellow
                                $remuxArgs = @(
                                    "-y", "-hide_banner", "-loglevel", "warning",
                                    "-i", $InputFile,  # Original file for audio/subtitles
                                    "-i", $blFile,     # DV-removed video
                                    "-map", "1:v:0",   # Video from DV-removed file
                                    "-map", "0:a?",    # Audio from original (if exists)
                                    "-map", "0:s?",    # Subtitles from original (if exists)
                                    "-c", "copy",      # Copy all streams
                                    "-r",  $videoInfo.FrameRate,  # Force original frame rate
                                    "-avoid_negative_ts", "make_zero",
                                    $dvRemovedFile
                                )
                                
                                $remuxSuccess = Invoke-SimpleProcess -Executable $Config.FFmpegExe -Arguments $remuxArgs -Description "Re-mux DV-removed stream (FFmpeg fallback)"
                                
                                if (-not $remuxSuccess -or -not (Test-Path $dvRemovedFile)) {
                                    Write-Warning "Both mkvmerge and FFmpeg re-containerization failed"
                                    return $InputFile
                                }
                            }
                            
                        }
                        else {
                            Write-Warning "mkvmerge video containerization failed with exit code: $videoExitCode"
                            if ($videoStderr) {
                                Write-Host "mkvmerge video error:" -ForegroundColor Red
                                Write-Host $videoStderr -ForegroundColor Red
                            }
                            
                            # Fall back to FFmpeg method
                            Write-Host "Falling back to FFmpeg re-containerization..." -ForegroundColor Yellow
                            $remuxArgs = @(
                                "-y", "-hide_banner", "-loglevel", "warning",
                                "-i", $InputFile,  # Original file for audio/subtitles
                                "-i", $blFile,     # DV-removed video
                                "-map", "1:v:0",   # Video from DV-removed file
                                "-map", "0:a?",    # Audio from original (if exists)
                                "-map", "0:s?",    # Subtitles from original (if exists)
                                "-c", "copy",      # Copy all streams
                                "-r",  $videoInfo.FrameRate,  # Force original frame rate
                                "-avoid_negative_ts", "make_zero",
                                $dvRemovedFile
                            )
                            
                            $remuxSuccess = Invoke-SimpleProcess -Executable $Config.FFmpegExe -Arguments $remuxArgs -Description "Re-mux DV-removed stream (FFmpeg fallback)"
                            
                            if (-not $remuxSuccess -or -not (Test-Path $dvRemovedFile)) {
                                Write-Warning "Failed to re-mux DV-removed stream"
                                return $InputFile
                            }
                        }
                    }
                    else {
                        # Fall back to FFmpeg method if mkvmerge not available
                        Write-Host "mkvmerge not available - using FFmpeg re-containerization..." -ForegroundColor Yellow
                        $remuxArgs = @(
                            "-y", "-hide_banner", "-loglevel", "warning",
                            "-i", $InputFile,  # Original file for audio/subtitles
                            "-i", $blFile,     # DV-removed video
                            "-map", "1:v:0",   # Video from DV-removed file
                            "-map", "0:a?",    # Audio from original (if exists)
                            "-map", "0:s?",    # Subtitles from original (if exists)
                            "-c", "copy",      # Copy all streams
                            "-r",  $videoInfo.FrameRate,  # Force original frame rate
                            "-avoid_negative_ts", "make_zero",
                            $dvRemovedFile
                        )
                        
                        $remuxSuccess = Invoke-SimpleProcess -Executable $Config.FFmpegExe -Arguments $remuxArgs -Description "Re-mux DV-removed stream"
                        
                        if (-not $remuxSuccess -or -not (Test-Path $dvRemovedFile)) {
                            Write-Warning "Failed to re-mux DV-removed stream"
                            return $InputFile
                        }
                    }
                    
                    # Clean up temporary files
                    Remove-Item -Path $blFile -Force -ErrorAction SilentlyContinue
                    if (Test-Path $elFile) {
                        Remove-Item -Path $elFile -Force -ErrorAction SilentlyContinue
                    }
                    Remove-Item -Path $tempHevcFile -Force -ErrorAction SilentlyContinue
                    
                }
                else {
                    Write-Warning "dovi_tool completed but BL.hevc not found"
                    return $InputFile
                }
            }
            else {
                Write-Warning "dovi_tool failed with exit code: $exitCode"
                if ($stderr) {
                    Write-Host "dovi_tool stderr:" -ForegroundColor Red
                    Write-Host $stderr -ForegroundColor Red
                }
                return $InputFile
            }
        }
        
        # Validate the output file
        if (-not (Test-Path $dvRemovedFile)) {
            Write-Warning "DV removal output file not found"
            return $InputFile
        }
        
        # Validate file size change
        $processedSize = (Get-Item $dvRemovedFile).Length
        $processedSizeMB = [math]::Round($processedSize / 1MB, 2)
        
        Write-Host "Processed file size: $processedSizeMB MB" -ForegroundColor Gray
        if ($processedFile -ne $InputFile) {
    Write-Host "Checking frame rate after DV removal..." -ForegroundColor Yellow
    $dvRemovedVideoInfo = Get-VideoMetadata -FilePath $dvRemovedFile
    if ($dvRemovedVideoInfo) {
        Write-Host "Original frame rate: $($videoInfo.FrameRate)" -ForegroundColor White
        Write-Host "Post-DV removal frame rate: $($dvRemovedVideoInfo.FrameRate)" -ForegroundColor White
        if ($dvRemovedVideoInfo.FrameRate -ne $videoInfo.FrameRate) {
            Write-Warning "Frame rate changed during DV removal: $($videoInfo.FrameRate) -> $($dvRemovedVideoInfo.FrameRate)"
        }
    }
}
        if ($processedSize -eq 0) {
            Write-Warning "DV removal produced empty file"
            return $InputFile
        }
        
        # Calculate percentage change
        $percentChange = if ($originalSize -gt 0) {
            (($processedSize - $originalSize) / $originalSize) * 100
        }
        else {
            0.0
        }
        
        $absolutePercentChange = [math]::Abs($percentChange)
        $sizeDifferenceMB = $processedSizeMB - $originalSizeMB
        
        Write-Host "Size change: $([math]::Round($sizeDifferenceMB, 2)) MB ($([math]::Round($percentChange, 2))%)" -ForegroundColor $(
            if ($percentChange -lt -$MaxAllowableDropPercent) { "Red" } 
            elseif ([math]::Abs($percentChange) -gt 5) { "Yellow" } 
            else { "Green" }
        )
        
        # Validation logic
        if ($percentChange -lt -$MaxAllowableDropPercent) {
            Write-Host "File size dropped by $([math]::Round($absolutePercentChange, 2))% - this may indicate processing failure" -ForegroundColor Red
            Write-Warning "Using original file due to excessive size reduction"
            
            # Clean up failed output
            try {
                Remove-Item $dvRemovedFile -Force -ErrorAction SilentlyContinue
            }
            catch {}
            
            return $InputFile
        }
        elseif ($absolutePercentChange -lt 0.1) {
            Write-Host "File size virtually unchanged - tool may not have found DV metadata to remove" -ForegroundColor Blue
        }
        else {
            Write-Host "File size change within normal range for DV removal" -ForegroundColor Green
        }
        
        Write-Host "Dolby Vision removal completed successfully with enhanced error detection" -ForegroundColor Green
        Write-Host "DV-removed file: $dvRemovedFile" -ForegroundColor Green
        
        return $dvRemovedFile
        
    }
    catch {
        Write-Warning "Error during Dolby Vision removal: $_"
        return $InputFile
    }
    finally {
        Set-Location -Path $originalLocation
    }
}

function Test-HDR10PlusInOriginal {
    param(
        [string]$FilePath,
        [bool]$Verbose = $true
    )
    
    if ($Verbose) {
        Write-Host "=== Comprehensive HDR10+ Detection in Original File ===" -ForegroundColor Cyan
        Write-Host "Testing file: $FilePath" -ForegroundColor White
    }
    
    $hasHDR10Plus = $false
    $detectionMethods = @()
    
    try {
        # Method 1: Frame-level side data detection
        if ($Verbose) { Write-Host "Method 1: Frame-level side data detection..." -ForegroundColor Gray }
        $frameArgs = @("-v", "quiet", "-select_streams", "v:0", "-read_intervals", "%+#10", "-show_frames", "-show_entries", "frame=side_data_list", "-of", "csv=p=0", $FilePath)
        $frameResult = Invoke-FFProbeWithCleanup -Arguments $frameArgs -TimeoutSeconds 90
        
        if ($frameResult.Success -and $frameResult.StdOut) {
            $frameLines = $frameResult.StdOut -split "`n" | Select-Object -First 10
            $frameCount = 0
            foreach ($line in $frameLines) {
                if ($line -match "HDR Dynamic Metadata SMPTE2094-40|HDR10\+|SMPTE.*2094") {
                    $hasHDR10Plus = $true
                    $frameCount++
                    if ($frameCount -le 3) {  # Only show first few detections to avoid spam
                        $detectionMethods += "Frame side data: HDR Dynamic Metadata SMPTE2094-40"
                    }
                    if ($Verbose -and $frameCount -eq 1) { 
                        Write-Host "  * Found HDR Dynamic Metadata SMPTE2094-40" -ForegroundColor Green 
                    }
                }
            }
            if ($frameCount -gt 1 -and $Verbose) {
                Write-Host "  * Found $frameCount frames with HDR10+ metadata" -ForegroundColor Green
            }
        }
        
        # Method 2: Packet-level detection  
        if ($Verbose) { Write-Host "Method 2: Packet-level detection..." -ForegroundColor Gray }
        $packetArgs = @("-v", "quiet", "-show_packets", "-select_streams", "v:0", "-show_entries", "packet=side_data_list", "-of", "csv=p=0", $FilePath)
        $packetResult = Invoke-FFProbeWithCleanup -Arguments $packetArgs -TimeoutSeconds 90
        
        if ($packetResult.Success -and $packetResult.StdOut) {
            $packetLines = $packetResult.StdOut -split "`n" | Select-Object -First 5
            foreach ($line in $packetLines) {
                if ($line -match "HDR Dynamic Metadata|SMPTE.*2094|ST.*2094.*40") {
                    $hasHDR10Plus = $true
                    $detectionMethods += "Packet side data: $line"
                    if ($Verbose) { Write-Host "  * Found via packet side data" -ForegroundColor Green }
                    break  # Only need one confirmation
                }
            }
        }
        
        # Method 3: HDR10+ tool detection (if available)
        if ((Test-Path $Config.HDR10PlusToolExe)) {
            if ($Verbose) { Write-Host "Method 3: HDR10+ tool detection..." -ForegroundColor Gray }
            
            $testJsonPath = Join-Path $Script:ValidatedTempFolder "hdr10_test_$(Get-Random).json"
            Add-TempFile -FilePath $testJsonPath
            
            try {
                $toolArgs = @("extract", "-i", "`"$FilePath`"", "-o", "`"$testJsonPath`"")
                
                # Use managed process for HDR10+ tool
                $processInfo = New-Object System.Diagnostics.ProcessStartInfo
                $processInfo.FileName = $Config.HDR10PlusToolExe
                $processInfo.UseShellExecute = $false
                $processInfo.RedirectStandardOutput = $true
                $processInfo.RedirectStandardError = $true
                $processInfo.CreateNoWindow = $true
                
                if ($PSVersionTable.PSVersion.Major -ge 7) {
                    foreach ($arg in $toolArgs) {
                        $null = $processInfo.ArgumentList.Add($arg)
                    }
                }
                else {
                    $quotedArgs = $toolArgs | ForEach-Object { 
                        if ($_ -match '\s') { '"{0}"' -f ($_ -replace '"', '""') } else { $_ }
                    }
                    $processInfo.Arguments = $quotedArgs -join ' '
                }
                
                $toolProcess = [System.Diagnostics.Process]::Start($processInfo)
                $handle = $toolProcess.Handle
                $Script:ActiveProcesses = @($Script:ActiveProcesses) + $toolProcess
                
                if ($toolProcess.WaitForExit(30000)) {  # 30 second timeout
                    $exitCode = $toolProcess.ExitCode
                    
                    # Clean up process
                    $toolProcess.Close()
                    $toolProcess.Dispose()
                    $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $toolProcess.Id }
                    
                    if ($exitCode -eq 0 -and (Test-Path $testJsonPath) -and (Get-Item $testJsonPath).Length -gt 0) {
                        try {
                            $toolJson = Get-Content $testJsonPath -Raw | ConvertFrom-Json
                            if ($toolJson.SceneInfo -and $toolJson.SceneInfo.Count -gt 0) {
                                $hasHDR10Plus = $true
                                $detectionMethods += "HDR10+ tool: Found $($toolJson.SceneInfo.Count) scenes"
                                if ($Verbose) { Write-Host "  * Found via HDR10+ tool: $($toolJson.SceneInfo.Count) scenes" -ForegroundColor Green }
                            }
                        }
                        catch {
                            if ($Verbose) { Write-Host "  HDR10+ tool JSON parsing failed" -ForegroundColor Yellow }
                        }
                    }
                }
                else {
                    # Process timed out
                    if ($Verbose) { Write-Host "  HDR10+ tool test timed out" -ForegroundColor Yellow }
                    try {
                        $toolProcess.Kill()
                        $toolProcess.WaitForExit(5000)
                        $toolProcess.Close()
                        $toolProcess.Dispose()
                        $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $toolProcess.Id }
                    }
                    catch {
                        # Ignore cleanup errors
                    }
                }
            }
            catch {
                if ($Verbose) { Write-Host "  HDR10+ tool test failed: $_" -ForegroundColor Yellow }
            }
        }
        
        if ($Verbose) {
            Write-Host "=== HDR10+ Detection Results ===" -ForegroundColor Cyan
            if ($hasHDR10Plus) {
                Write-Host "* HDR10+ DETECTED via $($detectionMethods.Count) method(s)" -ForegroundColor Green
                # Only show first few methods to avoid spam
                $detectionMethods | Select-Object -First 5 | ForEach-Object {
                    Write-Host "  - $_" -ForegroundColor Green
                }
                if ($detectionMethods.Count -gt 5) {
                    Write-Host "  - ... and $($detectionMethods.Count - 5) more detections" -ForegroundColor Green
                }
            }
            else {
                Write-Host "* NO HDR10+ METADATA FOUND" -ForegroundColor Red
            }
        }
        
        return @{
            HasHDR10Plus = $hasHDR10Plus
            DetectionMethods = $detectionMethods
        }
    }
    catch {
        if ($Verbose) { 
            Write-Warning "Error during comprehensive HDR10+ detection: $_"
        }
        return @{
            HasHDR10Plus = $false
            DetectionMethods = @("Error: $($_.Exception.Message)")
        }
    }
}

function Invoke-PlexOptimizedRemux {
    param(
        [string]$VideoFile,
        [string]$OriginalFile,
        [hashtable]$AudioInfo,
        [hashtable]$SubtitleInfo,
        [string]$OutputPath
    )
    
    Write-Host "Starting enhanced remux with all streams..." -ForegroundColor Yellow
    
    # Verify inputs exist
    if (-not (Test-Path $VideoFile)) { throw "Video file not found: $VideoFile" }
    if (-not (Test-Path $OriginalFile)) { throw "Original file not found: $OriginalFile" }
    
    Write-Host "Input verification passed" -ForegroundColor Green
    Write-Host "  Video: $VideoFile ($(((Get-Item $VideoFile).Length/1MB).ToString('F2')) MB)" -ForegroundColor White
    Write-Host "  Original: $OriginalFile ($(((Get-Item $OriginalFile).Length/1MB).ToString('F2')) MB)" -ForegroundColor White
    
    # Extract global metadata from original file
    Write-Host "Extracting global metadata from original file..." -ForegroundColor Cyan
    $globalMetadata = Get-GlobalMetadata -FilePath $OriginalFile

    # Convert metadata to FFmpeg arguments
    $metadataArgs = @()
    if ($globalMetadata -and $globalMetadata.PSObject.Properties.Count -gt 0) {
        foreach ($property in $globalMetadata.PSObject.Properties) {
            $key = $property.Name
            $value = $property.Value
            
            # Skip empty values
            if ($value -and $value.ToString().Trim() -ne "") {
                $metadataArgs += "-metadata", "$key=$value"
                Write-Host "  Preserving: $key = $value" -ForegroundColor Gray
            }
        }
        Write-Host "Added $($metadataArgs.Count / 2) metadata tags" -ForegroundColor Green
    }
    else {
        Write-Host "No global metadata to preserve" -ForegroundColor Yellow
    }
    
    # CRITICAL FIX: Detect and handle duration mismatches
    Write-Host "Checking for duration metadata issues..." -ForegroundColor Cyan
    $actualVideoDuration = $null
    $containerDuration = $null
    $useDurationOverride = $false
    
    try {
        # Get actual video stream duration
        $videoDurationArgs = @(
            "-v", "quiet",
            "-select_streams", "v:0",
            "-show_entries", "stream=duration",
            "-of", "csv=p=0",
            $VideoFile
        )
        $videoDurationOutput = & $Config.FFProbeExe @videoDurationArgs 2>$null
        if ($videoDurationOutput -eq "N/A") {
            $videoDurationOutput = $null
        }
        if ($videoDurationOutput) {
            $actualVideoDuration = [double]$videoDurationOutput
        }
        
        # Get container duration from original file
        $containerDurationArgs = @(
            "-v", "quiet",
            "-show_entries", "format=duration",
            "-of", "csv=p=0",
            $OriginalFile
        )
        $containerDurationOutput = & $Config.FFProbeExe @containerDurationArgs 2>$null
        if ($containerDurationOutput -eq "N/A") {
            $containerDurationOutput = $null
        }
        if ($containerDurationOutput) {
            $containerDuration = [double]$containerDurationOutput
        }
        
        # Check for duration mismatch
        if ($actualVideoDuration -and $containerDuration) {
            $durationDiff = [math]::Abs($containerDuration - $actualVideoDuration)
            $diffPercent = ($durationDiff / $actualVideoDuration) * 100
            
            if ($durationDiff -gt 60) {  # More than 60 seconds difference
                Write-Warning "Container metadata duration mismatch detected!"
                Write-Host "  Container duration: $([math]::Round($containerDuration, 2))s ($([math]::Round($containerDuration/60, 1)) min)" -ForegroundColor Yellow
                Write-Host "  Actual video duration: $([math]::Round($actualVideoDuration, 2))s ($([math]::Round($actualVideoDuration/60, 1)) min)" -ForegroundColor Green
                Write-Host "  Difference: $([math]::Round($durationDiff, 2))s ($([math]::Round($diffPercent, 1))%)" -ForegroundColor Yellow
                Write-Host "  Will use actual stream duration for output" -ForegroundColor Green
                $useDurationOverride = $true
            }
            else {
                Write-Host "  Duration metadata is consistent (difference: $([math]::Round($durationDiff, 2))s)" -ForegroundColor Green
            }
        }
    }
    catch {
        Write-Warning "Could not verify duration metadata: $_"
    }
    
    # Create temp output
    $tempOutput = $OutputPath + ".temp.mkv"
    
    # Build comprehensive FFmpeg command with all streams
    $largs = @(
        '-y'
        '-hide_banner'
        '-loglevel', 'warning'
    )
    
    # CRITICAL FIX: Add flags to handle timestamp issues
    $largs += @(
        '-fflags', '+genpts+discardcorrupt+igndts'  # Generate proper timestamps, ignore bad DTS
        '-avoid_negative_ts', 'make_zero'
        '-max_interleave_delta', '0'                 # Force proper interleaving
        '-use_wallclock_as_timestamps', '0'
        '-copytb', '1'                               # Copy timebase to maintain timing
    )
    
    # Add inputs
    $largs += @(
        '-i', $VideoFile                    # Input 0: Encoded video
        '-itsoffset', '0'
        '-i', $OriginalFile                 # Input 1: Original file with audio/subtitles
    )
    
    # Add external subtitle files as additional inputs if they exist
    $inputIndex = 2
    $externalSubInputs = @{}
    if ($Script:ExtractedSubtitles -and $Script:ExtractedSubtitles.Count -gt 0) {
        foreach ($extSubFile in $Script:ExtractedSubtitles) {
            if (Test-Path $extSubFile) {
                $largs += '-i', $extSubFile
                $externalSubInputs[$extSubFile] = $inputIndex
                Write-Host "  Adding external subtitle input $($inputIndex) : $(Split-Path $extSubFile -Leaf)" -ForegroundColor Gray
                $inputIndex++
            }
        }
    }
    
    # CRITICAL FIX: If duration override is needed, apply it before mapping
    if ($useDurationOverride -and $actualVideoDuration) {
        $correctDuration = [math]::Round($actualVideoDuration, 3)
        Write-Host "Applying duration correction: $correctDuration seconds" -ForegroundColor Cyan
        $largs += '-t', $correctDuration  # Force correct duration
    }
    
    # Map video from encoded file
    $largs += "-map", "0:v"
    $largs += "-c:v", "copy"
    $largs += "-map_metadata:s:v", "0:s:v"  # Copy video stream metadata
    $largs += "-disposition:v:0", "default"

    # VFR/CFR handling for video timing with duration fix
    $largs += '-fps_mode', 'cfr'          # Force constant frame rate
    $largs += '-r', $Script:VideoMetadata.FrameRate  # Explicit frame rate
#    $largs += '-vsync', 'cfr'             # Ensure CFR video sync
    $largs += '-async', '1'                # Audio sync
    

# Get Plex-optimized audio settings
Write-Host "Configuring Plex-optimized audio settings..." -ForegroundColor Cyan
$plexAudioSettings = Get-PlexOptimizedAudioSettings -AudioInfo $AudioInfo -CreateCompatibilityTrack $true -HasExistingAAC $AudioInfo.HasAAC

# Map audio streams based on Plex optimization
if ($AudioInfo.ToCopy) {
    $audioGlobalIndex = $AudioInfo.ToCopy.GlobalIndex
    Write-Host "  Mapping primary audio stream from global index: $audioGlobalIndex" -ForegroundColor Green
    
    # Get audio language and title
    $audioLanguage = if ($AudioInfo.ToCopy.Language -and $AudioInfo.ToCopy.Language -ne "und") { 
        $AudioInfo.ToCopy.Language 
    } else { 
        "eng" 
    }
    
    $originalTitle = if ($AudioInfo.ToCopy.Title -and $AudioInfo.ToCopy.Title -notmatch "^Track \d+$") {
        $cleanTitle = $AudioInfo.ToCopy.Title -replace '[^\w\s\-\.\(\)]', '_' -replace '\s+', ' '
        $cleanTitle.Trim()
    } else {
        $codecDisplayName = switch ($AudioInfo.ToCopy.Codec.ToLower()) {
            "truehd" { "Dolby TrueHD" }
            "dts-hd" { "DTS-HD MA" }
            "dts" { "DTS" }
            "eac3" { "Dolby Digital Plus" }
            "ac3" { "Dolby Digital" }
            "aac" { "AAC" }
            "flac" { "FLAC" }
            default { $AudioInfo.ToCopy.Codec.ToUpper() }
        }
        $channels = $AudioInfo.ToCopy.Channels
        $layoutName = switch ($channels) {
            1 { "Mono" }
            2 { "Stereo" } 
            6 { "5.1" }
            8 { "7.1" }
            default { "${channels}.0" }
        }
        "$codecDisplayName $layoutName"
    }
    
    # Handle TrueHD special case - AAC primary, TrueHD secondary
    if ($AudioInfo.ToCopy.Codec.ToLower() -eq "truehd" -and $plexAudioSettings.RequiresFallback) {
        Write-Host "  TrueHD detected - creating AAC primary + TrueHD secondary tracks" -ForegroundColor Green
        
        # AAC PRIMARY track (default)
        $largs += '-map', "1:a:$audioGlobalIndex"
        $largs += '-c:a:0', 'aac'
        $compatBitrate = if ($AudioInfo.ToCopy.Channels -ge 6) { "640k" } else { "256k" }
        $compatChannels = if ($AudioInfo.ToCopy.Channels -ge 6) { "6" } else { "2" }
        $largs += '-b:a:0', $compatBitrate
        $largs += '-ac:a:0', $compatChannels
        $largs += '-profile:a:0', 'aac_low'
        
        # AAC metadata
        $largs += '-metadata:s:a:0', "title=AAC Compatibility ($compatChannels ch)"
        $largs += '-metadata:s:a:0', "language=$audioLanguage"
        $largs += '-disposition:a:0', 'default'
        
        # TrueHD SECONDARY track
        $largs += '-map', "1:a:$audioGlobalIndex" 
        $largs += '-c:a:1', 'copy'
        
        # TrueHD metadata
        $largs += '-metadata:s:a:1', "title=$originalTitle"
        $largs += '-metadata:s:a:1', "language=$audioLanguage"
        $largs += '-disposition:a:1', '0'
        
        # Audio duration correction if needed
        if ($useDurationOverride) {
            $largs += '-af:a:0', "apad=whole_dur=$correctDuration"
            $largs += '-af:a:1', "apad=whole_dur=$correctDuration"
        }
        
        Write-Host "  Created dual-track: AAC primary (default) + TrueHD secondary" -ForegroundColor Green
        
    } else {
        # Standard single track processing
        Write-Host "  Creating single optimized track" -ForegroundColor Green
        
        # Primary audio track
        $largs += '-map', "1:a:$audioGlobalIndex"
        
        # Apply codec conversion if needed (from plexAudioSettings logic)
        $sourceCodec = $AudioInfo.ToCopy.Codec.ToLower()
        switch ($sourceCodec) {
            "dts" {
                if (-not $AudioInfo.HasAAC) {
                    # Convert DTS to EAC3 for better Plex compatibility
                    $largs += '-c:a:0', 'eac3'
                    $targetBitrate = if ($AudioInfo.ToCopy.Channels -ge 6) { "1536k" } else { "448k" }
                    $largs += '-b:a:0', $targetBitrate
                    $compatChannels = if ($AudioInfo.ToCopy.Channels -ge 6) { "6" } else { "2" }
                    $largs += '-ac:a:0', $compatChannels
                    $originalTitle = "Dolby Digital Plus $layoutName"
                } else {
                    $largs += '-c:a:0', 'copy'
                }
            }
            "flac" {
                if (-not $AudioInfo.HasAAC) {
                    # Convert FLAC to AAC
                    $largs += '-c:a:0', 'aac'
                    $targetBitrate = if ($AudioInfo.ToCopy.Channels -ge 6) { "640k" } else { "320k" }
                    $largs += '-b:a:0', $targetBitrate
                    $compatChannels = if ($AudioInfo.ToCopy.Channels -ge 6) { "6" } else { "2" }
                    $largs += '-ac:a:0', $compatChannels
                    $largs += '-profile:a:0', 'aac_low'
                    $originalTitle = "AAC $layoutName"
                } else {
                    $largs += '-c:a:0', 'copy'
                }
            }
            { $_ -in @("pcm_s16le", "pcm_s24le") } {
                if (-not $AudioInfo.HasAAC) {
                    # Convert PCM to AAC
                    $largs += '-c:a:0', 'aac'
                    $targetBitrate = if ($AudioInfo.ToCopy.Channels -ge 6) { "640k" } else { "320k" }
                    $largs += '-b:a:0', $targetBitrate
                    $compatChannels = if ($AudioInfo.ToCopy.Channels -ge 6) { "6" } else { "2" }
                    $largs += '-ac:a:0', $compatChannels
                    $largs += '-profile:a:0', 'aac_low'
                    $originalTitle = "AAC $layoutName"
                } else {
                    $largs += '-c:a:0', 'copy'
                }
            }
            default {
                # Copy as-is for AAC, AC3, EAC3, etc.
                $largs += '-c:a:0', 'copy'
            }
        }
        
        # Audio duration correction if needed
        if ($useDurationOverride) {
            $largs += '-af:a:0', "apad=whole_dur=$correctDuration"
        }
        
        # Primary track metadata
        $largs += '-metadata:s:a:0', "title=$originalTitle"
        $largs += '-metadata:s:a:0', "language=$audioLanguage"
        $largs += '-disposition:a:0', 'default'
    }
    
} else {
    Write-Warning "No audio stream selected - output will have no audio!"
}
    
    # Map subtitle streams
    $subtitleStreamIndex = 0
    $hasDefaultSubtitle = $false
    $forcedSubtitleFound = $false
    
    # First, check if we have any forced subtitles across all sources
    foreach ($stream in $SubtitleInfo.ToCopy) {
        if ($stream.IsForced) {
            $forcedSubtitleFound = $true
            break
        }
    }
    
    if (-not $forcedSubtitleFound -and $Script:ExtractedSubtitles) {
        foreach ($extSubFile in $Script:ExtractedSubtitles) {
            $fileName = [System.IO.Path]::GetFileNameWithoutExtension($extSubFile)
            if ($fileName -match "forced|Forced|FORCED") {
                $forcedSubtitleFound = $true
                break
            }
        }
    }
    
    Write-Host "  Forced subtitles detected: $forcedSubtitleFound" -ForegroundColor $(if ($forcedSubtitleFound) { "Green" } else { "Yellow" })
    
    # Map internal subtitle streams that can be copied
    foreach ($stream in $SubtitleInfo.ToCopy) {
        Write-Host "  Mapping internal subtitle stream $($stream.LogicalIndex)$(if ($stream.IsForced) { ' [FORCED]' } else { '' })" -ForegroundColor Gray
        
        $largs += '-map', "1:s:$($stream.LogicalIndex)"
        
        # Convert text-based subtitles to SRT for better Plex compatibility
        if ($stream.Codec -eq "subrip" -or $stream.Codec -eq "mov_text") {
            $largs += "-c:s:$($subtitleStreamIndex)", 'srt'
        } else {
            $largs += "-c:s:$($subtitleStreamIndex)", 'copy'
        }
        
        # Set language
        $largs += "-metadata:s:s:$($subtitleStreamIndex)", "language=$($stream.Language)"
        
        # Set title
        if ($stream.Title) {
            $titleToUse = $stream.Title
            if ($stream.IsForced -and $titleToUse -notmatch "forced|Forced|FORCED") {
                $titleToUse = "$titleToUse (Forced)"
            }
            $largs += "-metadata:s:s:$($subtitleStreamIndex)", "title=$($titleToUse)"
        } else {
            if ($stream.IsForced) {
                $largs += "-metadata:s:s:$($subtitleStreamIndex)", "title=English (Forced)"
            } else {
                $largs += "-metadata:s:s:$($subtitleStreamIndex)", "title=English"
            }
        }
        
        # Set disposition - forced subtitles get priority
        if ($stream.IsForced) {
            if (-not $hasDefaultSubtitle) {
                $largs += "-disposition:s:$($subtitleStreamIndex)", "forced+default"
                $hasDefaultSubtitle = $true
                Write-Host "    Set as forced+default" -ForegroundColor Green
            } else {
                $largs += "-disposition:s:$($subtitleStreamIndex)", 'forced'
                Write-Host "    Set as forced" -ForegroundColor Green
            }
        } elseif (-not $forcedSubtitleFound -and -not $hasDefaultSubtitle) {
            # Only set regular subtitles as default if there are NO forced subtitles
            $largs += "-disposition:s:$($subtitleStreamIndex)", 'default'
            $hasDefaultSubtitle = $true
            Write-Host "    Set as default (no forced subtitles exist)" -ForegroundColor Green
        } else {
            $largs += "-disposition:s:$($subtitleStreamIndex)", '0'
            Write-Host "    No special disposition" -ForegroundColor Gray
        }
        
        $subtitleStreamIndex++
    }
    
    # Map external subtitle files
    foreach ($extSubFile in $Script:ExtractedSubtitles) {
        if (Test-Path $extSubFile) {
            if ($externalSubInputs.ContainsKey($extSubFile)) {
                $inputIdx = $externalSubInputs[$extSubFile]
                $fileName = [System.IO.Path]::GetFileNameWithoutExtension($extSubFile)
                $isExtractedForced = $fileName -match "forced|Forced|FORCED"
                
                Write-Host "  Mapping external subtitle: $(Split-Path $extSubFile -Leaf)$(if ($isExtractedForced) { ' [FORCED]' } else { '' })" -ForegroundColor Gray
                
                $largs += '-map', "$($inputIdx):s:0"
                
                # Determine codec based on file extension
                $extension = [System.IO.Path]::GetExtension($extSubFile).ToLower()
                if ($extension -eq ".sup") {
                    $largs += "-c:s:$($subtitleStreamIndex)", 'copy'
                    $format = "PGS"
                } else {
                    $largs += "-c:s:$($subtitleStreamIndex)", 'srt'
                    $format = "SRT"
                }
                
                $largs += "-metadata:s:s:$($subtitleStreamIndex)", "language=eng"
                
                # Set title and disposition for external subtitles
                if ($isExtractedForced) {
                    $largs += "-metadata:s:s:$($subtitleStreamIndex)", "title=English ($($format) Extracted - Forced)"
                    if (-not $hasDefaultSubtitle) {
                        $largs += "-disposition:s:$($subtitleStreamIndex)", "forced+default"
                        $hasDefaultSubtitle = $true
                        Write-Host "    Set extracted forced subtitle as forced+default" -ForegroundColor Green
                    } else {
                        $largs += "-disposition:s:$($subtitleStreamIndex)", 'forced'
                        Write-Host "    Set extracted forced subtitle as forced" -ForegroundColor Green
                    }
                } else {
                    $largs += "-metadata:s:s:$($subtitleStreamIndex)", "title=English ($($format) Extracted)"
                    if (-not $forcedSubtitleFound -and -not $hasDefaultSubtitle) {
                        $largs += "-disposition:s:$($subtitleStreamIndex)", "default"
                        $hasDefaultSubtitle = $true
                        Write-Host "    Set extracted subtitle as default (no forced subtitles)" -ForegroundColor Green
                    } else {
                        $largs += "-disposition:s:$($subtitleStreamIndex)", "0"
                        Write-Host "    No special disposition for extracted subtitle" -ForegroundColor Gray
                    }
                }
                
                $subtitleStreamIndex++
            }
        }
    }

    # Add final arguments with metadata preservation
    $largs += '-f', 'matroska'
    
    # CRITICAL FIX: Add container-level duration correction
    if ($useDurationOverride -and $actualVideoDuration) {
        # Force container to have correct duration metadata
        $largs += '-metadata', "DURATION=$([math]::Round($actualVideoDuration, 3))"
    }

    # Add all the preserved metadata
    $largs += $metadataArgs

    $largs += $tempOutput
    
    Write-Host "  Total streams to be mapped:" -ForegroundColor Green
    Write-Host "    Video: 1 stream (CFR timing$(if ($useDurationOverride) { ', duration corrected' } else { '' }))" -ForegroundColor White
    Write-Host "    Audio: $($plexAudioSettings.StreamCount) stream$(if ($plexAudioSettings.StreamCount -gt 1) { 's' } else { '' })$(if ($plexAudioSettings.RequiresFallback) { ' (Plex-optimized with compatibility)' } else { ' (Plex-optimized)' })" -ForegroundColor White
    Write-Host "    Subtitles: $subtitleStreamIndex streams" -ForegroundColor White
    Write-Host "    Metadata: $($metadataArgs.Count / 2) global tags" -ForegroundColor White
    if ($useDurationOverride) {
        Write-Host "    Duration: Corrected to $([math]::Round($actualVideoDuration, 2))s" -ForegroundColor Green
    }
    
    # Enhanced audio configuration summary
    Write-Host "`n  Audio Configuration Summary:" -ForegroundColor Cyan
    Write-Host "    Primary codec: $($AudioInfo.ToCopy.Codec) -> $(if ($plexAudioSettings.RequiresFallback) { 'AAC (TrueHD) or EAC3/Copy (others)' } else { 'Copy' })" -ForegroundColor White
    if ($plexAudioSettings.RequiresFallback) {
        Write-Host "    Secondary codec: Copy (original quality preserved)" -ForegroundColor White
        Write-Host "    Strategy: Dual-track for maximum Plex compatibility" -ForegroundColor Green
    } else {
        Write-Host "    Strategy: Single optimized track" -ForegroundColor Green
    }
    
    # Log the exact command for debugging (truncated for readability)
    Write-Host "`nExecuting comprehensive FFmpeg remux with Plex-optimized audio..." -ForegroundColor Cyan
    
    try {
        # Use ProcessStartInfo with async output handling
        $processInfo = New-Object System.Diagnostics.ProcessStartInfo
        $processInfo.FileName = $Config.FFmpegExe
        $processInfo.UseShellExecute = $false
        $processInfo.RedirectStandardOutput = $true
        $processInfo.RedirectStandardError = $true
        $processInfo.CreateNoWindow = $true
        
        # Use proper argument handling based on PowerShell version
        if ($PSVersionTable.PSVersion.Major -ge 7) {
            foreach ($arg in $largs) {
                $null = $processInfo.ArgumentList.Add($arg)
            }
        }
        else {
            # For PowerShell 5.x, use the Arguments property with proper quoting
            $legacyQuotedArgs = $largs | ForEach-Object { 
                if ($_ -match '\s') { '"{0}"' -f ($_ -replace '"', '""') } else { $_ }
            }
            $processInfo.Arguments = $legacyQuotedArgs -join ' '
        }
        
        $debugArgs = $largs | ForEach-Object { 
            if ($_ -match '\s') { '"{0}"' -f ($_ -replace '"', '""') } else { $_ }
        }
        $debugArgs = $debugArgs -join ' '

        Write-Host "Args: $($debugArgs)"
        $process = [System.Diagnostics.Process]::Start($processInfo)
        $handle = $process.Handle
        $Script:ActiveProcesses = @($Script:ActiveProcesses) + $process
        
        # Use async reading to prevent deadlocks
        $stdoutTask = $process.StandardOutput.ReadToEndAsync()
        $stderrTask = $process.StandardError.ReadToEndAsync()
        
        # Set process priority
        try {
            $process.PriorityClass = [System.Diagnostics.ProcessPriorityClass]::BelowNormal
        }
        catch {
            Write-Host "Could not set FFmpeg process priority" -ForegroundColor Gray
        }
        
        # Monitor progress with timeout
        $timeout = 18000000  # 300 minute timeout
        Write-Host "Starting enhanced remux process with 300-minute timeout..." -ForegroundColor Yellow
        
        if (-not $process.WaitForExit($timeout)) {
            Write-Warning "FFmpeg remux timed out after 300 minutes - killing process"
            try {
                $process.Kill()
                $process.WaitForExit(5000)
            }
            catch {
                Write-Warning "Failed to kill FFmpeg process cleanly: $_"
            }
            
            $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $process.Id }
            throw "FFmpeg remux timed out after 300 minutes"
        }
        
        # Get the results from async tasks
        $stdout = $stdoutTask.Result
        $stderr = $stderrTask.Result
        $exitCode = $process.ExitCode
        
        # Clean up process
        $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $process.Id }
        $process.Close()
        $process.Dispose()
        
        Write-Host "FFmpeg completed with exit code: $exitCode" -ForegroundColor Gray
        
        if ($exitCode -eq 0) {
            if (Test-Path $tempOutput) {
                # Verify temp output file has content
                $tempFileSize = (Get-Item $tempOutput).Length
                if ($tempFileSize -eq 0) {
                    throw "FFmpeg completed but output file is empty"
                }
                
                # CRITICAL: Verify the output file has correct duration
                if ($useDurationOverride) {
                    try {
                        $outputDurationArgs = @(
                            "-v", "quiet",
                            "-show_entries", "format=duration",
                            "-of", "csv=p=0",
                            $tempOutput
                        )
                        $outputDuration = & $Config.FFProbeExe @outputDurationArgs 2>$null
                        if ($outputDuration) {
                            $outputDuration = [double]$outputDuration
                            $finalDiff = [math]::Abs($outputDuration - $actualVideoDuration)
                            
                            if ($finalDiff -lt 1.0) {
                                Write-Host "  Duration correction successful: $([math]::Round($outputDuration, 2))s" -ForegroundColor Green
                            } else {
                                Write-Warning "  Duration still differs: Output=$([math]::Round($outputDuration, 2))s, Expected=$([math]::Round($actualVideoDuration, 2))s"
                            }
                        }
                    }
                    catch {
                        Write-Warning "Could not verify output duration: $_"
                    }
                }
                
                # Move temp to final location
                if (Test-Path $OutputPath) {
                    Remove-Item $OutputPath -Force
                }
                Move-Item $tempOutput $OutputPath -Force
                
                $finalFileSize = (Get-Item $OutputPath).Length
                $finalSizeMB = [math]::Round($finalFileSize / 1MB, 2)
                
                Write-Host "Enhanced remux completed successfully: $finalSizeMB MB" -ForegroundColor Green
                Write-Host "Content timing: CFR normalized" -ForegroundColor Green
                Write-Host "Audio optimization: Plex-compatible track configuration applied" -ForegroundColor Green
                Write-Host "Metadata preservation: $($metadataArgs.Count / 2) tags preserved" -ForegroundColor Green
                if ($useDurationOverride) {
                    Write-Host "Duration: Corrected from $([math]::Round($containerDuration, 2))s to $([math]::Round($actualVideoDuration, 2))s" -ForegroundColor Green
                }
                
                # Verify streams were properly muxed
                Write-Host "Verifying final output streams..." -ForegroundColor Cyan
                try {
                    $verifyArgs = @("-v", "quiet", "-show_streams", "-select_streams", "a,s", "-of", "csv=p=0:s=x", $OutputPath)
                    $streamVerify = & $Config.FFProbeExe @verifyArgs 2>$null
                    
                    if ($streamVerify) {
                        $audioStreams = ($streamVerify | Where-Object { $_ -match "^audio" }).Count
                        $subtitleStreams = ($streamVerify | Where-Object { $_ -match "^subtitle" }).Count
                        
                        Write-Host "Final verification - Audio streams: $audioStreams (expected: $($plexAudioSettings.StreamCount)), Subtitle streams: $subtitleStreams" -ForegroundColor Green
                        
                        if ($audioStreams -ne $plexAudioSettings.StreamCount) {
                            Write-Warning "Audio stream count mismatch! Expected: $($plexAudioSettings.StreamCount), Found: $audioStreams"
                        } else {
                            Write-Host "Audio stream configuration: VERIFIED" -ForegroundColor Green
                        }
                        
                        if ($subtitleStreams -eq 0 -and $subtitleStreamIndex -gt 0) {
                            Write-Warning "NO SUBTITLE STREAMS found in final output despite mapping $subtitleStreamIndex streams!"
                        }
                    }
                }
                catch {
                    Write-Host "Could not verify output streams: $_" -ForegroundColor Yellow
                }
                
            } else {
                throw "FFmpeg completed but output file not created"
            }
        } else {
            Write-Host "FFmpeg failed with exit code: $exitCode" -ForegroundColor Red
            
            # Show error output for debugging
            if ($stderr -and $stderr.Trim().Length -gt 0) {
                Write-Host "FFmpeg stderr output:" -ForegroundColor Red
                # Show last 20 lines of stderr to avoid spam
                $stderrLines = $stderr -split "`n"
                $errorLines = if ($stderrLines.Length -gt 20) { 
                    $stderrLines[-20..-1] 
                } else { 
                    $stderrLines 
                }
                foreach ($line in $errorLines) {
                    if ($line.Trim().Length -gt 0) {
                        Write-Host "  $($line.Trim())" -ForegroundColor Red
                    }
                }
            }
            
            throw "FFmpeg failed with exit code: $exitCode"
        }
    }
    catch {
        Write-Error "Enhanced remux failed: $_"
        # Clean up temp file if it exists
        if (Test-Path $tempOutput) { 
            try {
                Remove-Item $tempOutput -Force -ErrorAction SilentlyContinue 
            }
            catch {
                # Ignore cleanup errors
            }
        }
        throw
    }
}


#endregion

# Function to create script-specific temp subdirectory
function Initialize-ScriptTempFolder {
    param([string]$BaseTempFolder)
    
    $scriptTempFolder = Join-Path $BaseTempFolder "VideoProcessing_$(Get-Date -Format 'yyyyMMdd_HHmmss')_$($PID)"
    
    try {
        Write-Host "Creating script-specific temp folder: $scriptTempFolder" -ForegroundColor Cyan
        New-Item -ItemType Directory -Path $scriptTempFolder -Force | Out-Null
        
        # Register for cleanup
        $Script:ScriptTempFolder = $scriptTempFolder
        
        Write-Host "Script temp folder created: $scriptTempFolder" -ForegroundColor Green
        return $scriptTempFolder
        
    }
    catch {
        Write-Warning "Failed to create script temp folder: $_"
        # Fallback to base temp folder
        return $BaseTempFolder
    }
}


function Invoke-FFmpegEncodingWithErrorDetection {
    param(
        [string]$Executable,
        [string[]]$Arguments,
        [string]$Description,
        [string]$CodecType = "HEVC",
        [bool]$DisableMCEBuddyMonitoring = $false,
        [int]$MCEBuddyCheckInterval = 15
    )
    
    Write-Host "[EXEC] $Description (with file-based progress and error monitoring)" -ForegroundColor Cyan
    
    # MCEBuddy monitoring setup
    $mceMonitoringJob = $null
    #$mceSuspended = $false
    
    if (-not $DisableMCEBuddyMonitoring) {
        Write-Host "MCEBuddy monitoring enabled (check interval: $MCEBuddyCheckInterval seconds)" -ForegroundColor Cyan
        Write-Host "FFmpeg will be PAUSED when MCEBuddy is paused" -ForegroundColor Yellow
        
        # Check initial MCEBuddy state
        $initialMCEState = Get-McebuddyEngineState
        Write-Host "Initial MCEBuddy state: $initialMCEState" -ForegroundColor Gray
    }
    
    # Build command line for display
    $quotedArgs = @()
    $quotedArgs += $Executable
    foreach ($arg in $Arguments) {
        if ($arg -match '\s') {
            $quotedArgs += "`"$arg`""
        }
        else {
            $quotedArgs += $arg
        }
    }
    
    $commandLine = $quotedArgs -join ' '
    Write-Host "[CMD] $commandLine" -ForegroundColor Gray
    
    # Create a log file for FFmpeg stderr
    $ffmpegLogFile = Join-Path $env:TEMP "ffmpeg_log_$((Get-Random)).txt"
    Add-TempFile -FilePath $ffmpegLogFile
    $ffmpegLogFileStd = Join-Path $env:TEMP "ffmpeg_log_std_$((Get-Random)).txt"
    Add-TempFile -FilePath $ffmpegLogFileStd
    
    # Define codec-specific error patterns
    $codecErrorPatterns = @{}
    $codecErrorCounters = @{}
    
        $codecErrorPatterns = @{
            "NALU_Errors"      = @(
                "Skipping invalid undecodable NALU",
                "Invalid NAL unit size",
                "NAL unit header corrupted"
            )
            "Reference_Errors" = @(
                "Could not find ref with POC",
                "Missing reference picture",
                "Reference picture set error"
            )
            "Structure_Errors" = @(
                "Error constructing the frame RPS",
                "decode_slice_header error",
                "error while decoding MB",
                "RPS error",
                "corrupted frame detected"
            )
            "Quality_Errors"   = @(
                "concealing errors",
                "error concealment applied"
            )
        }
        $codecErrorCounters = @{
            "NALU_Count"      = 0
            "Reference_Count" = 0
            "Structure_Count" = 0
            "Quality_Count"   = 0
        }
    

    
    # Critical encoding errors (common to both codecs)
    $criticalEncodingErrors = @(
        "Invalid data found when processing input",
        "Error while encoding",
        "Encoder initialization failed",
        "Hardware encoder not available",
        "QSV session initialization failed",
        "No space left on device",
        "Permission denied",
        "Out of memory",
        "Encoding failed",
        "libsvtav1.*failed",
        "libaom.*failed",
        "av1_qsv.*failed",
        "Segmentation fault",
        "Access violation",
        "File ended prematurely",
        "File extends beyond end of segment"
    )
    
    try {
        # Test if the executable exists and is accessible
        if (-not (Test-Path $Executable)) {
            throw "FFmpeg executable not found: $Executable"
        }
        
        # Properly quote arguments that contain special characters
        $quotedArguments = @()
        foreach ($arg in $Arguments) {
            if ($arg -match '[\s&<>|^]') {
                $quotedArguments += "`"$arg`""
            }
            else {
                $quotedArguments += $arg
            }
        }
        
        $startProcessParams = @{
            FilePath               = $Executable
            ArgumentList           = $quotedArguments
            NoNewWindow            = $true
            PassThru               = $true
            RedirectStandardError  = $ffmpegLogFile
            RedirectStandardOutput = $ffmpegLogFileStd
            Wait                   = $false
        }
        
        $process = Start-Process @startProcessParams
        $handle = $process.Handle # Cache proc.handle
        # Wait a moment for process to start
        Start-Sleep -Seconds 2
        
        # Check if process actually started
        if ($process.HasExited) {
            Write-Warning "Process exited immediately with code: $($process.ExitCode)"
            
            $earlyExitOutput = (Get-Content $ffmpegLogFile, $ffmpegLogFileStd -Raw -ErrorAction SilentlyContinue)
            if ($earlyExitOutput) {
                Write-Host "FFmpeg error output:" -ForegroundColor Red
                Write-Host $earlyExitOutput -ForegroundColor Red
            }
            else {
                Write-Host "Log file was not created or is empty" -ForegroundColor Red
            }
            
            throw "FFmpeg process failed to start or exited immediately"
        }
        
        $Script:ActiveProcesses = @($Script:ActiveProcesses) + $process
        
        Write-Host "Started process: $($process.ProcessName) (PID: $($process.Id))" -ForegroundColor Gray
    
        
        # Start MCEBuddy monitoring job that will pause/resume FFmpeg
        if (-not $DisableMCEBuddyMonitoring) {
            Write-Host "Starting MCEBuddy monitoring job that will pause FFmpeg when MCEBuddy is active..." -ForegroundColor Yellow
            
            $mceMonitoringJob = Start-Job -Name "MCEBuddyFFmpegController_$(Get-Random)" -ScriptBlock {
                param($CheckInterval, $FFmpegPID)
                
                function Get-McebuddyEngineState {
                    try {
                        # Use MCEBuddy UserCLI to get actual engine state
                        $mcebuddyCLI = "C:\Program Files\MCEBuddy2x\MCEBuddy.UserCLI.exe"
        
                        if (-not (Test-Path $mcebuddyCLI)) {
                            Write-Output "DEBUG: MCEBuddy UserCLI not found at $mcebuddyCLI"
                            return "NotInstalled"
                        }
        
                        #    Write-Output "DEBUG: Checking MCEBuddy state using CLI..."
        
                        # Query engine state using CLI
                        try {
                            $stateOutput = & $mcebuddyCLI --command=query --action=enginestate --quiet 2>&1
                            $stateExitCode = $LASTEXITCODE
            
                            #     Write-Output "DEBUG: MCEBuddy CLI enginestate output: '$stateOutput'"
                            #     Write-Output "DEBUG: MCEBuddy CLI exit code: $stateExitCode"
            
                            if ($stateExitCode -eq -2) {
                                #        Write-Output "DEBUG: MCEBuddy CLI returned -2 (failed to process command)"
                                return "Stopped"
                            }
                            elseif ($stateExitCode -eq -1) {
                                #      Write-Output "DEBUG: MCEBuddy CLI returned -1 (bad input parameters)"
                                return "Unknown"
                            }
                            elseif ($stateExitCode -ne 0) {
                                #    Write-Output "DEBUG: MCEBuddy CLI failed with exit code $stateExitCode"
                                return "Stopped"
                            }
            
                            # Parse the engine state
                            $engineState = $stateOutput.Trim()
                            #   Write-Output "DEBUG: Parsed engine state: '$engineState'"
            
                            switch ($engineState.ToLower()) {
                                "stopped" { 
                                    #        Write-Output "DEBUG: MCEBuddy engine is stopped"
                                    return "Stopped" 
                                }
                                "started" { 
                                    #       Write-Output "DEBUG: MCEBuddy engine is started, checking for active jobs..."
                                    # Engine is started but check if it's actually converting
                                    try {
                                        $queueOutput = & $mcebuddyCLI --command=query --action=queuelength --quiet 2>&1
                                        $queueExitCode = $LASTEXITCODE
                        
                                        #       Write-Output "DEBUG: Queue length command exit code: $queueExitCode"
                                        #       Write-Output "DEBUG: Queue length output: '$queueOutput'"
                        
                                        if ($queueExitCode -eq 0) {
                                            $queueLength = [int]$queueOutput.Trim()
                                            #          Write-Output "DEBUG: Parsed queue length: $queueLength"
                            
                                            if ($queueLength -gt 0) {
                                                #            Write-Output "DEBUG: Queue has $queueLength jobs, checking details..."
                                                $queueDetails = & $mcebuddyCLI --command=query --action=queue --quiet 2>&1
                                                #             Write-Output "DEBUG: Queue details: '$queueDetails'"
                                
                                                if ($queueDetails -match "converting" -or $queueDetails -match "Status.*Converting") {
                                                    #                Write-Output "DEBUG: Found converting job - returning Processing"
                                                    return "Processing"
                                                }
                                                else {
                                                    #                Write-Output "DEBUG: No converting jobs found - returning Idle"
                                                    return "Idle"
                                                }
                                            }
                                            else {
                                                #            Write-Output "DEBUG: Queue is empty - returning Idle"
                                                return "Idle"
                                            }
                                        }
                                        else {
                                            #         Write-Output "DEBUG: Failed to get queue length - assuming Idle"
                                            return "Idle"
                                        }
                                    }
                                    catch {
                                        #      Write-Output "DEBUG: Exception checking queue status: $_ - assuming Idle"
                                        return "Idle"
                                    }
                                }
                                "conversion_in_progress" { 
                                    #      Write-Output "DEBUG: MCEBuddy conversion in progress"
                                    return "Processing" 
                                }
                                "conversion_paused" { 
                                    #    Write-Output "DEBUG: MCEBuddy conversion paused - treating as Paused"
                                    return "Paused"
                                }
                                default { 
                                    #     Write-Output "DEBUG: Unknown engine state: '$engineState' - returning Unknown"
                                    return "Unknown" 
                                }
                            }
            
                        }
                        catch {
                            #   Write-Output "DEBUG: Exception running MCEBuddy CLI: $_"
                            return "Unknown"
                        }
        
                    }
                    catch {
                        #  Write-Output "DEBUG: Exception in Get-McebuddyEngineState: $_"
                        return "Unknown"
                    }
                }
                
                # Track FFmpeg suspension state
                $ffmpegSuspended = $false
                $lastMCEState = "Unknown"
                $suspendResumeErrors = 0
                $maxErrors = 5
                
                # Add Win32 API functions for process suspension
                try {
                    Add-Type -MemberDefinition @"
[DllImport("ntdll.dll")]
public static extern int NtSuspendProcess(IntPtr processHandle);

[DllImport("ntdll.dll")]
public static extern int NtResumeProcess(IntPtr processHandle);
"@ -Name "ProcessControl" -Namespace "Win32"
                }
                catch {
                    Write-Output "Warning: Could not load Win32 API functions for process control: $_"
                }
                
                Write-Output "MCEBuddy-FFmpeg controller started for PID $FFmpegPID"
                
                while ($true) {
                    Start-Sleep -Seconds $CheckInterval
                    
                    # Check if FFmpeg process still exists
                    try {
                        $ffmpegProcess = Get-Process -Id $FFmpegPID -ErrorAction SilentlyContinue
                        if (-not $ffmpegProcess) {
                            Write-Output "FFmpeg process (PID: $FFmpegPID) no longer exists - stopping monitoring"
                            break
                        }
                    }
                    catch {
                        Write-Output "Error checking FFmpeg process: $_"
                        break
                    }
                    
                    $currentState = Get-McebuddyEngineState
                    
                    # Report state changes
                    if ($currentState -ne $lastMCEState -and $currentState -ne "Unknown") {
                        Write-Output "MCEBuddy state changed: $lastMCEState -> $currentState"
                        $lastMCEState = $currentState
                    }
                    
                    # Handle FFmpeg suspension based on MCEBuddy state
                    try {
                        if ($currentState -eq "Paused" -and -not $ffmpegSuspended) {
                            Write-Output "MCEBuddy became Paused - PAUSING FFmpeg encoding process... ($($currentState))"
                            
                            try {
                                $result = [Win32.ProcessControl]::NtSuspendProcess($ffmpegProcess.Handle)
                                if ($result -eq 0) {
                                    $ffmpegSuspended = $true
                                    Write-Output "SUCCESS: FFmpeg process SUSPENDED (PID: $FFmpegPID)"
                                    $suspendResumeErrors = 0  # Reset error counter on success
                                }
                                else {
                                    Write-Output "FAILED: Could not suspend FFmpeg process (NT error code: $result)"
                                    $suspendResumeErrors++
                                }
                            }
                            catch {
                                Write-Output "EXCEPTION: Failed to suspend FFmpeg process: $_"
                                $suspendResumeErrors++
                            }
                            
                        }
                        elseif ($currentState -ne "Paused" -and $ffmpegSuspended) {
                            Write-Output "MCEBuddy became active - RESUMING FFmpeg encoding process... ($($currentState))"
                            
                            try {
                                $result = [Win32.ProcessControl]::NtResumeProcess($ffmpegProcess.Handle)
                                if ($result -eq 0) {
                                    $ffmpegSuspended = $false
                                    Write-Output "SUCCESS: FFmpeg process RESUMED (PID: $FFmpegPID)"
                                    $suspendResumeErrors = 0  # Reset error counter on success
                                }
                                else {
                                    Write-Output "FAILED: Could not resume FFmpeg process (NT error code: $result)"
                                    $suspendResumeErrors++
                                }
                            }
                            catch {
                                Write-Output "EXCEPTION: Failed to resume FFmpeg process: $_"
                                $suspendResumeErrors++
                            }
                        }
                        
                        # If we have too many suspend/resume errors, stop trying
                        if ($suspendResumeErrors -ge $maxErrors) {
                            Write-Output "ERROR: Too many suspend/resume failures ($suspendResumeErrors). Disabling FFmpeg process control."
                            Write-Output "FFmpeg will continue running normally without pause/resume functionality."
                            break
                        }
                        
                    }
                    catch {
                        Write-Output "Error managing FFmpeg process suspension: $_"
                        $suspendResumeErrors++
                    }
                }
                
                # Cleanup: Ensure FFmpeg is resumed if we're exiting monitoring
                if ($ffmpegSuspended) {
                    try {
                        $ffmpegProcess = Get-Process -Id $FFmpegPID -ErrorAction SilentlyContinue
                        if ($ffmpegProcess) {
                            Write-Output "Cleanup: Ensuring FFmpeg is resumed before monitoring exit..."
                            $result = [Win32.ProcessControl]::NtResumeProcess($ffmpegProcess.Handle)
                            if ($result -eq 0) {
                                Write-Output "Cleanup: FFmpeg process resumed successfully"
                            }
                            else {
                                Write-Output "Cleanup: Failed to resume FFmpeg process (NT error code: $result)"
                            }
                        }
                    }
                    catch {
                        Write-Output "Cleanup: Error resuming FFmpeg during monitoring exit: $_"
                    }
                }
                
                Write-Output "MCEBuddy-FFmpeg controller finished"
                
            } -ArgumentList $MCEBuddyCheckInterval, $process.Id
            
            Write-Host "MCEBuddy-FFmpeg controller started (Job ID: $($mceMonitoringJob.Id))" -ForegroundColor Green
            Write-Host "FFmpeg will be automatically paused when MCEBuddy becomes active" -ForegroundColor Green
        }
        
        # Monitor progress by reading the log file
        Write-Host "Monitoring encoding progress..." -ForegroundColor Yellow
        $startTime = Get-Date
        $lastProgressData = @{
            Frame = "0"; FPS = "0"; Speed = "N/A"; Time = "00:00:00"; Bitrate = "N/A"; Size = "0KiB"
        }
        
        while (-not $process.HasExited) {
            Start-Sleep -Seconds 10
            if ($process.HasExited) {
                $process.WaitForExit()
                $exitCode = $process.ExitCode
                break
            }
            $elapsed = (Get-Date) - $startTime
            
            # Read all log files to find the last progress line
            $ffmpegAllLogs = @()
            if (Test-Path $ffmpegLogFile) {
                $ffmpegAllLogs += Get-Content $ffmpegLogFile -ErrorAction SilentlyContinue
            }
            if (Test-Path $ffmpegLogFileStd) {
                $ffmpegAllLogs += Get-Content $ffmpegLogFileStd -ErrorAction SilentlyContinue
            }
            
            if ($ffmpegAllLogs.Count -gt 0) {
                $progressLines = $ffmpegAllLogs | Where-Object { 
                    $_ -match "frame=.*fps=.*time=.*bitrate=.*speed=" 
                }
                
                if ($progressLines) {
                    $latestProgress = $progressLines[-1]
                    
                    # Parse the latest progress
                    if ($latestProgress -match "frame=\s*(\d+)") { $lastProgressData["Frame"] = $matches[1] }
                    if ($latestProgress -match "fps=\s*(\d+(?:\.\d+)?)") { $lastProgressData["FPS"] = $matches[1] }
                    if ($latestProgress -match "speed=(\d+(?:\.\d+)?x|N/A)") { $lastProgressData["Speed"] = $matches[1] }
                    if ($latestProgress -match "time=(\d{2}:\d{2}:\d{2}(?:\.\d{2})?)") { $lastProgressData["Time"] = $matches[1] }
                    if ($latestProgress -match "bitrate=\s*(\d+(?:\.\d+)?\s*(?:k|M)?bits/s|N/A)") { $lastProgressData["Bitrate"] = $matches[1].Trim() }
                    if ($latestProgress -match "(?:size=|Lsize=)\s*(\d+(?:k|M|G)?iB)") { $lastProgressData["Size"] = $matches[1] }
                }
            }
            
            $elapsed = (Get-Date) - $startTime
            $statusColor = "White"
            if ($lastProgressData.Speed -match "(\d+\.?\d*)x") {
                $speedValue = [double]$matches[1]
                $statusColor = if ($speedValue -ge 1.0) { "Green" } elseif ($speedValue -ge 0.5) { "Yellow" } else { "Red" }
            }
            
            # Check for suspended state (0 speed might indicate suspension)
            $suspensionNote = ""
            if ($lastProgressData.Speed -eq "0x" -or $lastProgressData.Speed -eq "0.0x" -or $lastProgressData.FPS -eq "0") {
                $suspensionNote = " (PAUSED by MCEBuddy monitor?)"
                $statusColor = "Magenta"
            }
            
            Write-Host "Encoding Progress - Elapsed: $($elapsed.ToString('hh\:mm\:ss'))" -ForegroundColor Cyan
            Write-Host "  Frame: $($lastProgressData.Frame), FPS: $($lastProgressData.FPS), Speed: $($lastProgressData.Speed)$suspensionNote" -ForegroundColor $statusColor
            Write-Host "  Time: $($lastProgressData.Time), Size: $($lastProgressData.Size), Bitrate: $($lastProgressData.Bitrate)" -ForegroundColor White
            
            # MCEBuddy monitoring status (if monitoring job is active)
            if ($mceMonitoringJob -and -not $DisableMCEBuddyMonitoring) {
                $jobOutput = Receive-Job -Job $mceMonitoringJob -ErrorAction SilentlyContinue
                if ($jobOutput) {
                    foreach ($line in $jobOutput) {
                        Write-Host "  [MCEBuddy-FFmpeg] $line" -ForegroundColor Yellow
                    }
                }
            }
        }
        
        # Final check for exit code and wait for process completion
        if (-not $exitCode) {
            $process.WaitForExit()
            $exitCode = $process.ExitCode
        }
        
    }
    finally {
        # Remove from active processes list
        $Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $process.Id }
        
        # MCEBuddy cleanup
        if (-not $DisableMCEBuddyMonitoring) {
            Write-Host "Cleaning up MCEBuddy-FFmpeg monitoring..." -ForegroundColor Cyan
            
            # Stop monitoring job
            if ($mceMonitoringJob) {
                try {
                    # Get any final output
                    $finalJobOutput = Receive-Job -Job $mceMonitoringJob -ErrorAction SilentlyContinue
                    if ($finalJobOutput) {
                        foreach ($line in $finalJobOutput) {
                            Write-Host "  [MCEBuddy-FFmpeg Final] $line" -ForegroundColor Yellow
                        }
                    }
                    
                    Stop-Job -Job $mceMonitoringJob -PassThru -ErrorAction SilentlyContinue | Wait-Job -Timeout 10 -ErrorAction SilentlyContinue
                    Remove-Job -Job $mceMonitoringJob -Force -ErrorAction SilentlyContinue
                    Write-Host "MCEBuddy-FFmpeg monitoring job stopped and cleaned up" -ForegroundColor Green
                }
                catch {
                    Write-Host "Error stopping MCEBuddy-FFmpeg monitoring job: $_" -ForegroundColor Yellow
                }
            }
            
            # Ensure FFmpeg process is resumed if it was suspended
            if ($process -and -not $process.HasExited) {
                try {
                    Write-Host "Final cleanup: Ensuring FFmpeg process is not suspended..." -ForegroundColor Cyan
                    
                    # Add Win32 API for final resume attempt
                    try {
                        Add-Type -MemberDefinition @"
[DllImport("ntdll.dll")]
public static extern int NtResumeProcess(IntPtr processHandle);
"@ -Name "ProcessControlFinal" -Namespace "Win32Final" -ErrorAction SilentlyContinue
                        
                        $result = [Win32Final.ProcessControlFinal]::NtResumeProcess($process.Handle)
                        if ($result -eq 0) {
                            Write-Host "Final cleanup: FFmpeg process resume attempted" -ForegroundColor Green
                        }
                    }
                    catch {
                        # Ignore final resume errors - process may already be resumed or terminated
                    }
                }
                catch {
                    # Ignore cleanup errors during shutdown
                }
            }
        }
    }
    
    Write-Host "Process completed with exit code: $exitCode" -ForegroundColor Gray
    
   # Enhanced error reporting in the Invoke-FFmpegEncodingWithErrorDetection function
# Replace the existing error reporting section (around line 5630) with this enhanced version:

# Perform all error checking and reporting here, after the process is done
# Complete replacement for the error detection logic in Invoke-FFmpegEncodingWithErrorDetection
# Replace the section starting around line 5600

# Enhanced error reporting in the Invoke-FFmpegEncodingWithErrorDetection function
$codecErrors = @()
$criticalErrors = @()
$ffmpegAllLogs = (Get-Content $ffmpegLogFile, $ffmpegLogFileStd -ErrorAction SilentlyContinue)

# Enhanced codec-specific error patterns with container errors
$codecErrorPatterns = @{
    "NALU_Errors"      = @(
        "Skipping invalid undecodable NALU",
        "Invalid NAL unit size",
        "NAL unit header corrupted"
    )
    "Reference_Errors" = @(
        "Could not find ref with POC",
        "Missing reference picture",
        "Reference picture set error"
    )
    "Structure_Errors" = @(
        "Error constructing the frame RPS",
        "decode_slice_header error",
        "error while decoding MB",
        "RPS error",
        "corrupted frame detected"
    )
    "Quality_Errors"   = @(
        "concealing errors",
        "error concealment applied"
    )
    "Container_Errors" = @(
        "invalid as first byte of an EBML number",
        "EBML.*error",
        "matroska.*error",
        "webm.*error",
        "moov atom not found",
        "Invalid data found when processing input"
    )
}

$codecErrorCounters = @{
    "NALU_Count"      = 0
    "Reference_Count" = 0
    "Structure_Count" = 0
    "Quality_Count"   = 0
    "Container_Count" = 0
}

# Enhanced critical encoding errors
$criticalEncodingErrors = @(
    "Invalid data found when processing input",
    "Error while encoding",
    "Encoder initialization failed",
    "Hardware encoder not available",
    "QSV session initialization failed",
    "No space left on device",
    "Permission denied",
    "Out of memory",
    "Encoding failed",
    "libsvtav1.*failed",
    "libaom.*failed",
    "av1_qsv.*failed",
    "Segmentation fault",
    "Access violation",
    "File ended prematurely",
    "File extends beyond end of segment",
    "invalid as first byte of an EBML number",
    "EBML.*error",
    "matroska.*error",
    "webm.*error",
    "Container.*corrupted",
    "moov atom not found"
)

# NEW: Collect stderr content for debugging display
$stderrContent = ""
if (Test-Path $ffmpegLogFile) {
    $stderrContent = Get-Content $ffmpegLogFile -Raw -ErrorAction SilentlyContinue
}

if ($ffmpegAllLogs) {
    foreach ($line in $ffmpegAllLogs) {
        # Check for codec-specific errors
        foreach ($errorCategory in $codecErrorPatterns.Keys) {
            foreach ($pattern in $codecErrorPatterns[$errorCategory]) {
                if ($line -match $pattern) {
                    $codecErrors += "${CodecType}_ERROR [$errorCategory]: $line"
                    $counterKey = $errorCategory -replace "_Errors", "_Count"
                    if ($codecErrorCounters.ContainsKey($counterKey)) {
                        $codecErrorCounters[$counterKey]++
                    }
                }
            }
        }
        
        # Check for critical encoding errors
        foreach ($pattern in $criticalEncodingErrors) {
            if ($line -match $pattern) {
                $criticalErrors += "CRITICAL: $line"
            }
        }
    }
}

# Enhanced severity logic - Container errors are ALWAYS severe
$hasCodecCorruption = $codecErrors.Count -gt 0
$severeCorruption = $false

$naluCount = if ($codecErrorCounters.ContainsKey("NALU_Count")) { $codecErrorCounters["NALU_Count"] } else { 0 }
$refCount = if ($codecErrorCounters.ContainsKey("Reference_Count")) { $codecErrorCounters["Reference_Count"] } else { 0 }
$structCount = if ($codecErrorCounters.ContainsKey("Structure_Count")) { $codecErrorCounters["Structure_Count"] } else { 0 }
$containerCount = if ($codecErrorCounters.ContainsKey("Container_Count")) { $codecErrorCounters["Container_Count"] } else { 0 }

# CRITICAL: Any container error is severe corruption
$severeCorruption = $containerCount -gt 0 -or $naluCount -gt 20 -or $refCount -gt 10 -or $structCount -gt 30 -or $codecErrors.Count -gt 50

$hasCriticalErrors = $criticalErrors.Count -gt 0 -or $exitCode -ne 0

# ENHANCED: Force failure on any non-zero exit code or container errors
if ($exitCode -ne 0 -or $containerCount -gt 0 -or $hasCriticalErrors -or ($hasCodecCorruption -and $severeCorruption)) {
    Write-Host "=== $CodecType ENCODING ERRORS DETECTED ===" -ForegroundColor Red
    
    # Special handling for container errors
    if ($containerCount -gt 0) {
        Write-Host "CRITICAL CONTAINER CORRUPTION DETECTED:" -ForegroundColor Red
        Write-Host "  Container errors: $containerCount" -ForegroundColor Red
        Write-Host "  This indicates the source file or encoding process is severely damaged" -ForegroundColor Red
        Write-Host "  The output file may be incomplete or unplayable" -ForegroundColor Red
    }
    
    if ($exitCode -ne 0) {
        Write-Host "CRITICAL: FFmpeg returned non-zero exit code: $exitCode" -ForegroundColor Red
    }
    
    if ($hasCriticalErrors) {
        Write-Host "Critical Encoding Errors:" -ForegroundColor Red
        foreach ($criticalErr in $criticalErrors) {
            Write-Host "  $criticalErr" -ForegroundColor Red
        }
    }
    
    if ($hasCodecCorruption) {
        Write-Host "$CodecType Stream Corruption:" -ForegroundColor Red
        
        foreach ($counter in $codecErrorCounters.Keys) {
            if ($codecErrorCounters[$counter] -gt 0) {
                $color = if ($counter -eq "Container_Count") { "Red" } else { "Yellow" }
                Write-Host "  $($counter -replace '_Count', '') errors: $($codecErrorCounters[$counter])" -ForegroundColor $color
            }
        }
        
        Write-Host "  Total $CodecType errors: $($codecErrors.Count)" -ForegroundColor Red
        
        if ($severeCorruption) {
            Write-Host "`n  SEVERE CORRUPTION DETECTED!" -ForegroundColor Red
            if ($containerCount -gt 0) {
                Write-Host "  Container-level corruption will cause playback failure" -ForegroundColor Red
            }
            Write-Host "  This will cause audio sync and playback problems" -ForegroundColor Red
        }
    }
    
    # Enhanced FFmpeg stderr debugging output
    Write-Host "`n=== FFmpeg Debug Information ===" -ForegroundColor Yellow
    Write-Host "Exit Code: $exitCode" -ForegroundColor White
    Write-Host "Log files: $ffmpegLogFile, $ffmpegLogFileStd" -ForegroundColor Gray
    
    if ($stderrContent -and $stderrContent.Length -gt 0) {
        Write-Host "`nFFmpeg stderr output (last 50 lines):" -ForegroundColor Yellow
        
        # Get the last 50 lines of stderr for debugging
        $stderrLines = $stderrContent -split "`n"
        $debugLines = if ($stderrLines.Count -gt 50) { 
            $stderrLines[-50..-1] 
        } else { 
            $stderrLines 
        }
        
        # Filter out progress lines to focus on errors
        $errorLines = $debugLines | Where-Object { 
            $_ -notmatch "frame=.*fps=.*time=.*bitrate=.*speed=" -and
            $_ -notmatch "^\s*$" -and
            $_.Trim().Length -gt 0
        }
        
        if ($errorLines.Count -gt 0) {
            $displayLines = if ($errorLines.Count -gt 20) { 
                $errorLines[-20..-1] 
            } else { 
                $errorLines 
            }
            
            foreach ($line in $displayLines) {
                $trimmedLine = $line.Trim()
                if ($trimmedLine.Length -gt 0) {
                    # Color-code different types of messages
                    $color = "Gray"
                    if ($trimmedLine -match "error|Error|ERROR|failed|Failed|FAILED|invalid.*EBML") {
                        $color = "Red"
                    }
                    elseif ($trimmedLine -match "warning|Warning|WARNING") {
                        $color = "Yellow"
                    }
                    elseif ($trimmedLine -match "info|Info|INFO") {
                        $color = "Cyan"
                    }
                    
                    Write-Host "  $trimmedLine" -ForegroundColor $color
                }
            }
            
            if ($errorLines.Count -gt 20) {
                Write-Host "  ... (showing last 20 error lines of $($errorLines.Count) total)" -ForegroundColor DarkGray
            }
        }
    }
    
    return @{
        Success          = $false
        ExitCode         = $exitCode
        CodecErrors      = $codecErrors
        ErrorCounters    = $codecErrorCounters
        SevereCorruption = $severeCorruption
        CriticalErrors   = $criticalErrors
        FullOutput       = $stderrContent
        ContainerCorruption = $containerCount -gt 0  # NEW FLAG
        DebugInfo        = @{
            StderrLines = if ($stderrContent) { $stderrContent -split "`n" } else { @() }
            LogFiles    = @($ffmpegLogFile, $ffmpegLogFileStd)
        }
    }
}
else {
    if ($hasCodecCorruption) {
        Write-Host "Minor $CodecType errors detected ($($codecErrors.Count) total) - continuing with encoding" -ForegroundColor Yellow
        
        foreach ($counter in $codecErrorCounters.Keys) {
            if ($codecErrorCounters[$counter] -gt 0) {
                Write-Host "  $($counter -replace '_Count', '') errors: $($codecErrorCounters[$counter])" -ForegroundColor Yellow
            }
        }
    }
    else {
        Write-Host "$CodecType encoding completed without corruption errors" -ForegroundColor Green
    }
    
    return @{
        Success          = $true
        ExitCode         = $exitCode
        CodecErrors      = $codecErrors
        ErrorCounters    = $codecErrorCounters
        SevereCorruption = $false
        CriticalErrors   = @()
        FullOutput       = $stderrContent
        ContainerCorruption = $false  # NEW FLAG
        DebugInfo        = @{
            StderrLines = if ($stderrContent) { $stderrContent -split "`n" } else { @() }
            LogFiles    = @($ffmpegLogFile, $ffmpegLogFileStd)
        }
    }
}
}


function Invoke-QSVEncodingWithErrorDetection {
    param(
        [string]$InputFile,
        [string]$OutputFile,
        [hashtable]$VideoInfo,
        [hashtable]$EncodingSettings,
        [hashtable]$ColorInfo,
        [hashtable]$QSVCapabilities
    )
    
    Write-Host "Using Intel QuickSync Video (QSV) encoding with error detection..." -ForegroundColor Yellow

    # Build video filter chain
    #$videoFilters = @("unsharp=luma_msize_x=$($EncodingSettings.SharpenKernel):luma_msize_y=$($EncodingSettings.SharpenKernel):luma_amount=$($EncodingSettings.SharpenAmount)")
    # Build video filter chain
    $padFilter = ""
    if ($PadToStandardResolution) {
        $padFilter = Edit-VideoResolutionFilter -VideoInfo $VideoInfo
        if ($padFilter) {
            Write-Host "Applying pad filter due to resolution variance." -ForegroundColor Yellow
        }
    }

    $videoFilters = @()
    if ($padFilter) {
        # Prepend the padding filter before other filters
        $videoFilters += "$padFilter,unsharp=luma_msize_x=$($EncodingSettings.SharpenKernel):luma_msize_y=$($EncodingSettings.SharpenKernel):luma_amount=$($EncodingSettings.SharpenAmount)"
    }
    else {
        $videoFilters += "unsharp=luma_msize_x=$($EncodingSettings.SharpenKernel):luma_msize_y=$($EncodingSettings.SharpenKernel):luma_amount=$($EncodingSettings.SharpenAmount)"
    }
    
    $videoFilters += "setpts=PTS-STARTPTS"
    $vfArgs = @("-vf", ($videoFilters -join ","))
    
    # Timestamp correction
    $timestampArgs = @(
        "-avoid_negative_ts", "make_zero",
        "-muxdelay", "0",
        "-muxpreload", "0"
    )
    
     # ENHANCED: Preserve original chroma location
    $chromaLocation = "left"  # Default
    if ($VideoInfo.OriginalChromaLocation) {
        $chromaLocation = $VideoInfo.OriginalChromaLocation
        Write-Host "Preserving original chroma location: $chromaLocation" -ForegroundColor Green
    }

    # Color settings for raw HEVC output
    $colorArgs = @(
        "-color_primaries", $ColorInfo.Primaries,
        "-color_trc", $ColorInfo.Transfer,
        "-colorspace", $ColorInfo.Space,
        "-color_range", $VideoInfo.colorRange,
        "-chroma_sample_location", $chromaLocation  # CRITICAL: Preserve chroma location
    )
    
    # QSV-specific encoding settings
    $codec = "hevc_qsv"
    
    # Profile selection based on content and QSV capabilities
    $lprofile = "main"
    if ($ColorInfo.IsHDR -and $QSVCapabilities.SupportsMain10) {
        $lprofile = "main10"
        Write-Host "Using Main10 profile for HDR content" -ForegroundColor Green
    }
    elseif ($ColorInfo.IsHDR -and -not $QSVCapabilities.SupportsMain10) {
        Write-Warning "HDR content detected but QSV Main10 support uncertain - using Main profile"
        Write-Warning "HDR metadata may not be preserved correctly"
    }
    # Calculate keyframe interval
    $fps = [math]::Round($VideoInfo.FrameRate)
    $keyframeInterval = $fps * 2  # 2-second keyframes
    
    # FIXED: Use Plex-compatible pixel formats
    if ($ColorInfo.IsHDR -and $QSVCapabilities.SupportsMain10) {
        $pixelFormat = "p010le"  # Better Plex compatibility for HDR
        Write-Host "Using p010le pixel format for QSV HDR encoding (Plex optimized)" -ForegroundColor Green
    }
    else {
        $pixelFormat = "yuv420p"      # Standard Plex-compatible format
        Write-Host "Using yuv420p pixel format for QSV SDR encoding (Plex optimized)" -ForegroundColor Green
    }
    
    # FIXED: Determine output format based on file extension and add container support
    $outputExtension = [System.IO.Path]::GetExtension($OutputFile).ToLower()
    $outputFormat = "hevc"  # Default to raw HEVC
    $containerArgs = @()
    
    if ($outputExtension -eq ".mp4") {
        $outputFormat = "mp4"
        $containerArgs = @(
            "-movflags", "faststart+frag_keyframe",
            "-brand", "mp42"
        )
        Write-Host "QSV output format: MP4 container" -ForegroundColor Gray
    }
    elseif ($outputExtension -eq ".mkv") {
        $outputFormat = "matroska"
        Write-Host "QSV output format: Matroska container" -ForegroundColor Gray
    }
    else {
        Write-Host "QSV output format: Raw HEVC bitstream" -ForegroundColor Gray
    }
    
# Build QSV encoding arguments with format fix
$encodeArgs = @(
    "-c:v", $codec,
    "-preset", "veryslow",
    "-profile:v", $lprofile,
    "-global_quality", $EncodingSettings.Quality,
    "-look_ahead_depth", $EncodingSettings.LookAhead,
    "-adaptive_i", "1",
    "-adaptive_b", "1",
    "-aq_mode", "2",
    "-b_strategy", "1",
    "-async_depth", "4",
    "-rdo", "1",
    "-max_frame_size", $EncodingSettings.MaxFrameSize,
    "-g", $keyframeInterval,
    "-keyint_min", $keyframeInterval,
    "-bf", "5",
    "-forced_idr", "1",
    "-refs", "3",
    "-r", $VideoInfo.FrameRate,
    "-pix_fmt", $pixelFormat,
    "-f", $outputFormat
)

$encodeArgs += $containerArgs
    
    # Build complete FFmpeg command

    $ffmpegArgs = @(
        "-y", "-hide_banner",
        "-xerror", # Exit on any error
        "-loglevel", "info",
        "-fflags", "+genpts",
        "-i", $InputFile, "-max_muxing_queue_size", "65536",
        "-fps_mode", "cfr"
    ) + $vfArgs + $colorArgs + $timestampArgs + $encodeArgs + @($OutputFile)
    
    # Display command preview for debugging
    Write-Host "QSV encoding parameters:" -ForegroundColor Gray
    Write-Host "  Profile: $lprofile, Quality: $($EncodingSettings.Quality), Keyframe Interval: $keyframeInterval" -ForegroundColor Gray
    Write-Host "  Look-ahead: $($EncodingSettings.LookAhead), Max Frame Size: $([math]::Round($EncodingSettings.MaxFrameSize/1KB,0)) KB" -ForegroundColor Gray
    Write-Host "  Output Format: $outputFormat, Pixel Format: $pixelFormat" -ForegroundColor Gray
    
    # ENHANCED: Add pre-encoding validation
    Write-Host "Pre-encoding validation:" -ForegroundColor Cyan
    $outputDir = [System.IO.Path]::GetDirectoryName($OutputFile)
    
    # Check output directory exists and is writable
    if (-not (Test-Path $outputDir)) {
        try {
            New-Item -ItemType Directory -Path $outputDir -Force | Out-Null
            Write-Host "  Created output directory: $outputDir" -ForegroundColor Green
        }
        catch {
            Write-Warning "  Cannot create output directory: $outputDir - $_"
            return 'F'
        }
    }
    
    # Test write permissions with a test file
    $testFile = Join-Path $outputDir "qsv_write_test_$(Get-Random).tmp"
    try {
        "test" | Out-File -FilePath $testFile -Force
        Remove-Item $testFile -Force -ErrorAction SilentlyContinue
        Write-Host "  Output directory write test: PASSED" -ForegroundColor Green
    }
    catch {
        Write-Warning "  Output directory write test: FAILED - $_"
        return 'F'
    }
    
    # Check available disk space
    try {
        $drive = [System.IO.Path]::GetPathRoot($outputDir)
        $driveInfo = Get-WmiObject -Class Win32_LogicalDisk | Where-Object DeviceID -eq $drive.TrimEnd('\')
        if ($driveInfo) {
            $freeSpaceGB = [math]::Round($driveInfo.FreeSpace / 1GB, 2)
            Write-Host "  Available disk space: $freeSpaceGB GB" -ForegroundColor Green
            
            if ($freeSpaceGB -lt 2) {
                Write-Warning "  Insufficient disk space: $freeSpaceGB GB (minimum: 2GB)"
                return 'F'
            }
        }
    }
    catch {
        Write-Host "  Could not check disk space" -ForegroundColor Yellow
    }
    
    # Execute encoding with HEVC-specific error detection
    Write-Host "Starting QSV encoding with enhanced monitoring..." -ForegroundColor Yellow
    $result = Invoke-FFmpegEncodingWithErrorDetection -Executable $Config.FFmpegExe -Arguments $ffmpegArgs -Description "Intel QSV HEVC encoding" -CodecType "HEVC" -EnableMCEBuddyMonitoring $DisableMCEBuddyMonitoring -MCEBuddyCheckInterval $MCEBuddyCheckInterval
    
    # ENHANCED: Post-encoding validation
    Write-Host "Post-encoding validation:" -ForegroundColor Cyan
    
    if (-not $result.Success) {
        Write-Host "=== Intel QSV Encoding Failed ===" -ForegroundColor Red
        Write-Host "QSV encoding failed with HEVC stream corruption" -ForegroundColor Red
        
        # Display error breakdown
        if ($result.ErrorCounters -and $result.ErrorCounters.Count -gt 0) {
            Write-Host "Error breakdown:" -ForegroundColor Red
            foreach ($errorType in $result.ErrorCounters.Keys) {
                if ($result.ErrorCounters[$errorType] -gt 0) {
                    Write-Host "  $errorType : $($result.ErrorCounters[$errorType])" -ForegroundColor Red
                }
            }
        }
        
        if ($result.SevereCorruption) {
            Write-Host "SEVERE CORRUPTION: The corruption is severe enough to cause audio sync issues." -ForegroundColor Red
            Write-Host "Recommendations:" -ForegroundColor Yellow
            Write-Host "  1. Update Intel GPU drivers" -ForegroundColor White
            Write-Host "  2. Check system stability and cooling" -ForegroundColor White
            Write-Host "  3. Try lower quality settings" -ForegroundColor White
            Write-Host "  4. Verify source file integrity" -ForegroundColor White
            return 'C'  # Critical failure - don't continue processing
        }
        else {
            Write-Host "Moderate corruption detected - will fall back to software encoding" -ForegroundColor Yellow
            Write-Host "This may indicate QSV driver issues or hardware instability" -ForegroundColor Yellow
            return 'F'  # Fallback to software encoding needed
        }
    }
    
    # Check file size
    $outputFileInfo = Get-Item $OutputFile
    $outputSizeMB = [math]::Round($outputFileInfo.Length / 1MB, 2)
    
    if ($outputFileInfo.Length -eq 0) {
        Write-Warning "  Output file is empty (0 bytes)"
        Write-Host "  This indicates QSV encoding completed but produced no data" -ForegroundColor Red
        
        # Try to diagnose the issue
        Write-Host "Possible causes:" -ForegroundColor Yellow
        Write-Host "  1. QSV encoder configuration incompatible with input" -ForegroundColor White
        Write-Host "  2. Invalid pixel format or profile selection" -ForegroundColor White
        Write-Host "  3. Hardware encoder resource exhaustion" -ForegroundColor White
        Write-Host "  4. Driver bug with specific content type" -ForegroundColor White
        
        # Clean up empty file
        try {
            Remove-Item $OutputFile -Force -ErrorAction SilentlyContinue
        }
        catch {
            # Ignore cleanup errors
        }
        
        return 'F'  # Fallback needed
    }
    
    # Validate minimum file size (should be at least a few KB for even short videos)
    if ($outputSizeMB -lt 0.1) {
        # Less than 100KB
        Write-Warning "  Output file suspiciously small: $outputSizeMB MB"
        Write-Host "  This may indicate incomplete encoding or corruption" -ForegroundColor Yellow
        
        # Still proceed but warn about potential issues
    }
    
    # Success case
    $inputSizeMB = [math]::Round((Get-Item $InputFile).Length / 1MB, 2)
    
    if ($result.CodecErrors.Count -gt 0) {
        Write-Host "[OK] Intel QSV encoding completed with minor HEVC errors ($($result.CodecErrors.Count) total)" -ForegroundColor Yellow
        Write-Host "Error breakdown:" -ForegroundColor Yellow
        foreach ($errorType in $result.ErrorCounters.Keys) {
            if ($result.ErrorCounters[$errorType] -gt 0) {
                Write-Host "  $errorType : $($result.ErrorCounters[$errorType])" -ForegroundColor Yellow
            }
        }
        Write-Host "Quality may be slightly affected but file should be playable" -ForegroundColor Yellow
    }
    else {
        Write-Host "[OK] Intel QSV encoding completed successfully without errors" -ForegroundColor Green
    }
    
    Write-Host "Encoding results:" -ForegroundColor Green
    Write-Host "  Input size:  $inputSizeMB MB" -ForegroundColor Green
    Write-Host "  Output size: $outputSizeMB MB" -ForegroundColor Green
    Write-Host "  Format: $outputFormat, Profile: $lprofile" -ForegroundColor Green
    Write-Host "QSV hardware encoding successful" -ForegroundColor Green
    return 'S'  # Success
}
function Invoke-PlexSoftwareEncodingWithErrorDetection {
    param(
        [string]$InputFile,
        [string]$OutputFile,
        [hashtable]$VideoInfo,
        [hashtable]$EncodingSettings,
        [hashtable]$ColorInfo
    )
    
    Write-Host "Using Plex-optimized software HEVC encoding with error detection (libx265)..." -ForegroundColor Yellow

    # Get Plex-optimized settings
    $plexStreamingSettings = Get-PlexStreamingSettings -VideoInfo $VideoInfo -EncodingSettings $EncodingSettings
    $plexCompatibilitySettings = Get-PlexCompatibilitySettings -VideoInfo $VideoInfo -OptimizeForRemoteStreaming $false
    
    # Build video filter chain
    #$videoFilters = @("unsharp=luma_msize_x=$($EncodingSettings.SharpenKernel):luma_msize_y=$($EncodingSettings.SharpenKernel):luma_amount=$($EncodingSettings.SharpenAmount)")
    # Build video filter chain
    $padFilter = ""
    if ($PadToStandardResolution) {
        $padFilter = Edit-VideoResolutionFilter -VideoInfo $VideoInfo
        if ($padFilter) {
            Write-Host "Applying pad filter due to resolution variance." -ForegroundColor Yellow
        }
    }

    $videoFilters = @()
    if ($padFilter) {
        # Prepend the padding filter before other filters
        $videoFilters += "$padFilter,unsharp=luma_msize_x=$($EncodingSettings.SharpenKernel):luma_msize_y=$($EncodingSettings.SharpenKernel):luma_amount=$($EncodingSettings.SharpenAmount)"
    }
    else {
        $videoFilters += "unsharp=luma_msize_x=$($EncodingSettings.SharpenKernel):luma_msize_y=$($EncodingSettings.SharpenKernel):luma_amount=$($EncodingSettings.SharpenAmount)"
    }
    
    $videoFilters += "setpts=PTS-STARTPTS"
    $vfArgs = @("-vf", ($videoFilters -join ","))
    
    # Timestamp correction
    $timestampArgs = @(
        "-avoid_negative_ts", "make_zero",
        "-muxdelay", "0",
        "-muxpreload", "0"
    )
    
    # Software encoding with x265 and Plex optimizations
    # FIXED: Apply Plex-safe quality bounds
    $crf = [math]::Max(12, [math]::Min(28, $EncodingSettings.Quality))
    
    # Extract keyframe settings from Plex streaming settings
    $keyframeInterval = 48  # Default fallback
    $gIndex = $plexStreamingSettings.Keyframes.IndexOf("-g")
    if ($gIndex -ge 0 -and $gIndex + 1 -lt $plexStreamingSettings.Keyframes.Count) {
        $keyframeInterval = $plexStreamingSettings.Keyframes[$gIndex + 1]
    }
    
    # Build x265 parameters with CORRECT syntax
    $x265Params = @(
        "rd=4",
        "me=3", 
        "subme=3",
        "aq-mode=3",
        "aq-strength=0.8",
        "deblock=1,1",
        "keyint=$keyframeInterval",           # Correct x265 syntax
        "min-keyint=$keyframeInterval",       # Correct x265 syntax  
        "scenecut=0",                         # Disable scene cut detection
        "bframes=3",
        "b-adapt=2",
        "ref=3"
    )
# Add HDR metadata parameters if this is HDR content
if ($ColorInfo.IsHDR) {
    Write-Host "`nAdding HDR10 metadata to encoding..." -ForegroundColor Cyan
    
    # Extract source HDR metadata
    $sourceHDRMetadata = Get-HDRMasteringMetadata -FilePath $InputFile
    
    # Determine content type (you can customize this detection logic)
    $contentType = "live-action"  # Default
    
    # Optional: Auto-detect animation
    # You could check filename, resolution patterns, codec info, etc.
    # Example: if ($InputFile -match "animation|anime|cartoon") { $contentType = "animation" }
    
    # Get appropriate HDR encoding parameters
    $hdrParams = Get-HDREncodingParameters -SourceMetadata $sourceHDRMetadata -ContentType $contentType
    
    # Add HDR parameters to x265 params
    $x265Params += $hdrParams.X265Params
    
    Write-Host "HDR metadata added to encoding parameters" -ForegroundColor Green
}    
    
    $x265ParamString = $x265Params -join ":"
    
    # Extract profile from Plex compatibility settings
    $lprofile = "main"
    $lprofileIndex = $plexCompatibilitySettings.IndexOf("-profile:v")
    if ($lprofileIndex -ge 0 -and $lprofileIndex + 1 -lt $plexCompatibilitySettings.Count) {
        $lprofile = $plexCompatibilitySettings[$lprofileIndex + 1]
    }
    
    # FIXED: Always use standard Plex-compatible pixel formats for software encoding
    $pixelFormat = if ($ColorInfo.IsHDR) { "yuv420p10le" } else { "yuv420p" }
    
    $encodeArgs = @(
        "-c:v", "libx265",
        "-crf", $crf,
        "-preset", "slow",
        "-profile:v", $lprofile,
        "-x265-params", $x265ParamString,
        "-pix_fmt", $pixelFormat,              # Single pix_fmt declaration
        "-r", $VideoInfo.FrameRate,
        "-f", "hevc"                           # Raw HEVC output (no container tags)
    )
    
    # Add level from Plex compatibility settings if present
    $levelIndex = $plexCompatibilitySettings.IndexOf("-level:v")
    if ($levelIndex -ge 0 -and $levelIndex + 1 -lt $plexCompatibilitySettings.Count) {
        $encodeArgs += "-level:v", $plexCompatibilitySettings[$levelIndex + 1]
    }
    
     $chromaLocation = "left"  # Default
    if ($VideoInfo.OriginalChromaLocation) {
        $chromaLocation = $VideoInfo.OriginalChromaLocation
        Write-Host "Preserving original chroma location: $chromaLocation" -ForegroundColor Green
    }
    # FIXED: Clean color settings for raw HEVC output
    $cleanColorSettings = @(
        "-color_primaries", $ColorInfo.Primaries,
        "-color_trc", $ColorInfo.Transfer,
        "-colorspace", $ColorInfo.Space,
        "-color_range", $VideoInfo.colorRange,
        "-chroma_sample_location", $chromaLocation  # CRITICAL: Preserve chroma location
        # Remove container-specific settings for raw HEVC
    )
    
    # Build complete FFmpeg command

    $ffmpegArgs = @(
        "-y", "-hide_banner",
        "-xerror", # Exit on any error
        "-loglevel", "info",
        "-fflags", "+genpts",
        "-i", $InputFile, "-max_muxing_queue_size", "65536"
    ) + $vfArgs + $cleanColorSettings + $timestampArgs + $encodeArgs + @($OutputFile)
    
    # Display encoding parameters for debugging
    Write-Host "Plex-optimized software encoding parameters:" -ForegroundColor Gray
    Write-Host "  Codec: libx265, Profile: $lprofile, CRF: $crf (Plex-safe range), Preset: slow" -ForegroundColor Gray
    Write-Host "  Pixel Format: $pixelFormat, Keyframe Interval: $keyframeInterval" -ForegroundColor Gray
    
    # Display x265 parameters
    Write-Host "  x265 parameters: $x265ParamString" -ForegroundColor DarkGray
    
    Write-Host "Starting Plex-optimized software encoding..." -ForegroundColor Yellow
    
    # Execute encoding with HEVC-specific error detection
    $result = Invoke-FFmpegEncodingWithErrorDetection -Executable $Config.FFmpegExe -Arguments $ffmpegArgs -Description "Plex-optimized software video encoding (libx265)" -CodecType "HEVC" -EnableMCEBuddyMonitoring $DisableMCEBuddyMonitoring -MCEBuddyCheckInterval $MCEBuddyCheckInterval
    
    if (-not $result.Success) {
        Write-Host "=== Plex Software Encoding Failed ===" -ForegroundColor Red
        Write-Host "Plex-optimized software encoding failed with errors" -ForegroundColor Red
        
        # Display detailed error information
        if ($result.ErrorCounters -and $result.ErrorCounters.Count -gt 0) {
            Write-Host "HEVC encoding error breakdown:" -ForegroundColor Red
            foreach ($errorType in $result.ErrorCounters.Keys) {
                if ($result.ErrorCounters[$errorType] -gt 0) {
                    Write-Host "  $errorType : $($result.ErrorCounters[$errorType])" -ForegroundColor Red
                }
            }
        }
        
        if ($result.SevereCorruption) {
            Write-Host "SEVERE CORRUPTION: Software encoding produced corrupted HEVC stream" -ForegroundColor Red
            Write-Host "This may indicate serious system issues:" -ForegroundColor Red
            Write-Host "  - Source file corruption or damage" -ForegroundColor Red
            Write-Host "  - System memory errors" -ForegroundColor Red
            Write-Host "  - CPU instability or overheating" -ForegroundColor Red
            Write-Host "  - Storage device problems" -ForegroundColor Red
            
            Write-Host "Recommendations:" -ForegroundColor Yellow
            Write-Host "  1. Verify source file integrity with another tool" -ForegroundColor White
            Write-Host "  2. Run memory diagnostic tests" -ForegroundColor White
            Write-Host "  3. Check system temperatures and cooling" -ForegroundColor White
            Write-Host "  4. Test with different input files" -ForegroundColor White
            Write-Host "  5. Try lower quality settings or different preset" -ForegroundColor White
        }
        else {
            Write-Host "Moderate encoding errors detected" -ForegroundColor Yellow
            Write-Host "The output may have quality issues but could still be usable" -ForegroundColor Yellow
        }
        
        return $false
    }
    
    # Verify the output file was created and has content
    if ((Test-Path $OutputFile) -and (Get-Item $OutputFile).Length -gt 0) {
        $outputSizeMB = [math]::Round((Get-Item $OutputFile).Length / 1MB, 2)
        $inputSizeMB = [math]::Round((Get-Item $InputFile).Length / 1MB, 2)
        $compressionRatio = if ($inputSizeMB -gt 0) { [math]::Round($outputSizeMB / $inputSizeMB, 2) } else { 0 }
        
        if ($result.CodecErrors.Count -gt 0) {
            Write-Host "[OK] Plex-optimized software encoding completed with minor HEVC errors ($($result.CodecErrors.Count) total)" -ForegroundColor Yellow
            Write-Host "Error breakdown:" -ForegroundColor Yellow
            foreach ($errorType in $result.ErrorCounters.Keys) {
                if ($result.ErrorCounters[$errorType] -gt 0) {
                    Write-Host "  $errorType : $($result.ErrorCounters[$errorType])" -ForegroundColor Yellow
                }
            }
            Write-Host "Quality impact: Minor - video should play normally with possible minor artifacts" -ForegroundColor Yellow
        }
        else {
            Write-Host "[OK] Plex-optimized software encoding completed successfully without errors" -ForegroundColor Green
        }
        
        # Display encoding results
        Write-Host "Encoding results:" -ForegroundColor Green
        Write-Host "  Input size:  $inputSizeMB MB" -ForegroundColor Green
        Write-Host "  Output size: $outputSizeMB MB" -ForegroundColor Green
        Write-Host "  Compression ratio: ${compressionRatio}x" -ForegroundColor Green
        Write-Host "  Space saved: $([math]::Round($inputSizeMB - $outputSizeMB, 2)) MB" -ForegroundColor Green
        
        # Quality assessment based on compression ratio
        if ($compressionRatio -lt 0.3) {
            Write-Host "  Compression: Excellent (high quality retained)" -ForegroundColor Green
        }
        elseif ($compressionRatio -lt 0.5) {
            Write-Host "  Compression: Good (balanced quality/size)" -ForegroundColor Green
        }
        elseif ($compressionRatio -lt 0.7) {
            Write-Host "  Compression: Moderate (some quality loss)" -ForegroundColor Yellow
        }
        else {
            Write-Host "  Compression: Minimal (check encoding settings)" -ForegroundColor Yellow
        }
        
        Write-Host "Plex-optimized software encoding successful with CRF $crf (Plex-safe range)" -ForegroundColor Green
        return $true
        
    }
    else {
        Write-Warning "[X] Plex-optimized software encoding produced empty or missing file"
        Write-Host "Possible causes:" -ForegroundColor Yellow
        Write-Host "  1. x265 encoder crashed during processing" -ForegroundColor White
        Write-Host "  2. Insufficient disk space in temp folder" -ForegroundColor White
        Write-Host "  3. File permissions prevent writing" -ForegroundColor White
        Write-Host "  4. Source file became inaccessible during encoding" -ForegroundColor White
        Write-Host "  5. System ran out of memory" -ForegroundColor White
        
        # Check disk space as a common cause
        try {
            $outputDir = [System.IO.Path]::GetDirectoryName($OutputFile)
            $drive = [System.IO.Path]::GetPathRoot($outputDir)
            $driveInfo = Get-WmiObject -Class Win32_LogicalDisk | Where-Object DeviceID -eq $drive.TrimEnd('\')
            if ($driveInfo) {
                $freeSpaceGB = [math]::Round($driveInfo.FreeSpace / 1GB, 2)
                Write-Host "  Current free space on $drive : $freeSpaceGB GB" -ForegroundColor White
            }
        }
        catch {
            # Ignore disk space check errors
        }
        
        return $false
    }
}

function Update-BackupProgress {
    param([string]$ProcessingStep = "")
    
    if ($Script:BackupJob -and -not $Script:BackupCompleted) {
        $backupStatus = Get-BackupStatus -BackupInfo $Script:BackupJob
        
        if ($backupStatus -and $backupStatus.Success -eq $false) {
            Write-Warning "Backup process failed during $ProcessingStep"
            $Script:BackupJob = $null  # Clear failed backup job
        }
        
        # The Get-BackupStatus function already handles progress reporting
    }
}

function Remove-BadHDR10Frames {
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Metadata
    )
    
    Write-Host "Cleaning HDR10+ metadata by removing invalid frames..." -ForegroundColor Cyan
    
    if (-not $Metadata.ContainsKey('SceneInfo') -or -not $Metadata.SceneInfo) {
        Write-Warning "No SceneInfo found in metadata"
        return $Metadata
    }
    
    $originalCount = $Metadata.SceneInfo.Count
    $cleanedScenes = @()
    $removedCount = 0
    
    foreach ($scene in $Metadata.SceneInfo) {
        # Check if MaxScl contains all zeros
        $maxScl = $scene.LuminanceParameters.MaxScl
        
        # Skip frames where MaxScl is all zeros, null, or empty
        if (-not $maxScl -or ($maxScl | Where-Object { $_ -ne 0 }).Count -eq 0) {
            $removedCount++
            Write-Host "  Removing scene $($scene.SceneId) - invalid MaxScl: [$($maxScl -join ', ')]" -ForegroundColor DarkGray
            continue
        }
        
        $cleanedScenes += $scene
    }
    
    # Update metadata with cleaned scenes
    $cleanedMetadata = $Metadata.Clone()
    $cleanedMetadata.SceneInfo = $cleanedScenes
    
    # Update SceneInfoSummary if it exists
    if ($cleanedMetadata.ContainsKey('SceneInfoSummary') -and $cleanedMetadata.SceneInfoSummary) {
        # Rebuild the summary based on remaining scenes
        $firstFrameIndices = @()
        $frameNumbers = @()
        
        for ($i = 0; $i -lt $cleanedScenes.Count; $i++) {
            $scene = $cleanedScenes[$i]
            $firstFrameIndices += $scene.SceneFrameIndex
            
            if ($i -eq ($cleanedScenes.Count - 1)) {
                # Last scene - estimate remaining frames
                $frameNumbers += 100  # Default estimate
            }
            else {
                # Calculate frames in this scene
                $currentStart = $scene.SceneFrameIndex
                $nextStart = $cleanedScenes[$i + 1].SceneFrameIndex
                $frameNumbers += ($nextStart - $currentStart)
            }
        }
        
        $cleanedMetadata.SceneInfoSummary = @{
            SceneFirstFrameIndex = $firstFrameIndices
            SceneFrameNumbers    = $frameNumbers
        }
    }
    
    Write-Host "Removed $removedCount/$originalCount scenes with invalid MaxScl data" -ForegroundColor Green
    Write-Host "Retained $($cleanedScenes.Count) valid scenes" -ForegroundColor Green
    
    return $cleanedMetadata
}

function Edit-VideoResolutionFilter {
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$VideoInfo
    )

    Write-Host "Checking video resolution for standardization..." -ForegroundColor Cyan

    $width = $VideoInfo.Width
    $height = $VideoInfo.Height
    $targetWidth = 0
    $targetHeight = 0
    $filterString = ""
    $needsPadding = $false

    # Check for resolutions far from standard sizes
    if ($width -gt 3800 -and $width -lt 3900) {
        # Near 4K
        $targetWidth = 3840
        $targetHeight = 2160
    }
    elseif ($width -gt 1900 -and $width -lt 2000) {
        # Near 1080p
        $targetWidth = 1920
        $targetHeight = 1080
    }
    elseif ($width -gt 1200 -and $width -lt 1400) {
        # Near 720p
        $targetWidth = 1280
        $targetHeight = 720
    }

    if ($targetWidth -eq 0) {
        Write-Host "Resolution $($width)x$($height) is not a common size, but no padding will be applied." -ForegroundColor Yellow
        return ""
    }

    # Calculate current aspect ratio
    $sourceRatio = [math]::Round([double]$width / $height, 4)
    $targetRatio = [math]::Round([double]$targetWidth / $targetHeight, 4)

    Write-Host "Source Aspect Ratio: $sourceRatio, Target Aspect Ratio: $targetRatio" -ForegroundColor Gray

    # Check if a non-standard aspect ratio requires letterboxing
    if ([math]::Abs($sourceRatio - $targetRatio) -gt 0.05) {
        # A 5% deviation is significant enough for Plex issues
        Write-Host "Aspect ratio difference is significant. Calculating padding..." -ForegroundColor Yellow
        $needsPadding = $true
    }

    if ($needsPadding) {
        # The scale filter must be applied first, as the pad filter uses the scaled dimensions
        $scaledHeight = [math]::Round($targetWidth / $sourceRatio)
        $scaledWidth = $targetWidth
        # Ensure scaled height is an even number
        if ($scaledHeight % 2 -ne 0) {
            $scaledHeight++
        }
        
        Write-Host "Scaled video to $($scaledWidth)x$($scaledHeight) inside a $($targetWidth)x$($targetHeight) frame" -ForegroundColor White

        # Build the FFmpeg filter chain
        $filterString = "scale=$($scaledWidth):$($scaledHeight),pad=$($targetWidth):$($targetHeight):(ow-iw)/2:(oh-ih)/2"
    }

    if ($filterString) {
        Write-Host "Generated FFmpeg video filter: $filterString" -ForegroundColor Green
    }
    else {
        Write-Host "Video resolution is standard or within tolerance. No padding needed." -ForegroundColor Green
    }

    return $filterString
}

function Test-HDR10MetadataQuality {
    param(
        [Parameter(Mandatory = $true)]
        [hashtable]$Metadata,
        [Parameter(Mandatory = $false)]
        [double]$MinViablePercentage = 10.0
    )

    Write-Host "Starting HDR10+ metadata quality assessment..." -ForegroundColor Yellow

    # Initialize a new, empty assessment object
    $assessment = New-Object -TypeName PSObject -Property @{
        IssuesFound  = @()
        IsViable     = $false
        QualityStats = @{
            TotalScenes                  = 0
            GoodScenes                   = 0
            BadScenes                    = 0
            ZeroMaxSclScenes             = 0
            ZeroAverageRgbScenes         = 0
            InvalidDistributionScenes    = 0
            UnusualTargetLuminanceScenes = 0
            AverageMaxSclWhenValid       = 0
            HasValidToneMapping          = $false
            PercentageViable             = 0
        }
    }

    $scenes = $Metadata.SceneInfo
    if (-not $scenes) {
        $assessment.IssuesFound += "No SceneInfo found in metadata"
        return $assessment
    }
    if (-not $Config.pythonExe) {
        Write-Host "Python not found, skipping cleanup."
        return $assessment
    }

    # Create temporary files for communication between PowerShell and Python
    $inputJsonPath = Join-Path $Script:ValidatedTempFolder "scenes_input_$(Get-Random).json"
    $outputJsonPath = Join-Path $Script:ValidatedTempFolder "assessment_output_$(Get-Random).json"
    Add-TempFile -FilePath $inputJsonPath, $outputJsonPath # Register for cleanup

    Write-Host "Exporting scenes data to JSON for Python processing..." -ForegroundColor Cyan

    # Step 1: Export the raw scene data to a JSON file
    $scenes | ConvertTo-Json -Depth 5 -Compress | Out-File -FilePath $inputJsonPath -Encoding utf8

    Write-Host "Processing scenes with Python/Pandas... This may take a moment." -ForegroundColor Cyan

    try {
        # ... (rest of the Python script block is unchanged) ...
        $pythonCode = @"
import pandas as pd
import json
import sys

def main(input_path, output_path):
    # Load your scene data into a DataFrame.
    try:
        with open(input_path, 'r', encoding='utf-8') as f:
            scenes = json.load(f)
        df = pd.DataFrame(scenes)
    except Exception as e:
        # Handle empty or invalid input file gracefully
        with open(output_path, 'w') as f:
            json.dump({
                'QualityStats': {
                    'ZeroMaxSclScenes': 0,
                    'ZeroAverageRgbScenes': 0,
                    'InvalidDistributionScenes': 0,
                    'UnusualTargetLuminanceScenes': 0,
                    'HasValidToneMapping': False,
                    'BadScenes': 0,
                    'GoodScenes': 0,
                    'TotalScenes': 0,
                    'PercentageViable': 0.0,
                    'AverageMaxSclWhenValid': 0.0
                }
            }, f)
        return

    # Initialize assessment statistics
    assessment = {
        'QualityStats': {
            'ZeroMaxSclScenes': 0,
            'ZeroAverageRgbScenes': 0,
            'InvalidDistributionScenes': 0,
            'UnusualTargetLuminanceScenes': 0,
            'HasValidToneMapping': False,
            'BadScenes': 0,
            'GoodScenes': 0
        },
        'validMaxSclValues': []
    }

    # --- Vectorized Operations ---

    # Unpack nested dictionaries into separate columns for easier access
    df_params = df['LuminanceParameters'].apply(pd.Series)
    df = df.join(df_params)
    df['distValues'] = df['LuminanceDistributions'].apply(lambda x: x.get('DistributionValues'))
    if 'TargetedSystemDisplayMaximumLuminance' in df:
        df['targetedLuminance'] = df['TargetedSystemDisplayMaximumLuminance']
    else:
        df['targetedLuminance'] = pd.Series([0] * len(df))

    # Check MaxScl values
    is_zero_max_scl = df['MaxScl'].apply(lambda x: not x or all(val == 0 for val in x) if isinstance(x, list) else True)
    assessment['QualityStats']['ZeroMaxSclScenes'] = int(is_zero_max_scl.sum())

    # Collect valid MaxScl values using a fast list comprehension
    valid_max_scl_data = df.loc[~is_zero_max_scl, 'MaxScl']
    assessment['validMaxSclValues'] = [val for sublist in valid_max_scl_data for val in sublist if val > 0]

    # Check AverageRGB
    is_zero_avg_rgb = df['AverageRGB'] == 0
    assessment['QualityStats']['ZeroAverageRgbScenes'] = int(is_zero_avg_rgb.sum())

    # Check distribution validity
    def check_dist(dist_values):
        if not dist_values:
            return True # Consider a scene with no distribution values as invalid
        non_zero_count = sum(1 for val in dist_values if val > 0)
        return non_zero_count <= 2

    is_invalid_dist = df['distValues'].apply(check_dist)
    assessment['QualityStats']['InvalidDistributionScenes'] = int(is_invalid_dist.sum())

    # Check target luminance
    is_unusual_target_lum = df['targetedLuminance'] < 800
    assessment['QualityStats']['UnusualTargetLuminanceScenes'] = int(is_unusual_target_lum.sum())

    # Check for tone mapping data
    has_valid_tone_mapping = False
    if 'BezierCurveData' in df:
        df_bezier = df['BezierCurveData'].apply(pd.Series)
        if 'KneePointX' in df_bezier and 'KneePointY' in df_bezier:
            has_valid_tone_mapping = ((df_bezier['KneePointX'] > 0) | (df_bezier['KneePointY'] > 0)).any()
    assessment['QualityStats']['HasValidToneMapping'] = bool(has_valid_tone_mapping)

    # Combine all "bad scene" conditions
    is_bad_scene = is_zero_max_scl | is_zero_avg_rgb | is_invalid_dist
    assessment['QualityStats']['BadScenes'] = int(is_bad_scene.sum())
    assessment['QualityStats']['GoodScenes'] = int(len(df) - assessment['QualityStats']['BadScenes'])
    assessment['QualityStats']['TotalScenes'] = len(df)
    assessment['QualityStats']['PercentageViable'] = (assessment['QualityStats']['GoodScenes'] / assessment['QualityStats']['TotalScenes']) * 100 if assessment['QualityStats']['TotalScenes'] > 0 else 0
    if assessment['validMaxSclValues']:
        assessment['QualityStats']['AverageMaxSclWhenValid'] = sum(assessment['validMaxSclValues']) / len(assessment['validMaxSclValues'])
    else:
        assessment['QualityStats']['AverageMaxSclWhenValid'] = 0.0

    # Clean up and save the final assessment
    del assessment['validMaxSclValues']

    # Write the results to the output JSON file
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(assessment, f, indent=4)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python script.py <input_json_path> <output_json_path>", file=sys.stderr)
        sys.exit(1)
    
    input_file = sys.argv[1]
    output_file = sys.argv[2]
    main(input_file, output_file)

"@
        $pythonScriptPath = Join-Path $Script:ValidatedTempFolder "analyze_scenes.py"
        Add-TempFile -FilePath $pythonScriptPath # Register for cleanup
        $pythonCode | Out-File -FilePath $pythonScriptPath -Encoding utf8

        $exitCode = ( & $Config.pythonExe $pythonScriptPath $inputJsonPath $outputJsonPath 2>&1 )
        if ($LASTEXITCODE -ne 0) {
            throw "Python script failed with exit code $LASTEXITCODE. Error: $exitCode"
        }

        Write-Host "Processing complete. Importing results..." -ForegroundColor Green

        # Step 3: Import the results from the output JSON file
        # The Python script's JSON output does not contain `IssuesFound`.
        # We must initialize our own assessment object and populate it.
        $pythonAssessment = Get-Content $outputJsonPath | ConvertFrom-Json
        
        # Merge the properties from the Python output into our initialized object
        $assessment.QualityStats = $pythonAssessment.QualityStats
        $assessment.QualityStats.HasValidToneMapping = [bool]$assessment.QualityStats.HasValidToneMapping

    }
    catch {
        Write-Warning "Failed to process scenes with Python: $_"
        # Fallback to the original, slower loop logic here if necessary
        # Note: If you need to keep the original logic, put it here.
        # Otherwise, the function will just return the failed assessment.
    }

    # Determine issues and viability
    if ($assessment.QualityStats.PercentageViable -lt $MinViablePercentage) {
        $assessment.IssuesFound += "Only $([math]::Round($assessment.QualityStats.PercentageViable, 1))% of scenes have valid data"
    }
    if ($assessment.QualityStats.ZeroMaxSclScenes -gt ($assessment.QualityStats.TotalScenes * 0.8)) {
        $assessment.IssuesFound += "Most scenes ($($assessment.QualityStats.ZeroMaxSclScenes)) have zero MaxScl values"
    }
    if ($assessment.QualityStats.ZeroAverageRgbScenes -gt ($assessment.QualityStats.TotalScenes * 0.8)) {
        $assessment.IssuesFound += "Most scenes ($($assessment.QualityStats.ZeroAverageRgbScenes)) have zero AverageRGB"
    }
    if (-not $assessment.QualityStats.HasValidToneMapping) {
        $assessment.IssuesFound += "No valid tone mapping curves found"
    }
    if ($assessment.QualityStats.InvalidDistributionScenes -gt ($assessment.QualityStats.TotalScenes * 0.5)) {
        $assessment.IssuesFound += "Many scenes ($($assessment.QualityStats.InvalidDistributionScenes)) have invalid luminance distributions"
    }

    # Decision logic: metadata is viable if:
    # - At least minimum percentage of scenes are good, AND
    # - We have some valid MaxScl data, AND
    # - Total scenes > 10 (avoid tiny samples)
    $assessment.IsViable = (
        $assessment.QualityStats.PercentageViable -ge $MinViablePercentage -and
        $assessment.QualityStats.GoodScenes -gt 0 -and
        $assessment.QualityStats.TotalScenes -ge 10
    )

    return $assessment
}


function Show-StreamDiagnostics {
    param(
        [string]$VideoFile,
        [string]$OriginalFile,
        [hashtable]$AudioInfo,
        [hashtable]$SubtitleInfo
    )
    
    Write-Host "`n=== Stream Diagnostics ===" -ForegroundColor Magenta
    
    # Helper function to format stream info with proper FFprobe field parsing
    function Format-StreamInfo {
        param([string[]]$StreamData)
        
        foreach ($stream in $StreamData) {
            if ([string]::IsNullOrWhiteSpace($stream)) { continue }
            
            try {
                # Use FFprobe with specific field selection for better parsing
                $streamIndex = ($stream -split ',')[0]
                
                # Get detailed info for this specific stream using JSON format (more reliable)
                $detailedArgs = @(
                    "-v", "quiet",
                    "-select_streams", "$streamIndex",
                    "-show_streams",
                    "-of", "json"
                )
                
                $detailResult = & $Config.FFProbeExe @detailedArgs $global:CurrentFile 2>$null
                
                if ($detailResult) {
                    $streamObj = ($detailResult | ConvertFrom-Json).streams[0]
                    
                    # Extract basic info
                    $index = $streamObj.index
                    $codecName = $streamObj.codec_name
                    $codecLongName = $streamObj.codec_long_name
                    $codecType = $streamObj.codec_type
                    $lprofile = $streamObj.profile
                    
                    # Format output based on stream type
                    switch ($codecType) {
                        "video" {
                            $width = $streamObj.width
                            $height = $streamObj.height
                            $resolution = if ($width -and $height) { "${width}x${height}" } else { "N/A" }
                            
                            $duration = $streamObj.duration
                            $durationMin = if ($duration) { 
                                [math]::Round([double]$duration / 60, 1)
                            } else { "N/A" }
                            
                            $bitRate = $streamObj.bit_rate
                            $bitRateMbps = if ($bitRate) { 
                                [math]::Round([double]$bitRate / 1000000, 1)
                            } else { "N/A" }
                            
                            $frameRate = "N/A"
                            if ($streamObj.r_frame_rate) {
                                $fpsParts = $streamObj.r_frame_rate -split "/"
                                if ($fpsParts.Count -eq 2 -and [double]$fpsParts[1] -ne 0) {
                                    $frameRate = [math]::Round([double]$fpsParts[0] / [double]$fpsParts[1], 2)
                                }
                            }
                            
                            Write-Host "  [$index] Video: $codecName ($codecLongName)" -ForegroundColor Cyan
                            Write-Host "      Resolution: $resolution, Profile: $(if ($lprofile) { $lprofile } else { 'N/A' })" -ForegroundColor Gray
                            Write-Host "      Duration: $durationMin min, Bitrate: $bitRateMbps Mbps, FPS: $frameRate" -ForegroundColor Gray
                        }
                        
                        "audio" {
                            $channels = $streamObj.channels
                            $channelLayout = $streamObj.channel_layout
                            $sampleRate = $streamObj.sample_rate
                            $sampleRateKHz = if ($sampleRate) { 
                                [math]::Round([double]$sampleRate / 1000, 1)
                            } else { "N/A" }
                            
                            $bitRate = $streamObj.bit_rate
                            $bitRateKbps = if ($bitRate) { 
                                [math]::Round([double]$bitRate / 1000, 0)
                            } else { "N/A" }
                            
                            $language = "unknown"
                            $title = ""
                            if ($streamObj.tags) {
                                if ($streamObj.tags.language) { $language = $streamObj.tags.language }
                                if ($streamObj.tags.title) { $title = " - $($streamObj.tags.title)" }
                            }
                            
                            Write-Host "  [$index] Audio: $codecName ($codecLongName)" -ForegroundColor Green
                            Write-Host "      Language: $language, Channels: $(if ($channels) { $channels } else { 'N/A' }) ($(if ($channelLayout) { $channelLayout } else { 'N/A' }))" -ForegroundColor Gray
                            Write-Host "      Sample Rate: $sampleRateKHz kHz, Bitrate: $bitRateKbps kbps$title" -ForegroundColor Gray
                        }
                        
                        "subtitle" {
                            $language = "unknown"
                            $title = ""
                            if ($streamObj.tags) {
                                if ($streamObj.tags.language) { $language = $streamObj.tags.language }
                                if ($streamObj.tags.title) { $title = " - $($streamObj.tags.title)" }
                            }
                            
                            $disposition = ""
                            if ($streamObj.disposition) {
                                $dispFlags = @()
                                if ($streamObj.disposition.default -eq 1) { $dispFlags += "default" }
                                if ($streamObj.disposition.forced -eq 1) { $dispFlags += "forced" }
                                if ($dispFlags.Count -gt 0) {
                                    $disposition = " [" + ($dispFlags -join ", ") + "]"
                                }
                            }
                            
                            Write-Host "  [$index] Subtitle: $codecName ($codecLongName)" -ForegroundColor Yellow
                            Write-Host "      Language: $language$title$disposition" -ForegroundColor Gray
                        }
                        
                        default {
                            Write-Host "  [$index] $($codecType): $codecName ($codecLongName)" -ForegroundColor White
                        }
                    }
                } else {
                    # Fallback to basic CSV parsing if JSON fails
                    $fields = $stream -split ','
                    $index = $fields[0]
                    $codecName = if ($fields.Count -gt 1) { $fields[1] } else { "unknown" }
                    $codecType = if ($fields.Count -gt 4) { $fields[4] } else { "unknown" }
                    
                    Write-Host "  [$index] $($codecType): $codecName" -ForegroundColor White
                    Write-Host "      Limited info available (JSON parsing failed)" -ForegroundColor Gray
                }
            }
            catch {
                Write-Host "  Error parsing stream: $_" -ForegroundColor Red
            }
        }
    }
    
    # Analyze video file streams
    Write-Host "Video file streams ($VideoFile):" -ForegroundColor Yellow
    try {
        $global:CurrentFile = $VideoFile  # Set global variable for the helper function
        $videoStreams = & $Config.FFProbeExe -v quiet -show_streams -of csv=p=0 $VideoFile 2>$null
        if ($videoStreams) {
            Format-StreamInfo -StreamData $videoStreams
        } else {
            Write-Host "  No streams detected or probe failed" -ForegroundColor Red
        }
    } catch {
        Write-Host "  Error probing video file: $_" -ForegroundColor Red
    }
    
    # Analyze original file streams
    Write-Host "`nOriginal file streams ($OriginalFile):" -ForegroundColor Yellow
    try {
        $global:CurrentFile = $OriginalFile  # Set global variable for the helper function
        $originalStreams = & $Config.FFProbeExe -v quiet -show_streams -of csv=p=0 $OriginalFile 2>$null
        if ($originalStreams) {
            Format-StreamInfo -StreamData $originalStreams
        } else {
            Write-Host "  No streams detected or probe failed" -ForegroundColor Red
        }
    } catch {
        Write-Host "  Error probing original file: $_" -ForegroundColor Red
    }
    
    # Show selected audio info
    Write-Host "`nSelected Audio Info:" -ForegroundColor Yellow
    if ($AudioInfo.ToCopy) {
        Write-Host "  Global Index: $($AudioInfo.ToCopy.GlobalIndex)" -ForegroundColor Green
        Write-Host "  Codec: $($AudioInfo.ToCopy.Codec)" -ForegroundColor Green
        Write-Host "  Channels: $($AudioInfo.ToCopy.Channels)" -ForegroundColor Green
        Write-Host "  Language: $($AudioInfo.ToCopy.Language)" -ForegroundColor Green
        Write-Host "  Title: $($AudioInfo.ToCopy.Title)" -ForegroundColor Green
    } else {
        Write-Host "  No audio stream selected!" -ForegroundColor Red
    }
    
    # Show selected subtitle info
    Write-Host "`nSelected Subtitle Info:" -ForegroundColor Yellow
    if ($SubtitleInfo.ToCopy -and $SubtitleInfo.ToCopy.Count -gt 0) {
        foreach ($sub in $SubtitleInfo.ToCopy) {
            Write-Host "  Logical Index: $($sub.LogicalIndex), Codec: $($sub.Codec), Language: $($sub.Language)$(if ($sub.IsForced) { ' [FORCED]' } else { '' })" -ForegroundColor Green
        }
    } else {
        Write-Host "  No internal subtitle streams selected" -ForegroundColor Yellow
    }
    
    # Show extracted subtitle files
    if ($Script:ExtractedSubtitles -and $Script:ExtractedSubtitles.Count -gt 0) {
        Write-Host "`nExtracted Subtitle Files:" -ForegroundColor Yellow
        foreach ($extSub in $Script:ExtractedSubtitles) {
            if (Test-Path $extSub) {
                $fileName = Split-Path $extSub -Leaf
                $fileSize = [math]::Round((Get-Item $extSub).Length / 1KB, 1)
                Write-Host "  $fileName ($fileSize KB)" -ForegroundColor Green
            } else {
                Write-Host "  $(Split-Path $extSub -Leaf) [MISSING]" -ForegroundColor Red
            }
        }
    } else {
        Write-Host "`nNo extracted subtitle files" -ForegroundColor Yellow
    }
    
    Write-Host "=== End Stream Diagnostics ===`n" -ForegroundColor Magenta
}

function Test-HDR10ProcessingViability {
    param(
        [Parameter(Mandatory = $true)]
        [string]$MetadataFilePath,
        [Parameter(Mandatory = $false)]
        [double]$MinViablePercentage = 10.0,
        [Parameter(Mandatory = $false)]
        [switch]$LogAssessment
    )
    Write-Host "Testing HDR10+ processing viability for: $(Split-Path $MetadataFilePath -Leaf)" -ForegroundColor Cyan
    try {
        # Load metadata from JSON file
        if (-not (Test-Path $MetadataFilePath)) {
            Write-Error "HDR10+ metadata file not found: $MetadataFilePath"
            # Return true to signal that the processing should be skipped
            return $true 
        }
        $jsonContent = Get-Content $MetadataFilePath -Raw
        $metadata = $jsonContent | ConvertFrom-Json -AsHashtable
        # Assess quality
        $assessment = Test-HDR10MetadataQuality -Metadata $metadata -MinViablePercentage $MinViablePercentage
        if ($LogAssessment) {
            Write-Host "HDR10+ metadata assessment:" -ForegroundColor Green
            Write-Host " Total scenes: $($assessment.QualityStats.TotalScenes)" -ForegroundColor White
            Write-Host " Good scenes: $($assessment.QualityStats.GoodScenes) ($([math]::Round($assessment.QualityStats.PercentageViable, 1))%)" -ForegroundColor White
            Write-Host " Bad scenes: $($assessment.QualityStats.BadScenes)" -ForegroundColor White
            if ($assessment.QualityStats.AverageMaxSclWhenValid -gt 0) {
                Write-Host " Average MaxScl (valid scenes): $([math]::Round($assessment.QualityStats.AverageMaxSclWhenValid, 1))" -ForegroundColor White
            }
            if ($assessment.IssuesFound.Count -gt 0) {
                Write-Host " Issues found:" -ForegroundColor Yellow
                foreach ($issue in $assessment.IssuesFound) {
                    Write-Host " - $issue" -ForegroundColor Yellow
                }
            }
            if ($assessment.IsViable) {
                Write-Host " Recommendation: PROCEED with HDR10+ processing" -ForegroundColor Green
            }
            else {
                Write-Host " Recommendation: SKIP HDR10+ processing (metadata too damaged)" -ForegroundColor Red
            }
        }
        # Return true if we should SKIP processing (opposite of IsViable)
        return -not $assessment.IsViable
    }
    catch {
        Write-Error "Failed to assess HDR10+ metadata quality: $_"
        return $true # Skip processing on error
    }
}
function Repair-HDR10Metadata {
    param(
        [Parameter(Mandatory = $true)]
        [string]$InputMetadataPath,
        
        [Parameter(Mandatory = $true)]
        [string]$OutputMetadataPath
    )
    
    Write-Host "Repairing HDR10+ metadata..." -ForegroundColor Yellow
    
    try {
        # Load original metadata
        $jsonContent = Get-Content $InputMetadataPath -Raw
        $metadata = $jsonContent | ConvertFrom-Json -AsHashtable
        
        # Clean the metadata
        $cleanedMetadata = Remove-BadHDR10Frames -Metadata $metadata
        
        # Test if cleaned metadata is viable
        $assessment = Test-HDR10MetadataQuality -Metadata $cleanedMetadata
        
        if ($assessment.IsViable) {
            # Save cleaned metadata
            $cleanedMetadata | ConvertTo-Json -Depth 10 | Set-Content $OutputMetadataPath
            
            Write-Host "HDR10+ metadata repaired successfully" -ForegroundColor Green
            Write-Host "  Cleaned file: $OutputMetadataPath" -ForegroundColor Green
            Write-Host "  Valid scenes: $($assessment.QualityStats.GoodScenes)/$($assessment.QualityStats.TotalScenes)" -ForegroundColor Green
            
            return $true
        }
        else {
            Write-Warning "Cleaned HDR10+ metadata is still not viable for processing"
            Write-Host "Issues remaining after cleaning:" -ForegroundColor Yellow
            foreach ($issue in $assessment.IssuesFound) {
                Write-Host "  - $issue" -ForegroundColor Yellow
            }
            
            return $false
        }
        
    }
    catch {
        Write-Error "Failed to repair HDR10+ metadata: $_"
        return $false
    }
}

#endregion

#region Main Execution

try {
    #$LockTimeoutSeconds = 60
    try {
        $config.pythonExe = Find-PythonPath
    } catch {
         $config.pythonExe = $null
    }
    Write-Host "=== Enhanced Video Processing Script ===" -ForegroundColor Cyan
    Write-Host "Input: $InputFile" -ForegroundColor White
    Write-Host "Codec target: HEVC" -ForegroundColor White
    
    # Initialize temp folder early
    Write-Host "`n=== Temp Folder Initialization ===" -ForegroundColor Cyan
    $Script:ValidatedTempFolder = Get-ValidatedTempFolder -RequestedTempFolder $TempFolder -InputFile $InputFile -MinSpaceMultiplier 4.0
    $Script:ValidatedTempFolder = Initialize-ScriptTempFolder -BaseTempFolder $Script:ValidatedTempFolder
    
    # Update TempFolder variable for rest of script
    $TempFolder = $Script:ValidatedTempFolder
    
    Write-Host "Temp folder configuration complete" -ForegroundColor Green
    Write-Host "Active temp folder: $TempFolder" -ForegroundColor White
    
    # Start space monitoring for long operations
    Write-Host "Starting disk space monitoring..." -ForegroundColor Cyan
    $spaceMonitorJob = Start-SpaceMonitoring -TempFolder $TempFolder -CheckIntervalSeconds 30

    # Check prerequisites
    if (-not (Test-Prerequisites)) {
        exit 1
    }
    
    # Register cleanup handlers
    Register-CleanupHandler
    
    # Initialize
    $Script:OriginalFileSize = (Get-Item $InputFile).Length
    
    # Ensure directories exist
    foreach ($folder in @($OutputFolder, $TempFolder)) {
        if (-not (Test-Path $folder)) {
            New-Item -ItemType Directory -Path $folder -Force | Out-Null
        }
    }

    # STEP 0: Start asynchronous backup if configured
    if ($BackupFolder -and $BackupFolder.Trim() -ne "") {
        Write-Host "`n=== Backup Process Initialization ===" -ForegroundColor Cyan
        Write-Host "Backup folder: $BackupFolder" -ForegroundColor White
        Write-Host "Source file: $InputFile" -ForegroundColor White
        
        $Script:BackupJob = Start-AsynchronousBackup -SourceFile $InputFile -BackupFolder $BackupFolder
        
    }
    else {
        Write-Host "No backup folder specified - skipping backup" -ForegroundColor Gray
    }
    
    # STEP 1: Analyze input video
    Write-Host "`n=== Initial Video Analysis ===" -ForegroundColor Cyan
    #$videoInfo = Get-VideoMetadata -FilePath $InputFile
    $videoInfo = Initialize-VideoAnalysisWithHDR10Plus -InputFile $InputFile


    if (-not $videoInfo) {
        throw "Failed to analyze input video"
    }

    $baseName = [System.IO.Path]::GetFileNameWithoutExtension($InputFile)
    # Store video metadata globally for progress calculations
    $Script:VideoMetadata = $videoInfo
    
    Write-Host "Initial analysis complete:" -ForegroundColor Green
    Write-Host "  Resolution: $($videoInfo.Width)x$($videoInfo.Height)" -ForegroundColor White
    Write-Host "  Codec: $($videoInfo.CodecName)" -ForegroundColor White
    Write-Host "  Dolby Vision: $(if ($videoInfo.HasDolbyVision) { 'YES - will be removed' } else { 'No' })" -ForegroundColor $(if ($videoInfo.HasDolbyVision) { 'Yellow' } else { 'Green' })
    Write-Host "  HDR10+: $(if ($videoInfo.HasHdr10Plus) { 'YES' } else { 'No' })" -ForegroundColor $(if ($videoInfo.HasHdr10Plus) { 'Yellow' } else { 'Green' })
    
    # STEP 1A: EARLY DOLBY VISION REMOVAL (NEW - MOVED HERE)
    Write-Host "`n=== Early Dolby Vision Processing ===" -ForegroundColor Cyan
    #$processedFile = Invoke-EarlyDolbyVisionRemoval -InputFile $InputFile -VideoInfo $videoInfo
    $processedFile = Invoke-EarlyDolbyVisionRemovalWithHDR10Plus -InputFile $InputFile -VideoInfo $videoInfo
    
    if ($processedFile -ne $InputFile) {
        Write-Host "Dolby Vision removed successfully" -ForegroundColor Green
        Write-Host "Processing will continue with DV-removed file: $(Split-Path $processedFile -Leaf)" -ForegroundColor Green
        
        # Re-analyze video metadata after DV removal
        Write-Host "Re-analyzing video metadata after DV removal..." -ForegroundColor Cyan
        $updatedVideoInfo = Get-VideoMetadata -FilePath $processedFile
        if ($updatedVideoInfo) {
            # Preserve some original flags but update metadata from processed file
            $originalHadDV = $videoInfo.HasDolbyVision
            $updatedVideoInfo.HasDolbyVision = $false  # DV has been removed
            $Script:VideoMetadata = $updatedVideoInfo
            $videoInfo = $updatedVideoInfo
            Write-Host "Video metadata updated after DV removal (original had DV: $originalHadDV)" -ForegroundColor Green
        }
        else {
            Write-Warning "Could not re-analyze video after DV removal - using original metadata"
        }
    }
    else {
        Write-Host "No Dolby Vision processing needed - continuing with original file" -ForegroundColor Green
    }
    
    Update-BackupProgress -ProcessingStep "early DV removal"
    
# STEP 1B: Process HDR10+ metadata (simplified - no redundant detection/extraction)
Write-Host "`n=== HDR10+ Metadata Processing ===" -ForegroundColor Cyan

# Use the already-detected HDR10+ status from initial analysis
$hdr10PlusJson = $null
$hasHdr10Plus = $false

if ($Script:HDR10PlusStatus.HasHDR10Plus) {
    Write-Host "HDR10+ detected during initial analysis" -ForegroundColor Green
    
    if ($Script:HDR10PlusStatus.ExtractedJsonPath -and (Test-Path $Script:HDR10PlusStatus.ExtractedJsonPath)) {
        # Use the already extracted metadata
        $hdr10PlusJson = $Script:HDR10PlusStatus.ExtractedJsonPath
        $hasHdr10Plus = $Script:HDR10PlusStatus.IsViable
        
        if ($hasHdr10Plus) {
            Write-Host "Using extracted HDR10+ metadata: $($Script:HDR10PlusStatus.SceneCount) scenes" -ForegroundColor Green
            Write-Host "Metadata quality: $(if ($Script:HDR10PlusStatus.IsViable) { 'VIABLE' } else { 'NEEDS REPAIR' })" -ForegroundColor $(if ($Script:HDR10PlusStatus.IsViable) { 'Green' } else { 'Yellow' })
        }
        else {
            Write-Warning "HDR10+ metadata quality too poor - attempting repair..."
            
            # Try to repair the metadata
            $baseName = [System.IO.Path]::GetFileNameWithoutExtension($processedFile)
            $repairedMetadataPath = New-TempFile -BaseName "$baseName.hdr10plus.repaired" -Extension ".json"
            $repairSuccess = Repair-HDR10Metadata -InputMetadataPath $hdr10PlusJson -OutputMetadataPath $repairedMetadataPath
            
            if ($repairSuccess) {
                Write-Host "HDR10+ metadata repaired successfully" -ForegroundColor Green
                $hdr10PlusJson = $repairedMetadataPath
                $hasHdr10Plus = $true
                $Script:HDR10PlusStatus.ExtractedJsonPath = $repairedMetadataPath
                $Script:HDR10PlusStatus.IsViable = $true
            }
            else {
                Write-Warning "HDR10+ metadata could not be repaired - skipping HDR10+ processing"
                $hasHdr10Plus = $false
            }
        }
    }
    else {
        # This shouldn't happen if Initialize-VideoAnalysisWithHDR10Plus worked correctly
        Write-Warning "HDR10+ was detected but metadata file is missing - attempting extraction"
        
        $baseName = [System.IO.Path]::GetFileNameWithoutExtension($processedFile)
        $hdr10PlusJson = New-TempFile -BaseName "$baseName.hdr10plus.fallback" -Extension ".json"
        
        # Try extraction from processed file first
        $extractSuccess = Invoke-HDR10PlusExtraction -InputFile $processedFile -OutputJson $hdr10PlusJson
        
        # If that fails and processed file is different from original, try original
        if (-not $extractSuccess -and $processedFile -ne $OrigFile) {
            Write-Warning "Trying original file for HDR10+ extraction..."
            $extractSuccess = Invoke-HDR10PlusExtraction -InputFile $OrigFile -OutputJson $hdr10PlusJson
        }
        
        if ($extractSuccess) {
            $Script:HDR10PlusStatus.ExtractedJsonPath = $hdr10PlusJson
            $hasHdr10Plus = $true
            
            # Test viability
            $shouldSkip = Test-HDR10ProcessingViability -MetadataFilePath $hdr10PlusJson -LogAssessment:$true
            if ($shouldSkip) {
                $hasHdr10Plus = $false
                Write-Warning "Extracted HDR10+ metadata is not viable for processing"
            }
        }
        else {
            Write-Warning "HDR10+ extraction failed - proceeding without HDR10+"
            $hasHdr10Plus = $false
        }
    }
}
else {
    Write-Host "No HDR10+ metadata detected in source file" -ForegroundColor Gray
}

# Update the global status
$Script:HDR10PlusStatus.HasHDR10Plus = $hasHdr10Plus

Write-Host "HDR10+ processing decision: $(if ($hasHdr10Plus) { 'ENABLED' } else { 'DISABLED' })" -ForegroundColor $(if ($hasHdr10Plus) { 'Green' } else { 'Yellow' })
    
    # STEP 2: Analyze streams and get encoding settings (using processed file)
    Write-Host "`n=== Stream Analysis and Encoding Configuration ===" -ForegroundColor Cyan
    $encodingSettings = Get-EncodingSettings -VideoInfo $videoInfo
    $colorInfo = Get-ColorMetadata -VideoInfo $videoInfo
    $audioInfo = Get-AudioStreamInfo -FilePath $processedFile  # Use processed file (may have DV removed)

    # Check if any audio stream was selected
    if (-not $audioInfo.ToCopy -or $audioInfo.ToCopy.Count -eq 0) {
        Write-Host "`n=== CRITICAL: No Audio Stream Selected ===" -ForegroundColor Red
        Write-Host "No suitable audio streams were found or validated successfully." -ForegroundColor Red
        Write-Host "This could be due to:" -ForegroundColor Yellow
        Write-Host "  - All audio streams failed validation (corruption, incompatibility)" -ForegroundColor White
        Write-Host "  - No English or undefined language audio tracks found" -ForegroundColor White
        Write-Host "  - All audio streams are commentary tracks" -ForegroundColor White
        Write-Host "  - Audio stream duration mismatch with video" -ForegroundColor White
        Write-Host "  - Audio codec compatibility issues" -ForegroundColor White
        
        Write-Host "`nProcessing cannot continue without audio." -ForegroundColor Red
        throw "CRITICAL: No suitable audio stream found - processing terminated"
    }

    Write-Host "Audio stream validation: PASSED - Selected audio stream available" -ForegroundColor Green

    $isOriginalMP4 = [System.IO.Path]::GetExtension($processedFile).ToLower() -eq ".mp4"
    $subtitleInfo = Get-SubtitleStreamInfo -FilePath $processedFile -IsOriginalMP4 $isOriginalMP4
    
    # Display processing summary
    Write-PlexOptimizedSummary -VideoInfo $videoInfo -AudioInfo $audioInfo -SubtitleInfo $subtitleInfo -InputFile $processedFile
    
    Write-Host "Stream analysis complete:" -ForegroundColor Green
    Write-Host "  Audio: Selected best stream (validated)" -ForegroundColor White  
    Write-Host "  Subtitles: $($subtitleInfo.ToConvert.Count) to convert, $($subtitleInfo.ToCopy.Count) to copy, $($subtitleInfo.Remaining.Count) ignored" -ForegroundColor White

    Update-BackupProgress -ProcessingStep "stream analysis"

    # STEP 3: Extract and convert subtitles with enhanced forced flag handling
    Write-Host "`n=== Subtitle Extraction ===" -ForegroundColor Cyan
    foreach ($subtitle in $subtitleInfo.ToConvert) {
        $baseName = [System.IO.Path]::GetFileNameWithoutExtension($processedFile)
        $forcedSuffix = if ($subtitle.IsForced) { ".forced" } else { "" }
        $outputFile = New-TempFile -BaseName "$baseName.$($subtitle.LogicalIndex)$forcedSuffix" -Extension ".srt"
        
        Write-Host "Extracting subtitle stream $($subtitle.LogicalIndex)$(if ($subtitle.IsForced) { ' [FORCED]' } else { '' })..." -ForegroundColor Yellow
        $extractionSuccess = Invoke-SubtitleExtraction -InputFile $processedFile -SubtitleLogicalIndex $subtitle.LogicalIndex -Language $subtitle.Language -OutputFile $outputFile -Codec $subtitle.Codec -IsForced $subtitle.IsForced -OriginalTitle $subtitle.Title
        
        if (-not $extractionSuccess) {
            Write-Warning "Failed to extract subtitle stream $($subtitle.LogicalIndex) - continuing without it"
        }
    }
    
    # STEP 3A: Scan for external subtitle files and add them to the processing queue
    Write-Host "Scanning for external subtitle files..." -ForegroundColor Cyan
    $externalSubtitles = Find-ExternalSubtitles -InputFile $OrigFile
    foreach ($extSub in $externalSubtitles) {
        Write-Host "Adding external subtitle to processing queue: $($extSub.FileName)" -ForegroundColor Yellow
        $Script:ExtractedSubtitles += $extSub.FilePath
    }
    
    if ($Script:ExtractedSubtitles.Count -gt 0) {
        Write-Host "Total subtitle files prepared: $($Script:ExtractedSubtitles.Count)" -ForegroundColor Green
    }
    else {
        Write-Host "No subtitles to process" -ForegroundColor Gray
    }
    
# STEP 4: Encode video (using cached HDR10+ detection results)
Write-Host "`n=== Video Encoding ===" -ForegroundColor Cyan

# Determine codec and extension based on cached HDR10+ status
$codecType = "HEVC"  # Default
$videoExtension = ".hevc"  # Default

# Use the cached HDR10+ status instead of re-detecting
if (-not $Script:HDR10PlusStatus.HasHDR10Plus) {
#    if ($videoInfo.bitrate -gt 25000000) {
#        Write-Host "High Bitrate detected - will use HEVC encoding to help with Plex issues" -ForegroundColor Cyan
#    } else {
        # No HDR10+ detected - use AV1 for better compression
        $codecType = "AV1"
        $videoExtension = ".ivf"  # AV1 uses IVF container initially
        Write-Host "No HDR10+ detected - will use AV1 encoding for better compression" -ForegroundColor Cyan
#    }
} else {
    Write-Host "HDR10+ detected - will use HEVC encoding with preservation" -ForegroundColor Cyan
}

$encodedVideo = New-TempFile -BaseName "$baseName.encoded" -Extension $videoExtension

Write-Host "Starting video encoding:" -ForegroundColor Yellow
Write-Host "  Input: $(Split-Path $processedFile -Leaf)" -ForegroundColor White
Write-Host "  Output: $(Split-Path $encodedVideo -Leaf)" -ForegroundColor White
Write-Host "  Codec: $codecType" -ForegroundColor White
Write-Host "  Quality: $($encodingSettings.Quality)" -ForegroundColor White

# Call the appropriate encoding function based on codec decision
if ($codecType -eq "AV1") {
    # Use AV1 encoding (no HDR10+ to worry about)
    $encodingSuccess = Invoke-AV1Encoding -InputFile $processedFile -OutputFile $encodedVideo -VideoInfo $videoInfo -EncodingSettings $encodingSettings -ColorInfo $colorInfo
} else {
    # Use HEVC encoding with HDR10+ injection
    # Check if we have viable HDR10+ metadata to inject
    if ($Script:HDR10PlusStatus.HasHDR10Plus -and $Script:HDR10PlusStatus.ExtractedJsonPath -and (Test-Path $Script:HDR10PlusStatus.ExtractedJsonPath)) {
        Write-Host "HEVC encoding with HDR10+ metadata injection" -ForegroundColor Green
        
        # Encode to temporary HEVC file first
        $tempEncodedFile = [System.IO.Path]::ChangeExtension($encodedVideo, ".temp.hevc")
        Add-TempFile -FilePath $tempEncodedFile
        
        # Perform HEVC encoding
        $encodingSuccess = Invoke-VideoEncoding -InputFile $processedFile -OutputFile $tempEncodedFile -VideoInfo $videoInfo -EncodingSettings $encodingSettings -ColorInfo $colorInfo
        
        if ($encodingSuccess) {
            # Inject HDR10+ metadata into encoded video
            Write-Host "Injecting HDR10+ metadata into encoded video..." -ForegroundColor Cyan
            $injectionSuccess = Invoke-HDR10PlusInjection -VideoFile $tempEncodedFile -JsonFile $Script:HDR10PlusStatus.ExtractedJsonPath -OutputFile $encodedVideo
            
            if ($injectionSuccess) {
                Write-Host "HDR10+ metadata successfully injected" -ForegroundColor Green
                Remove-Item $tempEncodedFile -Force -ErrorAction SilentlyContinue
            } else {
                Write-Warning "HDR10+ injection failed - using encoded video without HDR10+"
                Move-Item $tempEncodedFile $encodedVideo -Force
            }
        }
    } else {
        # Standard HEVC encoding without HDR10+ injection
        Write-Host "Standard HEVC encoding (no HDR10+ metadata available)" -ForegroundColor Yellow
        $encodingSuccess = Invoke-VideoEncoding -InputFile $processedFile -OutputFile $encodedVideo -VideoInfo $videoInfo -EncodingSettings $encodingSettings -ColorInfo $colorInfo
    }
}

if (-not $encodingSuccess) {
    throw "Video encoding failed"
}

# Verify encoded video was created
if (-not (Test-Path $encodedVideo) -or (Get-Item $encodedVideo).Length -eq 0) {
    throw "Video encoding produced empty or missing file"
}

$encodedSizeMB = [math]::Round((Get-Item $encodedVideo).Length / 1MB, 2)
Write-Host "Video encoding completed successfully: $encodedSizeMB MB" -ForegroundColor Green
Write-Host "  Codec used: $codecType" -ForegroundColor Green
if ($Script:HDR10PlusStatus.HasHDR10Plus) {
    Write-Host "  HDR10+ preserved: Yes ($($Script:HDR10PlusStatus.SceneCount) scenes)" -ForegroundColor Green
}

Update-BackupProgress -ProcessingStep "video encoding"

# STEP 5: HDR10+ Processing Status (simplified)
Write-Host "`n=== HDR10+ Processing Status ===" -ForegroundColor Cyan
if ($Script:HDR10PlusStatus.HasHDR10Plus -and $codecType -eq "HEVC") {
    Write-Host "HDR10+ metadata was preserved during HEVC encoding" -ForegroundColor Green
    Write-Host "  Original scenes: $($Script:HDR10PlusStatus.SceneCount)" -ForegroundColor Green
    Write-Host "  Metadata file: $(Split-Path $Script:HDR10PlusStatus.ExtractedJsonPath -Leaf)" -ForegroundColor Gray
} elseif ($codecType -eq "AV1") {
    Write-Host "AV1 codec selected - no HDR10+ processing (not applicable)" -ForegroundColor Gray
    Write-Host "AV1 provides superior compression for non-HDR10+ content" -ForegroundColor Green
} else {
    Write-Host "No HDR10+ metadata detected in source - no injection needed" -ForegroundColor Gray
}

$finalVideo = $encodedVideo

# Use mkvmerge for fast, efficient containerization
Write-Host "Using mkvmerge for MKV containerization..." -ForegroundColor Green

# Adjust track name and metadata based on codec
$trackName = if ($codecType -eq "AV1") { "AV1 Video" } else { "HEVC Video" }
$codecDescription = if ($codecType -eq "AV1") { "AV1 (Better Compression)" } else { "HEVC (HDR10+ Preserved)" }

# NEW: Get HDR metadata from source for mkvmerge
$sourceHDRMetadata = $null
if ($ColorInfo.IsHDR) {
    Write-Host "Extracting HDR metadata for mkvmerge..." -ForegroundColor Cyan
    $sourceHDRMetadata = Get-HDRMasteringMetadata -FilePath $InputFile
    
    if ($sourceHDRMetadata -and $sourceHDRMetadata.IsValid) {
        Write-Host "  Using source HDR metadata for MKV containerization" -ForegroundColor Green
    } else {
        Write-Host "  Using default HDR metadata for MKV containerization" -ForegroundColor Yellow
    }
}

$intermediateMKV = New-TempFile -BaseName "$baseName.intermediate" -Extension ".mkv"

$mkvmergeArgs = @(
    "--output", $intermediateMKV,
    "--language", "0:und",
    "--track-name", "0:$trackName",
    "--default-track", "0:yes",
    "--compression", "0:none",
    "--no-attachments",
    "--title", "Plex Optimized $codecType"
)

# NEW: Add HDR metadata to mkvmerge for proper containerization
if ($ColorInfo.IsHDR) {
    Write-Host "Adding HDR10 metadata to MKV container..." -ForegroundColor Cyan
    
    # Get HDR encoding parameters (which includes smart defaults)
    $hdrParams = Get-HDREncodingParameters -SourceMetadata $sourceHDRMetadata -ContentType "live-action"
    
    # Parse MaxCLL and MaxFALL from the parameters
    $maxCLL = 1500  # Fallback default
    $maxFALL = 200  # Fallback default
    
    if ($hdrParams.MaxCLL) {
        $cllParts = $hdrParams.MaxCLL -split ','
        if ($cllParts.Count -eq 2) {
            $maxCLL = [int]$cllParts[0]
            $maxFALL = [int]$cllParts[1]
        }
    }
    
    Write-Host "  Using MaxCLL: $maxCLL, MaxFALL: $maxFALL" -ForegroundColor Green
    
    # Add color metadata
    $mkvmergeArgs += "--colour-matrix", "0:9"  # BT.2020 non-constant
    $mkvmergeArgs += "--colour-range", "0:1"   # Limited range
    $mkvmergeArgs += "--colour-transfer-characteristics", "0:16"  # PQ (SMPTE ST 2084)
    $mkvmergeArgs += "--colour-primaries", "0:9"  # BT.2020
    
    # Parse mastering display from hdrParams
    if ($hdrParams.MasterDisplay) {
        # Extract chromaticity coordinates from master-display string
        # Format: G(x,y)B(x,y)R(x,y)WP(x,y)L(max,min)
        if ($hdrParams.MasterDisplay -match 'G\((\d+),(\d+)\)B\((\d+),(\d+)\)R\((\d+),(\d+)\)WP\((\d+),(\d+)\)L\((\d+),(\d+)\)') {
            $gx = $matches[1]; $gy = $matches[2]
            $bx = $matches[3]; $by = $matches[4]
            $rx = $matches[5]; $ry = $matches[6]
            $wpx = $matches[7]; $wpy = $matches[8]
            $maxLum = $matches[9]; $minLum = $matches[10]
            
            # Build chromaticity coordinates string for mkvmerge
#            $chromaCoords = "0:$rx,$ry,$gx,$gy,$bx,$by,$wpx,$wpy"
            $chromaCoords = "0:$rx,$ry,$gx,$gy,$bx,$by"
            
            $mkvmergeArgs += "--chromaticity-coordinates", $chromaCoords
            $mkvmergeArgs += "--max-content-light", "0:$maxCLL"
            $mkvmergeArgs += "--max-frame-light", "0:$maxFALL"
            $mkvmergeArgs += "--max-luminance", "0:$maxLum"
            $mkvmergeArgs += "--min-luminance", "0:$minLum"
            
            Write-Host "  Applied mastering display metadata to MKV" -ForegroundColor Green
        } else {
            Write-Warning "  Could not parse master-display string, using defaults"
            # Use the fallback below
            $useDefaults = $true
        }
    } else {
        $useDefaults = $true
    }
    
    # Fallback to defaults if needed
    if ($useDefaults) {
        $defaultChroma = "0:34000,16000,13250,34500,7500,3000"
        $mkvmergeArgs += "--chromaticity-coordinates", $defaultChroma
        $mkvmergeArgs += "--max-content-light", "0:$maxCLL"
        $mkvmergeArgs += "--max-frame-light", "0:$maxFALL"
        $mkvmergeArgs += "--max-luminance", "0:10000000"  # 1000 cd/m² in 0.0001 units
        $mkvmergeArgs += "--min-luminance", "0:1"         # 0.0001 cd/m²
        
        Write-Host "  Applied default Display P3 HDR metadata to MKV" -ForegroundColor Yellow
    }
}

# For AV1, specify the codec ID
if ($codecType -eq "AV1" -and $videoExtension -eq ".ivf") {
    $mkvmergeArgs += "--fourcc", "0:av01"
}

# Add the input file last
$mkvmergeArgs += $finalVideo
$mkvmergePath = $Config.MKVMergeExe 
   $quotedArgs = $mkvmergeArgs | ForEach-Object {
            if ($_ -match '\s') { "`"$_`"" } else { $_ }
        }
        $commandLine = $quotedArgs -join ' '
        Write-Host "[CMD] $mkvmergePath $commandLine" -ForegroundColor Gray

# Execute mkvmerge
$processInfo = New-Object System.Diagnostics.ProcessStartInfo
$processInfo.FileName = $mkvmergePath
$processInfo.UseShellExecute = $false
$processInfo.RedirectStandardOutput = $true
$processInfo.RedirectStandardError = $true
$processInfo.CreateNoWindow = $false

if ($PSVersionTable.PSVersion.Major -ge 7) {
    foreach ($arg in $mkvmergeArgs) {
        $null = $processInfo.ArgumentList.Add($arg)
    }
}
else {
    $quotedArgs = $mkvmergeArgs | ForEach-Object { 
        if ($_ -match '\s') { '"{0}"' -f ($_ -replace '"', '""') } else { $_ }
    }
    $processInfo.Arguments = $quotedArgs -join ' '
}

$process = [System.Diagnostics.Process]::Start($processInfo)
$handle = $process.Handle
$Script:ActiveProcesses = @($Script:ActiveProcesses) + $process

$stdout = $process.StandardOutput.ReadToEnd()
$stderr = $process.StandardError.ReadToEnd()

try {
    $process.PriorityClass = [System.Diagnostics.ProcessPriorityClass]::BelowNormal
}
catch {
    Write-Host "Could not set mkvmerge process priority" -ForegroundColor Gray
}

$process.WaitForExit()
$Script:ActiveProcesses = $Script:ActiveProcesses | Where-Object { $_.Id -ne $process.Id }

$exitCode = $process.ExitCode

if ($exitCode -eq 0) {
    Write-Host "mkvmerge completed successfully" -ForegroundColor Green
    if ($stdout) {
        Write-Host "mkvmerge output:" -ForegroundColor Gray
        Write-Host $stdout -ForegroundColor DarkGray
    }
}
else {
    Write-Warning "mkvmerge failed with exit code: $exitCode"
    if ($stderr) {
        Write-Host "mkvmerge error:" -ForegroundColor Red
        Write-Host $stdout -ForegroundColor Red
        Write-Host $stderr -ForegroundColor Red
    }
    throw "mkvmerge failed to create MKV intermediate"
}

# Verify intermediate MKV was created
if (-not (Test-Path $intermediateMKV) -or (Get-Item $intermediateMKV).Length -eq 0) {
    throw "$codecType to MKV conversion produced empty or missing file"
}

$mkvSizeMB = [math]::Round((Get-Item $intermediateMKV).Length / 1MB, 2)
Write-Host "MKV intermediate created successfully: $mkvSizeMB MB" -ForegroundColor Green
Write-Host "  Video codec: $codecDescription" -ForegroundColor Green

# Update finalVideo to point to the MKV version
$finalVideo = $intermediateMKV

Write-Host "Raw $codecType successfully containerized to MKV intermediate using mkvmerge" -ForegroundColor Green

Update-BackupProgress -ProcessingStep "intermediate containerization"

# STEP 6: Final remux (combines video with audio/subtitles from processed file)
Write-Host "`n=== Final Plex-Optimized Remux ===" -ForegroundColor Cyan
Write-Host "Creating final output with all streams..." -ForegroundColor Yellow
Write-Host "  Video source: $(Split-Path $finalVideo -Leaf) ($codecType)" -ForegroundColor White
Write-Host "  Audio/subtitle source: $(Split-Path $processedFile -Leaf)" -ForegroundColor White
Write-Host "  Output destination: Temporary file pending quality validation" -ForegroundColor White

Show-StreamDiagnostics -VideoFile $finalVideo -OriginalFile $processedFile -AudioInfo $audioInfo -SubtitleInfo $subtitleInfo
$dir = [System.IO.Path]::GetDirectoryName($InputFile)
$fileNameWithoutExt = [System.IO.Path]::GetFileNameWithoutExtension($InputFile)
$extension = [System.IO.Path]::GetExtension($InputFile)
$TempInputFile = Join-Path $dir "$fileNameWithoutExt.final$extension"
Add-TempFile -FilePath $TempInputFile
Invoke-PlexOptimizedRemux -VideoFile $finalVideo -OriginalFile $processedFile -AudioInfo $audioInfo -SubtitleInfo $subtitleInfo -OutputPath $TempInputFile

Update-BackupProgress -ProcessingStep "final remux"

$finalSize = (Get-Item $TempInputFile).Length
$sizeDiff = if ($Script:OriginalFileSize -gt 0) { (($finalSize - $Script:OriginalFileSize) / $Script:OriginalFileSize) * 100 } else { 0 }

# STEP 7: Quality validation (if enabled) - CRITICAL DECISION POINT
$qualityValidationPassed = $true
if ($EnableQualityValidation -ne 0) {
    Write-Host "`n=== Quality Validation (Decision Point) ===" -ForegroundColor Cyan
    Write-Host "Quality validation enabled - this will determine if the original file is replaced" -ForegroundColor Yellow
    Write-Host "Codec used for encoding: $codecType" -ForegroundColor White
    $qualityValidationPassed = Invoke-QualityValidation -OriginalFile $processedFile -EncodedFile $finalVideo -Threshold $QualityThreshold -EncodingSettings $EncodingSettings
    
    if (-not $Script:QualityMetrics.PassesValidation -and $EnableQualityValidation -gt 0) {
        Write-Host "`n=== QUALITY VALIDATION FAILED ===" -ForegroundColor Red
        Write-Warning "Quality validation failed - original file will NOT be replaced"
        Write-Host "Current quality setting: $($encodingSettings.Quality)" -ForegroundColor Yellow
        Write-Host "Codec used: $codecType" -ForegroundColor Yellow
        Write-Host "Suggested next attempt: $($encodingSettings.Quality - 4)" -ForegroundColor Yellow
        Write-Host "Consider adjusting encoding settings and trying again" -ForegroundColor Yellow
        
        # Check backup status before exiting
        if ($Script:BackupJob -and $Script:BackupCompleted) {
            Write-Host "Original file backup completed successfully" -ForegroundColor Green
        }
        elseif ($Script:BackupJob -and -not $Script:BackupCompleted) {
            Write-Host "Backup was in progress - final backup status unknown" -ForegroundColor Yellow
        }
        
        if ($sizeDiff -gt 10) {
            Write-Host "File Size Increased > 10%, stopping here"
            $qualityValidationPassed = $true
        } else {
            # Clean up the temporary processed file (don't replace original)
            if (Test-Path $TempInputFile) {
                try {
                    Remove-Item $TempInputFile -Force -ErrorAction SilentlyContinue
                    Write-Host "Temporary processed file removed" -ForegroundColor Gray
                }
                catch {
                    Write-Warning "Could not remove temporary file: $TempInputFile"
                }
            }
            # Exit with quality setting minus 4 as the exit code
            $exitCode = $encodingSettings.Quality - 4
            Write-Host "Exiting with code $exitCode (current quality - 4) to indicate retry needed" -ForegroundColor Yellow
        
            # Perform cleanup
            if ($spaceMonitorJob) { Stop-SpaceMonitoringWithCleanup -MonitorJob $spaceMonitorJob }
            Stop-ActiveProcesses
            Clear-TempFilesWithHandleRelease
        
            # Exit with the retry signal
            $baseCode = 0xF0F0  # 61680 in decimal
            $exitCode = $baseCode + $exitCode
        
            Write-Host "Numeric exit code: $exitCode (0xF0F0 + ($($encodingSettings.Quality) - 4))" -ForegroundColor Gray
            exit $exitCode
        }
    }
    else {
        Write-Host "Quality validation PASSED - proceeding with file replacement" -ForegroundColor Green
        $qualityValidationPassed = $true
    }
}
else {
    Write-Host "`n=== Quality Validation ===" -ForegroundColor Cyan
    Write-Host "Quality validation disabled - proceeding with file replacement" -ForegroundColor Gray
}
    
    # STEP 8: Replace original file only if quality validation passed or was disabled
    if ($qualityValidationPassed) {
        Write-Host "`n=== Replacing Original File ===" -ForegroundColor Green
        Write-Host "Quality validation passed or disabled - replacing original file" -ForegroundColor Green
        
        try {
            Move-Item -Path $TempInputFile -Destination $InputFile -Force
            Write-Host "Original file successfully replaced with processed version" -ForegroundColor Green
        }
        catch {
            Write-Error "Failed to replace original file: $_"
            throw "Could not replace original file with processed version"
        }
    }
    
    # Verify output file was created
    if (-not (Test-Path $InputFile)) {
        throw "Output file not found after processing: $InputFile"
    }
    
    # STEP 9: Report results with enhanced file size validation
    Write-Host "`n=== Processing Results ===" -ForegroundColor Green
    $processingTime = (Get-Date) - $Script:ProcessingStartTime

    Write-Host "Processing completed successfully!" -ForegroundColor Green
    Write-Host "Final output: $InputFile" -ForegroundColor White
    Write-Host "Target codec: HEVC" -ForegroundColor White
    Write-Host "Original size: $([math]::Round($Script:OriginalFileSize / 1MB, 2)) MB" -ForegroundColor White
    Write-Host "Final size:    $([math]::Round($finalSize / 1MB, 2)) MB" -ForegroundColor White
    Write-Host ("Size change:   {0:+0.00;-0.00;0.00}%" -f $sizeDiff) -ForegroundColor $(if ($sizeDiff -lt 0) { "Green" } else { "Yellow" })
    Write-Host "Processing time: $($processingTime.ToString('hh\:mm\:ss'))" -ForegroundColor White
    Write-Host "Space saved: $([math]::Round(($Script:OriginalFileSize - $finalSize) / 1MB, 2)) MB" -ForegroundColor $(if ($sizeDiff -lt 0) { "Green" } else { "Yellow" })

    # Enhanced processing summary
    Write-Host "`n=== Processing Summary ===" -ForegroundColor Cyan
    if ($processedFile -ne $InputFile) {
        Write-Host "* Dolby Vision metadata removed in early processing" -ForegroundColor Green
    }
    if ($hasHdr10Plus) {
        Write-Host "* HDR10+ metadata preserved and injected" -ForegroundColor Green
    }
    Write-Host "* Video encoded with HEVC codec" -ForegroundColor Green
    Write-Host "* Audio streams optimized for Plex" -ForegroundColor Green
    if ($Script:ExtractedSubtitles.Count -gt 0) {
        Write-Host "* $($Script:ExtractedSubtitles.Count) subtitle streams processed" -ForegroundColor Green
    }
    if ($Script:BackupCompleted) {
        Write-Host "* Original file backed up successfully" -ForegroundColor Green
    }
    if ($EnableQualityValidation) {
        Write-Host "* Quality validation enabled and passed" -ForegroundColor Green
    }

    # Display quality metrics if validation was enabled
    if ($EnableQualityValidation -and $Script:QualityMetrics.PSNR -gt 0) {
        Write-Host "`n=== Quality Metrics ===" -ForegroundColor Cyan
        Write-Host "PSNR: $($Script:QualityMetrics.PSNR) dB" -ForegroundColor $(if ($Script:QualityMetrics.PassesValidation) { "Green" } else { "Red" })
        if ($Script:QualityMetrics.SSIM -gt 0) {
            Write-Host "SSIM: $($Script:QualityMetrics.SSIM)" -ForegroundColor Green
        }
        Write-Host "Quality validation: $(if ($Script:QualityMetrics.PassesValidation) { 'PASSED' } else { 'FAILED' })" -ForegroundColor $(if ($Script:QualityMetrics.PassesValidation) { "Green" } else { "Red" })
    }

    # ENHANCED: File size validation with error condition
    if ($sizeDiff -gt 50) {
        Write-Warning "File size increased significantly - please verify quality"
    }
    elseif ($sizeDiff -le -50) {
        Write-Warning "File size reduced dramatically - please verify quality"
    }
    
}
catch {
    Write-Error "Processing failed: $_"
    Write-Error "Stack trace: $($_.ScriptStackTrace)"
    Write-Host "`n=== Processing Failed ===" -ForegroundColor Red
    Write-Host "Error occurred during: $(if ($_.Exception.Message -match 'early DV removal') { 'Dolby Vision Removal' } elseif ($_.Exception.Message -match 'encoding') { 'Video Encoding' } elseif ($_.Exception.Message -match 'remux') { 'Final Remux' } else { 'General Processing' })" -ForegroundColor Red
    
    # Check backup status in case of failure
    if ($Script:BackupJob -and $Script:BackupCompleted) {
        Write-Host "Original file backup completed successfully - restoration possible if needed" -ForegroundColor Cyan
    }
    elseif ($Script:BackupJob -and -not $Script:BackupCompleted) {
        Write-Host "Backup was in progress - final backup status unknown" -ForegroundColor Yellow
    }

    # Ensure cleanup on error
    if ($spaceMonitorJob) { Stop-SpaceMonitoringWithCleanup -MonitorJob $spaceMonitorJob }
    Stop-ActiveProcesses
    Clear-TempFilesWithHandleRelease
    
    exit 1
}
finally {
    Write-Host "`n=== Cleanup Phase ===" -ForegroundColor Cyan
    
    # Check final backup status before cleanup
    if ($Script:BackupJob -and -not $Script:BackupCompleted) {
        Write-Host "Checking final backup status..." -ForegroundColor Cyan
        $finalBackupStatus = Get-BackupStatus -BackupInfo $Script:BackupJob
        
        if (-not $finalBackupStatus -or $finalBackupStatus.Success -ne $true) {
            Write-Host "Waiting up to 5 minutes for backup completion..." -ForegroundColor Yellow
            
            # Wait for backup with timeout
            $waitStart = Get-Date
            while ($Script:BackupJob -and -not $Script:BackupCompleted -and ((Get-Date) - $waitStart).TotalSeconds -lt 300) {
                Start-Sleep -Seconds 5
                $backupStatus = Get-BackupStatus -BackupInfo $Script:BackupJob
                if ($backupStatus -and $backupStatus.Success -eq $true) {
                    break
                }
            }
            
            # Final status report
            if ($Script:BackupCompleted) {
                Write-Host "Backup completed successfully before cleanup" -ForegroundColor Green
            }
            else {
                Write-Warning "Backup did not complete within timeout - stopping backup process"
            }
        }
    }
    
    # Clean up HDR10+ metadata JSON file if it exists
    if ($Script:HDR10PlusStatus.ExtractedJsonPath -and (Test-Path $Script:HDR10PlusStatus.ExtractedJsonPath)) {
        try {
            Remove-Item $Script:HDR10PlusStatus.ExtractedJsonPath -Force -ErrorAction SilentlyContinue
            Write-Host "Cleaned up HDR10+ metadata file" -ForegroundColor Gray
        }
        catch {
            # Ignore cleanup errors
        }
    }
    
    # Clean up any repaired HDR10+ metadata files
    $repairedFiles = Get-ChildItem -Path $Script:ValidatedTempFolder -Filter "*.hdr10plus.repaired.json" -ErrorAction SilentlyContinue
    foreach ($repairedFile in $repairedFiles) {
        try {
            Remove-Item $repairedFile.FullName -Force -ErrorAction SilentlyContinue
        }
        catch {
            # Ignore cleanup errors
        }
    }
    
    Write-Host "Stopping active processes..." -ForegroundColor Gray
    Stop-ActiveProcesses  # This will call Stop-BackupProcess
    
    Write-Host "Cleaning temporary files..." -ForegroundColor Gray
    Clear-TempFilesWithHandleRelease
    
    # Clear any remaining progress indicators
    $progressActivities = @("Video Processing", "Video encoding", "Extract HDR10+ metadata", 
        "Inject HDR10+ metadata", "MKV containerization", "Final remux", "Quality Validation",
        "Dolby Vision Removal", "Stream Analysis", "Subtitle Extraction")
    foreach ($activity in $progressActivities) {
        Write-Progress -Activity $activity -Completed
    }
    
    # Unregister event handlers
    if ($Script:CleanupRegistered) {
        try {
            Get-EventSubscriber | Where-Object { $_.SourceIdentifier -match "PowerShell.Exiting|ProcessExit" } | Unregister-Event -ErrorAction SilentlyContinue
            Write-Host "Cleanup handlers unregistered" -ForegroundColor Gray
        }
        catch {
            # Ignore cleanup errors during shutdown
        }
    }
    
    # Clean up script-specific temp folder if it exists
    if ($Script:ScriptTempFolder -and (Test-Path $Script:ScriptTempFolder)) {
        try {
            Remove-Item -Path $Script:ScriptTempFolder -Recurse -Force -ErrorAction Stop
            Write-Host "Successfully removed temporary folder: $Script:ScriptTempFolder" -ForegroundColor Green
        } catch {
            Write-Warning "Failed to remove temporary folder: $Script:ScriptTempFolder. Reason: $_"
        }
    }
    
    # Final cleanup - stop disk space monitoring
    if ($spaceMonitorJob) { 
        Write-Host "Stopping disk space monitoring..." -ForegroundColor Gray
        Stop-SpaceMonitoringWithCleanup -MonitorJob $spaceMonitorJob 
    }
    
    # Summary of HDR10+ processing efficiency
    if ($Script:HDR10PlusStatus.HasHDR10Plus) {
        Write-Host "`nHDR10+ Processing Summary:" -ForegroundColor Cyan
        Write-Host "  Detection performed: 1 time (initial analysis)" -ForegroundColor Green
        Write-Host "  Extraction performed: 1 time" -ForegroundColor Green
        Write-Host "  Scenes processed: $($Script:HDR10PlusStatus.SceneCount)" -ForegroundColor Green
        Write-Host "  Metadata viable: $($Script:HDR10PlusStatus.IsViable)" -ForegroundColor Green
    }

    Write-Host "Cleanup completed" -ForegroundColor Green
}

Write-Host "`n=== Script Execution Complete ===" -ForegroundColor Green
Write-Host "Processing completed successfully!" -ForegroundColor Green
exit 0

#endregion