<#
.SYNOPSIS
    Parent wrapper script for automatic video processing with retry logic

.DESCRIPTION
    SETUP REQUIREMENTS:
    
    1. MAIN SCRIPT DEPENDENCY:
       - Update $innerScriptPath to point to your main processing script
       - Default: "E:\Plex\convertHdr10+ToAV1.ps1"
       - The main script must be fully configured with all dependencies
    
    2. ALL DEPENDENCIES FROM MAIN SCRIPT REQUIRED:
       - Python 3.8+ with modules: pip install pandas numpy
       - FFmpeg with QSV support (ffmpeg.exe, ffprobe.exe)
       - MKVToolNix (mkvmerge.exe)
       - Optional: HDR10+ Tool, Dolby Vision tools, MCEBuddy CLI
    
    3. RETRY LOGIC:
       - Automatically retries up to 3 times on quality validation failures
       - Adjusts quality settings based on exit codes from main script
       - Disables quality validation on final attempt to ensure completion
    
    4. EXIT CODE HANDLING:
       - 0: Success
       - 0xF0F0 + quality: Retry needed with adjusted quality
       - Other: General failure
    
    5. CONFIGURATION:
       - Ensure main script path is correct
       - Test main script independently first
       - Verify all tool paths in main script's $Config hashtable

.NOTES
    This script passes all parameters through to the main processing script.
    Test the main script individually before using this wrapper.
#>

#region Parameter Definition

# This script accepts the same parameters as the inner script.
[CmdletBinding()]
param(
     [Parameter(Mandatory = $true, ValueFromPipeline = $true)]
    [ValidateScript({ Test-Path -LiteralPath $_ -PathType Leaf })]
    [string]$InputFile,

    [Parameter(Mandatory = $true, ValueFromPipeline = $true)]
    [ValidateScript({ Test-Path -LiteralPath $_ -PathType Leaf })]
    [string]$OrigFile,

    [Parameter(Mandatory = $false)]
    [switch]$incomingGlobalQuality,
    
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
    [decimal]$QualityThreshold = 30.0,
    
    [Parameter(Mandatory = $false)]
    [switch]$EnableSoftwareFallback
)

#endregion
$innerScriptPath = "E:\Plex\convertHdr10+ToAV1.ps1"


# Use splatting to pass all received parameters to the inner script.
# This approach is flexible and automatically handles all defined parameters.
$params = @{}
$PSBoundParameters.GetEnumerator() | ForEach-Object { $params.Add($_.Key, $_.Value) }

# Initialize variables for the loop
$loopResult = 1 # Start with a non-zero value to enter the loop
$runCount = 1

Write-Host "Starting video conversion loop..." -ForegroundColor Yellow

do {
    Write-Host "`n--- Running conversion, attempt #$runCount ---" -ForegroundColor Cyan

    # Check if a quality value from a previous run needs to be passed
    if ($runCount -gt 1) {
        if ($loopResult -gt 0) {
            # Since incomingGlobalQuality is a switch, we remove it and add a new
            # parameter that accepts the integer value.
            if ($params.ContainsKey('incomingGlobalQuality')) {
                $params.Remove('incomingGlobalQuality')
            }
            $params.Add('incomingGlobalQuality', $loopResult)

            # You would need to add a parameter like 'QualityValue' to your inner script
            # to accept the return value. For now, we'll just remove the switch.
        }
    }
    # only go 3 and then skip quality check
    if ($runCount -eq 3) {
        $params.Remove('EnableQualityValidation')
        $params.Add('EnableQualityValidation', -1)
    }
    # Call the inner script with the captured parameters
    # The `&` operator executes the script and ensures `$LASTEXITCODE` is set.
    try {
    & $innerScriptPath @params
    $loopResult = $LASTEXITCODE
    } catch {
        Write-Host "Script failed $($LASTEXITCODE)"
        $loopResult = -1
    }
    
    if ($loopResult -ne 0) {
     # Check if exit code indicates a quality retry is needed
     $baseRetryCode = 0xF0F0  # 61680 in decimal

        if ($loopResult -ge $baseRetryCode -and $loopResult -lt ($baseRetryCode + 100)) {
        # Extract the suggested quality from the exit code
         $loopResult = $loopResult - $baseRetryCode
        } else {
         $loopResult = -1
        }
    }

    Write-Host "`n--- Script returned exit code: $loopResult ---" -ForegroundColor Green

    if ($loopResult -gt 0) {
        Write-Host "Return value > 0. Looping to try again..." -ForegroundColor Yellow
        $runCount++
    }
} while ($loopResult -gt 0 -and $runCount -lt 4) # Added a max loop count to prevent infinite loops

#if ($runCount -eq 3 -and $loopResult -eq -1) {
#    $loopresult = 0
#}
Write-Host "`n--- Loop finished. Final return value: $loopResult ---" -ForegroundColor Green

# Return the final exit code of the loop to the calling process
exit $loopResult