Param (
    [Parameter(Mandatory)] [string] $DiskName,
    [Parameter(Mandatory)] [string] $DiskResourceGroup,
    [Parameter(Mandatory)] [string] $SnapshotLabel,
    [Parameter(Mandatory)] [ValidateSet('yearly', 'monthly', 'weekly', 'daily', 'hourly')] [string] $BackupPeriod,
    [Parameter(Mandatory)] [int] $RetentionPeriodInDays,
    [Parameter(Mandatory=$false)] [string] $SnapshotResourceGroup
)

Function Invoke-DiskIncrementalSnapshotWithRetentionPolicy {
    Param (
        [Parameter(Mandatory)] [string] $DiskName,
        [Parameter(Mandatory)] [string] $DiskResourceGroup,
        [Parameter(Mandatory)] [string] $SnapshotLabel,
        [Parameter(Mandatory)] [ValidateSet('yearly', 'monthly', 'weekly', 'daily', 'hourly')] [string] $BackupPeriod,
        [Parameter(Mandatory)] [int] $RetentionPeriodInDays,
        [Parameter(Mandatory=$false)] [string] $SnapshotResourceGroup
    )
    Process {
        if (([string]::IsNullOrEmpty($SnapshotResourceGroup))) {
            $SnapshotResourceGroup = $DiskResourceGroup
        }

        $runDatetime = Get-Date
        $timestamp = $runDatetime.ToString("yyyyMMddHHmmss")
        $snapshotName = "$($SnapshotLabel)-$($BackupPeriod)-$($timestamp)"

        # Create snapshot
        $managedDisk = Get-AzDisk -ResourceGroupName $DiskResourceGroup -DiskName $DiskName
        $snapshot = New-AzSnapshotConfig -SourceUri $managedDisk.Id -Location $managedDisk.Location -CreateOption copy -Incremental
        New-AzSnapshot -Snapshot $snapshot -SnapshotName $snapshotName -ResourceGroupName $SnapshotResourceGroup

        # Apply retention policy if retention period is set
        if ($RetentionPeriodInDays -gt 0)
        {
            $incrementalSnapshotsForPeriod = Get-AzSnapshot -ResourceGroupName $SnapshotResourceGroup | `
                Where-Object {
                    $_.Incremental `
                    -and $_.CreationData.SourceResourceId -eq $managedDisk.Id `
                    -and $_.CreationData.SourceUniqueId -eq $managedDisk.UniqueId `
                    -and $_.Name -like "$($SnapshotLabel)-$($BackupPeriod)-*"
                }
            
            $retentionDateTimeFrom = $runDatetime.AddDays(-$RetentionPeriodInDays).AddMinutes(10) # We add 10 minutes to prevent an additional and unwanted snapshot to be retained (example: hourly snapshot with 1 day retention must have no more than 24 snapshots)
            $incrementalSnapshotsForPeriod | Where-Object { $_.TimeCreated -le $retentionDateTimeFrom } | `
                foreach {
                    Remove-AzSnapshot -ResourceGroupName $SnapshotResourceGroup -SnapshotName $_.Name -Force
                }
        }
    }
}

$connectionName = "AzureRunAsConnection"

try {
    #Getting the service principal connection "AzureRunAsConnection"
    $servicePrincipalConnection = Get-AutomationConnection -name $connectionName
    
    "Logging into Azure..."
    Add-AzAccount -ServicePrincipal -TenantId $servicePrincipalConnection.TenantID -ApplicationId $servicePrincipalConnection.ApplicationID -CertificateThumbprint $servicePrincipalConnection.CertificateThumbprint
} catch {
    if (!$servicePrincipalConnection) {
        $ErrorMessage = "Connection $connectionName not found."
        throw $ErrorMessage
    } else {
        Write-Error -Message $_.Exception
        throw $_.Exception
    }
}
if ($err) {
    throw $err
}

Invoke-DiskIncrementalSnapshotWithRetentionPolicy `
    -DiskName $DiskName `
    -DiskResourceGroup $DiskResourceGroup `
    -SnapshotLabel $SnapshotLabel `
    -BackupPeriod $BackupPeriod `
    -RetentionPeriodInDays $RetentionPeriodInDays `
    -SnapshotResourceGroup $SnapshotResourceGroup
