Write-Output "Packaging Lambda app"
if (Test-Path .\package) {
  Remove-Item .\package -Recurse -Force
}

New-Item .\package -type directory -f | Out-Null
New-Item .\package\temp -type directory -f | Out-Null
Write-Output "Copying dependencies..."
Copy-Item .\backup.js .\package\temp\
Copy-Item .\diff.js .\package\temp\
Copy-Item .\s3-backfill.js .\package\temp\
Copy-Item .\s3-snapshot.js .\package\temp\
robocopy  .\node_modules\ .\package\temp\ /E | Out-Null
Write-Output "Dependencies sorted"

Write-Output "Generating output..."
Copy-Item .\index.js .\package\temp\
Write-Output "Output generated"

Add-Type -assembly "system.io.compression.filesystem"
$currentPath = (Get-Item -Path ".\" -Verbose).FullName
$sourcePath = $currentPath + "\package\temp"
$outputFile = $currentPath + "\LambdaFunction.zip"

if (Test-Path $outputFile) {
  Remove-Item $outputFile -Force
}

[io.compression.zipfile]::CreateFromDirectory($sourcePath, $outputFile)

Write-Output "λ function ready to be uploaded at: $($outputFile)"