$prefix = 'visualize/2020-2025/Data_Training_Raw_NPZ/'
$target = 454
$maxRounds = 12

for ($i = 1; $i -le $maxRounds; $i++) {
  $cnt = [int]((node scripts/r2_count_prefix.js $prefix) | Select-Object -Last 1)
  Write-Host "ROUND $i COUNT=$cnt"
  if ($cnt -ge $target) { break }

  $env:SKIP_DELETE = '1'
  node scripts/r2_reset_visualize_raw_npz.js
  Start-Sleep -Seconds 2
}

$final = [int]((node scripts/r2_count_prefix.js $prefix) | Select-Object -Last 1)
Write-Host "FINAL COUNT=$final"

