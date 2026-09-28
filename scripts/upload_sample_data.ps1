param(
    [string]$Catalog = "retail_lakehouse"
)

$ErrorActionPreference = "Stop"
$Root = Split-Path -Parent $PSScriptRoot
$Data = Join-Path $Root "data"

$datasets = @("customers", "products", "orders", "order_items")

foreach ($dataset in $datasets) {
    $target = "dbfs:/Volumes/$Catalog/landing/raw/$dataset/"
    databricks fs mkdir $target

    $source = Join-Path $Data "$dataset.csv"
    databricks fs cp $source $target --overwrite
}

Write-Host "Sample data upload complete."
