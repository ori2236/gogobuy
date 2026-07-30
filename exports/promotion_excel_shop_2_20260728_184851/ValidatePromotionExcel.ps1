param(
    [Parameter(Position = 0)]
    [string]$ExcelFile
)

$ErrorActionPreference = "Stop"
$BaseDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProductsFile = Join-Path $BaseDir "promotion_products.json"
if (-not (Test-Path -LiteralPath $ProductsFile)) {
    throw "לא נמצא הקובץ promotion_products.json בתיקייה."
}

if ([string]::IsNullOrWhiteSpace($ExcelFile)) {
    $Candidate = Get-ChildItem -LiteralPath $BaseDir -Filter "מבצעים_*.xlsx" -File |
        Where-Object { $_.Name -notlike "~$*" } |
        Sort-Object LastWriteTime -Descending |
        Select-Object -First 1
    if ($null -eq $Candidate) {
        throw "לא נמצא קובץ מבצעים לבדיקה."
    }
    $ExcelFile = $Candidate.FullName
} elseif (-not [System.IO.Path]::IsPathRooted($ExcelFile)) {
    $ExcelFile = Join-Path $BaseDir $ExcelFile
}

if (-not (Test-Path -LiteralPath $ExcelFile)) {
    throw "קובץ האקסל לא נמצא: $ExcelFile"
}

$Catalog = Get-Content -LiteralPath $ProductsFile -Raw -Encoding UTF8 | ConvertFrom-Json
$ProductsBySelector = @{}
$ProductsById = @{}
$ProductsByName = @{}
foreach ($Product in $Catalog.products) {
    $Selector = if ($Product.selector) { [string]$Product.selector } else { "{0} [ID:{1}]" -f $Product.name, $Product.product_id }
    $ProductsBySelector[$Selector.Trim()] = $Product
    $ProductsById[[string]$Product.product_id] = $Product
    $Name = ([string]$Product.name).Trim()
    if (-not $ProductsByName.ContainsKey($Name)) { $ProductsByName[$Name] = @() }
    $ProductsByName[$Name] = @($ProductsByName[$Name]) + @($Product)
}

$ExpectedHeaders = @(
    "שם מבצע", "תיאור מבצע", "סוג מבצע", "מוצר", "מקסימום מימושים להזמנה",
    "מבצע יום השוק", "תאריך התחלה", "תאריך סוף", "אחוז הנחה", "מחיר קבוע",
    "כמות במבצע", "מחיר כולל במבצע", "סכום הנחה", "סכום סל מינימלי",
    "דמי משלוח במבצע", "כמות מתנה", "מחיר מיוחד למוצר", "סטטוס", "שגיאות"
)
$AllowedTypes = @(
    "אחוז הנחה", "מחיר קבוע", "כמות בסכום", "הנחה בשקלים",
    "מבצע משלוח לפי סכום סל", "מתנה לפי סכום סל", "מחיר מיוחד למוצר לפי סכום סל"
)

$Excel = $null
$Workbook = $null
$Sheet = $null

function Release-ComObject([object]$Object) {
    if ($null -ne $Object) {
        [void][System.Runtime.InteropServices.Marshal]::FinalReleaseComObject($Object)
    }
}

function Clean-Text($Value) {
    if ($null -eq $Value) { return "" }
    return ([string]$Value -replace "\s+", " ").Trim()
}

function Is-Blank($Value) {
    return [string]::IsNullOrWhiteSpace((Clean-Text $Value))
}

function Parse-Number($Value, [double]$Minimum, [double]$Maximum, [bool]$Integer, [string]$Label, [System.Collections.Generic.List[string]]$Errors, [bool]$Required) {
    if (Is-Blank $Value) {
        if ($Required) { $Errors.Add("$Label: שדה חובה") }
        return $null
    }
    $Text = (Clean-Text $Value).Replace("₪", "").Replace("%", "").Replace(",", "")
    $Number = 0.0
    if (-not [double]::TryParse($Text, [Globalization.NumberStyles]::Any, [Globalization.CultureInfo]::InvariantCulture, [ref]$Number)) {
        if (-not [double]::TryParse($Text, [ref]$Number)) {
            $Errors.Add("$Label: חייב להיות מספר")
            return $null
        }
    }
    if ($Integer -and $Number -ne [math]::Truncate($Number)) {
        $Errors.Add("$Label: חייב להיות מספר שלם")
    }
    if ($Number -lt $Minimum) { $Errors.Add("$Label: חייב להיות לפחות $Minimum") }
    if ($Number -gt $Maximum) { $Errors.Add("$Label: חייב להיות לכל היותר $Maximum") }
    return $Number
}

function Parse-DateValue($Value, [string]$Label, [System.Collections.Generic.List[string]]$Errors) {
    if (Is-Blank $Value) {
        $Errors.Add("$Label: שדה חובה")
        return $null
    }
    try {
        if ($Value -is [double] -or $Value -is [int]) {
            return [datetime]::FromOADate([double]$Value).Date
        }
        return ([datetime]$Value).Date
    } catch {
        $Errors.Add("$Label: תאריך לא תקין")
        return $null
    }
}

function Resolve-Product([string]$Value) {
    $Clean = (Clean-Text $Value)
    if ([string]::IsNullOrWhiteSpace($Clean)) { return $null }
    if ($ProductsBySelector.ContainsKey($Clean)) { return $ProductsBySelector[$Clean] }
    if ($Clean -match "\[ID:(\d+)\]\s*$" -and $ProductsById.ContainsKey($Matches[1])) {
        return $ProductsById[$Matches[1]]
    }
    if ($ProductsByName.ContainsKey($Clean) -and @($ProductsByName[$Clean]).Count -eq 1) {
        return @($ProductsByName[$Clean])[0]
    }
    return $null
}

function Ensure-Blank($Values, [string[]]$Labels, [System.Collections.Generic.List[string]]$Errors) {
    for ($Index = 0; $Index -lt $Labels.Count; $Index++) {
        if (-not (Is-Blank $Values[$Index])) {
            $Errors.Add("$($Labels[$Index]): לא רלוונטי לסוג המבצע ויש להשאיר ריק")
        }
    }
}

try {
    $Excel = New-Object -ComObject Excel.Application
    $Excel.Visible = $false
    $Excel.DisplayAlerts = $false
    $Workbook = $Excel.Workbooks.Open($ExcelFile, 0, $false)

    try {
        $Sheet = $Workbook.Worksheets.Item("מבצעים")
    } catch {
        $Sheet = $Workbook.Worksheets.Item(1)
    }

    $HeaderRow = 0
    for ($Row = 1; $Row -le 20; $Row++) {
        if ((Clean-Text $Sheet.Cells.Item($Row, 1).Value2) -eq "שם מבצע") {
            $HeaderRow = $Row
            break
        }
    }
    if ($HeaderRow -eq 0) { throw "לא נמצאה שורת הכותרות של המבצעים." }

    for ($Column = 1; $Column -le $ExpectedHeaders.Count; $Column++) {
        $Actual = Clean-Text $Sheet.Cells.Item($HeaderRow, $Column).Value2
        if ($Actual -ne $ExpectedHeaders[$Column - 1]) {
            throw "העמודות שונו. בעמודה $Column נדרש '$($ExpectedHeaders[$Column - 1])' ונמצא '$Actual'."
        }
    }

    $LastRow = [Math]::Min(1004, [Math]::Max($HeaderRow + 1, $Sheet.UsedRange.Row + $Sheet.UsedRange.Rows.Count - 1))
    $ValidRows = New-Object System.Collections.Generic.List[object]
    $InvalidRows = New-Object System.Collections.Generic.List[object]

    for ($Row = $HeaderRow + 1; $Row -le $LastRow; $Row++) {
        $Values = @()
        for ($Column = 1; $Column -le 17; $Column++) {
            $Values += $Sheet.Cells.Item($Row, $Column).Value2
        }
        $HasContent = $false
        foreach ($Value in $Values) {
            if (-not (Is-Blank $Value)) { $HasContent = $true; break }
        }
        if (-not $HasContent) {
            $Sheet.Cells.Item($Row, 18).Value2 = ""
            $Sheet.Cells.Item($Row, 19).Value2 = ""
            continue
        }

        $Errors = New-Object System.Collections.Generic.List[string]
        $Title = Clean-Text $Values[0]
        $Description = Clean-Text $Values[1]
        $Type = Clean-Text $Values[2]
        $ProductText = Clean-Text $Values[3]
        $MarketDay = Clean-Text $Values[5]

        if ([string]::IsNullOrWhiteSpace($Title)) { $Errors.Add("שם מבצע: שדה חובה") }
        if ($Title.Length -gt 255) { $Errors.Add("שם מבצע: עד 255 תווים") }
        if ($Description.Length -gt 1000) { $Errors.Add("תיאור מבצע: עד 1000 תווים") }
        if ($AllowedTypes -notcontains $Type) { $Errors.Add("סוג מבצע: יש לבחור ערך מהרשימה") }
        if (@("כן", "לא") -notcontains $MarketDay) { $Errors.Add("מבצע יום השוק: יש לבחור כן או לא") }

        $StartDate = Parse-DateValue $Values[6] "תאריך התחלה" $Errors
        $EndDate = Parse-DateValue $Values[7] "תאריך סוף" $Errors
        if ($null -ne $StartDate -and $null -ne $EndDate -and $EndDate -lt $StartDate) {
            $Errors.Add("תאריך סוף: חייב להיות שווה או מאוחר מתאריך ההתחלה")
        }

        $MaxRedemptions = Parse-Number $Values[4] 1 100000 $true "מקסימום מימושים להזמנה" $Errors $false
        $Product = Resolve-Product $ProductText
        $ProductRequired = $Type -ne "מבצע משלוח לפי סכום סל"
        if ($ProductRequired -and [string]::IsNullOrWhiteSpace($ProductText)) {
            $Errors.Add("מוצר: שדה חובה בסוג המבצע שנבחר")
        } elseif (-not [string]::IsNullOrWhiteSpace($ProductText) -and $null -eq $Product) {
            $Errors.Add("מוצר: המוצר אינו קיים ברשימת המוצרים של הסניף")
        }
        if (-not $ProductRequired -and -not [string]::IsNullOrWhiteSpace($ProductText)) {
            $Errors.Add("מוצר: במבצע משלוח יש להשאיר את המוצר ריק")
        }

        $SpecificLabels = @("אחוז הנחה", "מחיר קבוע", "כמות במבצע", "מחיר כולל במבצע", "סכום הנחה", "סכום סל מינימלי", "דמי משלוח במבצע", "כמות מתנה", "מחיר מיוחד למוצר")
        $SpecificValues = @($Values[8], $Values[9], $Values[10], $Values[11], $Values[12], $Values[13], $Values[14], $Values[15], $Values[16])

        $PercentOff = $null
        $FixedPrice = $null
        $BundleQty = $null
        $BundlePrice = $null
        $AmountOff = $null
        $Threshold = $null
        $DeliveryFee = $null
        $GiftQty = $null
        $RewardFixedPrice = $null

        switch ($Type) {
            "אחוז הנחה" {
                $PercentOff = Parse-Number $Values[8] 0.01 100 $false "אחוז הנחה" $Errors $true
                Ensure-Blank @($Values[9],$Values[10],$Values[11],$Values[12],$Values[13],$Values[14],$Values[15],$Values[16]) @($SpecificLabels[1..8]) $Errors
            }
            "מחיר קבוע" {
                $FixedPrice = Parse-Number $Values[9] 0 1000000 $false "מחיר קבוע" $Errors $true
                Ensure-Blank @($Values[8],$Values[10],$Values[11],$Values[12],$Values[13],$Values[14],$Values[15],$Values[16]) @($SpecificLabels[0],$SpecificLabels[2],$SpecificLabels[3],$SpecificLabels[4],$SpecificLabels[5],$SpecificLabels[6],$SpecificLabels[7],$SpecificLabels[8]) $Errors
            }
            "כמות בסכום" {
                $BundleQty = Parse-Number $Values[10] 2 100000 $true "כמות במבצע" $Errors $true
                $BundlePrice = Parse-Number $Values[11] 0 1000000 $false "מחיר כולל במבצע" $Errors $true
                Ensure-Blank @($Values[8],$Values[9],$Values[12],$Values[13],$Values[14],$Values[15],$Values[16]) @($SpecificLabels[0],$SpecificLabels[1],$SpecificLabels[4],$SpecificLabels[5],$SpecificLabels[6],$SpecificLabels[7],$SpecificLabels[8]) $Errors
            }
            "הנחה בשקלים" {
                $AmountOff = Parse-Number $Values[12] 0.01 1000000 $false "סכום הנחה" $Errors $true
                Ensure-Blank @($Values[8],$Values[9],$Values[10],$Values[11],$Values[13],$Values[14],$Values[15],$Values[16]) @($SpecificLabels[0],$SpecificLabels[1],$SpecificLabels[2],$SpecificLabels[3],$SpecificLabels[5],$SpecificLabels[6],$SpecificLabels[7],$SpecificLabels[8]) $Errors
            }
            "מבצע משלוח לפי סכום סל" {
                $Threshold = Parse-Number $Values[13] 0 1000000 $false "סכום סל מינימלי" $Errors $true
                $DeliveryFee = Parse-Number $Values[14] 0 1000000 $false "דמי משלוח במבצע" $Errors $true
                Ensure-Blank @($Values[8],$Values[9],$Values[10],$Values[11],$Values[12],$Values[15],$Values[16]) @($SpecificLabels[0],$SpecificLabels[1],$SpecificLabels[2],$SpecificLabels[3],$SpecificLabels[4],$SpecificLabels[7],$SpecificLabels[8]) $Errors
                if ($null -ne $MaxRedemptions) { $Errors.Add("מקסימום מימושים להזמנה: לא רלוונטי למבצע משלוח ויש להשאיר ריק") }
            }
            "מתנה לפי סכום סל" {
                $Threshold = Parse-Number $Values[13] 0 1000000 $false "סכום סל מינימלי" $Errors $true
                $GiftQty = Parse-Number $Values[15] 1 100000 $true "כמות מתנה" $Errors $true
                Ensure-Blank @($Values[8],$Values[9],$Values[10],$Values[11],$Values[12],$Values[14],$Values[16]) @($SpecificLabels[0],$SpecificLabels[1],$SpecificLabels[2],$SpecificLabels[3],$SpecificLabels[4],$SpecificLabels[6],$SpecificLabels[8]) $Errors
                if ($null -ne $MaxRedemptions) { $Errors.Add("מקסימום מימושים להזמנה: לא רלוונטי למתנה לפי סכום סל ויש להשאיר ריק") }
            }
            "מחיר מיוחד למוצר לפי סכום סל" {
                $Threshold = Parse-Number $Values[13] 0 1000000 $false "סכום סל מינימלי" $Errors $true
                $RewardFixedPrice = Parse-Number $Values[16] 0 1000000 $false "מחיר מיוחד למוצר" $Errors $true
                Ensure-Blank @($Values[8],$Values[9],$Values[10],$Values[11],$Values[12],$Values[14],$Values[15]) @($SpecificLabels[0],$SpecificLabels[1],$SpecificLabels[2],$SpecificLabels[3],$SpecificLabels[4],$SpecificLabels[6],$SpecificLabels[7]) $Errors
            }
        }

        if ($Errors.Count -eq 0) {
            $Sheet.Cells.Item($Row, 18).Value2 = "תקין"
            $Sheet.Cells.Item($Row, 19).Value2 = ""
            $Sheet.Cells.Item($Row, 18).Interior.Color = 13561798
            $Sheet.Cells.Item($Row, 19).Interior.Color = 13561798

            $InternalType = switch ($Type) {
                "אחוז הנחה" { "PERCENT_OFF" }
                "מחיר קבוע" { "FIXED_PRICE" }
                "כמות בסכום" { "BUNDLE" }
                "הנחה בשקלים" { "AMOUNT_OFF" }
                "מבצע משלוח לפי סכום סל" { "DELIVERY_FEE_OVERRIDE" }
                "מתנה לפי סכום סל" { "GIFT_PRODUCT" }
                "מחיר מיוחד למוצר לפי סכום סל" { "THRESHOLD_PRODUCT_FIXED_PRICE" }
            }

            $ValidRows.Add([ordered]@{
                excel_row = $Row
                title = $Title
                description = if ($Description) { $Description } else { $null }
                type = $Type
                internal_type = $InternalType
                product_id = if ($null -ne $Product) { [int]$Product.product_id } else { $null }
                product_name = if ($null -ne $Product) { [string]$Product.name } else { $null }
                max_redemptions_per_order = $MaxRedemptions
                is_market_day = ($MarketDay -eq "כן")
                start_date = $StartDate.ToString("yyyy-MM-dd")
                end_date = $EndDate.ToString("yyyy-MM-dd")
                percent_off = $PercentOff
                fixed_price = $FixedPrice
                bundle_qty = $BundleQty
                bundle_price = $BundlePrice
                amount_off = $AmountOff
                threshold_amount = $Threshold
                delivery_fee = $DeliveryFee
                gift_qty = $GiftQty
                reward_fixed_price = $RewardFixedPrice
            })
        } else {
            $ErrorText = $Errors -join " | "
            $Sheet.Cells.Item($Row, 18).Value2 = "שגיאה"
            $Sheet.Cells.Item($Row, 19).Value2 = $ErrorText
            $Sheet.Cells.Item($Row, 18).Interior.Color = 13551615
            $Sheet.Cells.Item($Row, 19).Interior.Color = 13551615
            $InvalidRows.Add([ordered]@{ excel_row = $Row; errors = @($Errors) })
        }
    }

    $Sheet.Range("R:S").EntireColumn.AutoFit() | Out-Null
    if ($Sheet.Columns.Item(19).ColumnWidth -gt 60) { $Sheet.Columns.Item(19).ColumnWidth = 60 }
    $Sheet.Range("S$($HeaderRow + 1):S$LastRow").WrapText = $true
    $Workbook.Save()

    $ReportPath = Join-Path $BaseDir "validation_report.json"
    $Report = [ordered]@{
        generated_at = (Get-Date).ToString("o")
        shop_id = [int]$Catalog.shop_id
        shop_name = [string]$Catalog.shop_name
        excel_file = [System.IO.Path]::GetFileName($ExcelFile)
        valid_count = $ValidRows.Count
        invalid_count = $InvalidRows.Count
        invalid_rows = @($InvalidRows)
    }
    $Report | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $ReportPath -Encoding UTF8

    if ($InvalidRows.Count -eq 0) {
        $JsonPath = Join-Path $BaseDir "מבצעים_מאומתים.json"
        $Payload = [ordered]@{
            schema_version = 1
            generated_at = (Get-Date).ToString("o")
            shop_id = [int]$Catalog.shop_id
            shop_name = [string]$Catalog.shop_name
            source_file = [System.IO.Path]::GetFileName($ExcelFile)
            promotion_count = $ValidRows.Count
            promotions = @($ValidRows)
        }
        $Payload | ConvertTo-Json -Depth 12 | Set-Content -LiteralPath $JsonPath -Encoding UTF8
        Write-Host "הקובץ תקין. נמצאו $($ValidRows.Count) מבצעים."
        Write-Host "נוצר JSON מאומת: $JsonPath"
        exit 0
    }

    Write-Host "נמצאו $($InvalidRows.Count) שורות עם שגיאות."
    Write-Host "פתח את הקובץ ובדוק את העמודות סטטוס ושגיאות."
    Write-Host "דוח: $ReportPath"
    exit 1
}
finally {
    if ($null -ne $Workbook) {
        try { $Workbook.Close($false) } catch {}
    }
    if ($null -ne $Excel) {
        try { $Excel.Quit() } catch {}
    }
    Release-ComObject $Sheet
    Release-ComObject $Workbook
    Release-ComObject $Excel
    [GC]::Collect()
    [GC]::WaitForPendingFinalizers()
}
