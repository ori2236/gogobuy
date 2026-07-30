$ErrorActionPreference = "Stop"

$BaseDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$ProductsFile = Join-Path $BaseDir "promotion_products.json"
if (-not (Test-Path -LiteralPath $ProductsFile)) {
    throw "לא נמצא הקובץ promotion_products.json בתיקייה."
}

$Catalog = Get-Content -LiteralPath $ProductsFile -Raw -Encoding UTF8 | ConvertFrom-Json
if (-not $Catalog.products -or $Catalog.products.Count -eq 0) {
    throw "קובץ המוצרים ריק."
}

$Timestamp = Get-Date -Format "yyyyMMdd_HHmmss"
$OutputFile = Join-Path $BaseDir ("מבצעים_{0}.xlsx" -f $Timestamp)
$Excel = $null
$Workbook = $null
$Sheet = $null

function Release-ComObject([object]$Object) {
    if ($null -ne $Object) {
        [void][System.Runtime.InteropServices.Marshal]::FinalReleaseComObject($Object)
    }
}

function Add-ListValidation($Range, [string]$Formula, [string]$Title, [string]$Message) {
    $Range.Validation.Delete()
    $Range.Validation.Add(3, 1, 1, $Formula)
    $Range.Validation.IgnoreBlank = $true
    $Range.Validation.InCellDropdown = $true
    $Range.Validation.ShowInput = $true
    $Range.Validation.InputTitle = $Title
    $Range.Validation.InputMessage = $Message
    $Range.Validation.ShowError = $true
    $Range.Validation.ErrorTitle = "ערך לא תקין"
    $Range.Validation.ErrorMessage = "יש לבחור ערך מהרשימה בלבד."
}

function Add-WholeNumberValidation($Range, [double]$Minimum, [double]$Maximum, [string]$Title, [string]$Message) {
    $Range.Validation.Delete()
    $Range.Validation.Add(1, 1, 1, $Minimum, $Maximum)
    $Range.Validation.IgnoreBlank = $true
    $Range.Validation.ShowInput = $true
    $Range.Validation.InputTitle = $Title
    $Range.Validation.InputMessage = $Message
    $Range.Validation.ShowError = $true
    $Range.Validation.ErrorTitle = "ערך לא תקין"
    $Range.Validation.ErrorMessage = "יש להזין מספר שלם בטווח המותר."
}

function Add-DecimalValidation($Range, [double]$Minimum, [double]$Maximum, [string]$Title, [string]$Message) {
    $Range.Validation.Delete()
    $Range.Validation.Add(2, 1, 1, $Minimum, $Maximum)
    $Range.Validation.IgnoreBlank = $true
    $Range.Validation.ShowInput = $true
    $Range.Validation.InputTitle = $Title
    $Range.Validation.InputMessage = $Message
    $Range.Validation.ShowError = $true
    $Range.Validation.ErrorTitle = "ערך לא תקין"
    $Range.Validation.ErrorMessage = "יש להזין מספר בטווח המותר."
}

try {
    $Excel = New-Object -ComObject Excel.Application
    $Excel.Visible = $false
    $Excel.DisplayAlerts = $false

    $Workbook = $Excel.Workbooks.Add()
    $Sheet = $Workbook.Worksheets.Item(1)
    $Sheet.Name = "מבצעים"
    try { $Excel.ActiveWindow.DisplayRightToLeft = $true } catch {}

    $Headers = @(
        "שם מבצע",
        "תיאור מבצע",
        "סוג מבצע",
        "מוצר",
        "מקסימום מימושים להזמנה",
        "מבצע יום השוק",
        "תאריך התחלה",
        "תאריך סוף",
        "אחוז הנחה",
        "מחיר קבוע",
        "כמות במבצע",
        "מחיר כולל במבצע",
        "סכום הנחה",
        "סכום סל מינימלי",
        "דמי משלוח במבצע",
        "כמות מתנה",
        "מחיר מיוחד למוצר",
        "סטטוס",
        "שגיאות"
    )

    $Types = @(
        "אחוז הנחה",
        "מחיר קבוע",
        "כמות בסכום",
        "הנחה בשקלים",
        "מבצע משלוח לפי סכום סל",
        "מתנה לפי סכום סל",
        "מחיר מיוחד למוצר לפי סכום סל"
    )

    $HeaderRow = 4
    $FirstDataRow = 5
    $LastDataRow = 1004

    $Sheet.Range("A1:S1").Merge()
    $Sheet.Range("A1").Value2 = "קובץ מבצעים - $($Catalog.shop_name)"
    $Sheet.Range("A1").Font.Bold = $true
    $Sheet.Range("A1").Font.Size = 18
    $Sheet.Range("A1").HorizontalAlignment = -4108
    $Sheet.Range("A1").Interior.Color = 10053171
    $Sheet.Range("A1").Font.Color = 16777215
    $Sheet.Rows.Item(1).RowHeight = 30

    $Sheet.Range("A2:S2").Merge()
    $Sheet.Range("A2").Value2 = "ממלאים שורה אחת לכל מבצע. צהוב = חובה לפי סוג המבצע, כחול = שדה אופציונלי, אפור = לא רלוונטי. לסיום יש להריץ את 02_validate_promotion_excel.bat."
    $Sheet.Range("A2").WrapText = $true
    $Sheet.Range("A2").HorizontalAlignment = -4108
    $Sheet.Range("A2").Interior.Color = 15921906
    $Sheet.Rows.Item(2).RowHeight = 38

    for ($i = 0; $i -lt $Headers.Count; $i++) {
        $Sheet.Cells.Item($HeaderRow, $i + 1).Value2 = $Headers[$i]
    }

    $HeaderRange = $Sheet.Range("A$HeaderRow:S$HeaderRow")
    $HeaderRange.Font.Bold = $true
    $HeaderRange.Font.Color = 16777215
    $HeaderRange.Interior.Color = 4473924
    $HeaderRange.HorizontalAlignment = -4108
    $HeaderRange.VerticalAlignment = -4108
    $HeaderRange.WrapText = $true
    $Sheet.Rows.Item($HeaderRow).RowHeight = 42

    $AllDataRange = $Sheet.Range("A$FirstDataRow:S$LastDataRow")
    $AllDataRange.Borders.LineStyle = 1
    $AllDataRange.Borders.Color = 14277081
    $AllDataRange.VerticalAlignment = -4160

    $RequiredColor = 13434828
    $OptionalColor = 15773696
    $InactiveColor = 15132390

    $Sheet.Range("A$FirstDataRow:A$LastDataRow").Interior.Color = $RequiredColor
    $Sheet.Range("B$FirstDataRow:B$LastDataRow").Interior.Color = $OptionalColor
    $Sheet.Range("C$FirstDataRow:C$LastDataRow").Interior.Color = $RequiredColor
    $Sheet.Range("D$FirstDataRow:D$LastDataRow").Interior.Color = $InactiveColor
    $Sheet.Range("E$FirstDataRow:E$LastDataRow").Interior.Color = $InactiveColor
    $Sheet.Range("F$FirstDataRow:H$LastDataRow").Interior.Color = $RequiredColor
    $Sheet.Range("I$FirstDataRow:Q$LastDataRow").Interior.Color = $InactiveColor
    $Sheet.Range("R$FirstDataRow:S$LastDataRow").Interior.Color = 16777215

    $Sheet.Range("R$FirstDataRow:R$LastDataRow").Value2 = "טרם נבדק"

    # Helper values on the same worksheet. These columns are hidden later.
    $Sheet.Range("U1").Value2 = "סוגי מבצעים"
    for ($i = 0; $i -lt $Types.Count; $i++) {
        $Sheet.Cells.Item($i + 2, 21).Value2 = $Types[$i]
    }
    $Sheet.Range("V1").Value2 = "כן לא"
    $Sheet.Range("V2").Value2 = "כן"
    $Sheet.Range("V3").Value2 = "לא"
    $Sheet.Range("W1").Value2 = "מוצרים"
    for ($i = 0; $i -lt $Catalog.products.Count; $i++) {
        $Product = $Catalog.products[$i]
        $Selector = if ($Product.selector) { [string]$Product.selector } else { "{0} [ID:{1}]" -f $Product.name, $Product.product_id }
        $Sheet.Cells.Item($i + 2, 23).Value2 = $Selector
    }

    Add-ListValidation $Sheet.Range("C$FirstDataRow:C$LastDataRow") "=`$U`$2:`$U`$8" "סוג מבצע" "בחר את סוג המבצע. השדות הצהובים ישתנו בהתאם."
    Add-ListValidation $Sheet.Range("D$FirstDataRow:D$LastDataRow") ("=`$W`$2:`$W`$" + ($Catalog.products.Count + 1)) "מוצר" "בחר מוצר מרשימת המוצרים של הסניף. ניתן להקליד חלק מהשם כדי לחפש בגרסאות Excel תומכות."
    Add-ListValidation $Sheet.Range("F$FirstDataRow:F$LastDataRow") "=`$V`$2:`$V`$3" "יום השוק" "בחר כן או לא."
    Add-WholeNumberValidation $Sheet.Range("E$FirstDataRow:E$LastDataRow") 1 100000 "מקסימום מימושים" "שדה אופציונלי. הזן מספר שלם חיובי."
    Add-DecimalValidation $Sheet.Range("I$FirstDataRow:I$LastDataRow") 0.01 100 "אחוז הנחה" "לדוגמה: 20"
    Add-DecimalValidation $Sheet.Range("J$FirstDataRow:J$LastDataRow") 0 1000000 "מחיר קבוע" "לדוגמה: 5.90"
    Add-WholeNumberValidation $Sheet.Range("K$FirstDataRow:K$LastDataRow") 2 100000 "כמות במבצע" "לדוגמה: 2"
    Add-DecimalValidation $Sheet.Range("L$FirstDataRow:L$LastDataRow") 0 1000000 "מחיר כולל" "לדוגמה: 9.90"
    Add-DecimalValidation $Sheet.Range("M$FirstDataRow:M$LastDataRow") 0.01 1000000 "סכום הנחה" "לדוגמה: 5"
    Add-DecimalValidation $Sheet.Range("N$FirstDataRow:N$LastDataRow") 0 1000000 "סכום סל מינימלי" "לדוגמה: 300"
    Add-DecimalValidation $Sheet.Range("O$FirstDataRow:O$LastDataRow") 0 1000000 "דמי משלוח" "לדוגמה: 10. עבור משלוח חינם הזן 0."
    Add-WholeNumberValidation $Sheet.Range("P$FirstDataRow:P$LastDataRow") 1 100000 "כמות מתנה" "לדוגמה: 1"
    Add-DecimalValidation $Sheet.Range("Q$FirstDataRow:Q$LastDataRow") 0 1000000 "מחיר מיוחד" "לדוגמה: 5"

    $DateRange = $Sheet.Range("G$FirstDataRow:H$LastDataRow")
    $DateRange.NumberFormat = "dd/mm/yyyy"
    $DateRange.Validation.Delete()
    $DateRange.Validation.Add(4, 1, 1, ([datetime]"2020-01-01").ToOADate(), ([datetime]"2100-12-31").ToOADate())
    $DateRange.Validation.IgnoreBlank = $true
    $DateRange.Validation.ShowError = $true
    $DateRange.Validation.ErrorTitle = "תאריך לא תקין"
    $DateRange.Validation.ErrorMessage = "יש להזין תאריך תקין."

    # Conditional formatting highlights only the fields required by the selected type.
    $ProductRange = $Sheet.Range("D$FirstDataRow:D$LastDataRow")
    $ProductCondition = $ProductRange.FormatConditions.Add(2, 0, '=AND($C5<>"",$C5<>"מבצע משלוח לפי סכום סל")')
    $ProductCondition.Interior.Color = $RequiredColor
    Release-ComObject $ProductCondition
    Release-ComObject $ProductRange

    $MaxRange = $Sheet.Range("E$FirstDataRow:E$LastDataRow")
    $MaxCondition = $MaxRange.FormatConditions.Add(2, 0, '=OR($C5="אחוז הנחה",$C5="מחיר קבוע",$C5="כמות בסכום",$C5="הנחה בשקלים",$C5="מחיר מיוחד למוצר לפי סכום סל")')
    $MaxCondition.Interior.Color = $OptionalColor
    Release-ComObject $MaxCondition
    Release-ComObject $MaxRange

    $ConditionalRules = @(
        @{ Range = "I$FirstDataRow:I$LastDataRow"; Formula = '=$C5="אחוז הנחה"' },
        @{ Range = "J$FirstDataRow:J$LastDataRow"; Formula = '=$C5="מחיר קבוע"' },
        @{ Range = "K$FirstDataRow:L$LastDataRow"; Formula = '=$C5="כמות בסכום"' },
        @{ Range = "M$FirstDataRow:M$LastDataRow"; Formula = '=$C5="הנחה בשקלים"' },
        @{ Range = "N$FirstDataRow:O$LastDataRow"; Formula = '=$C5="מבצע משלוח לפי סכום סל"' },
        @{ Range = "N$FirstDataRow:N$LastDataRow"; Formula = '=OR($C5="מתנה לפי סכום סל",$C5="מחיר מיוחד למוצר לפי סכום סל")' },
        @{ Range = "P$FirstDataRow:P$LastDataRow"; Formula = '=$C5="מתנה לפי סכום סל"' },
        @{ Range = "Q$FirstDataRow:Q$LastDataRow"; Formula = '=$C5="מחיר מיוחד למוצר לפי סכום סל"' }
    )
    foreach ($Rule in $ConditionalRules) {
        $Target = $Sheet.Range($Rule.Range)
        $Condition = $Target.FormatConditions.Add(2, 0, $Rule.Formula)
        $Condition.Interior.Color = $RequiredColor
        Release-ComObject $Condition
        Release-ComObject $Target
    }

    $Sheet.Range("A$HeaderRow:S$LastDataRow").AutoFilter() | Out-Null
    $Sheet.Application.ActiveWindow.SplitRow = $HeaderRow
    $Sheet.Application.ActiveWindow.FreezePanes = $true

    $Widths = @{
        "A" = 28; "B" = 32; "C" = 34; "D" = 50; "E" = 18; "F" = 15;
        "G" = 14; "H" = 14; "I" = 13; "J" = 13; "K" = 13; "L" = 16;
        "M" = 13; "N" = 16; "O" = 16; "P" = 13; "Q" = 17; "R" = 13; "S" = 52
    }
    foreach ($Column in $Widths.Keys) {
        $Sheet.Range("${Column}:${Column}").EntireColumn.ColumnWidth = $Widths[$Column]
    }
    $Sheet.Range("A$FirstDataRow:S$LastDataRow").WrapText = $true
    $Sheet.Range("I$FirstDataRow:Q$LastDataRow").NumberFormat = "0.00"
    $Sheet.Range("K$FirstDataRow:K$LastDataRow").NumberFormat = "0"
    $Sheet.Range("P$FirstDataRow:P$LastDataRow").NumberFormat = "0"
    $Sheet.Range("E$FirstDataRow:E$LastDataRow").NumberFormat = "0"

    $Sheet.Range("U:W").EntireColumn.Hidden = $true
    $Workbook.SaveAs($OutputFile, 51)
    Write-Host "נוצר קובץ: $OutputFile"
    Write-Host "מספר מוצרים ברשימה: $($Catalog.products.Count)"
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
