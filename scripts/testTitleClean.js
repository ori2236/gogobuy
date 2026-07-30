const titles = [
  'תבניות אלומניום "משי" 4 ב- 18',
  'כפפות אלסטיות שחורות 3 ב- 9.90',
  'שלוקים 2 ב-18',
  'ירק ביכורי שדה',
  'ירק מהדרין ביכורי שדה 3 ב- 12.90',
  'נייר גלגול ריזלה 5 ב10',
  'פילטרים 3 ב10',
  'טיטולים בייביסיטר 3 ב- 84.90',
  'גלידת בן&ג\'ריס',
];

function cleanPromotionTitle(title) {
  if (!title) return "";
  return String(title)
    .replace(/\s*\d+\s*ב-?\s*\d+(\.\d+)?\s*$/gi, "")
    .trim();
}

for (const t of titles) {
  console.log(`Original: "${t}" => Cleaned: "${cleanPromotionTitle(t)}"`);
}
