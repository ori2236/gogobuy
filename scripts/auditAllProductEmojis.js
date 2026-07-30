require("dotenv").config();
const db = require("../config/db");

async function run() {
  console.log("====================================================");
  console.log("🔍 Detailed Emoji Analysis for Shop 2 & All Active Products");
  console.log("====================================================\n");

  const [products] = await db.query(`
    SELECT id, shop_id, name, category, emoji
    FROM product
    WHERE shop_id = 2
    ORDER BY id ASC
  `);

  console.log(`Auditing ${products.length} products for Shop 2...\n`);

  const recommendations = [];

  for (const p of products) {
    const name = String(p.name || "").trim();
    const currentEmoji = p.emoji ? String(p.emoji).trim() : "ללא (NULL)";

    let suggested = null;
    let category = null;

    // 1. Eggs (explicitly eggs, not noodles/chocolate/tools/bread)
    if (/ביצים|ביצה/i.test(name) && !/אטריות|נודלס|קינדר|פורס|סנדביצ|לחמני/i.test(name)) {
      suggested = "🥚";
      category = "ביצים";
    }
    // 2. Milk (pure milk or milk substitute)
    else if (/^חלב\b|\bחלב\b/i.test(name) && !/שוקולד|שוקולית|עוגיות|מילקי|מעדן/i.test(name)) {
      suggested = "🥛";
      category = "חלב";
    }
    // 3. Cheeses & Cream
    else if (/גבינה|קוטג'|מוצרלה|צהובה|בולגרית|פטה|שמנת|ריקוטה/i.test(name) && !/בורקס|מאפה|עוגת/i.test(name)) {
      suggested = "🧀";
      category = "גבינות ושמנת";
    }
    // 4. Butter
    else if (/חמאה/i.test(name) && !/עוגיות|ביסקוויט/i.test(name)) {
      suggested = "🧈";
      category = "חמאה";
    }
    // 5. Fresh Meat / Beef
    else if (/בשר בקר|אנטריקוט|סטייק|בקר טחון|צלעות בקר|צלי בקר/i.test(name)) {
      suggested = "🥩";
      category = "בשר בקר";
    }
    // 6. Chicken / Poultry
    else if (/חזה עוף|פרגית|פרגיות|כנפיים|כרעיים|שוקיים|עוף שלם|כרעי עוף/i.test(name)) {
      suggested = "🍗";
      category = "עוף";
    }
    // 7. Sausages / Hot Dogs / Deli Meats
    else if (/נקניקיות|נקניק|סלמי|פסטרמה|קבנוס/i.test(name)) {
      suggested = "🌭";
      category = "נקניקים";
    }
    // 8. Fish
    else if (/פילה סלמון|דג סלמון|טונה|פילה אמנון|דג דניס|נסיכת הנילוס|פילה דג/i.test(name) && !/שימורי טונה/i.test(name)) {
      suggested = "🐟";
      category = "דגים";
    }
    // 9. Breads & Challah
    else if (/לחם|חלה|חלות/i.test(name) && !/לחמניה|לחמניות|פיתה|קראנצ'/i.test(name)) {
      suggested = "🍞";
      category = "לחם וחלה";
    }
    // 10. Buns & Bagels
    else if (/לחמניה|לחמניות|בייגל/i.test(name)) {
      suggested = "🥯";
      category = "לחמניות";
    }
    // 11. Pita & Lafa
    else if (/פיתה|פיתות|לאפה|סלוף/i.test(name)) {
      suggested = "🫓";
      category = "פיתות";
    }
    // 12. Baguette
    else if (/בגט|באגט/i.test(name)) {
      suggested = "🥖";
      category = "בגטים";
    }
    // 13. Cakes
    else if (/עוגת|עוגה|קראנצ'|פס עוגה/i.test(name) && !/עוגיות|עוגיה/i.test(name)) {
      suggested = "🍰";
      category = "עוגות";
    }
    // 14. Cookies & Biscuits
    else if (/עוגיות|עוגיה|ביסקוויט|ביסקויט|וופל|ואפל/i.test(name)) {
      suggested = "🍪";
      category = "עוגיות";
    }
    // 15. Croissants & Pastries
    else if (/קרואסון|רוגעלך|דניש|מאפה שוקולד/i.test(name)) {
      suggested = "🥐";
      category = "מאפים";
    }
    // 16. Tomatoes
    else if (/עגבניה|עגבנייה|עגבניות|שרי/i.test(name) && !/רסק|רוטב/i.test(name)) {
      suggested = "🍅";
      category = "עגבניות";
    }
    // 17. Cucumbers
    else if (/מלפפון|מלפפונים/i.test(name) && !/חמוצים|במלח|בחומץ/i.test(name)) {
      suggested = "🥒";
      category = "מלפפונים";
    }
    // 18. Onions
    else if (/בצל יבש|בצל סגול|בצל לבן|בצלים/i.test(name)) {
      suggested = "🧅";
      category = "בצלים";
    }
    // 19. Potatoes & Sweet Potatoes
    else if (/תפוח אדמה|תפוחי אדמה|בטטה|בטטות/i.test(name) && !/צ'יפס/i.test(name)) {
      suggested = "🥔";
      category = "תפוחי אדמה";
    }
    // 20. Carrots
    else if (/גזר גמדי|גזר ארוז|גזר בתפזורת/i.test(name)) {
      suggested = "🥕";
      category = "גזר";
    }
    // 21. Garlic
    else if (/שום יבש|שום טרי|ראש שום/i.test(name)) {
      suggested = "🧄";
      category = "שום";
    }
    // 22. Watermelon
    else if (/אבטיח/i.test(name)) {
      suggested = "🍉";
      category = "אבטיח";
    }
    // 23. Bananas
    else if (/בננה|בננות/i.test(name)) {
      suggested = "🍌";
      category = "בננות";
    }
    // 24. Apples
    else if (/תפוח עץ|תפוח פינק|תפוח גרני|תפוח חרמון|תפוח זהוב/i.test(name)) {
      suggested = "🍎";
      category = "תפוחים";
    }
    // 25. Grapes
    else if (/ענבים/i.test(name)) {
      suggested = "🍇";
      category = "ענבים";
    }
    // 26. Lemons
    else if (/לימון/i.test(name) && !/משקה|מיץ|ספרינג|פריגת/i.test(name)) {
      suggested = "🍋";
      category = "לימון";
    }
    // 27. Mushrooms
    else if (/פטריות|פטריה|פורטובלו|שמפיניון/i.test(name) && !/שימורי/i.test(name)) {
      suggested = "🍄";
      category = "פטריות";
    }
    // 28. Avocado
    else if (/אבוקדו/i.test(name)) {
      suggested = "🥑";
      category = "אבוקדו";
    }
    // 29. Ice Cream Tub / Pint
    else if (/גלידת|גלידה|קרמסימו|בן&ג'ריס|בן וג'ריס/i.test(name)) {
      suggested = "🍨";
      category = "גלידות";
    }
    // 30. Ice Cream Popsicle / Cone
    else if (/טילון|טילונים|ארטיק|שלגון|מגנום/i.test(name)) {
      suggested = "🍦";
      category = "ארטיקים וטילונים";
    }
    // 31. Chocolates
    else if (/חפיסת שוקולד|שוקולד פרה|פסק זמן|מקופלת|טורטית|טוויסט|קליק|מרס|סניקרס|באונטי|קינדר בואנו|קינדר אצבעות/i.test(name)) {
      suggested = "🍫";
      category = "שוקולד";
    }
    // 32. Snacks (Bamba, Bisli, Chips)
    else if (/במבה|ביסלי|תפוצ'יפס|דורטוס|אפרופו|קראנץ' חטיף|פופקורן/i.test(name)) {
      suggested = "🍿";
      category = "חטיפים";
    }
    // 33. Candies & Gum
    else if (/סוכריות|סוכריה|מסטיק|סוכריות גומי/i.test(name)) {
      suggested = "🍬";
      category = "סוכריות ומסטיקים";
    }
    // 34. Nuts & Seeds
    else if (/גרעינים|פיסטוק|קשיו|שקדים|אגוזי מלך|פיצוחים/i.test(name)) {
      suggested = "🥜";
      category = "פיצוחים";
    }
    // 35. Soft drinks / Soda / Energy
    else if (/קוקה קולה|קולה זירו|ספרייט|פנטה|בלו 250|אקסל 250|XL 250|XL אנרגיה|מונסטר|רד בול/i.test(name)) {
      suggested = "🥤";
      category = "שתייה קלה";
    }
    // 36. Juices
    else if (/מיץ תפוזים|מיץ תפוחים|נקטר|פריגת 1\.5|ספרינג 1\.5/i.test(name)) {
      suggested = "🧃";
      category = "מיצים";
    }
    // 37. Water
    else if (/מים מינרליים|מי עדן|נביעות|סן פלגרינו|סודה 1\.5/i.test(name)) {
      suggested = "💧";
      category = "מים";
    }
    // 38. Coffee & Tea
    else if (/קפה שחור|נס קפה|קפסולות קפה|קפה טורקי|תה ויסוצקי/i.test(name)) {
      suggested = "☕";
      category = "קפה ותה";
    }
    // 39. Beer
    else if (/בירה גולדסטאר|בירה היינכן|בירה קרלסברג|בירה טובורג|בירה קורונה|בירה 330/i.test(name)) {
      suggested = "🍺";
      category = "בירה";
    }
    // 40. Wine
    else if (/יין אדום|יין לבן|יין קברנה|יין מרלו|יין שרדונה|יין הר חרמון/i.test(name)) {
      suggested = "🍷";
      category = "יין";
    }
    // 41. Hard Alcohol
    else if (/וודקה אבסולוט|וודקה פינלנדיה|ערק איילות|עראק|וויסקי|ויסקי/i.test(name)) {
      suggested = "🥃";
      category = "אלכוהול חריף";
    }
    // 42. Toilet Paper / Tissues
    else if (/נייר טואלט|טישו בקופסה|גלילי נייר/i.test(name)) {
      suggested = "🧻";
      category = "נייר וטישו";
    }
    // 43. Wipes
    else if (/מגבונים לחים|מגבוני תינוקות/i.test(name)) {
      suggested = "👶";
      category = "מגבונים";
    }
    // 44. Diapers
    else if (/חיתולים|חיתולי|טיטולים/i.test(name)) {
      suggested = "🧷";
      category = "חיתולים";
    }
    // 45. Soap
    else if (/סבון ידיים|סבון נוזלי|סבון מוצק/i.test(name)) {
      suggested = "🧼";
      category = "סבון";
    }
    // 46. Shampoo & Conditioner
    else if (/שמפו|מרכך שיער|תחליב רחצה/i.test(name)) {
      suggested = "🧴";
      category = "שמפו ורחצה";
    }
    // 47. Laundry
    else if (/אבקת כביסה|מרכך כביסה|ג'ל כביסה/i.test(name)) {
      suggested = "🧺";
      category = "כביסה";
    }

    if (suggested && currentEmoji !== suggested) {
      recommendations.push({
        id: p.id,
        shop_id: p.shop_id,
        name,
        category: p.category || category,
        ruleCategory: category,
        currentEmoji,
        suggestedEmoji: suggested
      });
    }
  }

  console.log(`Found ${recommendations.length} accurate emoji changes for Shop 2!\n`);

  const byRuleCat = {};
  for (const item of recommendations) {
    if (!byRuleCat[item.ruleCategory]) byRuleCat[item.ruleCategory] = [];
    byRuleCat[item.ruleCategory].push(item);
  }

  for (const cat in byRuleCat) {
    console.log(`=== קטגוריה: ${cat} (${byRuleCat[cat].length} מוצרים) ===`);
    for (const item of byRuleCat[cat]) {
      console.log(`• ID #${item.id} | "${item.name}" | לפני: ${item.currentEmoji} ➔ מוצע: ${item.suggestedEmoji}`);
    }
    console.log("");
  }

  process.exit(0);
}

run().catch(err => {
  console.error("Error:", err);
  process.exit(1);
});
