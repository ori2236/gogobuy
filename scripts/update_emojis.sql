-- SQL Script to update product emojis in MySQL database

-- 1. Garlic & Vegetables (שום וירקות)
UPDATE product SET emoji = '🧄' WHERE id IN (40356, 40357, 40358, 37743);
UPDATE product SET emoji = '🥔' WHERE id IN (35143, 39985, 39986);
UPDATE product SET emoji = '🧅' WHERE id IN (35131, 35205);

-- 2. Cucumbers (מלפפונים טריים)
UPDATE product SET emoji = '🥒' WHERE id IN (35906, 39549, 40152, 40153, 40740, 40469, 40594);

-- 3. Peppers & Gamba (פלפלים וגמבה)
UPDATE product SET emoji = '🫑' WHERE id IN (35134, 35255, 40315, 40744, 40758, 40469, 40494, 40511, 40619, 40636);

-- 4. Corn (תירס קלחים)
UPDATE product SET emoji = '🌽' WHERE id IN (35458, 36141, 39502);

-- 5. Watermelon (אבטיח)
UPDATE product SET emoji = '🍉' WHERE id = 40596;

-- 6. Baby Wipes (מגבונים לחים)
UPDATE product SET emoji = '👶' WHERE id IN (460, 461, 462, 37228, 37348, 38657);

-- 7. Bakery & Pretzels (מאפים, בגטים ובייגלה)
UPDATE product SET emoji = '🥖' WHERE id = 39975;
UPDATE product SET emoji = '🥨' WHERE id IN (36684, 36702, 37551, 37570, 37797, 37826);

-- 8. Water & Cones (מים, ארטיקים וגלידה)
UPDATE product SET emoji = '💧' WHERE id IN (35741, 35792, 38091);
UPDATE product SET emoji = '🍦' WHERE id IN (12517, 21492);
UPDATE product SET emoji = '🍨' WHERE id = 38477;

-- 9. Shower Gels & Toiletries (תחליבי רחצה)
UPDATE product SET emoji = '🧴' WHERE id IN (37578, 37680, 38315, 39064);

-- 10. Fish & Tuna (דגים ושימורים)
UPDATE product SET emoji = '🐟' WHERE id IN (37378, 37626, 38564, 39288);
