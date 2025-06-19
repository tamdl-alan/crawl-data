async function clickLocationPreferencesIfPresent(page) {
  try {
    // 1. Chờ chính xác nút location preference xuất hiện
    await page.waitForSelector('button[data-qa="location_preference_menu_button"]', { visible: true, timeout: 5000 });

    // 2. Click nút
    await page.click('button[data-qa="location_preference_menu_button"]');
    
    console.log('🍪 Đã click nút "Location Preferences"');
  } catch (err) {
    console.log('⚠️ Không tìm thấy nút "Location Preferences" trong 5 giây — bỏ qua bước này');
  }
}



async function acceptCookiesIfPresent(page, textElm = 'Accept All Cookies', timeoutFind = 5000) {
  try {
    await page.waitForFunction(() => {
    return [...document.querySelectorAll('button')].some(
      btn => btn.innerText.trim().includes('Accept All Cookies')
    );
  }, { timeout: timeoutFind });

  // 2. Click nút đó
  await page.evaluate(() => {
    const buttons = [...document.querySelectorAll('button')];
    const acceptBtn = buttons.find(btn => btn.innerText.trim().includes('Accept All Cookies'));
    if (acceptBtn) acceptBtn.click();
  });

  console.log('🍪 Đã click nút ', 'Accept All Cookies');
} catch (err) {
  console.log('⚠️ Không tìm thấy nút ', 'Accept All Cookies' ,' trong 5 giây — bỏ qua bước này');
}
}



/**
 * Hover vào menu item có nội dung text chính xác, ví dụ "Account"
 * @param {object} page - Puppeteer Page instance
 * @param {string} targetText - Nội dung cần khớp chính xác để hover
 * @param {number} delayMs - Thời gian chờ sau hover (mặc định 500ms)
 * @param {string|null} screenshotPath - Nếu cung cấp, sẽ chụp hình sau khi hover
 */
async function hoverMenuItemByText(page, targetText, delayMs = 500, screenshotPath = null) {
  const menuItems = await page.$$('div[data-qa="menu_item"]');
  let hovered = false;

  for (const el of menuItems) {
    const text = await el.evaluate(el => el.textContent.trim());
    if (text === targetText) {
      await el.hover();
      await new Promise(resolve => setTimeout(resolve, delayMs));
      console.log(`🖱️ Hovered menu item: "${targetText}"`);

      if (screenshotPath) {
        await page.screenshot({ path: screenshotPath });
        console.log(`📸 Screenshot saved: ${screenshotPath}`);
      }

      hovered = true;
      break;
    }
  }

  if (!hovered) {
    console.warn(`⚠️ Không tìm thấy menu item với nội dung "${targetText}"`);
  }
}


  await page.evaluate(() => {
    console.log('🖱️ Hovering over Account menu');
  const dropdown = document.querySelector('div.HoverDropdownMenu__DropdownMenu-sc-172nfd7-1.jZlTFI');
  if (dropdown) {
    dropdown.style.visibility = 'visible';
    dropdown.style.opacity = '1'; // cần nếu trang dùng opacity để ẩn
    dropdown.style.pointerEvents = 'auto'; // để có thể click được
    }
  });