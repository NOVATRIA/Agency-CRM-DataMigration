/**
 * DataMigration.js — Đọc dữ liệu từ Sheet nguồn → ghi thẳng vào CRM Database
 *
 * Entry point:  buildAll()       — chạy thủ công hoặc qua daily trigger
 *               setupTrigger()   — tạo trigger chạy mỗi ngày
 *               removeTrigger()  — xóa trigger
 *
 * Luồng:
 *   1. Đọc Master → lấy ID tất cả file CRM Database
 *   2. Đọc sheet nguồn (Topup, Đối chiếu, TK trong kho)
 *   3. So sánh với CRM Database → chỉ thêm dòng mới (dedup)
 *   4. Tính lại quỹ KH cho các KH có GD mới
 *   5. Ghi log + lỗi vào 1-Database/Logs/MigrationLogs
 *
 * Version: 2.0 — Config từ Script Properties + Google Sheet
 */

// ============================================================
// CẤU HÌNH
// ============================================================

// Đọc từ Script Properties (Apps Script → Project Settings → Script Properties)
// Cần set: MASTER_SS_ID, SOURCE_ID
var _props_ = PropertiesService.getScriptProperties();
var MASTER_SS_ID = _props_.getProperty('MASTER_SS_ID');
var SOURCE_ID    = _props_.getProperty('SOURCE_ID');
var TAB_TOPUP  = 'Topup';
var TAB_DOI_CHIEU = 'Đối chiếu T.chính';
var TAB_TK_KHO = 'TK trong kho';

// ── CẤU HÌNH NGÀY ──────────────────────────────────────────
var DATE_FROM      = new Date(2026, 0, 1);    // 01/01/2026 — lấy GD từ ngày này
var QUY_GOC_DATE   = new Date(2025, 11, 31);  // 31/12/2025 — quỹ gốc cố định
var KICKOFF_DATE   = new Date(2026, 2, 31);   // 31/03/2026 — ngày tạo GD cân bằng
var DOICHIEU_FROM  = new Date(2026, 2, 31);   // 31/03/2026 — verify Kick-Off + đối chiếu từ đây
// DATE_TO: tự tìm ngày cuối cùng có data trong "Tổng hợp"
// ────────────────────────────────────────────────────────────

// Mã KH đặc biệt = GD NCC → bỏ qua
var NCC_MA_KH = [
  'LLK-Bank nguồn', 'LLK-bank nguồn',
  'LLK-Nhập kho', 'LLK-nhập kho',
  'LLK-Nạp Quỹ', 'LLK-nạp quỹ'
];

var TZ = 'Asia/Bangkok';
var NAM = '2026';

// NCC_LIST đã chuyển sang quản lý trực tiếp trên Google Sheet (DanhMuc_NCC trong CRM)

// Validate ma_kh: phải đúng format "LLK-XXXXXX" (LLK- + 6 chữ số)
var MA_KH_REGEX = /^LLK-\d{6}$/;

// Mảng lưu lỗi trong quá trình xử lý
var _errors = [];

// Counter cho sinh mã GD (reset mỗi ngày mới)
var _maGdCounters = {}; // { 'GD-KH-20260315': 3, 'GD-NCC-20260315': 2 }

// ============================================================
// ĐỌC CRM CONFIG TỪ MASTER
// ============================================================

/**
 * Mở Master → đọc tab Spreadsheet_ID → trả về map { key: spreadsheet_id }
 */
function _loadCrmIds_() {
  var ss = SpreadsheetApp.openById(MASTER_SS_ID);
  var sheet = ss.getSheetByName('Spreadsheet_ID');
  if (!sheet) throw new Error('Không tìm thấy tab "Spreadsheet_ID" trong Master');

  var data = sheet.getDataRange().getValues();
  var map = {};
  for (var i = 1; i < data.length; i++) {
    if (data[i][0]) map[data[i][0].toString().trim()] = data[i][1].toString().trim();
  }
  Logger.log('CRM IDs loaded: ' + Object.keys(map).join(', '));
  return map;
}

/**
 * Mở CRM spreadsheet theo config key
 */
function _openCrm_(crmIds, key) {
  var id = crmIds[key];
  if (!id) throw new Error('Không tìm thấy config "' + key + '" trong Master/Spreadsheet_ID');
  return SpreadsheetApp.openById(id);
}

// ============================================================
// WEB APP — Admin actions via doGet
// ============================================================

function doGet(e) {
  var action = (e && e.parameter && e.parameter.action) || '';

  // Chạy từ editor (không có param) → resetAndRebuild
  if (!action) action = 'resetAndRebuild';

  try {
    if (action === 'resetAndRebuild') {
      resetAndRebuild();
      return ContentService.createTextOutput(JSON.stringify({ success: true, message: 'resetAndRebuild done.' }))
        .setMimeType(ContentService.MimeType.JSON);
    }
    if (action === 'buildAll') {
      buildAll();
      return ContentService.createTextOutput(JSON.stringify({ success: true, message: 'buildAll done.' }))
        .setMimeType(ContentService.MimeType.JSON);
    }
    return ContentService.createTextOutput(JSON.stringify({ error: 'Unknown action: ' + action }))
      .setMimeType(ContentService.MimeType.JSON);
  } catch (err) {
    return ContentService.createTextOutput(JSON.stringify({ error: err.message, stack: err.stack }))
      .setMimeType(ContentService.MimeType.JSON);
  }
}

// ============================================================
// MAIN
// ============================================================

function buildAll(skipDoiChieu) {
  var start = new Date();
  _errors = [];

  // Bước 1: Đọc CRM IDs từ Master
  var crmIds = _loadCrmIds_();

  // Bước 2: Đọc Config mapping nguồn → ma_ncc (từ file staging cũ, giờ đọc từ chính nó)
  var nccMap = _readNccMap();

  // Bước 3: Đọc dữ liệu từ sheet nguồn
  var topupRows = _readTopup();
  Logger.log('Topup rows sau lọc: ' + topupRows.length);

  var dcRows = _readDoiChieu();
  Logger.log('Đối chiếu rows sau lọc: ' + dcRows.length);

  var khoResult = _readTKTrongKho();
  var khoData = khoResult.list;
  var khoGroupMap = khoResult.groupMap;
  Logger.log('TK trong kho: ' + khoData.length + ' CID');

  var nccGDRows = _readDoiChieuNCC();

  var allRows = topupRows.concat(dcRows);

  // Bước 4: Sync vào CRM Database (chỉ thêm mới)
  var statsKH     = _syncKH(crmIds, allRows);
  var statsKho    = _syncKho(crmIds, allRows, nccMap, khoData);
  var statsGD     = _syncGDKH(crmIds, allRows, nccMap);
  var statsNCC    = _syncNCC(crmIds, nccGDRows, nccMap);
  var statsGDNCC  = _syncGDNCC(crmIds, nccGDRows, nccMap);

  // Bước 5: Import quỹ gốc KH + NCC
  _importQuyGoc(crmIds);
  var nccQuyGocMap = _importQuyGocNCC(crmIds, nccMap) || {};

  // Bước 6: Tính lại quỹ cho KH + NCC có GD mới
  if (statsGD.newMaKHs && statsGD.newMaKHs.length > 0) {
    _recomputeQuyKH(crmIds, statsGD.newMaKHs);
  }
  if (statsGDNCC.newMaNCCs && statsGDNCC.newMaNCCs.length > 0) {
    _recomputeQuyNCC(crmIds, statsGDNCC.newMaNCCs, nccQuyGocMap);
  }

  // Bước 7: Đối chiếu (bỏ qua khi gọi từ resetAndRebuild — sẽ chạy sau Kick-Off)
  if (!skipDoiChieu) {
    _doiChieuQuy(crmIds);
    _doiChieuQuyNCC(crmIds, nccMap);
  }

  // Bước 8: Ghi log vào 1-Database/Logs/MigrationLogs
  var elapsed = ((new Date() - start) / 1000).toFixed(1);
  _writeLog(crmIds, {
    topup_rows: topupRows.length,
    dc_rows: dcRows.length,
    kh_new: statsKH.added,
    kho_new: statsKho.added,
    gd_new: statsGD.added,
    ncc_new: statsNCC.added,
    gd_ncc_new: statsGDNCC.added,
    errors: _errors.length,
    elapsed: elapsed
  });

  // Bước 9: Ghi chi tiết lỗi (nếu có)
  _writeErrors();

  // Bước 10: Luôn gửi Telegram báo cáo tổng kết
  var icon = _errors.length > 0 ? '🚨' : '✅';
  _sendTelegram(icon + ' *DataMigration hoàn tất*\n\n'
    + 'KH: ' + statsKH.added + ' | Kho: ' + statsKho.added + '\n'
    + 'GD KH: ' + statsGD.added + ' | GD NCC: ' + statsGDNCC.added + '\n'
    + 'Errors: ' + _errors.length + '\n'
    + 'Thời gian: ' + elapsed + 's'
    + (_errors.length > 0 ? '\n\nXem chi tiết trong MigrationLogs/Error\\_Log' : ''));

  Logger.log('=== BUILD ALL DONE — ' + elapsed + 's, Errors: ' + _errors.length + ' ===');
}

// ============================================================
// RESET & REBUILD — Xoá GD + Kho + Đối Soát → sync lại từ đầu
// ============================================================

/**
 * Xoá toàn bộ data trong các sheet CRM rồi chạy buildAll() để sync lại từ nguồn.
 * Dùng khi kế toán đã sửa GD trên sheet nguồn (Topup / Đối chiếu) và cần CRM khớp lại.
 *
 * CẢNH BÁO: Hàm này xoá data — chỉ nên chạy thủ công, KHÔNG đặt trigger tự động.
 */
function resetAndRebuild() {
  var crmIds = _loadCrmIds_();

  Logger.log('=== RESET & REBUILD — Bắt đầu xoá data CRM ===');

  // 1. Xoá Kho_TaiKhoan (data rows)
  _clearSheetData_(crmIds, 'KHO_TK', 'Kho_TaiKhoan');

  // 2. Xoá DanhMuc_KH (data rows — tạo lại từ GD 2026, KH cũ không có GD sẽ bị loại)
  _clearSheetData_(crmIds, 'KHACH_HANG', 'DanhMuc_KH');

  // 3. Xoá GD_KhachHang (data rows — sync lại toàn bộ từ nguồn)
  _clearSheetData_(crmIds, 'GD_KH_' + NAM, 'GD_KhachHang');

  // 4. Xoá DoiSoat_GD (data rows)
  _clearSheetData_(crmIds, 'DOI_SOAT_' + NAM, 'DoiSoat_GD');

  // 5. Xoá GD_NhaCungCap (data rows — sync lại toàn bộ từ nguồn)
  _clearSheetData_(crmIds, 'GD_NCC_' + NAM, 'GD_NhaCungCap');

  // 6. DanhMuc_NCC: GIỮ nguyên danh sách, chỉ reset quy_hien_tai (cột L = 12) về 0
  var ssNCC = _openCrm_(crmIds, 'NHA_CUNG_CAP');
  var sheetNCC = ssNCC.getSheetByName('DanhMuc_NCC');
  if (sheetNCC && sheetNCC.getLastRow() > 1) {
    var rowsNCC = sheetNCC.getLastRow() - 1;
    var zerosNCC = [];
    for (var j = 0; j < rowsNCC; j++) zerosNCC.push([0]);
    sheetNCC.getRange(2, 12, rowsNCC, 1).setValues(zerosNCC); // cột L
    Logger.log('DanhMuc_NCC: reset quy_hien_tai cho ' + rowsNCC + ' NCC');
  }

  // 7. Clear MigrationLogs (Run_Log, Error_Log, Warning_Log)
  var logSS = _getLogSpreadsheet_();
  var logTabs = ['Run_Log', 'Error_Log', 'Warning_Log'];
  logTabs.forEach(function(tabName) {
    var logSheet = logSS.getSheetByName(tabName);
    if (logSheet && logSheet.getLastRow() > 1) {
      logSheet.getRange(2, 1, logSheet.getLastRow() - 1, logSheet.getLastColumn()).clearContent();
      if (logSheet.getLastRow() > 2) {
        try { logSheet.deleteRows(3, logSheet.getLastRow() - 2); } catch(e) {}
      }
      Logger.log('MigrationLogs/' + tabName + ': đã xoá');
    }
  });

  // 8. Reset mã GD counter
  _maGdCounters = {};

  Logger.log('=== RESET XONG — Bắt đầu buildAll() ===');

  // 9. Chạy buildAll() để sync lại từ nguồn (skip đối chiếu — chạy sau Kick-Off)
  buildAll(true);

  // 10. Tạo GD Kick-Off CRM (cân bằng quỹ tại KICKOFF_DATE)
  var crmIds2 = _loadCrmIds_();
  var nccMap2 = _readNccMap();
  var kickKH = _createKickOffKH(crmIds2);
  var kickNCC = _createKickOffNCC(crmIds2, nccMap2);

  // 11. Recompute lại sau Kick-Off
  if (kickKH > 0 || kickNCC > 0) {
    var nccQuyGocMap2 = _importQuyGocNCC(crmIds2, nccMap2) || {};
    var allKHs = [];
    var ssKH3 = _openCrm_(crmIds2, 'KHACH_HANG');
    var khD3 = ssKH3.getSheetByName('DanhMuc_KH').getDataRange().getValues();
    for (var k3 = 1; k3 < khD3.length; k3++) {
      var mk3 = (khD3[k3][0] || '').toString().trim();
      if (mk3) allKHs.push(mk3);
    }
    _recomputeQuyKH(crmIds2, allKHs);

    var allNCCs = [];
    var ssNCC3 = _openCrm_(crmIds2, 'NHA_CUNG_CAP');
    var nccD3 = ssNCC3.getSheetByName('DanhMuc_NCC').getDataRange().getValues();
    for (var n3 = 1; n3 < nccD3.length; n3++) {
      var mn3 = (nccD3[n3][0] || '').toString().trim();
      if (mn3) allNCCs.push(mn3);
    }
    _recomputeQuyNCC(crmIds2, allNCCs, nccQuyGocMap2);

    Logger.log('Recompute sau Kick-Off: ' + allKHs.length + ' KH, ' + allNCCs.length + ' NCC');
  }

  // 12. Đối chiếu SAU Kick-Off (quỹ đã cân bằng tại 31/03 → chỉ lệch từ 01/04 mới là lỗi thật)
  _doiChieuQuy(crmIds2);
  _doiChieuQuyNCC(crmIds2, nccMap2);
}

/**
 * Xoá tất cả data rows (giữ header) của 1 sheet
 */
function _clearSheetData_(crmIds, configKey, sheetName) {
  var ss = _openCrm_(crmIds, configKey);
  var sheet = ss.getSheetByName(sheetName);
  if (!sheet) { Logger.log('WARNING: Không tìm thấy ' + sheetName); return; }
  var lastRow = sheet.getLastRow();
  if (lastRow <= 1) { Logger.log(sheetName + ': đã trống'); return; }
  // Dùng clearContent thay deleteRows để tránh lỗi frozen rows
  sheet.getRange(2, 1, lastRow - 1, sheet.getLastColumn()).clearContent();
  // Xoá dòng thừa (giữ lại 1 dòng trống sau header để tránh lỗi)
  if (lastRow > 2) {
    try { sheet.deleteRows(3, lastRow - 2); } catch(e) { /* bỏ qua nếu không xoá được */ }
  }
  Logger.log(sheetName + ': xoá ' + (lastRow - 1) + ' dòng');
}


// ============================================================
// SINH MÃ GD — Format: {prefix}-YYYYMMDD-NNN
// ============================================================

/**
 * Khởi tạo counter từ GD hiện có trong CRM
 * Đọc cột ma_gd → tìm counter cao nhất cho mỗi ngày
 */
function _initCountersFromExisting_(sheet, prefix) {
  var data = sheet.getDataRange().getValues();
  // ma_gd ở cột A (index 0)
  var pattern = new RegExp('^' + prefix.replace(/-/g, '\\-') + '-(\\d{8})-(\\d{3})$');
  for (var i = 1; i < data.length; i++) {
    var maGd = (data[i][0] || '').toString().trim();
    var m = maGd.match(pattern);
    if (m) {
      var dayKey = prefix + '-' + m[1];
      var num = parseInt(m[2], 10);
      if (!_maGdCounters[dayKey] || num > _maGdCounters[dayKey]) {
        _maGdCounters[dayKey] = num;
      }
    }
  }
}

/**
 * Sinh mã GD: GD-KH-20260315-001, GD-NCC-20260315-002
 * @param {string} prefix - 'GD-KH' hoặc 'GD-NCC'
 * @param {Date} ngay - ngày GD (dùng để tạo phần YYYYMMDD)
 */
function _generateMaGD_(prefix, ngay) {
  var dateStr = Utilities.formatDate(ngay, TZ, 'yyyyMMdd');
  var dayKey = prefix + '-' + dateStr;
  var counter = (_maGdCounters[dayKey] || 0) + 1;
  _maGdCounters[dayKey] = counter;
  return prefix + '-' + dateStr + '-' + ('000' + counter).slice(-3);
}

// ============================================================
// SYNC KHO — Đọc toàn bộ CID từ "TK trong kho" + bổ sung CID từ GD
// ============================================================

/**
 * Mapping trạng thái từ sheet nguồn → CRM
 */

// ============================================================
// ĐỌC TK TRONG KHO → Danh sách đầy đủ CID + trạng thái + thông tin
// ============================================================

/**
 * Đọc toàn bộ CID từ tab "TK trong kho" (header dòng 7, data từ dòng 8)
 * Trả về { list: [...], groupMap: { cid: ten_group } }
 *   list: mảng object { cid, ten_group, tinh_trang, ma_kh, ngay_nhap, ngay_ban, dong }
 *   groupMap: dùng cho lookup NCC (tương thích code cũ)
 */

// ============================================================
// ĐỌC QUỸ GỐC TỪ TAB "Tổng hợp" + ĐỐI CHIẾU QUỸ HÀNG NGÀY
// ============================================================


/**
 * Tìm index cột có ngày khớp targetDate trong dòng header (dòng 2)
 * Trả về index (0-based) hoặc -1 nếu không tìm thấy
 */
function _findDateCol(headerRow, targetDate) {
  var targetTime = targetDate.getTime();
  for (var c = 6; c < headerRow.length; c++) {
    var val = headerRow[c];
    var d = null;
    if (val instanceof Date && !isNaN(val.getTime())) {
      d = val;
    } else if (val) {
      d = _parseDate(val.toString().trim());
    }
    if (d && d.getFullYear() === targetDate.getFullYear()
        && d.getMonth() === targetDate.getMonth()
        && d.getDate() === targetDate.getDate()) {
      return c;
    }
  }
  return -1;
}

/**
 * Tìm cột ngày cuối cùng có data trong header row
 * Trả về { col: index, date: Date } hoặc null
 */
function _findLastDateCol(headerRow, startCol) {
  startCol = startCol || 6;
  var lastCol = -1;
  var lastDate = null;
  for (var c = startCol; c < headerRow.length; c++) {
    var val = headerRow[c];
    var d = null;
    if (val instanceof Date && !isNaN(val.getTime())) {
      d = val;
    } else if (val) {
      d = _parseDate(val.toString().trim());
    }
    if (d) { lastCol = c; lastDate = d; }
  }
  return lastCol >= 0 ? { col: lastCol, date: lastDate } : null;
}

// ============================================================
// ĐỐI CHIẾU QUỸ NCC — Tab "Nguồn"
// ============================================================


// ============================================================
// LOG — Ghi vào file MigrationLogs trong 1-Database/Logs/
// ============================================================

/**
 * Tìm hoặc tạo file MigrationLogs trong 1-Database/Logs/
 * Dùng MASTER_SS_ID → tìm folder cha (1-Database) → tạo Logs/MigrationLogs
 */
var MIGRATION_LOGS_ID = '1McqcuerdYK89A3g6Afyfm4nIz3mZM8qhdEtTdeBw_iw';

function _getLogSpreadsheet_() {
  return SpreadsheetApp.openById(MIGRATION_LOGS_ID);
}

function _writeLog(crmIds, stats) {
  var ss = _getLogSpreadsheet_();
  var sheet = ss.getSheetByName('Run_Log');
  if (!sheet) {
    sheet = ss.insertSheet('Run_Log');
    sheet.getRange(1, 1, 1, 10).setValues([[
      'Thời gian', 'Topup rows', 'Đối chiếu rows',
      'KH mới', 'Kho mới', 'GD KH mới', 'NCC mới', 'GD NCC mới', 'Errors', 'Thời gian (s)'
    ]]);
    sheet.getRange(1, 1, 1, 10).setBackground('#4285F4').setFontColor('#FFFFFF').setFontWeight('bold');
    sheet.setFrozenRows(1);
    // Xóa Sheet1 mặc định nếu còn
    var defaultSheet = ss.getSheetByName('Sheet1');
    if (defaultSheet) ss.deleteSheet(defaultSheet);
  }

  sheet.appendRow([
    new Date(),
    stats.topup_rows,
    stats.dc_rows || 0,
    stats.kh_new,
    stats.kho_new,
    stats.gd_new,
    stats.ncc_new || 0,
    stats.gd_ncc_new || 0,
    stats.errors || 0,
    stats.elapsed
  ]);
}

/**
 * Ghi chi tiết lỗi vào tab Error_Log
 */
function _writeErrors() {
  if (!_errors || _errors.length === 0) return;

  var ss = _getLogSpreadsheet_();
  var sheet = ss.getSheetByName('Error_Log');
  if (!sheet) {
    sheet = ss.insertSheet('Error_Log');
    sheet.getRange(1, 1, 1, 6).setValues([[
      'Thời gian', 'Tab nguồn', 'Dòng', 'Loại lỗi', 'Giá trị', 'Ghi chú'
    ]]);
    sheet.getRange(1, 1, 1, 6).setBackground('#EA4335').setFontColor('#FFFFFF').setFontWeight('bold');
    sheet.setFrozenRows(1);
  }

  // Đọc lỗi đã có trong Error_Log để không ghi trùng giữa các lần chạy
  var seen = {};
  var existingData = sheet.getDataRange().getValues();
  for (var i = 1; i < existingData.length; i++) {
    var existKey = [
      existingData[i][1] || '',  // tab
      existingData[i][2] || '',  // dong
      existingData[i][4] || ''   // gia_tri
    ].join('|');
    seen[existKey] = true;
  }

  // Lọc: chỉ giữ lỗi chưa từng ghi
  var newErrors = [];
  _errors.forEach(function(e) {
    var key = [e.tab || '', e.dong || '', e.gia_tri || ''].join('|');
    if (!seen[key]) {
      seen[key] = true;
      newErrors.push(e);
    }
  });

  if (newErrors.length === 0) {
    Logger.log('Không có lỗi mới — bỏ qua ghi Error_Log');
    return;
  }

  var now = new Date();
  var rows = newErrors.map(function(e) {
    return [
      now,
      e.tab || '',
      e.dong || '',
      e.loai_loi || '',
      e.gia_tri || '',
      e.ghi_chu || ''
    ];
  });

  var lastRow = sheet.getLastRow();
  sheet.getRange(lastRow + 1, 1, rows.length, rows[0].length).setValues(rows);
  Logger.log('Đã ghi ' + newErrors.length + ' lỗi mới vào Error_Log (bỏ qua ' + (_errors.length - newErrors.length) + ' trùng)');
}

// ============================================================
// TRIGGER — Chạy mỗi ngày lúc 6:00 sáng
// ============================================================

function setupTrigger() {
  removeTrigger();
  ScriptApp.newTrigger('buildAll')
    .timeBased()
    .everyDays(1)
    .atHour(6)
    .nearMinute(0)
    .inTimezone(TZ)
    .create();
  Logger.log('Đã tạo trigger: buildAll chạy mỗi ngày lúc 6:00 AM (ICT)');
}

function removeTrigger() {
  ScriptApp.getProjectTriggers().forEach(function(t) {
    if (t.getHandlerFunction() === 'buildAll') {
      ScriptApp.deleteTrigger(t);
      Logger.log('Đã xóa trigger: ' + t.getUniqueId());
    }
  });
}

// ============================================================
// CẤU HÌNH TỪ MASTER — Tab CauHinh
// ============================================================

var _cauHinhCache = null;

function _loadCauHinh_() {
  if (_cauHinhCache) return _cauHinhCache;
  var ss = SpreadsheetApp.openById(MASTER_SS_ID);
  var sheet = ss.getSheetByName('CauHinh');
  if (!sheet) {
    Logger.log('WARNING: Không tìm thấy tab "CauHinh" trong Master');
    return {};
  }
  var data = sheet.getDataRange().getValues();
  var map = {};
  for (var i = 1; i < data.length; i++) {
    var key = (data[i][0] || '').toString().trim();
    var val = (data[i][1] || '').toString().trim();
    if (key) map[key] = val;
  }
  _cauHinhCache = map;
  return map;
}

function _readCauHinh(key) {
  var config = _loadCauHinh_();
  return config[key] || '';
}

// ============================================================
// TELEGRAM — Gửi cảnh báo qua Bot API
// ============================================================

function _sendTelegram(message) {
  var token = _readCauHinh('BOT_TOKEN');
  var chatId = _readCauHinh('CHAT_ID');
  if (!token || !chatId) {
    Logger.log('WARNING: Thiếu BOT_TOKEN hoặc CHAT_ID trong CauHinh — bỏ qua gửi Telegram');
    return;
  }

  // Chia tin nhắn theo dòng nếu > 4000 ký tự (Telegram giới hạn 4096)
  if (message.length <= 4000) {
    _sendTelegramRaw(token, chatId, message);
  } else {
    var lines = message.split('\n');
    var chunk = '';
    var part = 1;
    for (var i = 0; i < lines.length; i++) {
      if ((chunk + lines[i] + '\n').length > 3900 && chunk.length > 0) {
        _sendTelegramRaw(token, chatId, chunk.trim() + '\n\n_(phần ' + part + ')_');
        part++;
        chunk = '';
      }
      chunk += lines[i] + '\n';
    }
    if (chunk.trim()) {
      _sendTelegramRaw(token, chatId, chunk.trim() + (part > 1 ? '\n\n_(phần ' + part + ')_' : ''));
    }
  }
}

function _sendTelegramRaw(token, chatId, text) {
  try {
    UrlFetchApp.fetch('https://api.telegram.org/bot' + token + '/sendMessage', {
      method: 'post',
      contentType: 'application/json',
      payload: JSON.stringify({ chat_id: chatId, text: text, parse_mode: 'Markdown' }),
      muteHttpExceptions: true
    });
  } catch (e) {
    Logger.log('ERROR gửi Telegram: ' + e.message);
  }
}

// ============================================================
// TIỆN ÍCH
// ============================================================

/**
 * Auto-correct Mã KH:
 * - Xoá khoảng trắng thừa: "LLK- 122501" → "LLK-122501"
 * - Đổi prefix sai: "LLD-112503" → "LLK-112503"
 */
function _fixMaKH(maKH) {
  if (!maKH) return '';
  maKH = maKH.replace(/\s+/g, '');
  maKH = maKH.replace(/^LLD-/i, 'LLK-');
  return maKH;
}


function _parseCIDs(raw, rowNum, tab) {
  if (!raw || raw === '-') return [];
  var parts = raw.split(/[\s,\n]+/);
  var result = [];
  for (var i = 0; i < parts.length; i++) {
    var part = parts[i].trim();
    if (!part || part === '-') continue;

    var digits = part.replace(/[-\s]/g, '');

    if (/^\d+$/.test(digits) && digits.length >= 10 && digits.length % 10 === 0) {
      for (var j = 0; j < digits.length; j += 10) {
        var d = digits.substring(j, j + 10);
        result.push(d.substring(0, 3) + '-' + d.substring(3, 6) + '-' + d.substring(6));
      }
    } else if (/^\d{3}-\d{3}-\d{4}$/.test(part)) {
      result.push(part);
    } else {
      if (!/\d/.test(part)) continue;
      _errors.push({
        tab: tab || 'Đối chiếu',
        dong: rowNum || '',
        loai_loi: 'CID không hợp lệ',
        gia_tri: part,
        ghi_chu: 'Không thể tách thành CID hợp lệ (10 chữ số)'
      });
    }
  }
  return result;
}

function _parseDate(val) {
  if (!val) return null;
  if (val instanceof Date) return isNaN(val.getTime()) ? null : val;

  var s = val.toString().trim();
  if (!s) return null;

  // DD/MM/YYYY HH:mm:ss hoặc DD/MM/YYYY
  var m1 = s.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})(?:\s+(\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
  if (m1) {
    var d = new Date(parseInt(m1[3]), parseInt(m1[2]) - 1, parseInt(m1[1]),
      parseInt(m1[4] || 0), parseInt(m1[5] || 0), parseInt(m1[6] || 0));
    return isNaN(d.getTime()) ? null : d;
  }

  // YYYY-MM-DD HH:mm:ss hoặc YYYY-MM-DD
  var m2 = s.match(/^(\d{4})-(\d{2})-(\d{2})(?:[T\s](\d{1,2}):(\d{2})(?::(\d{2}))?)?/);
  if (m2) {
    var d2 = new Date(parseInt(m2[1]), parseInt(m2[2]) - 1, parseInt(m2[3]),
      parseInt(m2[4] || 0), parseInt(m2[5] || 0), parseInt(m2[6] || 0));
    return isNaN(d2.getTime()) ? null : d2;
  }

  return null;
}

function _formatCID(val) {
  if (!val) return '';
  var s = val.toString().trim();
  if (!s || s === '-') return '';

  var digits = s.replace(/[-\s]/g, '');
  if (/^\d{10}$/.test(digits)) {
    return digits.substring(0, 3) + '-' + digits.substring(3, 6) + '-' + digits.substring(6);
  }
  return s;
}

function _parseNumber(val) {
  if (typeof val === 'number') return val;
  if (!val) return 0;
  var s = val.toString().trim()
    .replace(/\$/g, '')
    .replace(/\s/g, '')
    .replace(/,/g, '.');
  var n = parseFloat(s);
  return isNaN(n) ? 0 : n;
}

// ============================================================
// RESET RIÊNG — Chạy KH hoặc NCC độc lập
// ============================================================

/**
 * Reset + rebuild chỉ KH (không ảnh hưởng NCC)
 */
function resetKH() {
  var crmIds = _loadCrmIds_();

  Logger.log('=== RESET KH ===');
  _clearSheetData_(crmIds, 'KHACH_HANG', 'DanhMuc_KH');
  _clearSheetData_(crmIds, 'GD_KH_' + NAM, 'GD_KhachHang');
  _clearSheetData_(crmIds, 'DOI_SOAT_' + NAM, 'DoiSoat_GD');
  _maGdCounters = {};

  // Sync KH
  var nccMap = _readNccMap();
  var topupRows = _readTopup();
  var dcRows = _readDoiChieu();
  var allRows = topupRows.concat(dcRows);

  _syncKH(crmIds, allRows);
  var statsGD = _syncGDKH(crmIds, allRows, nccMap);

  _importQuyGoc(crmIds);
  if (statsGD.newMaKHs && statsGD.newMaKHs.length > 0) {
    _recomputeQuyKH(crmIds, statsGD.newMaKHs);
  }

  // Kick-Off KH
  _createKickOffKH(crmIds);
  // Recompute lại
  var ssKH = _openCrm_(crmIds, 'KHACH_HANG');
  var khData = ssKH.getSheetByName('DanhMuc_KH').getDataRange().getValues();
  var allMaKHs = [];
  for (var i = 1; i < khData.length; i++) {
    var mk = (khData[i][0] || '').toString().trim();
    if (mk) allMaKHs.push(mk);
  }
  _recomputeQuyKH(crmIds, allMaKHs);

  // Đối chiếu KH
  _doiChieuQuy(crmIds);

  Logger.log('=== RESET KH DONE ===');
}

/**
 * Reset + rebuild chỉ NCC (không ảnh hưởng KH)
 */
function resetNCC() {
  var crmIds = _loadCrmIds_();
  var nccMap = _readNccMap();

  Logger.log('=== RESET NCC ===');
  _clearSheetData_(crmIds, 'GD_NCC_' + NAM, 'GD_NhaCungCap');
  // Reset quỹ NCC
  var ssNCC = _openCrm_(crmIds, 'NHA_CUNG_CAP');
  var sheetNCC = ssNCC.getSheetByName('DanhMuc_NCC');
  if (sheetNCC && sheetNCC.getLastRow() > 1) {
    var rows = sheetNCC.getLastRow() - 1;
    var zeros = [];
    for (var j = 0; j < rows; j++) zeros.push([0]);
    sheetNCC.getRange(2, 12, rows, 1).setValues(zeros);
  }
  _maGdCounters = {};

  // Sync NCC
  var dcRows = _readDoiChieu();
  var nccGDRows = _readDoiChieuNCC();
  _syncNCC(crmIds, nccGDRows, nccMap);
  var statsGDNCC = _syncGDNCC(crmIds, nccGDRows, nccMap);

  var nccQuyGocMap = _importQuyGocNCC(crmIds, nccMap) || {};
  if (statsGDNCC.newMaNCCs && statsGDNCC.newMaNCCs.length > 0) {
    _recomputeQuyNCC(crmIds, statsGDNCC.newMaNCCs, nccQuyGocMap);
  }

  // Kick-Off NCC
  _createKickOffNCC(crmIds, nccMap);
  // Recompute lại
  var nccData = sheetNCC.getDataRange().getValues();
  var allMaNCCs = [];
  for (var n = 1; n < nccData.length; n++) {
    var mn = (nccData[n][0] || '').toString().trim();
    if (mn) allMaNCCs.push(mn);
  }
  _recomputeQuyNCC(crmIds, allMaNCCs, _importQuyGocNCC(crmIds, nccMap) || {});

  // Đối chiếu NCC
  _doiChieuQuyNCC(crmIds, nccMap);

  Logger.log('=== RESET NCC DONE ===');
}
