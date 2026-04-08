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
var DOICHIEU_FROM  = new Date(2026, 3, 1);    // 01/04/2026 — đối chiếu từng ngày từ đây
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

function buildAll() {
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

  // Bước 7: Đối chiếu quỹ CRM vs Kế Toán (ghi Warning_Log + Telegram nếu lệch)
  _doiChieuQuy(crmIds);
  _doiChieuQuyNCC(crmIds, nccMap);

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

  // Bước 10: Gửi Telegram nếu có lỗi
  if (_errors.length > 0) {
    _sendTelegram('🚨 *DataMigration có lỗi*\n\n'
      + 'Errors: ' + _errors.length + '\n'
      + 'KH mới: ' + statsKH.added + ', Kho mới: ' + statsKho.added + '\n'
      + 'GD KH mới: ' + statsGD.added + ', GD NCC mới: ' + statsGDNCC.added + '\n'
      + 'Thời gian: ' + elapsed + 's\n\n'
      + 'Xem chi tiết trong MigrationLogs/Error\\_Log');
  }

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

  // 9. Chạy buildAll() để sync lại từ nguồn
  buildAll();

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
// SYNC KH — So sánh + append KH mới vào DanhMuc_KH
// ============================================================

function _syncKH(crmIds, allRows) {
  var ss = _openCrm_(crmIds, 'KHACH_HANG');
  var sheet = ss.getSheetByName('DanhMuc_KH');
  if (!sheet) throw new Error('Không tìm thấy tab DanhMuc_KH');

  // Đọc ma_kh hiện có (cột A)
  var existing = {};
  var data = sheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) {
    var mk = (data[i][0] || '').toString().trim();
    if (mk) existing[mk] = true;
  }

  // Tìm KH mới từ source
  var khMap = {};
  allRows.forEach(function(r) {
    if (!MA_KH_REGEX.test(r.ma_kh)) return;
    if (existing[r.ma_kh]) return; // đã có trong CRM
    if (!khMap[r.ma_kh]) {
      khMap[r.ma_kh] = { ten_kh: r.ten_kh || '', ngay_dau: r.ngay };
    } else if (r.ngay < khMap[r.ma_kh].ngay_dau) {
      khMap[r.ma_kh].ngay_dau = r.ngay;
    }
  });

  // Append KH mới
  var newRows = [];
  var maKHs = Object.keys(khMap).sort();
  maKHs.forEach(function(mk) {
    var info = khMap[mk];
    newRows.push([
      mk,              // ma_kh
      info.ten_kh,     // ten_kh
      '',              // email
      0,               // quy_hien_tai (recompute tính)
      0,               // quy_goc
      0,               // so_cid
      'Hoat_dong',     // trang_thai
      '',              // ghi_chu
      info.ngay_dau,   // ngay_tao
      new Date()       // ngay_cap_nhat
    ]);
  });

  if (newRows.length > 0) {
    var lastRow = sheet.getLastRow();
    sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
  }

  Logger.log('SyncKH: ' + newRows.length + ' KH mới (đã có: ' + Object.keys(existing).length + ')');
  return { added: newRows.length, existing: Object.keys(existing).length };
}

// ============================================================
// SYNC KHO — Đọc toàn bộ CID từ "TK trong kho" + bổ sung CID từ GD
// ============================================================

/**
 * Mapping trạng thái từ sheet nguồn → CRM
 */
var _TRANG_THAI_MAP = {
  'chưa bán': 'Chua_ban',
  'chua ban':  'Chua_ban',
  'đã bán':   'Da_ban',
  'da ban':    'Da_ban',
  'huỷ':       'Huy',
  'hủy':       'Huy',
  'huy':       'Huy',
  'bảo hành':  'Bao_hanh',
  'bao hanh':  'Bao_hanh'
};

function _mapTrangThai(raw, maKH) {
  if (!raw) return maKH ? 'Da_ban' : 'Chua_ban';
  var key = raw.toString().trim().toLowerCase();
  var mapped = _TRANG_THAI_MAP[key];
  if (mapped) return mapped;
  // Giá trị lạ → log lỗi, mặc định dựa vào Mã KH
  return null;
}

function _syncKho(crmIds, allRows, nccMap, khoData) {
  var ss = _openCrm_(crmIds, 'KHO_TK');
  var sheet = ss.getSheetByName('Kho_TaiKhoan');
  if (!sheet) throw new Error('Không tìm thấy tab Kho_TaiKhoan');

  var CID_REGEX = /^\d{3}-\d{3}-\d{4}$/;

  // Đọc CID hiện có (cột A)
  var existing = {};
  var data = sheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) {
    var cid = (data[i][0] || '').toString().trim();
    if (cid) existing[cid] = true;
  }

  var newRows = [];

  // ── Nguồn 1: CID từ "TK trong kho" (chỉ từ 01/01/2026) ──
  khoData.forEach(function(tk) {
    if (existing[tk.cid]) return; // đã có trong CRM
    // Chỉ lấy CID nhập từ 01/01/2026
    if (tk.ngay_nhap && tk.ngay_nhap instanceof Date && tk.ngay_nhap < DATE_FROM) return;

    var maNcc = _lookupNcc(nccMap, tk.ten_group);
    if (!maNcc && tk.ten_group) {
      _errors.push({
        tab: 'TK trong kho', dong: tk.dong,
        loai_loi: 'Thiếu Tên Group', gia_tri: tk.ten_group,
        ghi_chu: 'Không tìm được mã NCC cho Tên Group này'
      });
    }

    var trangThai = _mapTrangThai(tk.tinh_trang, tk.ma_kh);
    if (trangThai === null) {
      _errors.push({
        tab: 'TK trong kho', dong: tk.dong,
        loai_loi: 'Tình trạng lạ', gia_tri: tk.tinh_trang,
        ghi_chu: 'Không nằm trong danh sách map: Chưa bán / Huỷ / Bảo hành'
      });
      trangThai = tk.ma_kh ? 'Da_ban' : 'Chua_ban';
    }

    existing[tk.cid] = true;
    newRows.push([
      tk.cid,           // cid
      maNcc,            // ma_ncc
      tk.ma_kh || '',   // ma_kh
      trangThai,        // trang_thai
      tk.ngay_nhap || '',  // ngay_nhap
      tk.ngay_ban || '',   // ngay_ban
      '',               // ghi_chu
      new Date()        // ngay_cap_nhat
    ]);
  });

  // ── Nguồn 2: CID từ GD (Topup + Đối chiếu) chưa có trong "TK trong kho" ──
  allRows.forEach(function(r) {
    if (!r.cid || r.cid === '-') return;
    if (!MA_KH_REGEX.test(r.ma_kh)) return;
    if (!CID_REGEX.test(r.cid)) return;
    if (existing[r.cid]) return;

    var maNcc = _lookupNcc(nccMap, r.nhom_nguon || '');
    existing[r.cid] = true;
    newRows.push([
      r.cid,          // cid
      maNcc,          // ma_ncc
      r.ma_kh,        // ma_kh
      'Da_ban',       // trang_thai (có GD → đã bán)
      r.ngay,         // ngay_nhap
      r.ngay,         // ngay_ban
      '',             // ghi_chu
      new Date()      // ngay_cap_nhat
    ]);
  });

  if (newRows.length > 0) {
    var lastRow = sheet.getLastRow();
    sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
  }

  Logger.log('SyncKho: ' + newRows.length + ' CID mới (đã có trước: ' + (Object.keys(existing).length - newRows.length) + ')');
  return { added: newRows.length, existing: Object.keys(existing).length - newRows.length };
}

// ============================================================
// SYNC GD KH — So sánh + append GD KH mới vào GD_KhachHang
// ============================================================

function _syncGDKH(crmIds, allRows, nccMap) {
  var ss = _openCrm_(crmIds, 'GD_KH_' + NAM);
  var sheet = ss.getSheetByName('GD_KhachHang');
  if (!sheet) throw new Error('Không tìm thấy tab GD_KhachHang');

  // Đọc GD hiện có → tạo Set composite key + khởi tạo counter
  var existing = {};
  var data = sheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) {
    var key = _gdKHKey(data[i][1], data[i][14], data[i][4], data[i][5], data[i][2]);
    existing[key] = true;
  }
  _initCountersFromExisting_(sheet, 'GD-KH');

  // Sort allRows theo ngày để sinh mã GD đúng thứ tự
  allRows.sort(function(a, b) {
    var da = a.ngay instanceof Date ? a.ngay.getTime() : 0;
    var db = b.ngay instanceof Date ? b.ngay.getTime() : 0;
    return da - db;
  });

  // Tìm GD mới
  var newRows = [];
  var newMaKHs = {}; // track KH bị ảnh hưởng để recompute

  allRows.forEach(function(r) {
    if (!MA_KH_REGEX.test(r.ma_kh)) return;

    var loaiGD, httt, soTien, phi, tongKHChuyen, cid;

    if (r.source === 'doichieu') {
      loaiGD = r.loai_gd;
      httt = r.httt;
      soTien = r.so_tien;
      phi = r.phi;
      tongKHChuyen = r.tong_kh_chuyen;
      cid = r.cid;
    } else {
      // Topup
      loaiGD = r.yeu_cau === 'DEPOSIT' ? 'Nap_CID' : 'Rut_CID';
      httt = '';
      soTien = r.so_tien;
      phi = 0;
      tongKHChuyen = r.so_tien;
      cid = r.cid;
    }

    var key = _gdKHKey(r.ma_kh, r.ngay, cid, soTien, loaiGD);
    if (existing[key]) return; // đã có

    existing[key] = true; // tránh trùng trong batch hiện tại
    newMaKHs[r.ma_kh] = true;

    var maGd = _generateMaGD_('GD-KH', r.ngay);

    newRows.push([
      maGd,             // ma_gd
      r.ma_kh,          // ma_kh
      loaiGD,           // loai_gd
      httt,             // hinh_thuc_tt
      cid,              // cid
      soTien,           // so_tien_goc
      phi,              // phi
      tongKHChuyen,     // tong_kh_chuyen
      '',               // quy_truoc (recompute)
      '',               // bien_dong (recompute)
      '',               // quy_sau (recompute)
      'Hoan_thanh',     // trang_thai
      r.nguoi_th,       // nguoi_thuc_hien
      r.ghi_chu || '',  // ghi_chu
      r.ngay,           // ngay_tao
      r.ngay            // ngay_thuc_hien
    ]);
  });

  if (newRows.length > 0) {
    var lastRow = sheet.getLastRow();
    sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);

    // Tạo record đối soát cho GD cần đối soát (trạng thái Da_doi_soat vì là dữ liệu lịch sử)
    var dsRows = [];
    newRows.forEach(function(row) {
      var gdLoai = row[2];  // loai_gd
      var gdHttt = row[3];  // hinh_thuc_tt
      if (_needsDoiSoat(gdLoai, gdHttt)) {
        dsRows.push([row[0], 'Da_doi_soat']); // [ma_gd, trang_thai_doi_soat]
      }
    });
    if (dsRows.length > 0) {
      var ssDoi = _openCrm_(crmIds, 'DOI_SOAT_' + NAM);
      var sheetDoi = ssDoi.getSheetByName('DoiSoat_GD');
      if (sheetDoi) {
        var lastDoi = sheetDoi.getLastRow();
        sheetDoi.getRange(lastDoi + 1, 1, dsRows.length, dsRows[0].length).setValues(dsRows);
        Logger.log('DoiSoat_GD: ' + dsRows.length + ' record mới');
      }
    }
  }

  Logger.log('SyncGDKH: ' + newRows.length + ' GD mới (đã có: ' + Object.keys(existing).length + ')');
  return { added: newRows.length, newMaKHs: Object.keys(newMaKHs) };
}

/**
 * Kiểm tra GD có cần đối soát không (logic khớp CRM Helpers.js:needsDoiSoat)
 */
function _needsDoiSoat(loaiGD, httt) {
  if (loaiGD === 'Nap_quy') return true;
  if (loaiGD === 'Refund')  return true;
  if ((loaiGD === 'Mua_TK' || loaiGD === 'Nap_CID') && httt === 'truc_tiep') return true;
  return false;
}

/**
 * Composite key cho GD KH (dedup)
 * ma_kh + ngay(yyyyMMdd) + cid + so_tien + loai_gd
 */
function _gdKHKey(maKH, ngay, cid, soTien, loaiGD) {
  var d = '';
  if (ngay instanceof Date && !isNaN(ngay.getTime())) {
    d = Utilities.formatDate(ngay, TZ, 'yyyyMMdd');
  }
  return [maKH || '', d, cid || '', Math.round((soTien || 0) * 100), loaiGD || ''].join('|');
}

// ============================================================
// SYNC NCC — So sánh + append NCC mới vào DanhMuc_NCC
// ============================================================

function _syncNCC(crmIds, nccGDRows, nccMap) {
  var ss = _openCrm_(crmIds, 'NHA_CUNG_CAP');
  var sheet = ss.getSheetByName('DanhMuc_NCC');
  if (!sheet) throw new Error('Không tìm thấy tab DanhMuc_NCC');

  // Đọc ma_ncc hiện có (cột A) + tên nhóm (cột D) để tránh tạo trùng
  var existing = {};
  var data = sheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) {
    var maNcc = (data[i][0] || '').toString().trim();
    if (maNcc) existing[maNcc] = true;
    // Đọc ten_nhom (cột D, index 3) → merge vào nccMap để dedup theo tên
    var tenNhom = (data[i][3] || '').toString().trim();
    if (maNcc && tenNhom) {
      var nhomKey = tenNhom.toLowerCase();
      if (!nccMap[nhomKey]) nccMap[nhomKey] = maNcc;
    }
  }

  // Phát hiện NCC mới từ GD NCC
  var newRows = [];
  var maxNum = 0;
  Object.keys(existing).forEach(function(k) {
    var m = k.match(/^NCC-(\d+)$/);
    if (m) { var n = parseInt(m[1]); if (n > maxNum) maxNum = n; }
  });

  nccGDRows.forEach(function(r) {
    if (!r.ten_nguon) return;
    var key = r.ten_nguon.trim().toLowerCase();
    if (nccMap[key]) return; // đã match
    // Tạo NCC mới
    maxNum++;
    var newMa = 'NCC-' + ('000' + maxNum).slice(-3);
    nccMap[key] = newMa;
    existing[newMa] = true;
    newRows.push([
      newMa, r.ten_nguon, '', r.ten_nguon, '', '', '', '', 0, 0, 0, 0, 0,
      'Hoat_dong', false, 'AUTO-DETECTED', '', new Date()
    ]);
    Logger.log('NEW NCC: "' + r.ten_nguon + '" → ' + newMa);
  });

  if (newRows.length > 0) {
    var lastRow = sheet.getLastRow();
    sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
  }

  Logger.log('SyncNCC: ' + newRows.length + ' NCC mới');
  return { added: newRows.length };
}

// ============================================================
// SYNC GD NCC — So sánh + append GD NCC mới vào GD_NhaCungCap
// ============================================================

function _syncGDNCC(crmIds, nccGDRows, nccMap) {
  var ss = _openCrm_(crmIds, 'GD_NCC_' + NAM);
  var sheet = ss.getSheetByName('GD_NhaCungCap');
  if (!sheet) throw new Error('Không tìm thấy tab GD_NhaCungCap');

  // Đọc GD NCC hiện có → composite key + khởi tạo counter
  var existing = {};
  var data = sheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) {
    var key = _gdNCCKey(data[i][2], data[i][1], data[i][5], data[i][3]);
    existing[key] = true;
  }
  _initCountersFromExisting_(sheet, 'GD-NCC');

  // Sort theo ngày để sinh mã đúng thứ tự
  nccGDRows.sort(function(a, b) {
    var da = a.ngay instanceof Date ? a.ngay.getTime() : 0;
    var db = b.ngay instanceof Date ? b.ngay.getTime() : 0;
    return da - db;
  });

  var newRows = [];
  var newMaNCCs = {}; // track NCC bị ảnh hưởng để recompute

  nccGDRows.forEach(function(r) {
    var maNcc = _lookupNcc(nccMap, r.ten_nguon);
    var key = _gdNCCKey(maNcc, r.ngay, r.so_tien, r.loai_gd);
    if (existing[key]) return;
    existing[key] = true;

    if (maNcc) newMaNCCs[maNcc] = true;

    var maGd = _generateMaGD_('GD-NCC', r.ngay);

    var doiSoat = _needsDoiSoat(r.loai_gd, r.httt);
    newRows.push([
      maGd,             // ma_gd
      r.ngay,           // ngay_gd
      maNcc,            // ma_ncc
      r.loai_gd,        // loai_gd
      r.httt,           // hinh_thuc_tt
      r.so_tien,        // so_tien_goc
      r.phi,            // phi
      r.tong_chuyen,    // tong_chuyen
      '',               // quy_truoc
      '',               // bien_dong
      '',               // quy_sau
      '',               // ma_gd_kh
      '',               // gom_rate
      '',               // tong_nhan
      'Hoan_thanh',     // trang_thai
      r.nguoi_th,       // nguoi_thuc_hien
      r.ghi_chu,        // ghi_chu
      r.ngay,           // ngay_tao
      '',               // trang_thai_doi_soat (ghi vào DoiSoat_GD thay vì đây)
      '',               // nguoi_doi_soat
      ''                // ngay_doi_soat
    ]);
  });

  if (newRows.length > 0) {
    var lastRow = sheet.getLastRow();
    sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);

    // Ghi DoiSoat_GD cho NCC (giống KH)
    var dsRows = [];
    newRows.forEach(function(row) {
      var gdLoai = row[3];  // loai_gd
      var gdHttt = row[4];  // hinh_thuc_tt
      if (_needsDoiSoat(gdLoai, gdHttt)) {
        dsRows.push([row[0], 'Da_doi_soat']); // [ma_gd, trang_thai_doi_soat]
      }
    });
    if (dsRows.length > 0) {
      var ssDoi = _openCrm_(crmIds, 'DOI_SOAT_' + NAM);
      var sheetDoi = ssDoi.getSheetByName('DoiSoat_GD');
      if (sheetDoi) {
        var lastDoi = sheetDoi.getLastRow();
        sheetDoi.getRange(lastDoi + 1, 1, dsRows.length, dsRows[0].length).setValues(dsRows);
        Logger.log('DoiSoat_GD (NCC): ' + dsRows.length + ' record mới');
      }
    }
  }

  Logger.log('SyncGDNCC: ' + newRows.length + ' GD NCC mới');
  return { added: newRows.length, newMaNCCs: Object.keys(newMaNCCs) };
}

/**
 * Composite key cho GD NCC (dedup)
 */
function _gdNCCKey(maNcc, ngay, soTien, loaiGD) {
  var d = '';
  if (ngay instanceof Date && !isNaN(ngay.getTime())) {
    d = Utilities.formatDate(ngay, TZ, 'yyyyMMdd');
  }
  return [maNcc || '', d, Math.round((soTien || 0) * 100), loaiGD || ''].join('|');
}

// ============================================================
// RECOMPUTE QUỸ KH — Tính lại quỹ cho KH có GD mới
// ============================================================

/**
 * Tính biến động quỹ KH — logic khớp CRM Helpers.js:calculateBienDong()
 */
function _calculateBienDongKH(loaiGD, httt, soTienGoc) {
  if (loaiGD === 'Nap_quy')   return soTienGoc;
  if (loaiGD === 'Rut_CID')   return soTienGoc;
  if (loaiGD === 'Cashback')  return soTienGoc;
  if (loaiGD === 'Refund')    return -soTienGoc;
  if (loaiGD === 'Nap_CID' || loaiGD === 'Mua_TK') {
    return (httt === 'truc_tiep') ? 0 : -soTienGoc;
  }
  return 0;
}

function _recomputeQuyKH(crmIds, affectedMaKHs) {
  if (!affectedMaKHs || affectedMaKHs.length === 0) return;
  Logger.log('Recompute quỹ cho ' + affectedMaKHs.length + ' KH: ' + affectedMaKHs.join(', '));

  // Đọc tất cả GD KH
  var ssGD = _openCrm_(crmIds, 'GD_KH_' + NAM);
  var sheetGD = ssGD.getSheetByName('GD_KhachHang');
  var gdData = sheetGD.getDataRange().getValues();

  // Đọc quỹ gốc từ DanhMuc_KH
  var ssKH = _openCrm_(crmIds, 'KHACH_HANG');
  var sheetKH = ssKH.getSheetByName('DanhMuc_KH');
  var khData = sheetKH.getDataRange().getValues();
  var quyGocMap = {}; // { ma_kh: quy_goc }
  for (var k = 1; k < khData.length; k++) {
    var mkk = (khData[k][0] || '').toString().trim();
    if (mkk) quyGocMap[mkk] = parseFloat(khData[k][4]) || 0; // cột E = quy_goc
  }

  // Group GD theo ma_kh, chỉ lấy affected
  var affected = {};
  affectedMaKHs.forEach(function(mk) { affected[mk] = true; });

  var gdByKH = {};
  for (var i = 1; i < gdData.length; i++) {
    var mk = (gdData[i][1] || '').toString().trim();
    if (!affected[mk]) continue;
    if (!gdByKH[mk]) gdByKH[mk] = [];
    gdByKH[mk].push({
      rowIndex: i,
      ngay: gdData[i][14], // ngay_tao
      loai_gd: (gdData[i][2] || '').toString().trim(),
      httt: (gdData[i][3] || '').toString().trim(),
      so_tien_goc: parseFloat(gdData[i][5]) || 0
    });
  }

  // Tính quỹ cho từng KH — batch update
  var quyResults = {};
  var gdUpdates = []; // { rowIndex, quyTruoc, bienDong, quySau }

  Object.keys(gdByKH).forEach(function(mk) {
    var gds = gdByKH[mk];
    gds.sort(function(a, b) {
      var da = a.ngay instanceof Date ? a.ngay.getTime() : 0;
      var db = b.ngay instanceof Date ? b.ngay.getTime() : 0;
      return da - db;
    });

    var quy = quyGocMap[mk] || 0; // bắt đầu từ quỹ gốc
    gds.forEach(function(gd) {
      var quyTruoc = quy;
      var bienDong = _calculateBienDongKH(gd.loai_gd, gd.httt, gd.so_tien_goc);
      quy = quyTruoc + bienDong;

      gdUpdates.push({
        rowIndex: gd.rowIndex,
        quyTruoc: quyTruoc,
        bienDong: bienDong,
        quySau: quy
      });
    });

    quyResults[mk] = quy;
  });

  // Batch write GD: cập nhật cột I(9), J(10), K(11)
  gdUpdates.forEach(function(u) {
    var rowNum = u.rowIndex + 1;
    sheetGD.getRange(rowNum, 9, 1, 3).setValues([[u.quyTruoc, u.bienDong, u.quySau]]);
  });

  // Batch write DanhMuc_KH: cập nhật quy_hien_tai + ngay_cap_nhat
  var now = new Date();
  for (var j = 1; j < khData.length; j++) {
    var mk2 = (khData[j][0] || '').toString().trim();
    if (quyResults[mk2] !== undefined) {
      sheetKH.getRange(j + 1, 4, 1, 1).setValue(quyResults[mk2]); // cột D = quy_hien_tai
      sheetKH.getRange(j + 1, 10, 1, 1).setValue(now); // cột J = ngay_cap_nhat
    }
  }

  Logger.log('Recompute KH done. Updated: ' + Object.keys(quyResults).length);
}

// ============================================================
// KICK-OFF CRM — Tạo GD cân bằng ngày 31/03 cho KH chênh lệch
// ============================================================

/**
 * So sánh quỹ CRM (sau recompute) với cột KICKOFF_DATE trong "Tổng hợp".
 * Nếu chênh lệch → tạo GD "Kick_Off" ngày KICKOFF_DATE để cân bằng.
 */
function _createKickOffKH(crmIds) {
  // Đọc quỹ KT tại KICKOFF_DATE
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheetTH = source.getSheetByName(TAB_TONG_HOP);
  if (!sheetTH) return;
  var thData = sheetTH.getDataRange().getValues();
  var headerRow = thData[1];
  var colKickOff = _findDateCol(headerRow, KICKOFF_DATE);
  if (colKickOff < 0) {
    Logger.log('WARNING: Không tìm thấy cột ' + Utilities.formatDate(KICKOFF_DATE, TZ, 'dd/MM/yyyy') + ' trong Tổng hợp');
    return;
  }

  var ktMap = {}; // { ma_kh: quỹ KT tại KICKOFF_DATE }
  for (var i = 5; i < thData.length; i++) {
    var mk = _fixMaKH((thData[i][3] || '').toString().trim());
    if (!mk || !MA_KH_REGEX.test(mk)) continue;
    ktMap[mk] = parseFloat(thData[i][colKickOff]) || 0;
  }

  // Đọc quỹ CRM hiện tại
  var ssKH = _openCrm_(crmIds, 'KHACH_HANG');
  var sheetKH = ssKH.getSheetByName('DanhMuc_KH');
  var khData = sheetKH.getDataRange().getValues();

  // Tìm KH chênh lệch → tạo GD Kick-Off
  var ssGD = _openCrm_(crmIds, 'GD_KH_' + NAM);
  var sheetGD = ssGD.getSheetByName('GD_KhachHang');
  _initCountersFromExisting_(sheetGD, 'GD-KH');

  var newRows = [];
  var dsRows = [];
  for (var j = 1; j < khData.length; j++) {
    var maKH = (khData[j][0] || '').toString().trim();
    if (!maKH || ktMap[maKH] === undefined) continue;
    var quyCRM = parseFloat(khData[j][3]) || 0; // quy_hien_tai
    var quyKT = ktMap[maKH];
    var diff = quyKT - quyCRM;
    if (Math.abs(diff) < 0.01) continue; // khớp rồi

    var maGd = _generateMaGD_('GD-KH', KICKOFF_DATE);
    var loai = diff > 0 ? 'Nap_quy' : 'Refund';
    var soTien = Math.abs(diff);

    newRows.push([
      maGd, maKH, loai, '', '', soTien, 0, soTien,
      '', '', '', 'Hoan_thanh', 'Migration', 'Kick-Off CRM',
      KICKOFF_DATE, KICKOFF_DATE
    ]);
    dsRows.push([maGd, 'Da_doi_soat']);
  }

  if (newRows.length > 0) {
    var lastRow = sheetGD.getLastRow();
    sheetGD.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);

    // Ghi DoiSoat
    var ssDoi = _openCrm_(crmIds, 'DOI_SOAT_' + NAM);
    var sheetDoi = ssDoi.getSheetByName('DoiSoat_GD');
    if (sheetDoi && dsRows.length > 0) {
      var lastDoi = sheetDoi.getLastRow();
      sheetDoi.getRange(lastDoi + 1, 1, dsRows.length, dsRows[0].length).setValues(dsRows);
    }
  }

  Logger.log('Kick-Off KH: ' + newRows.length + ' GD cân bằng tạo ngày ' + Utilities.formatDate(KICKOFF_DATE, TZ, 'dd/MM/yyyy'));
  return newRows.length;
}

/**
 * Tương tự cho NCC
 */
function _createKickOffNCC(crmIds, nccMap) {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheetNguon = source.getSheetByName(TAB_NGUON);
  if (!sheetNguon) return;
  var nguonData = sheetNguon.getDataRange().getValues();
  var headerRow = nguonData[0];
  var colKickOff = _findDateCol(headerRow, KICKOFF_DATE);
  if (colKickOff < 0) {
    Logger.log('WARNING: Không tìm thấy cột ' + Utilities.formatDate(KICKOFF_DATE, TZ, 'dd/MM/yyyy') + ' trong Nguồn');
    return;
  }

  var ktNCCMap = {};
  for (var i = 3; i < nguonData.length; i++) {
    var tenNguon = (nguonData[i][1] || '').toString().trim();
    if (!tenNguon) continue;
    var maNcc = _lookupNcc(nccMap, tenNguon);
    if (!maNcc) continue;
    ktNCCMap[maNcc] = parseFloat(nguonData[i][colKickOff]) || 0;
  }

  // Đọc quỹ NCC hiện tại
  var ssNCC = _openCrm_(crmIds, 'NHA_CUNG_CAP');
  var sheetNCC = ssNCC.getSheetByName('DanhMuc_NCC');
  var nccData = sheetNCC.getDataRange().getValues();

  var ssGD = _openCrm_(crmIds, 'GD_NCC_' + NAM);
  var sheetGD = ssGD.getSheetByName('GD_NhaCungCap');
  _initCountersFromExisting_(sheetGD, 'GD-NCC');

  var newRows = [];
  var dsRows = [];
  for (var j = 1; j < nccData.length; j++) {
    var maNcc = (nccData[j][0] || '').toString().trim();
    if (!maNcc || ktNCCMap[maNcc] === undefined) continue;
    var quyCRM = parseFloat(nccData[j][11]) || 0; // cột L = quy_hien_tai
    var quyKT = ktNCCMap[maNcc];
    var diff = quyKT - quyCRM;
    if (Math.abs(diff) < 0.01) continue;

    var maGd = _generateMaGD_('GD-NCC', KICKOFF_DATE);
    var loai = diff > 0 ? 'Nap_quy' : 'Refund';
    var soTien = Math.abs(diff);

    newRows.push([
      maGd, KICKOFF_DATE, maNcc, loai, '', soTien, 0, soTien,
      '', '', '', '', '', '', 'Hoan_thanh', 'Migration', 'Kick-Off CRM',
      KICKOFF_DATE, '', '', ''
    ]);
    // Ghi đối soát vào DoiSoat_GD (không ghi trực tiếp vào GD_NhaCungCap)
    dsRows.push([maGd, 'Da_doi_soat']);
  }

  if (newRows.length > 0) {
    var lastRow = sheetGD.getLastRow();
    sheetGD.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);

    var ssDoi = _openCrm_(crmIds, 'DOI_SOAT_' + NAM);
    var sheetDoi = ssDoi.getSheetByName('DoiSoat_GD');
    if (sheetDoi && dsRows.length > 0) {
      var lastDoi = sheetDoi.getLastRow();
      sheetDoi.getRange(lastDoi + 1, 1, dsRows.length, dsRows[0].length).setValues(dsRows);
    }
  }

  Logger.log('Kick-Off NCC: ' + newRows.length + ' GD cân bằng tạo ngày ' + Utilities.formatDate(KICKOFF_DATE, TZ, 'dd/MM/yyyy'));
  return newRows.length;
}

// ============================================================
// RECOMPUTE QUỸ NCC — Tính lại quỹ cho NCC có GD mới
// ============================================================

/**
 * Logic dòng tiền NCC (ngược KH):
 * - Nap_quy:  NCC nhận tiền → Quỹ NCC +
 * - Rut_CID:  Rút tiền từ CID → Quỹ NCC +
 * - Mua_TK:   Trả tiền mua TK → Quỹ NCC -
 * - Nap_CID:  Nạp tiền vào CID → Quỹ NCC -
 * - Refund:   NCC trả lại tiền → Quỹ NCC - (rút sạch)
 */
function _recomputeQuyNCC(crmIds, affectedMaNCCs, quyGocNCCMap) {
  if (!affectedMaNCCs || affectedMaNCCs.length === 0) return;
  Logger.log('Recompute quỹ NCC cho ' + affectedMaNCCs.length + ' NCC: ' + affectedMaNCCs.join(', '));

  var ssGD = _openCrm_(crmIds, 'GD_NCC_' + NAM);
  var sheetGD = ssGD.getSheetByName('GD_NhaCungCap');
  var gdData = sheetGD.getDataRange().getValues();

  var affected = {};
  affectedMaNCCs.forEach(function(mn) { affected[mn] = true; });

  var gdByNCC = {};
  for (var i = 1; i < gdData.length; i++) {
    var mn = (gdData[i][2] || '').toString().trim();
    if (!affected[mn]) continue;
    if (!gdByNCC[mn]) gdByNCC[mn] = [];
    gdByNCC[mn].push({
      rowIndex: i,
      ngay: gdData[i][1],
      loai_gd: (gdData[i][3] || '').toString().trim(),
      so_tien_goc: parseFloat(gdData[i][5]) || 0
    });
  }

  var quyResults = {};
  var gdUpdates = [];

  Object.keys(gdByNCC).forEach(function(mn) {
    var gds = gdByNCC[mn];
    gds.sort(function(a, b) {
      var da = a.ngay instanceof Date ? a.ngay.getTime() : 0;
      var db = b.ngay instanceof Date ? b.ngay.getTime() : 0;
      return da - db;
    });

    var quy = (quyGocNCCMap && quyGocNCCMap[mn]) ? quyGocNCCMap[mn] : 0; // bắt đầu từ quỹ gốc NCC
    gds.forEach(function(gd) {
      var quyTruoc = quy;
      var bienDong = 0;

      if (gd.loai_gd === 'Nap_quy')      bienDong = gd.so_tien_goc;
      else if (gd.loai_gd === 'Rut_CID')  bienDong = gd.so_tien_goc;
      else if (gd.loai_gd === 'Mua_TK')   bienDong = -gd.so_tien_goc;
      else if (gd.loai_gd === 'Nap_CID')  bienDong = -gd.so_tien_goc;
      else if (gd.loai_gd === 'Refund')   bienDong = -quyTruoc; // rút sạch

      quy = quyTruoc + bienDong;
      gdUpdates.push({ rowIndex: gd.rowIndex, quyTruoc: quyTruoc, bienDong: bienDong, quySau: quy });
    });

    quyResults[mn] = quy;
  });

  // Batch write GD NCC: cột I(9), J(10), K(11)
  gdUpdates.forEach(function(u) {
    sheetGD.getRange(u.rowIndex + 1, 9, 1, 3).setValues([[u.quyTruoc, u.bienDong, u.quySau]]);
  });

  // Cập nhật quy_hien_tai trong DanhMuc_NCC
  var ssNCC = _openCrm_(crmIds, 'NHA_CUNG_CAP');
  var sheetNCC = ssNCC.getSheetByName('DanhMuc_NCC');
  var nccData = sheetNCC.getDataRange().getValues();
  var now = new Date();

  for (var j = 1; j < nccData.length; j++) {
    var mn2 = (nccData[j][0] || '').toString().trim();
    if (quyResults[mn2] !== undefined) {
      sheetNCC.getRange(j + 1, 12).setValue(quyResults[mn2]); // cột L = quy_hien_tai
      sheetNCC.getRange(j + 1, 18).setValue(now); // cột R = ngay_cap_nhat
    }
  }

  Logger.log('Recompute NCC done. Updated: ' + Object.keys(quyResults).length);
}

// ============================================================
// ĐỌC DỮ LIỆU TOPUP
// ============================================================

function _readTopup() {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheet = source.getSheetByName(TAB_TOPUP);
  if (!sheet) throw new Error('Không tìm thấy tab "' + TAB_TOPUP + '" trong sheet nguồn');

  var data = sheet.getDataRange().getValues();
  if (data.length <= 1) return [];

  var headers = data[0];
  var col = {};
  headers.forEach(function(h, i) { col[h.toString().trim()] = i; });

  var needed = ['Thời gian', 'Mã khách hàng', 'ID Tài Khoản', 'Số tiền',
                'Loại tiền', 'Yêu cầu', 'Nguồn', 'Nhóm nguồn',
                'Người thực hiện', 'Tình trạng', 'Note'];
  needed.forEach(function(h) {
    if (col[h] === undefined) throw new Error('Thiếu cột "' + h + '" trong tab Topup');
  });

  var rows = [];
  for (var i = 1; i < data.length; i++) {
    var r = data[i];

    var ngay = _parseDate(r[col['Thời gian']]);
    if (!ngay || ngay < DATE_FROM) continue;

    var maKH = _fixMaKH((r[col['Mã khách hàng']] || '').toString().trim());
    if (!maKH) continue;

    if (NCC_MA_KH.indexOf(maKH) >= 0) continue;

    var cid = _formatCID(r[col['ID Tài Khoản']]);
    var tinhTrang = (r[col['Tình trạng']] || '').toString().trim().toLowerCase();
    if (tinhTrang !== 'done') continue;

    var yeuCau = (r[col['Yêu cầu']] || '').toString().trim().toUpperCase();
    if (yeuCau !== 'DEPOSIT' && yeuCau !== 'WITHDRAW') continue;

    rows.push({
      ngay: ngay,
      ma_kh: maKH,
      cid: cid,
      so_tien: _parseNumber(r[col['Số tiền']]),
      loai_tien: (r[col['Loại tiền']] || 'USD').toString().trim(),
      yeu_cau: yeuCau,
      nguon: (r[col['Nguồn']] || '').toString().trim(),
      nhom_nguon: (r[col['Nhóm nguồn']] || '').toString().trim(),
      nguoi_th: (r[col['Người thực hiện']] || '').toString().trim(),
      ghi_chu: (r[col['Note']] || '').toString().trim()
    });
  }

  return rows;
}

// ============================================================
// ĐỌC DỮ LIỆU ĐỐI CHIẾU TÀI CHÍNH
// ============================================================

function _readDoiChieu() {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheet = source.getSheetByName(TAB_DOI_CHIEU);
  if (!sheet) throw new Error('Không tìm thấy tab "' + TAB_DOI_CHIEU + '" trong sheet nguồn');

  var data = sheet.getDataRange().getValues();
  if (data.length <= 1) return [];

  var headers = data[0];
  var col = {};
  headers.forEach(function(h, i) { col[h.toString().trim()] = i; });

  var needed = ['Ngày', 'Mã KH', 'Tên Zalo/Telegram', 'Tên nguồn (Tài khoản)',
                'Giao dịch với khách', 'Số lượng TK',
                'Số tiền nạp (chưa tính % phí)', 'Phần trăm phí nạp (%)',
                'Số tiền (Tổng)', 'Hình thức thanh toán',
                'Ghi chú', 'ID Tài khoản ( Nếu nạp quỹ thì ghi - )', 'Người thực hiện'];
  needed.forEach(function(h) {
    if (col[h] === undefined) throw new Error('Thiếu cột "' + h + '" trong tab Đối chiếu');
  });

  var rows = [];
  for (var i = 1; i < data.length; i++) {
    var r = data[i];

    var ngay = _parseDate(r[col['Ngày']]);
    if (!ngay || ngay < DATE_FROM) continue;

    var maKH = _fixMaKH((r[col['Mã KH']] || '').toString().trim());
    if (!maKH) continue;

    if (NCC_MA_KH.indexOf(maKH) >= 0) continue;

    var gdKhach = (r[col['Giao dịch với khách']] || '').toString().trim();
    if (!gdKhach) continue;
    var gdKhachLower = gdKhach.toLowerCase();
    if (gdKhachLower !== 'khách nạp tiền' && gdKhachLower !== 'khách mua tk' && gdKhachLower !== 'refund') continue;

    var htttRaw = (r[col['Hình thức thanh toán']] || '').toString().trim();
    var httt = '';
    if (htttRaw === 'Trừ quỹ') {
      httt = 'tru_quy';
    } else if (htttRaw === 'Chuyển-Nhận') {
      httt = 'truc_tiep';
    } else if (htttRaw && gdKhachLower === 'khách mua tk') {
      if (htttRaw.toLowerCase().indexOf('ghim') >= 0) continue;
    }

    var phiRaw = (r[col['Phần trăm phí nạp (%)']] || '').toString().trim();
    var phi = 0;
    if (phiRaw) {
      var phiMatch = phiRaw.toString().match(/[\d.]+/);
      phi = phiMatch ? parseFloat(phiMatch[0]) : 0;
    }

    var soTienGoc = _parseNumber(r[col['Số tiền nạp (chưa tính % phí)']]);
    var tongKHChuyen = _parseNumber(r[col['Số tiền (Tổng)']]);
    var soLuongTK = parseInt(r[col['Số lượng TK']]) || 0;
    var nguon = (r[col['Tên nguồn (Tài khoản)']] || '').toString().trim();
    var nguoiTH = (r[col['Người thực hiện']] || '').toString().trim();
    var ghiChu = (r[col['Ghi chú']] || '').toString().trim();
    var cidRaw = (r[col['ID Tài khoản ( Nếu nạp quỹ thì ghi - )']] || '').toString().trim();

    if (gdKhachLower === 'khách nạp tiền') {
      rows.push({
        ngay: ngay, ma_kh: maKH, ten_kh: '', cid: '',
        so_tien: soTienGoc, phi: phi, tong_kh_chuyen: tongKHChuyen,
        loai_gd: 'Nap_quy', httt: '', nguon: nguon,
        nguoi_th: nguoiTH, ghi_chu: ghiChu, source: 'doichieu'
      });
    } else if (gdKhachLower === 'refund') {
      rows.push({
        ngay: ngay, ma_kh: maKH, ten_kh: '', cid: '',
        so_tien: soTienGoc, phi: 0, tong_kh_chuyen: soTienGoc,
        loai_gd: 'Refund', httt: '', nguon: nguon,
        nguoi_th: nguoiTH, ghi_chu: ghiChu, source: 'doichieu'
      });
    } else if (gdKhachLower === 'khách mua tk') {
      var cids = _parseCIDs(cidRaw, i + 1, 'Đối chiếu');
      var divisor = soLuongTK > 1 ? soLuongTK : 1;
      var soTienPerCID = soTienGoc / divisor;
      var tongPerCID = tongKHChuyen / divisor;

      if (cids.length === 0) {
        rows.push({
          ngay: ngay, ma_kh: maKH, ten_kh: '', cid: '',
          so_tien: soTienGoc, phi: phi, tong_kh_chuyen: tongKHChuyen,
          loai_gd: 'Mua_TK', httt: httt, nguon: nguon,
          nguoi_th: nguoiTH, ghi_chu: ghiChu, source: 'doichieu'
        });
      } else {
        for (var c = 0; c < cids.length; c++) {
          rows.push({
            ngay: ngay, ma_kh: maKH, ten_kh: '', cid: cids[c],
            so_tien: soTienPerCID, phi: phi, tong_kh_chuyen: tongPerCID,
            loai_gd: 'Mua_TK', httt: httt, nguon: nguon,
            nguoi_th: nguoiTH, ghi_chu: ghiChu, source: 'doichieu'
          });
        }
      }
    }
  }

  return rows;
}

// ============================================================
// ĐỌC DỮ LIỆU ĐỐI CHIẾU — PHẦN NCC
// ============================================================

function _readDoiChieuNCC() {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheet = source.getSheetByName(TAB_DOI_CHIEU);
  if (!sheet) return [];

  var data = sheet.getDataRange().getValues();
  if (data.length <= 1) return [];

  var headers = data[0];
  var col = {};
  headers.forEach(function(h, i) { col[h.toString().trim()] = i; });

  // Debug: log tên tất cả cột để tìm đúng tên cột NCC
  if (col['Giao dịch với Nguồn'] === undefined) {
    Logger.log('WARNING: Không tìm thấy cột "Giao dịch với Nguồn". Các cột hiện có:');
    headers.forEach(function(h, i) {
      var name = h.toString().trim();
      if (name) Logger.log('  Col ' + i + ': "' + name + '"');
    });
    // Thử tìm cột gần đúng
    headers.forEach(function(h, i) {
      var name = h.toString().trim().toLowerCase();
      if (name.indexOf('nguồn') >= 0 || name.indexOf('nguon') >= 0) {
        Logger.log('  → Có thể là cột NCC: Col ' + i + ': "' + h.toString().trim() + '"');
      }
    });
  }

  var rows = [];
  for (var i = 1; i < data.length; i++) {
    var r = data[i];

    var ngay = _parseDate(r[col['Ngày']]);
    if (!ngay || ngay < DATE_FROM) continue;

    var gdNguon = (r[col['Giao dịch với Nguồn']] || '').toString().trim();
    if (!gdNguon) continue;
    var gdNguonLower = gdNguon.toLowerCase();

    var loaiGD = '';
    if (gdNguonLower === 'nạp tiền cho nguồn') {
      loaiGD = 'Nap_quy';
    } else if (gdNguonLower === 'mua tài khoản') {
      loaiGD = 'Mua_TK';
    } else {
      continue;
    }

    var htttRaw = (r[col['Hình thức thanh toán']] || '').toString().trim();
    if (htttRaw.toLowerCase().indexOf('ghim') >= 0) continue;

    var httt = '';
    if (htttRaw === 'Trừ quỹ') {
      httt = 'tru_quy';
    } else if (htttRaw === 'Chuyển-Nhận') {
      httt = 'truc_tiep';
    }

    var tenNguon = (r[col['Tên Zalo/Telegram']] || '').toString().trim();

    var phiRaw = (r[col['Phần trăm phí nạp (%)']] || '').toString().trim();
    var phi = 0;
    if (phiRaw) {
      var phiMatch = phiRaw.toString().match(/[\d.]+/);
      phi = phiMatch ? parseFloat(phiMatch[0]) : 0;
    }

    var soTienGoc = _parseNumber(r[col['Số tiền nạp (chưa tính % phí)']]);
    var tongChuyen = _parseNumber(r[col['Số tiền (Tổng)']]);
    var soLuongTK = parseInt(r[col['Số lượng TK']]) || 0;
    var nguoiTH = (r[col['Người thực hiện']] || '').toString().trim();
    var ghiChu = (r[col['Ghi chú']] || '').toString().trim();
    var cidRaw = (r[col['ID Tài khoản ( Nếu nạp quỹ thì ghi - )']] || '').toString().trim();

    if (loaiGD === 'Nap_quy') {
      rows.push({
        ngay: ngay, ten_nguon: tenNguon, loai_gd: loaiGD, httt: httt,
        cid: '', so_tien: soTienGoc, phi: phi, tong_chuyen: tongChuyen,
        nguoi_th: nguoiTH, ghi_chu: ghiChu, dong: i + 1
      });
    } else if (loaiGD === 'Mua_TK') {
      var cids = _parseCIDs(cidRaw, i + 1, 'Đối chiếu NCC');
      var divisor = soLuongTK > 1 ? soLuongTK : 1;
      var soTienPerCID = soTienGoc / divisor;
      var tongPerCID = tongChuyen / divisor;

      if (cids.length === 0) {
        rows.push({
          ngay: ngay, ten_nguon: tenNguon, loai_gd: loaiGD, httt: httt,
          cid: '', so_tien: soTienGoc, phi: phi, tong_chuyen: tongChuyen,
          nguoi_th: nguoiTH, ghi_chu: ghiChu, dong: i + 1
        });
      } else {
        for (var c = 0; c < cids.length; c++) {
          rows.push({
            ngay: ngay, ten_nguon: tenNguon, loai_gd: loaiGD, httt: httt,
            cid: cids[c], so_tien: soTienPerCID, phi: phi, tong_chuyen: tongPerCID,
            nguoi_th: nguoiTH, ghi_chu: ghiChu, dong: i + 1
          });
        }
      }
    }
  }

  Logger.log('Đối chiếu NCC rows sau lọc: ' + rows.length);
  return rows;
}

// ============================================================
// ĐỌC TK TRONG KHO → Danh sách đầy đủ CID + trạng thái + thông tin
// ============================================================

/**
 * Đọc toàn bộ CID từ tab "TK trong kho" (header dòng 7, data từ dòng 8)
 * Trả về { list: [...], groupMap: { cid: ten_group } }
 *   list: mảng object { cid, ten_group, tinh_trang, ma_kh, ngay_nhap, ngay_ban, dong }
 *   groupMap: dùng cho lookup NCC (tương thích code cũ)
 */
var START_ROW_KHO = 2347; // Chỉ quét từ dòng này trở đi (tháng 01/2026)

function _readTKTrongKho() {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheet = source.getSheetByName(TAB_TK_KHO);
  if (!sheet) {
    Logger.log('WARNING: Không tìm thấy tab "' + TAB_TK_KHO + '"');
    return { list: [], groupMap: {} };
  }

  var data = sheet.getDataRange().getValues();
  if (data.length <= 7) return { list: [], groupMap: {} };

  var headers = data[6];
  var col = {};
  headers.forEach(function(h, i) { col[h.toString().trim()] = i; });

  var colCID = col['CID'];
  var colGroup = col['Tên Group'];
  if (colCID === undefined || colGroup === undefined) {
    Logger.log('WARNING: Tab "' + TAB_TK_KHO + '" thiếu cột CID hoặc Tên Group');
    return { list: [], groupMap: {} };
  }

  var colNgayNhap = col['Ngày nhập TK'];
  var colTinhTrang = col['Tình trạng'];
  var colMaKH = col['Mã KH'];
  var colNgayCap = col['Ngày cấp'];

  var CID_REGEX = /^\d{3}-\d{3}-\d{4}$/;
  var list = [];
  var groupMap = {};

  var startIdx = Math.max(7, START_ROW_KHO - 1); // START_ROW_KHO là 1-based, index là 0-based
  for (var i = startIdx; i < data.length; i++) {
    var row = data[i];
    var rawCid = row[colCID];
    var group = (row[colGroup] || '').toString().trim();

    // Bỏ qua dòng trống hoàn toàn
    if (!rawCid && !group) continue;

    var cid = _formatCID(rawCid);

    // Validation: CID trống
    if (!cid) {
      if (group) {
        _errors.push({
          tab: 'TK trong kho', dong: i + 1,
          loai_loi: 'CID trống', gia_tri: '',
          ghi_chu: 'Dòng có Tên Group "' + group + '" nhưng CID trống'
        });
      }
      continue;
    }

    // Validation: CID sai format
    if (!CID_REGEX.test(cid)) {
      _errors.push({
        tab: 'TK trong kho', dong: i + 1,
        loai_loi: 'CID sai format', gia_tri: cid,
        ghi_chu: 'Không đúng format XXX-XXX-XXXX'
      });
      continue;
    }

    var tinh_trang = (colTinhTrang !== undefined) ? (row[colTinhTrang] || '').toString().trim() : '';
    var ma_kh = (colMaKH !== undefined) ? (row[colMaKH] || '').toString().trim() : '';
    var ngay_nhap = (colNgayNhap !== undefined) ? _parseDate(row[colNgayNhap]) : null;
    var ngay_ban = (colNgayCap !== undefined) ? _parseDate(row[colNgayCap]) : null;

    // Auto-correct Mã KH
    ma_kh = _fixMaKH(ma_kh);

    // Validation: Mã KH sai format
    if (ma_kh && !MA_KH_REGEX.test(ma_kh)) {
      _errors.push({
        tab: 'TK trong kho', dong: i + 1,
        loai_loi: 'Mã KH sai format', gia_tri: ma_kh,
        ghi_chu: 'CID ' + cid + ' — Mã KH không đúng format LLK-XXXXXX'
      });
      ma_kh = ''; // bỏ qua Mã KH sai
    }

    list.push({
      cid: cid,
      ten_group: group,
      tinh_trang: tinh_trang,
      ma_kh: ma_kh,
      ngay_nhap: ngay_nhap,
      ngay_ban: ngay_ban,
      dong: i + 1
    });

    if (group) groupMap[cid] = group;
  }

  Logger.log('TK trong kho: ' + list.length + ' CID đọc được');
  return { list: list, groupMap: groupMap };
}

// ============================================================
// ĐỌC QUỸ GỐC TỪ TAB "Tổng hợp" + ĐỐI CHIẾU QUỸ HÀNG NGÀY
// ============================================================

var TAB_TONG_HOP = 'Tổng hợp';

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

/**
 * Đọc quỹ gốc từ tab "Tổng hợp" — cột tương ứng QUY_GOC_DATE
 * Trả về { ma_kh: quy_goc }
 */
function _readQuyGoc() {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheet = source.getSheetByName(TAB_TONG_HOP);
  if (!sheet) {
    Logger.log('WARNING: Không tìm thấy tab "' + TAB_TONG_HOP + '"');
    return {};
  }

  var data = sheet.getDataRange().getValues();
  if (data.length <= 5) return {};

  var colMaKH = 3; // D
  var headerRow = data[1]; // dòng 2 chứa ngày

  // Tìm cột quỹ gốc theo QUY_GOC_DATE
  var colQuyGoc = _findDateCol(headerRow, QUY_GOC_DATE);
  if (colQuyGoc < 0) {
    Logger.log('WARNING: Không tìm thấy cột ngày ' + Utilities.formatDate(QUY_GOC_DATE, TZ, 'dd/MM/yyyy') + ' trong Tổng hợp');
    return {};
  }
  Logger.log('Quỹ gốc: cột ' + colQuyGoc + ' = ' + Utilities.formatDate(QUY_GOC_DATE, TZ, 'dd/MM/yyyy'));

  // Đọc tất cả KH có mã hợp lệ
  var map = {};
  for (var i = 5; i < data.length; i++) {
    var maKH = _fixMaKH((data[i][colMaKH] || '').toString().trim());
    if (!maKH || !MA_KH_REGEX.test(maKH)) continue;
    var quyGoc = parseFloat(data[i][colQuyGoc]) || 0;
    map[maKH] = quyGoc;
  }

  Logger.log('Quỹ gốc: đọc ' + Object.keys(map).length + ' KH từ cột ' + colQuyGoc + ' (' + Utilities.formatDate(QUY_GOC_DATE, TZ, 'dd/MM/yyyy') + ')');
  return map;
}

/**
 * Import quỹ gốc vào DanhMuc_KH (cột E = quy_goc)
 */
function _importQuyGoc(crmIds) {
  var quyGocMap = _readQuyGoc();
  if (Object.keys(quyGocMap).length === 0) return;

  var ss = _openCrm_(crmIds, 'KHACH_HANG');
  var sheet = ss.getSheetByName('DanhMuc_KH');
  var data = sheet.getDataRange().getValues();
  var updated = 0;

  for (var i = 1; i < data.length; i++) {
    var maKH = (data[i][0] || '').toString().trim();
    if (quyGocMap[maKH] !== undefined) {
      sheet.getRange(i + 1, 5).setValue(quyGocMap[maKH]); // cột E = quy_goc
      updated++;
    }
  }

  Logger.log('Import quỹ gốc KH: ' + updated + ' KH cập nhật');
}

/**
 * Import quỹ gốc NCC từ tab "Nguồn" cột HZ (31/12/2025)
 * Ghi vào DanhMuc_NCC cột quy_goc (nếu có)
 */
function _importQuyGocNCC(crmIds, nccMap) {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheet = source.getSheetByName(TAB_NGUON);
  if (!sheet) {
    Logger.log('WARNING: Không tìm thấy tab "' + TAB_NGUON + '" để import quỹ gốc NCC');
    return;
  }

  var data = sheet.getDataRange().getValues();
  if (data.length <= 3) return;

  var colTenNguon = 1; // B
  // Tab Nguồn: dòng 1 (index 0) chứa ngày
  var colQuyGocNCC = _findDateCol(data[0], QUY_GOC_DATE);
  if (colQuyGocNCC < 0) {
    Logger.log('WARNING: Không tìm thấy cột ngày ' + Utilities.formatDate(QUY_GOC_DATE, TZ, 'dd/MM/yyyy') + ' trong tab Nguồn');
    return {};
  }

  // Đọc quỹ gốc theo tên NCC → map sang mã NCC
  var quyGocMap = {}; // { ma_ncc: quy_goc }
  for (var i = 3; i < data.length; i++) {
    var tenNguon = (data[i][colTenNguon] || '').toString().trim();
    if (!tenNguon) continue;
    var maNcc = _lookupNcc(nccMap, tenNguon);
    if (!maNcc) continue;
    var quyGoc = parseFloat(data[i][colQuyGocNCC]) || 0;
    quyGocMap[maNcc] = quyGoc;
  }

  if (Object.keys(quyGocMap).length === 0) {
    Logger.log('Import quỹ gốc NCC: không có NCC nào để import');
    return;
  }

  // Ghi vào DanhMuc_NCC — tìm cột quy_goc (nếu chưa có thì dùng cột khác)
  // DanhMuc_NCC hiện tại: cột L (12) = quy_hien_tai
  // Cần thêm cột quy_goc — dùng cột phụ hoặc tính trực tiếp trong recompute
  // Approach: không ghi cột riêng, thay vào đó truyền quyGocMap vào _recomputeQuyNCC
  Logger.log('Import quỹ gốc NCC: đọc ' + Object.keys(quyGocMap).length + ' NCC');
  return quyGocMap;
}

/**
 * Đối chiếu quỹ CRM vs quỹ Kế Toán (cột ngày mới nhất trong "Tổng hợp")
 * Ghi Warning_Log + gửi Telegram nếu có chênh lệch
 */
function _doiChieuQuy(crmIds) {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheet = source.getSheetByName(TAB_TONG_HOP);
  if (!sheet) return;

  var data = sheet.getDataRange().getValues();
  if (data.length <= 5) return;

  var headerRow = data[1];
  var colMaKH = 3;

  // Đọc quỹ CRM hiện tại
  var ssKH = _openCrm_(crmIds, 'KHACH_HANG');
  var sheetKH = ssKH.getSheetByName('DanhMuc_KH');
  var khData = sheetKH.getDataRange().getValues();
  var crmMap = {}; // { ma_kh: quy_hien_tai }
  for (var k = 1; k < khData.length; k++) {
    var mkk = (khData[k][0] || '').toString().trim();
    if (mkk) crmMap[mkk] = parseFloat(khData[k][3]) || 0;
  }

  // Đọc GD KH từ CRM (để tính quỹ đến từng ngày)
  var ssGD = _openCrm_(crmIds, 'GD_KH_' + NAM);
  var sheetGD = ssGD.getSheetByName('GD_KhachHang');
  var gdData = sheetGD ? sheetGD.getDataRange().getValues() : [];

  // Đọc quỹ gốc
  var quyGocMap = {};
  for (var kg = 1; kg < khData.length; kg++) {
    var mkg = (khData[kg][0] || '').toString().trim();
    if (mkg) quyGocMap[mkg] = parseFloat(khData[kg][4]) || 0; // cột E
  }

  // Tìm tất cả cột ngày từ DOICHIEU_FROM → cuối
  var dateCols = []; // { col, date, dateStr }
  for (var c = 6; c < headerRow.length; c++) {
    var val = headerRow[c];
    var d = null;
    if (val instanceof Date && !isNaN(val.getTime())) d = val;
    else if (val) d = _parseDate(val.toString().trim());
    if (d && d.getTime() >= DOICHIEU_FROM.getTime()) {
      dateCols.push({ col: c, date: d, dateStr: Utilities.formatDate(d, TZ, 'dd/MM/yyyy') });
    }
  }

  if (dateCols.length === 0) {
    Logger.log('Đối chiếu KH: không có cột ngày >= ' + Utilities.formatDate(DOICHIEU_FROM, TZ, 'dd/MM/yyyy'));
    return;
  }
  Logger.log('Đối chiếu KH: ' + dateCols.length + ' ngày từ ' + dateCols[0].dateStr + ' → ' + dateCols[dateCols.length-1].dateStr);

  // Đọc KH data từ Tổng hợp (chỉ KH có trong CRM)
  var khRows = {}; // { ma_kh: { rowIdx } }
  for (var i = 5; i < data.length; i++) {
    var maKH = _fixMaKH((data[i][colMaKH] || '').toString().trim());
    if (!maKH || !MA_KH_REGEX.test(maKH)) continue;
    if (crmMap[maKH] === undefined) continue; // chỉ KH có trong CRM
    khRows[maKH] = i;
  }

  // Đối chiếu từng ngày: tính quỹ CRM đến ngày đó
  var allWarnings = []; // { ma_kh, ngay, quyCRM, quyKT, lech }
  var logRows = [];
  var now = new Date();

  for (var dc = 0; dc < dateCols.length; dc++) {
    var dateInfo = dateCols[dc];
    var cutoffEnd = new Date(dateInfo.date.getFullYear(), dateInfo.date.getMonth(), dateInfo.date.getDate(), 23, 59, 59);

    // Tính quỹ CRM cho mỗi KH đến ngày này
    var crmAtDate = {}; // { ma_kh: quỹ }
    for (var mk in quyGocMap) {
      crmAtDate[mk] = quyGocMap[mk]; // bắt đầu từ quỹ gốc
    }
    // Cộng GD đến ngày
    for (var g = 1; g < gdData.length; g++) {
      var gdMK = (gdData[g][1] || '').toString().trim();
      if (!gdMK || crmAtDate[gdMK] === undefined) continue;
      var gdNgay = gdData[g][14]; // ngay_tao
      if (!(gdNgay instanceof Date) || gdNgay.getTime() > cutoffEnd.getTime()) continue;
      var gdLoai = (gdData[g][2] || '').toString().trim();
      var gdHttt = (gdData[g][3] || '').toString().trim();
      var gdSoTien = parseFloat(gdData[g][5]) || 0;
      crmAtDate[gdMK] += _calculateBienDongKH(gdLoai, gdHttt, gdSoTien);
    }

    // So sánh với KT
    for (var mk2 in khRows) {
      var rowIdx = khRows[mk2];
      var quyKT = parseFloat(data[rowIdx][dateInfo.col]) || 0;
      var quyCRM = crmAtDate[mk2] || 0;
      var diff = quyCRM - quyKT;
      if (Math.abs(diff) > 0.01) {
        allWarnings.push({ ma_kh: mk2, ngay: dateInfo.dateStr, quy_crm: quyCRM, quy_kt: quyKT, lech: diff });
        logRows.push([now, mk2, quyCRM, quyKT, diff, dateInfo.dateStr]);
      }
    }
  }

  // Ghi Warning_Log
  if (logRows.length > 0) {
    var logSS = _getLogSpreadsheet_();
    var warnSheet = logSS.getSheetByName('Warning_Log');
    if (!warnSheet) {
      warnSheet = logSS.insertSheet('Warning_Log');
      warnSheet.getRange(1, 1, 1, 6).setValues([['Thời gian', 'Mã', 'Quỹ CRM', 'Quỹ KT', 'Chênh lệch', 'Ngày']]);
      warnSheet.getRange(1, 1, 1, 6).setBackground('#FF9800').setFontColor('#FFFFFF').setFontWeight('bold');
      warnSheet.setFrozenRows(1);
    }
    var lr = warnSheet.getLastRow();
    warnSheet.getRange(lr + 1, 1, logRows.length, 6).setValues(logRows);
  }

  // Telegram — group theo KH, hiện từng ngày lệch
  var khWarnings = {}; // { ma_kh: [{ ngay, lech }] }
  allWarnings.forEach(function(w) {
    if (!khWarnings[w.ma_kh]) khWarnings[w.ma_kh] = [];
    khWarnings[w.ma_kh].push(w);
  });

  var khCount = Object.keys(khWarnings).length;
  if (khCount > 0) {
    var msg = '⚠️ *Chênh lệch quỹ KH* (' + dateCols[0].dateStr + ' → ' + dateCols[dateCols.length-1].dateStr + ')\n\n';
    var keys = Object.keys(khWarnings).sort();
    keys.forEach(function(mk) {
      var days = khWarnings[mk];
      msg += '• `' + mk + '`:\n';
      days.forEach(function(d) {
        msg += '  ' + d.ngay + ': CRM $' + d.quy_crm.toFixed(2) + ' vs KT $' + d.quy_kt.toFixed(2) + ' (lệch $' + d.lech.toFixed(2) + ')\n';
      });
    });
    msg += '\nTổng: ' + khCount + ' KH chênh lệch';

    // Telegram giới hạn 4096 ký tự — cắt nếu quá dài
    if (msg.length > 4000) {
      msg = msg.substring(0, 3950) + '\n...(cắt bớt, xem Warning_Log)';
    }
    _sendTelegram(msg);
    Logger.log('Đối chiếu KH: ' + khCount + ' KH chênh lệch, ' + allWarnings.length + ' dòng warning');
  } else {
    _sendTelegram('✅ *Đối chiếu quỹ KH* (' + dateCols[0].dateStr + ' → ' + dateCols[dateCols.length-1].dateStr + ')\n\n'
      + Object.keys(khRows).length + ' KH × ' + dateCols.length + ' ngày — tất cả khớp.');
    Logger.log('Đối chiếu KH: OK — tất cả khớp');
  }
}

// ============================================================
// ĐỐI CHIẾU QUỸ NCC — Tab "Nguồn"
// ============================================================

var TAB_NGUON = 'Nguồn';

function _doiChieuQuyNCC(crmIds, nccMap) {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheet = source.getSheetByName(TAB_NGUON);
  if (!sheet) { Logger.log('WARNING: Không tìm thấy tab Nguồn'); return; }

  var data = sheet.getDataRange().getValues();
  if (data.length <= 3) return;

  var headerRow = data[0];
  var colTenNguon = 1;

  // Map NCC row index + mã NCC
  var nccRows = {}; // { ma_ncc: rowIdx }
  for (var i = 3; i < data.length; i++) {
    var tenNguon = (data[i][colTenNguon] || '').toString().trim();
    if (!tenNguon) continue;
    var maNcc = _lookupNcc(nccMap, tenNguon);
    if (maNcc) nccRows[maNcc] = i;
  }

  // Đọc quỹ gốc NCC
  var colQuyGocNCC = _findDateCol(headerRow, QUY_GOC_DATE);
  var nccQuyGocMap = {};
  if (colQuyGocNCC >= 0) {
    for (var mn in nccRows) {
      nccQuyGocMap[mn] = parseFloat(data[nccRows[mn]][colQuyGocNCC]) || 0;
    }
  }

  // Tìm cột ngày từ DOICHIEU_FROM → cuối
  var dateCols = [];
  for (var c = 3; c < headerRow.length; c++) {
    var val = headerRow[c];
    var d = null;
    if (val instanceof Date && !isNaN(val.getTime())) d = val;
    else if (val) d = _parseDate(val.toString().trim());
    if (d && d.getTime() >= DOICHIEU_FROM.getTime()) {
      dateCols.push({ col: c, date: d, dateStr: Utilities.formatDate(d, TZ, 'dd/MM/yyyy') });
    }
  }

  if (dateCols.length === 0) { Logger.log('Đối chiếu NCC: không có cột ngày >= DOICHIEU_FROM'); return; }

  // Đọc GD NCC từ CRM
  var ssGD = _openCrm_(crmIds, 'GD_NCC_' + NAM);
  var sheetGD = ssGD.getSheetByName('GD_NhaCungCap');
  var gdData = sheetGD ? sheetGD.getDataRange().getValues() : [];

  // Đối chiếu từng ngày
  var allWarnings = [];
  var logRows = [];
  var now = new Date();

  for (var dc = 0; dc < dateCols.length; dc++) {
    var dateInfo = dateCols[dc];
    var cutoff = new Date(dateInfo.date.getFullYear(), dateInfo.date.getMonth(), dateInfo.date.getDate(), 23, 59, 59);

    // Tính quỹ CRM NCC đến ngày này (tuần tự vì Refund)
    for (var mn2 in nccRows) {
      var quy = nccQuyGocMap[mn2] || 0;
      // Collect + sort GD cho NCC này đến ngày
      var gds = [];
      for (var g = 1; g < gdData.length; g++) {
        var gdMN = (gdData[g][2] || '').toString().trim();
        if (gdMN !== mn2) continue;
        var gdNgay = gdData[g][1];
        if (!(gdNgay instanceof Date) || gdNgay.getTime() > cutoff.getTime()) continue;
        gds.push({ ngay: gdNgay, loai_gd: (gdData[g][3] || '').toString().trim(), so_tien: parseFloat(gdData[g][5]) || 0 });
      }
      gds.sort(function(a, b) { return (a.ngay ? a.ngay.getTime() : 0) - (b.ngay ? b.ngay.getTime() : 0); });
      gds.forEach(function(gd) {
        if (gd.loai_gd === 'Nap_quy') quy += gd.so_tien;
        else if (gd.loai_gd === 'Rut_CID') quy += gd.so_tien;
        else if (gd.loai_gd === 'Mua_TK') quy -= gd.so_tien;
        else if (gd.loai_gd === 'Nap_CID') quy -= gd.so_tien;
        else if (gd.loai_gd === 'Refund') quy = 0;
      });

      var quyKT = parseFloat(data[nccRows[mn2]][dateInfo.col]) || 0;
      var diff = quy - quyKT;
      if (Math.abs(diff) > 0.01) {
        allWarnings.push({ ma_ncc: mn2, ngay: dateInfo.dateStr, quy_crm: quy, quy_kt: quyKT, lech: diff });
        logRows.push([now, mn2, quy, quyKT, diff, dateInfo.dateStr]);
      }
    }
  }

  // Ghi Warning_Log
  if (logRows.length > 0) {
    var logSS = _getLogSpreadsheet_();
    var warnSheet = logSS.getSheetByName('Warning_Log');
    if (!warnSheet) {
      warnSheet = logSS.insertSheet('Warning_Log');
      warnSheet.getRange(1, 1, 1, 6).setValues([['Thời gian', 'Mã', 'Quỹ CRM', 'Quỹ KT', 'Chênh lệch', 'Ngày']]);
      warnSheet.getRange(1, 1, 1, 6).setBackground('#FF9800').setFontColor('#FFFFFF').setFontWeight('bold');
      warnSheet.setFrozenRows(1);
    }
    var lr = warnSheet.getLastRow();
    warnSheet.getRange(lr + 1, 1, logRows.length, 6).setValues(logRows);
  }

  // Telegram
  var nccWarnings = {};
  allWarnings.forEach(function(w) {
    if (!nccWarnings[w.ma_ncc]) nccWarnings[w.ma_ncc] = [];
    nccWarnings[w.ma_ncc].push(w);
  });

  var nccCount = Object.keys(nccWarnings).length;
  if (nccCount > 0) {
    var msg = '⚠️ *Chênh lệch quỹ NCC* (' + dateCols[0].dateStr + ' → ' + dateCols[dateCols.length-1].dateStr + ')\n\n';
    Object.keys(nccWarnings).sort().forEach(function(mn) {
      msg += '• `' + mn + '`:\n';
      nccWarnings[mn].forEach(function(d) {
        msg += '  ' + d.ngay + ': CRM $' + d.quy_crm.toFixed(2) + ' vs KT $' + d.quy_kt.toFixed(2) + ' (lệch $' + d.lech.toFixed(2) + ')\n';
      });
    });
    msg += '\nTổng: ' + nccCount + ' NCC chênh lệch';
    if (msg.length > 4000) msg = msg.substring(0, 3950) + '\n...(cắt bớt, xem Warning_Log)';
    _sendTelegram(msg);
    Logger.log('Đối chiếu NCC: ' + nccCount + ' NCC chênh lệch');
  } else {
    _sendTelegram('✅ *Đối chiếu quỹ NCC* (' + dateCols[0].dateStr + ' → ' + dateCols[dateCols.length-1].dateStr + ')\n\n'
      + Object.keys(nccRows).length + ' NCC × ' + dateCols.length + ' ngày — tất cả khớp.');
    Logger.log('Đối chiếu NCC: OK');
  }
}

// ============================================================
// ĐỌC CONFIG MAPPING NGUỒN → MA_NCC
// ============================================================

/**
 * Đọc mapping tên nguồn → mã NCC từ tab "Config_NccMap" trong file NHA_CUNG_CAP
 * Tab gồm 2 cột: ten_nguon | ma_ncc
 * Trả về map { ten_nguon_lower: ma_ncc }
 */
function _readNccMap() {
  var crmIds = _loadCrmIds_();
  var ss = _openCrm_(crmIds, 'NHA_CUNG_CAP');
  var sheet = ss.getSheetByName('Config_NccMap');
  var map = {};
  if (!sheet) {
    Logger.log('WARNING: Không tìm thấy tab "Config_NccMap" trong NHA_CUNG_CAP — dùng ten_nhom từ DanhMuc_NCC để dedup');
    return map;
  }

  var data = sheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) {
    var tenNguon = (data[i][0] || '').toString().trim();
    var maNcc = (data[i][1] || '').toString().trim();
    if (tenNguon && maNcc) {
      map[tenNguon.toLowerCase()] = maNcc;
    }
  }
  Logger.log('NccMap loaded: ' + Object.keys(map).length + ' mappings');
  return map;
}

function _lookupNcc(nccMap, nguon) {
  if (!nguon) return '';
  return nccMap[nguon.trim().toLowerCase()] || '';
}

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
  try {
    UrlFetchApp.fetch('https://api.telegram.org/bot' + token + '/sendMessage', {
      method: 'post',
      contentType: 'application/json',
      payload: JSON.stringify({
        chat_id: chatId,
        text: message,
        parse_mode: 'Markdown'
      }),
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

// (Audit functions removed — use git history if needed)
