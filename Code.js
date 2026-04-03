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

// Chỉ lấy GD từ ngày này trở đi
var DATE_FROM = new Date(2026, 0, 1); // 01/01/2026

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

  var khoGroupMap = _readTKTrongKho();
  Logger.log('TK trong kho: ' + Object.keys(khoGroupMap).length + ' CID');

  var nccGDRows = _readDoiChieuNCC();

  var allRows = topupRows.concat(dcRows);

  // Bước 4: Sync vào CRM Database (chỉ thêm mới)
  var statsKH     = _syncKH(crmIds, allRows);
  var statsKho    = _syncKho(crmIds, allRows, nccMap, khoGroupMap);
  var statsGD     = _syncGDKH(crmIds, allRows, nccMap);
  var statsNCC    = _syncNCC(crmIds, nccGDRows, nccMap);
  var statsGDNCC  = _syncGDNCC(crmIds, nccGDRows, nccMap);

  // Bước 5: Tính lại quỹ cho KH + NCC có GD mới
  if (statsGD.newMaKHs && statsGD.newMaKHs.length > 0) {
    _recomputeQuyKH(crmIds, statsGD.newMaKHs);
  }
  if (statsGDNCC.newMaNCCs && statsGDNCC.newMaNCCs.length > 0) {
    _recomputeQuyNCC(crmIds, statsGDNCC.newMaNCCs);
  }

  // Bước 6: Ghi log vào 1-Database/Logs/MigrationLogs
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

  // Bước 7: Ghi chi tiết lỗi (nếu có)
  _writeErrors();

  Logger.log('=== BUILD ALL DONE — ' + elapsed + 's, Errors: ' + _errors.length + ' ===');
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
// SYNC KHO — So sánh + append CID mới vào Kho_TaiKhoan
// ============================================================

function _syncKho(crmIds, allRows, nccMap, khoGroupMap) {
  var ss = _openCrm_(crmIds, 'KHO_TK');
  var sheet = ss.getSheetByName('Kho_TaiKhoan');
  if (!sheet) throw new Error('Không tìm thấy tab Kho_TaiKhoan');

  // Đọc CID hiện có (cột A)
  var existing = {};
  var data = sheet.getDataRange().getValues();
  for (var i = 1; i < data.length; i++) {
    var cid = (data[i][0] || '').toString().trim();
    if (cid) existing[cid] = true;
  }

  // Tìm CID mới
  var cidMap = {};
  var CID_REGEX = /^\d{3}-\d{3}-\d{4}$/;
  allRows.forEach(function(r) {
    if (!r.cid || r.cid === '-') return;
    if (!MA_KH_REGEX.test(r.ma_kh)) return;
    if (!CID_REGEX.test(r.cid)) return;
    if (existing[r.cid]) return; // đã có

    if (!cidMap[r.cid]) {
      cidMap[r.cid] = { ma_kh: r.ma_kh, nhom_nguon: r.nhom_nguon || '', ngay_dau: r.ngay };
    } else {
      if (!cidMap[r.cid].nhom_nguon && r.nhom_nguon) cidMap[r.cid].nhom_nguon = r.nhom_nguon;
      if (r.ngay < cidMap[r.cid].ngay_dau) {
        cidMap[r.cid].ngay_dau = r.ngay;
        cidMap[r.cid].ma_kh = r.ma_kh;
      }
    }
  });

  // Append CID mới
  var newRows = [];
  Object.keys(cidMap).sort().forEach(function(cid) {
    var info = cidMap[cid];
    var maNcc = _lookupNcc(nccMap, info.nhom_nguon);
    if (!maNcc && khoGroupMap && khoGroupMap[cid]) {
      maNcc = _lookupNcc(nccMap, khoGroupMap[cid]);
    }
    newRows.push([
      cid,            // cid
      maNcc,          // ma_ncc
      info.ma_kh,     // ma_kh
      'Da_ban',       // trang_thai
      info.ngay_dau,  // ngay_nhap
      info.ngay_dau,  // ngay_ban
      '',             // ghi_chu
      new Date()      // ngay_cap_nhat
    ]);
  });

  if (newRows.length > 0) {
    var lastRow = sheet.getLastRow();
    sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
  }

  Logger.log('SyncKho: ' + newRows.length + ' CID mới (đã có: ' + Object.keys(existing).length + ')');
  return { added: newRows.length, existing: Object.keys(existing).length };
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
  }

  Logger.log('SyncGDKH: ' + newRows.length + ' GD mới (đã có: ' + Object.keys(existing).length + ')');
  return { added: newRows.length, newMaKHs: Object.keys(newMaKHs) };
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
      '',               // trang_thai_doi_soat
      '',               // nguoi_doi_soat
      ''                // ngay_doi_soat
    ]);
  });

  if (newRows.length > 0) {
    var lastRow = sheet.getLastRow();
    sheet.getRange(lastRow + 1, 1, newRows.length, newRows[0].length).setValues(newRows);
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

function _recomputeQuyKH(crmIds, affectedMaKHs) {
  if (!affectedMaKHs || affectedMaKHs.length === 0) return;
  Logger.log('Recompute quỹ cho ' + affectedMaKHs.length + ' KH: ' + affectedMaKHs.join(', '));

  // Đọc tất cả GD KH
  var ssGD = _openCrm_(crmIds, 'GD_KH_' + NAM);
  var sheetGD = ssGD.getSheetByName('GD_KhachHang');
  var gdData = sheetGD.getDataRange().getValues();

  // Group GD theo ma_kh, chỉ lấy affected
  var affected = {};
  affectedMaKHs.forEach(function(mk) { affected[mk] = true; });

  var gdByKH = {}; // { ma_kh: [ {row_index, ngay, loai_gd, so_tien, phi, tong_kh_chuyen} ] }
  for (var i = 1; i < gdData.length; i++) {
    var mk = (gdData[i][1] || '').toString().trim();
    if (!affected[mk]) continue;
    if (!gdByKH[mk]) gdByKH[mk] = [];
    gdByKH[mk].push({
      rowIndex: i,
      ngay: gdData[i][14], // ngay_tao
      loai_gd: (gdData[i][2] || '').toString().trim(),
      so_tien_goc: parseFloat(gdData[i][5]) || 0,
      phi: parseFloat(gdData[i][6]) || 0,
      tong_kh_chuyen: parseFloat(gdData[i][7]) || 0
    });
  }

  // Tính quỹ cho từng KH
  var quyResults = {}; // { ma_kh: quy_hien_tai }
  Object.keys(gdByKH).forEach(function(mk) {
    var gds = gdByKH[mk];
    // Sort theo ngày
    gds.sort(function(a, b) {
      var da = a.ngay instanceof Date ? a.ngay.getTime() : 0;
      var db = b.ngay instanceof Date ? b.ngay.getTime() : 0;
      return da - db;
    });

    var quy = 0;
    gds.forEach(function(gd) {
      var quyTruoc = quy;
      var bienDong = 0;

      if (gd.loai_gd === 'Nap_quy') {
        bienDong = gd.tong_kh_chuyen;
      } else if (gd.loai_gd === 'Mua_TK') {
        bienDong = -gd.tong_kh_chuyen;
      } else if (gd.loai_gd === 'Nap_CID') {
        bienDong = -gd.so_tien_goc;
      } else if (gd.loai_gd === 'Rut_CID') {
        bienDong = gd.so_tien_goc;
      }

      quy = quyTruoc + bienDong;

      // Cập nhật cột quy_truoc (I=9), bien_dong (J=10), quy_sau (K=11)
      var rowNum = gd.rowIndex + 1; // 1-indexed
      sheetGD.getRange(rowNum, 9).setValue(quyTruoc);
      sheetGD.getRange(rowNum, 10).setValue(bienDong);
      sheetGD.getRange(rowNum, 11).setValue(quy);
    });

    quyResults[mk] = quy;
  });

  // Cập nhật quy_hien_tai trong DanhMuc_KH
  var ssKH = _openCrm_(crmIds, 'KHACH_HANG');
  var sheetKH = ssKH.getSheetByName('DanhMuc_KH');
  var khData = sheetKH.getDataRange().getValues();

  for (var j = 1; j < khData.length; j++) {
    var mk2 = (khData[j][0] || '').toString().trim();
    if (quyResults[mk2] !== undefined) {
      sheetKH.getRange(j + 1, 4).setValue(quyResults[mk2]); // cột D = quy_hien_tai
      sheetKH.getRange(j + 1, 10).setValue(new Date()); // cột J = ngay_cap_nhat
    }
  }

  Logger.log('Recompute done. KH updated: ' + Object.keys(quyResults).length);
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
function _recomputeQuyNCC(crmIds, affectedMaNCCs) {
  if (!affectedMaNCCs || affectedMaNCCs.length === 0) return;
  Logger.log('Recompute quỹ NCC cho ' + affectedMaNCCs.length + ' NCC: ' + affectedMaNCCs.join(', '));

  // Đọc tất cả GD NCC
  var ssGD = _openCrm_(crmIds, 'GD_NCC_' + NAM);
  var sheetGD = ssGD.getSheetByName('GD_NhaCungCap');
  var gdData = sheetGD.getDataRange().getValues();

  // GD_NCC columns: 0=ma_gd, 1=ngay_gd, 2=ma_ncc, 3=loai_gd, 4=httt,
  //   5=so_tien_goc, 6=phi, 7=tong_chuyen,
  //   8=quy_truoc(I), 9=bien_dong(J), 10=quy_sau(K)

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

  // Tính quỹ cho từng NCC
  var quyResults = {};
  Object.keys(gdByNCC).forEach(function(mn) {
    var gds = gdByNCC[mn];
    gds.sort(function(a, b) {
      var da = a.ngay instanceof Date ? a.ngay.getTime() : 0;
      var db = b.ngay instanceof Date ? b.ngay.getTime() : 0;
      return da - db;
    });

    var quy = 0;
    gds.forEach(function(gd) {
      var quyTruoc = quy;
      var bienDong = 0;

      if (gd.loai_gd === 'Nap_quy')      bienDong = gd.so_tien_goc;
      else if (gd.loai_gd === 'Rut_CID')  bienDong = gd.so_tien_goc;
      else if (gd.loai_gd === 'Mua_TK')   bienDong = -gd.so_tien_goc;
      else if (gd.loai_gd === 'Nap_CID')  bienDong = -gd.so_tien_goc;
      else if (gd.loai_gd === 'Refund')   bienDong = -quyTruoc; // rút sạch

      quy = quyTruoc + bienDong;

      // Cập nhật cột quy_truoc(I=9), bien_dong(J=10), quy_sau(K=11)
      var rowNum = gd.rowIndex + 1;
      sheetGD.getRange(rowNum, 9).setValue(quyTruoc);
      sheetGD.getRange(rowNum, 10).setValue(bienDong);
      sheetGD.getRange(rowNum, 11).setValue(quy);
    });

    quyResults[mn] = quy;
  });

  // Cập nhật quy_hien_tai trong DanhMuc_NCC (cột L = index 11, column 12)
  var ssNCC = _openCrm_(crmIds, 'NHA_CUNG_CAP');
  var sheetNCC = ssNCC.getSheetByName('DanhMuc_NCC');
  var nccData = sheetNCC.getDataRange().getValues();

  for (var j = 1; j < nccData.length; j++) {
    var mn2 = (nccData[j][0] || '').toString().trim();
    if (quyResults[mn2] !== undefined) {
      sheetNCC.getRange(j + 1, 12).setValue(quyResults[mn2]); // cột L = quy_hien_tai
      sheetNCC.getRange(j + 1, 18).setValue(new Date()); // cột R = ngay_cap_nhat
    }
  }

  Logger.log('Recompute NCC done. NCC updated: ' + Object.keys(quyResults).length);
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

    var maKH = (r[col['Mã khách hàng']] || '').toString().trim();
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

    var maKH = (r[col['Mã KH']] || '').toString().trim();
    if (!maKH) continue;
    if (NCC_MA_KH.indexOf(maKH) >= 0) continue;

    var gdKhach = (r[col['Giao dịch với khách']] || '').toString().trim();
    if (!gdKhach) continue;
    if (gdKhach !== 'Khách Nạp tiền' && gdKhach !== 'Khách mua TK') continue;

    var htttRaw = (r[col['Hình thức thanh toán']] || '').toString().trim();
    var httt = '';
    if (htttRaw === 'Trừ quỹ') {
      httt = 'tru_quy';
    } else if (htttRaw === 'Chuyển-Nhận') {
      httt = 'truc_tiep';
    } else if (htttRaw && gdKhach === 'Khách mua TK') {
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
    // Hệ thống cũ không lưu tên KH riêng, chỉ có mã KH
    var nguon = (r[col['Tên nguồn (Tài khoản)']] || '').toString().trim();
    var nguoiTH = (r[col['Người thực hiện']] || '').toString().trim();
    var ghiChu = (r[col['Ghi chú']] || '').toString().trim();
    var cidRaw = (r[col['ID Tài khoản ( Nếu nạp quỹ thì ghi - )']] || '').toString().trim();

    if (gdKhach === 'Khách Nạp tiền') {
      rows.push({
        ngay: ngay, ma_kh: maKH, ten_kh: '', cid: '',
        so_tien: soTienGoc, phi: phi, tong_kh_chuyen: tongKHChuyen,
        loai_gd: 'Nap_quy', httt: '', nguon: nguon,
        nguoi_th: nguoiTH, ghi_chu: ghiChu, source: 'doichieu'
      });
    } else if (gdKhach === 'Khách mua TK') {
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

  var rows = [];
  for (var i = 1; i < data.length; i++) {
    var r = data[i];

    var ngay = _parseDate(r[col['Ngày']]);
    if (!ngay || ngay < DATE_FROM) continue;

    var gdNguon = (r[col['Giao dịch với Nguồn']] || '').toString().trim();
    if (!gdNguon) continue;

    var loaiGD = '';
    if (gdNguon === 'Nạp tiền cho nguồn') {
      loaiGD = 'Nap_quy';
    } else if (gdNguon === 'Mua Tài khoản') {
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
// ĐỌC TK TRONG KHO → CID → Tên Group
// ============================================================

function _readTKTrongKho() {
  var source = SpreadsheetApp.openById(SOURCE_ID);
  var sheet = source.getSheetByName(TAB_TK_KHO);
  if (!sheet) {
    Logger.log('WARNING: Không tìm thấy tab "' + TAB_TK_KHO + '"');
    return {};
  }

  var data = sheet.getDataRange().getValues();
  if (data.length <= 7) return {};

  var headers = data[6];
  var col = {};
  headers.forEach(function(h, i) { col[h.toString().trim()] = i; });

  var colCID = col['CID'];
  var colGroup = col['Tên Group'];
  if (colCID === undefined || colGroup === undefined) {
    Logger.log('WARNING: Tab "' + TAB_TK_KHO + '" thiếu cột CID hoặc Tên Group');
    return {};
  }

  var map = {};
  for (var i = 7; i < data.length; i++) {
    var cid = _formatCID(data[i][colCID]);
    var group = (data[i][colGroup] || '').toString().trim();
    if (cid && group) map[cid] = group;
  }
  return map;
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
function _getLogSpreadsheet_() {
  // Tìm folder 1-Database (folder chứa Master)
  var masterFile = DriveApp.getFileById(MASTER_SS_ID);
  var parents = masterFile.getParents();
  if (!parents.hasNext()) throw new Error('Không tìm thấy folder cha của Master');
  var dbFolder = parents.next();

  // Tìm hoặc tạo folder Logs
  var logsFolder;
  var logsFolders = dbFolder.getFoldersByName('Logs');
  if (logsFolders.hasNext()) {
    logsFolder = logsFolders.next();
  } else {
    logsFolder = dbFolder.createFolder('Logs');
  }

  // Tìm hoặc tạo file MigrationLogs
  var files = logsFolder.getFilesByName('MigrationLogs');
  if (files.hasNext()) {
    return SpreadsheetApp.open(files.next());
  }

  var ss = SpreadsheetApp.create('MigrationLogs');
  DriveApp.getFileById(ss.getId()).moveTo(logsFolder);
  // Xóa Sheet1 mặc định sau khi tạo các tab
  return ss;
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

  var now = new Date();
  var rows = _errors.map(function(e) {
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
  Logger.log('Đã ghi ' + rows.length + ' lỗi vào Error_Log');
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
// TIỆN ÍCH
// ============================================================

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
